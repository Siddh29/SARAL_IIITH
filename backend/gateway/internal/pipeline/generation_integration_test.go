package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	goredis "github.com/redis/go-redis/v9"
	"github.com/saral/gateway/internal/db"
	"github.com/saral/gateway/internal/models"
	redisx "github.com/saral/gateway/internal/redis"
)

func TestGenerationEnqueueOnce(t *testing.T) {
	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	defer client.Close()
	ctx := context.Background()
	keys := []string{"generation-test:dedupe", redisx.StreamScript}
	for i := 0; i < 3; i++ {
		if err := enqueueGenerationOnce.Run(ctx, client, keys, "run_id", "test-run", "mode", "video").Err(); err != nil {
			t.Fatal(err)
		}
	}
	if length := client.XLen(ctx, redisx.StreamScript).Val(); length != 1 {
		t.Fatalf("duplicate dispatch queued %d jobs", length)
	}
}

// Run against an isolated, migrated PostgreSQL database:
// SARAL_TEST_DATABASE_URL=postgres://... go test ./internal/pipeline -run TestMultiFormat -v
// No provider credentials, external LLM requests or media tools are used.
func TestMultiFormatSharedExtraction(t *testing.T) {
	dsn := os.Getenv("SARAL_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set SARAL_TEST_DATABASE_URL to an isolated migrated test database")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	uid := "generation-test-" + uuid.NewString()
	userID, err := db.UpsertUser(ctx, pool, uid, uid+"@example.invalid", "local")
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Exec(context.Background(), `DELETE FROM users WHERE id=$1`, userID)
	paperID, err := db.CreatePaper(ctx, pool, userID, "gs://test/source.pdf")
	if err != nil {
		t.Fatal(err)
	}
	sourceID, err := db.CreatePipelineRun(ctx, pool, paperID, userID, "video")
	if err != nil {
		t.Fatal(err)
	}
	formats := []string{"podcast", "slides", "video"}
	if _, err := db.CreateGenerationBatch(ctx, pool, sourceID, userID, formats); !errors.Is(err, db.ErrExtractionNotReady) {
		t.Fatalf("unextracted request: %v", err)
	}
	extractionID, err := db.CreateStep(ctx, pool, sourceID, "pdf_extract")
	if err != nil {
		t.Fatal(err)
	}
	sharedPath := "gs://test/one-and-only/extracted.json"
	if err := db.CompleteStep(ctx, pool, extractionID, sharedPath); err != nil {
		t.Fatal(err)
	}

	batchID, err := db.CreateGenerationBatch(ctx, pool, sourceID, userID, formats)
	if err != nil {
		t.Fatal(err)
	}
	repeated, err := db.CreateGenerationBatch(ctx, pool, sourceID, userID, formats)
	if err != nil || repeated != batchID {
		t.Fatalf("idempotency: %s %v", repeated, err)
	}
	if _, err := db.CreateGenerationBatch(ctx, pool, sourceID, userID, []string{"video"}); !errors.Is(err, db.ErrFormatsConflict) {
		t.Fatalf("changed selection: %v", err)
	}
	if _, err := db.CreateGenerationBatch(ctx, pool, sourceID, uuid.New(), formats); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("foreign owner: %v", err)
	}
	if _, err := db.GetGenerationBatch(ctx, pool, batchID, uuid.New()); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("foreign status: %v", err)
	}
	batch, err := db.GetGenerationBatch(ctx, pool, batchID, userID)
	if err != nil || len(batch.Outputs) != 3 {
		t.Fatalf("three outputs: %+v %v", batch, err)
	}

	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr(), MaxRetries: -1})
	defer client.Close()
	for i := 0; i < 3; i++ {
		dispatched, err := DispatchGenerationJob(ctx, pool, client)
		if err != nil || !dispatched {
			t.Fatalf("dispatch %d: %v", i, err)
		}
	}
	if length := client.XLen(ctx, redisx.StreamPDF).Val(); length != 0 {
		t.Fatalf("queued %d extra extractions", length)
	}
	scripts, err := client.XRange(ctx, redisx.StreamScript, "-", "+").Result()
	if err != nil || len(scripts) != 3 {
		t.Fatalf("script fanout: %d %v", len(scripts), err)
	}
	modes := map[string]bool{}
	for _, job := range scripts {
		if job.Values["extracted_gcs_path"] != sharedPath {
			t.Fatalf("generator did not share extraction: %v", job.Values)
		}
		modes[job.Values["mode"].(string)] = true
		runID := uuid.MustParse(job.Values["run_id"].(string))
		scriptID := uuid.MustParse(job.Values["step_id"].(string))
		// Simulate completion by an existing script worker. Duplicate callbacks
		// must not schedule duplicate slide/TTS jobs.
		for i := 0; i < 2; i++ {
			handled, err := ContinueGenerationOutput(ctx, pool, runID, scriptID, "gs://test/script.json")
			if err != nil || !handled {
				t.Fatalf("continue %s: %v", job.Values["mode"], err)
			}
		}
	}
	if !modes["video"] || !modes["slides_deck"] || !modes["podcast"] {
		t.Fatalf("missing generator: %v", modes)
	}
	for {
		dispatched, err := DispatchGenerationJob(ctx, pool, client)
		if err != nil {
			t.Fatal(err)
		}
		if !dispatched {
			break
		}
	}
	if client.XLen(ctx, redisx.StreamBeamer).Val() != 2 || client.XLen(ctx, redisx.StreamAudio).Val() != 2 {
		t.Fatal("expected one video render, one slides render and two audio jobs")
	}
	// Simulate a crash after XADD but before the SQL delivery acknowledgement.
	if _, err := pool.Exec(ctx, `UPDATE generation_job_outbox SET dispatched_at=NULL WHERE run_id IN
        (SELECT id FROM pipeline_runs WHERE generation_batch_id=$1)`, batchID); err != nil {
		t.Fatal(err)
	}
	for {
		dispatched, err := DispatchGenerationJob(ctx, pool, client)
		if err != nil {
			t.Fatal(err)
		}
		if !dispatched {
			break
		}
	}
	if client.XLen(ctx, redisx.StreamScript).Val() != 3 || client.XLen(ctx, redisx.StreamAudio).Val() != 2 || client.XLen(ctx, redisx.StreamBeamer).Val() != 2 {
		t.Fatal("outbox recovery duplicated worker jobs")
	}
	var videoID uuid.UUID
	for _, output := range batch.Outputs {
		if output.Format == "video" {
			videoID = output.RunID
			if err := db.FailRunWithStep(ctx, pool, output.RunID, "script_gen", "simulated failure"); err != nil {
				t.Fatal(err)
			}
		} else if err := db.CompleteRun(ctx, pool, output.RunID); err != nil {
			t.Fatal(err)
		}
	}
	batch, err = db.GetGenerationBatch(ctx, pool, batchID, userID)
	if err != nil || batch.Status != "partial_failure" {
		t.Fatalf("independent failure: %+v %v", batch, err)
	}

	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Set("firebase_uid", uid)
		c.Set("email", uid+"@example.invalid")
		c.Set("provider", "local")
	})
	router.POST("/generations/:batch_id/outputs/:format/retry", RetryGenerationOutputHandler(pool))
	res := httptest.NewRecorder()
	router.ServeHTTP(res, httptest.NewRequest(http.MethodPost, "/generations/"+batchID.String()+"/outputs/video/retry", bytes.NewReader(nil)))
	if res.Code != http.StatusAccepted {
		t.Fatalf("retry: %d %s", res.Code, res.Body.String())
	}
	// A second click must not reset an already-retrying output.
	res = httptest.NewRecorder()
	router.ServeHTTP(res, httptest.NewRequest(http.MethodPost, "/generations/"+batchID.String()+"/outputs/video/retry", nil))
	if res.Code != http.StatusConflict {
		t.Fatalf("duplicate retry: %d", res.Code)
	}
	if dispatched, err := DispatchGenerationJob(ctx, pool, client); err != nil || !dispatched {
		t.Fatalf("retry dispatch: %v", err)
	}
	latest, _ := client.XRevRangeN(ctx, redisx.StreamScript, "+", "-", 1).Result()
	if len(latest) != 1 || latest[0].Values["run_id"] == videoID.String() || latest[0].Values["extracted_gcs_path"] != sharedPath {
		t.Fatal("retry did not retain shared extraction")
	}
	if client.XLen(ctx, redisx.StreamPDF).Val() != 0 {
		t.Fatal("retry re-parsed PDF")
	}
	// A late callback from the retired attempt cannot finish the new retry.
	if err := db.CompleteRun(ctx, pool, videoID); err != nil {
		t.Fatal(err)
	}
	batches, err := db.ListGenerationBatches(ctx, pool, paperID, userID)
	if err != nil || len(batches) != 1 || models.GenerationStatus(batches[0].Outputs) != "processing" {
		raw, _ := json.Marshal(batches)
		t.Fatalf("recovery list: %s %v", raw, err)
	}
	t.Log("one extraction → three generators; duplicate requests/callbacks and outbox recovery safe; partial failure isolated; retry reuses extraction")
}
