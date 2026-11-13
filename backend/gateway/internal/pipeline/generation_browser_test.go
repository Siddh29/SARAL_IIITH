package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	goredis "github.com/redis/go-redis/v9"
	"github.com/saral/gateway/internal/apiresp"
	"github.com/saral/gateway/internal/db"
	redisx "github.com/saral/gateway/internal/redis"
)

// Opt-in localhost-only fixture for manual browser verification. This file is
// excluded from production builds. Auth, ingestion and worker outputs are
// simulated; batch handlers, SQL persistence and the outbox are real.
func TestGenerationBrowserFixture(t *testing.T) {
	if os.Getenv("SARAL_BROWSER_FIXTURE") != "1" {
		t.Skip("set SARAL_BROWSER_FIXTURE=1 for the isolated browser fixture")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool, err := pgxpool.New(ctx, os.Getenv("SARAL_TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	uid := "browser-fixture-" + uuid.NewString()
	userID, err := db.UpsertUser(ctx, pool, uid, "dryrun@example.invalid", "local")
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Exec(context.Background(), `DELETE FROM users WHERE id=$1`, userID)
	redisServer := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: redisServer.Addr()})
	defer client.Close()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(gin.Recovery(), func(c *gin.Context) {
		c.Header("Access-Control-Allow-Origin", "*")
		c.Header("Access-Control-Allow-Headers", "Content-Type, Authorization")
		c.Header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}
		c.Set("firebase_uid", uid)
		c.Set("email", "dryrun@example.invalid")
		c.Set("provider", "local")
		c.Next()
	})
	r.POST("/auth/email/signin", func(c *gin.Context) {
		apiresp.OK(c, gin.H{"access_token": "local-browser-fixture", "user": gin.H{"id": userID, "name": "Local Feature Test", "email": "dryrun@example.invalid"}})
	})
	r.POST("/auth/logout", func(c *gin.Context) { apiresp.OK(c, gin.H{"message": "signed out"}) })
	ingest := func(c *gin.Context) {
		paperID, err := db.CreatePaper(ctx, pool, userID, "gs://test/source.pdf")
		if err != nil {
			c.AbortWithStatus(500)
			return
		}
		sourceID, err := db.CreatePipelineRun(ctx, pool, paperID, userID, "video")
		if err != nil {
			c.AbortWithStatus(500)
			return
		}
		extractionID, err := db.CreateStep(ctx, pool, sourceID, "pdf_extract")
		if err != nil {
			c.AbortWithStatus(500)
			return
		}
		if err = db.CompleteStep(ctx, pool, extractionID, "gs://test/shared/"+sourceID.String()+"/extracted.json"); err != nil {
			c.AbortWithStatus(500)
			return
		}
		apiresp.Accepted(c, gin.H{"run_id": sourceID, "paper_id": paperID, "user_id": userID})
	}
	r.POST("/api/papers/arxiv", ingest)
	r.POST("/api/papertovideo/upload", ingest)
	r.GET("/api/papertovideo/:run_id/status", StatusHandler(pool))
	r.GET("/api/papertovideo/:run_id/stream", func(c *gin.Context) {
		c.Header("Content-Type", "text/event-stream")
		fmt.Fprint(c.Writer, "data: {\"step\":\"metadata_extract\",\"status\":\"completed\",\"data\":{\"title\":\"Shared extraction demo paper\",\"authors\":\"SARAL local test\"}}\n\n")
		c.Writer.Flush()
	})
	r.POST("/api/papertovideo/:run_id/generate-formats", GenerateFormatsHandler(pool))
	r.GET("/api/generations/:batch_id", GenerationStatusHandler(pool))
	r.GET("/api/papers/:paper_id/generations", PaperGenerationsHandler(pool))
	r.POST("/api/generations/:batch_id/outputs/:format/retry", RetryGenerationOutputHandler(pool))
	script := gin.H{"run_id": "demo", "language": "en-IN", "sections": []gin.H{{"id": "introduction", "title": "Shared extraction", "narration": "One paper, three outputs.", "bullets": []string{"Extract once", "Generate in parallel"}}}}
	r.GET("/api/papertovideo/:run_id/script", func(c *gin.Context) { apiresp.OK(c, script) })
	r.GET("/api/papertoslides/:run_id/script", func(c *gin.Context) { apiresp.OK(c, script) })
	r.GET("/api/papertovideo/:run_id/audio", func(c *gin.Context) { apiresp.OK(c, gin.H{"slides": []any{}}) })
	r.GET("/api/papertopodcast/:run_id/script", func(c *gin.Context) {
		apiresp.OK(c, gin.H{"title": "Shared extraction demo", "language": "en-IN", "turns": []gin.H{{"speaker": "host_a", "text": "One upload powers all selected outputs."}}})
	})
	r.GET("/api/papertoslides/:run_id/deck", func(c *gin.Context) {
		apiresp.OK(c, gin.H{"slides_pdf_url": "http://127.0.0.1:8080/fixture/slides", "expires_in": 3600})
	})
	r.GET("/fixture/slides", func(c *gin.Context) {
		c.Header("Content-Type", "text/html")
		c.String(200, "<h1>Shared extraction</h1><p>Simulated slide preview for local UI verification.</p>")
	})
	listener, err := net.Listen("tcp", "127.0.0.1:8080")
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: r}
	defer server.Close()
	go server.Serve(listener)
	go StartGenerationDispatcher(ctx, pool, client)
	go func() {
		cursor := "0-0"
		for ctx.Err() == nil {
			messages, err := client.XRead(ctx, &goredis.XReadArgs{Streams: []string{redisx.StreamScript, cursor}, Block: time.Second}).Result()
			if err != nil {
				continue
			}
			for _, stream := range messages {
				for _, message := range stream.Messages {
					cursor = message.ID
					runID := uuid.MustParse(message.Values["run_id"].(string))
					stepID := uuid.MustParse(message.Values["step_id"].(string))
					scriptPath := "gs://test/" + runID.String() + "/script.json"
					db.CompleteStep(ctx, pool, stepID, scriptPath)
					ContinueGenerationOutput(ctx, pool, runID, stepID, scriptPath)
					// Leave time to inspect independent in-progress cards.
					go func(id uuid.UUID) {
						select {
						case <-ctx.Done():
							return
						case <-time.After(12 * time.Second):
						}
						db.CompleteRun(ctx, pool, id)
					}(runID)
				}
			}
		}
	}()
	t.Log("Browser fixture: http://127.0.0.1:8080; sign in on frontend with disposable test credentials; all generators simulated")
	// A bounded fixture lifetime, interruptible by terminating go test.
	select {
	case <-ctx.Done():
	case <-time.After(20 * time.Minute):
	}
	batches, _ := db.GetPapersByUser(ctx, pool, userID)
	raw, _ := json.Marshal(batches)
	t.Logf("Fixture papers: %s", raw)
}
