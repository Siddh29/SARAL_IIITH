package pipeline

import (
	"context"
	"errors"
	"net/http"
	"sort"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/saral/gateway/internal/apiresp"
	"github.com/saral/gateway/internal/db"
	redisx "github.com/saral/gateway/internal/redis"
)

func normalizeGenerationFormats(formats []string) ([]string, error) {
	if len(formats) == 0 || len(formats) > 3 {
		return nil, errors.New("select one or more of video, podcast, slides")
	}
	result := []string{}
	seen := map[string]bool{}
	for _, format := range formats {
		if format != "video" && format != "podcast" && format != "slides" {
			return nil, errors.New("supported formats are video, podcast, slides")
		}
		if !seen[format] {
			result = append(result, format)
			seen[format] = true
		}
	}
	sort.Strings(result)
	return result, nil
}

func generationUser(c *gin.Context, pool *pgxpool.Pool) (uuid.UUID, error) {
	return db.UpsertUser(c.Request.Context(), pool, c.GetString("firebase_uid"), c.GetString("email"), c.GetString("provider"))
}

func GenerateFormatsHandler(pool *pgxpool.Pool) gin.HandlerFunc {
	return func(c *gin.Context) {
		sourceID, err := uuid.Parse(c.Param("run_id"))
		if err != nil {
			apiresp.Error(c, 400, "invalid_run_id", "invalid source run_id")
			return
		}
		var body struct {
			Formats []string `json:"formats"`
		}
		if c.ShouldBindJSON(&body) != nil {
			apiresp.Error(c, 400, "invalid_body", "formats must be an array")
			return
		}
		formats, err := normalizeGenerationFormats(body.Formats)
		if err != nil {
			apiresp.Error(c, 400, "invalid_formats", err.Error())
			return
		}
		userID, err := generationUser(c, pool)
		if err != nil {
			apiresp.Error(c, 500, "user_error", "could not load user")
			return
		}
		id, err := db.CreateGenerationBatch(c.Request.Context(), pool, sourceID, userID, formats)
		switch {
		case errors.Is(err, pgx.ErrNoRows):
			apiresp.Error(c, 404, "run_not_found", "source run not found")
			return
		case errors.Is(err, db.ErrFormatsConflict):
			apiresp.Error(c, 409, "formats_conflict", err.Error())
			return
		case errors.Is(err, db.ErrExtractionNotReady):
			apiresp.Error(c, 409, "extraction_not_ready", err.Error())
			return
		case err != nil:
			apiresp.Error(c, 500, "generation_create_failed", "could not start generation")
			return
		}
		batch, err := db.GetGenerationBatch(c.Request.Context(), pool, id, userID)
		if err != nil {
			apiresp.Error(c, 500, "generation_lookup_failed", "could not load generation; retry this request safely")
			return
		}
		apiresp.Accepted(c, batch)
	}
}

func GenerationStatusHandler(pool *pgxpool.Pool) gin.HandlerFunc {
	return func(c *gin.Context) {
		id, err := uuid.Parse(c.Param("batch_id"))
		if err != nil {
			apiresp.Error(c, 400, "invalid_batch_id", "invalid generation id")
			return
		}
		userID, err := generationUser(c, pool)
		if err != nil {
			apiresp.Error(c, 500, "user_error", "could not load user")
			return
		}
		batch, err := db.GetGenerationBatch(c.Request.Context(), pool, id, userID)
		if errors.Is(err, pgx.ErrNoRows) {
			apiresp.Error(c, 404, "generation_not_found", "generation not found")
			return
		}
		if err != nil {
			apiresp.Error(c, 500, "generation_lookup_failed", "could not load generation")
			return
		}
		apiresp.OK(c, batch)
	}
}

func PaperGenerationsHandler(pool *pgxpool.Pool) gin.HandlerFunc {
	return func(c *gin.Context) {
		paperID, err := uuid.Parse(c.Param("paper_id"))
		if err != nil {
			apiresp.Error(c, 400, "invalid_paper_id", "invalid paper_id")
			return
		}
		userID, err := generationUser(c, pool)
		if err != nil {
			apiresp.Error(c, 500, "user_error", "could not load user")
			return
		}
		owner, err := db.GetPaperOwner(c.Request.Context(), pool, paperID)
		if err != nil || owner != userID {
			apiresp.Error(c, 404, "paper_not_found", "paper not found")
			return
		}
		batches, err := db.ListGenerationBatches(c.Request.Context(), pool, paperID, userID)
		if err != nil {
			apiresp.Error(c, 500, "generation_lookup_failed", "could not load generations")
			return
		}
		apiresp.OK(c, gin.H{"batches": batches})
	}
}

// ContinueGenerationOutput automatically renders batch scripts server-side.
// It is safe for duplicate script webhooks and does not depend on an open tab.
func ContinueGenerationOutput(ctx context.Context, pool *pgxpool.Pool, runID, scriptStepID uuid.UUID, scriptPath string) (bool, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	var batchID *uuid.UUID
	var paperID, userID uuid.UUID
	var mode string
	if err := tx.QueryRow(ctx, `SELECT generation_batch_id,paper_id,user_id,mode FROM pipeline_runs WHERE id=$1 FOR UPDATE`, runID).Scan(&batchID, &paperID, &userID, &mode); err != nil {
		return false, err
	}
	if batchID == nil {
		return false, nil
	} // Existing single-format review behavior.
	if mode != "video" && mode != "slides" && mode != "podcast" {
		return false, nil
	}
	key := "render:" + scriptStepID.String()
	var queued bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM generation_job_outbox WHERE dedupe_key=$1)`, key).Scan(&queued); err != nil {
		return true, err
	}
	if queued {
		return true, nil
	}
	var extracted string
	if err := tx.QueryRow(ctx, `SELECT extracted_gcs_path FROM generation_batches WHERE id=$1`, batchID).Scan(&extracted); err != nil {
		return true, err
	}
	if mode == "podcast" {
		ttsID := uuid.New()
		if _, err := tx.Exec(ctx, `INSERT INTO pipeline_steps(id,run_id,step_name) VALUES($1,$2,'podcast_tts')`, ttsID, runID); err != nil {
			return true, err
		}
		if _, err := tx.Exec(ctx, `UPDATE pipeline_runs SET current_step='podcast_tts',status='processing',updated_at=NOW() WHERE id=$1`, runID); err != nil {
			return true, err
		}
		if err := db.QueueGenerationJob(ctx, tx, runID, key, redisx.StreamAudio, redisx.JobData{
			"run_id": runID.String(), "step_id": ttsID.String(), "paper_id": paperID.String(), "user_id": userID.String(),
			"script_gcs_path": scriptPath, "pipeline_type": "podcast",
		}); err != nil {
			return true, err
		}
		return true, tx.Commit(ctx)
	}
	beamerID := uuid.New()
	if _, err := tx.Exec(ctx, `INSERT INTO pipeline_steps(id,run_id,step_name) VALUES($1,$2,'beamer_compile')`, beamerID, runID); err != nil {
		return true, err
	}
	if _, err := tx.Exec(ctx, `UPDATE pipeline_runs SET current_step='beamer_compile',status='processing',updated_at=NOW() WHERE id=$1`, runID); err != nil {
		return true, err
	}
	if err := db.QueueGenerationJob(ctx, tx, runID, key, redisx.StreamBeamer, redisx.JobData{
		"run_id": runID.String(), "step_id": beamerID.String(), "paper_id": paperID.String(), "user_id": userID.String(),
		"script_gcs_path": scriptPath, "extracted_gcs_path": extracted, "output_format": "beamer_pdf",
	}); err != nil {
		return true, err
	}
	if mode == "video" {
		audioID := uuid.New()
		if _, err := tx.Exec(ctx, `INSERT INTO pipeline_steps(id,run_id,step_name) VALUES($1,$2,'audio_gen')`, audioID, runID); err != nil {
			return true, err
		}
		if err := db.QueueGenerationJob(ctx, tx, runID, key+":audio", redisx.StreamAudio, redisx.JobData{
			"run_id": runID.String(), "step_id": audioID.String(), "paper_id": paperID.String(), "user_id": userID.String(), "script_gcs_path": scriptPath,
		}); err != nil {
			return true, err
		}
	}
	return true, tx.Commit(ctx)
}

// Retry just one failed branch from the retained extraction. Siblings and the
// source upload are untouched; no parser job is ever created by this endpoint.
func RetryGenerationOutputHandler(pool *pgxpool.Pool) gin.HandlerFunc {
	return func(c *gin.Context) {
		batchID, err := uuid.Parse(c.Param("batch_id"))
		format := c.Param("format")
		if err != nil || (format != "video" && format != "podcast" && format != "slides") {
			apiresp.Error(c, 400, "invalid_output", "invalid generation output")
			return
		}
		userID, err := generationUser(c, pool)
		if err != nil {
			apiresp.Error(c, 500, "user_error", "could not load user")
			return
		}
		tx, err := pool.Begin(c.Request.Context())
		if err != nil {
			apiresp.Error(c, 500, "db_error", "could not retry output")
			return
		}
		defer tx.Rollback(c.Request.Context())
		var runID, paperID uuid.UUID
		var status, extracted string
		err = tx.QueryRow(c.Request.Context(), `SELECT r.id,r.paper_id,r.status::text,b.extracted_gcs_path
            FROM pipeline_runs r JOIN generation_batches b ON b.id=r.generation_batch_id
            WHERE b.id=$1 AND b.user_id=$2 AND r.mode=$3 FOR UPDATE OF b,r`, batchID, userID, format).Scan(&runID, &paperID, &status, &extracted)
		if errors.Is(err, pgx.ErrNoRows) {
			apiresp.Error(c, 404, "output_not_found", "output not found")
			return
		}
		if err != nil {
			apiresp.Error(c, 500, "db_error", "could not load output")
			return
		}
		if status != "failed" {
			apiresp.Error(c, http.StatusConflict, "output_not_failed", "only failed outputs can be retried")
			return
		}
		// Retain the failed attempt for history but detach it from the active
		// batch. Late callbacks can only update that old run, never the retry.
		_, err = tx.Exec(c.Request.Context(), `UPDATE pipeline_runs SET generation_batch_id=NULL WHERE id=$1`, runID)
		if err == nil {
			runID, err = db.CreateGenerationOutput(c.Request.Context(), tx, batchID, paperID, userID, format, extracted)
		}
		if err == nil {
			err = tx.Commit(c.Request.Context())
		}
		if err != nil {
			apiresp.Error(c, 500, "retry_failed", "could not queue output retry")
			return
		}
		apiresp.Accepted(c, gin.H{"run_id": runID, "status": "processing"})
	}
}
