package db

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/saral/gateway/internal/models"
)

var ErrFormatsConflict = errors.New("this upload already has a generation request with different formats")
var ErrExtractionNotReady = errors.New("paper extraction is not complete")

func GetGenerationBatch(ctx context.Context, pool *pgxpool.Pool, id, userID uuid.UUID) (*models.GenerationBatch, error) {
	batch := &models.GenerationBatch{Outputs: []models.GenerationOutput{}}
	err := pool.QueryRow(ctx, `SELECT id, source_run_id, paper_id, formats FROM generation_batches
        WHERE id=$1 AND user_id=$2`, id, userID).Scan(&batch.ID, &batch.SourceRunID, &batch.PaperID, &batch.Formats)
	if err != nil {
		return nil, err
	}
	rows, err := pool.Query(ctx, `SELECT id, mode, status::text, COALESCE(current_step::text,''), COALESCE(error_message,''), updated_at
        FROM pipeline_runs WHERE generation_batch_id=$1 ORDER BY mode`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var output models.GenerationOutput
		if err := rows.Scan(&output.RunID, &output.Format, &output.Status, &output.CurrentStep, &output.ErrorMessage, &output.UpdatedAt); err != nil {
			return nil, err
		}
		batch.Outputs = append(batch.Outputs, output)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	batch.Status = models.GenerationStatus(batch.Outputs)
	return batch, nil
}

func ListGenerationBatches(ctx context.Context, pool *pgxpool.Pool, paperID, userID uuid.UUID) ([]*models.GenerationBatch, error) {
	rows, err := pool.Query(ctx, `SELECT id FROM generation_batches WHERE paper_id=$1 AND user_id=$2 ORDER BY created_at`, paperID, userID)
	if err != nil {
		return nil, err
	}
	ids := []uuid.UUID{}
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	batches := []*models.GenerationBatch{}
	for _, id := range ids {
		batch, err := GetGenerationBatch(ctx, pool, id, userID)
		if err != nil {
			return nil, err
		}
		batches = append(batches, batch)
	}
	return batches, nil
}

// CreateGenerationBatch locks the owned upload run, captures its extraction
// once, and atomically creates all branches + initial outbox jobs. Retrying an
// identical request returns the same batch; it never queues a second extractor.
func CreateGenerationBatch(ctx context.Context, pool *pgxpool.Pool, sourceID, userID uuid.UUID, formats []string) (uuid.UUID, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return uuid.Nil, err
	}
	defer tx.Rollback(ctx)
	var paperID uuid.UUID
	if err := tx.QueryRow(ctx, `SELECT paper_id FROM pipeline_runs
        WHERE id=$1 AND user_id=$2 AND mode='video' AND generation_batch_id IS NULL FOR UPDATE`, sourceID, userID).Scan(&paperID); err != nil {
		return uuid.Nil, err
	}
	var existing uuid.UUID
	var existingFormats []string
	err = tx.QueryRow(ctx, `SELECT id,formats FROM generation_batches WHERE source_run_id=$1`, sourceID).Scan(&existing, &existingFormats)
	if err == nil {
		if fmt.Sprint(existingFormats) != fmt.Sprint(formats) {
			return uuid.Nil, ErrFormatsConflict
		}
		return existing, tx.Commit(ctx)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return uuid.Nil, err
	}
	var extracted string
	err = tx.QueryRow(ctx, `SELECT gcs_output_path FROM pipeline_steps WHERE run_id=$1
        AND step_name='pdf_extract' AND status='completed' AND COALESCE(gcs_output_path,'')<>''
        ORDER BY completed_at DESC LIMIT 1`, sourceID).Scan(&extracted)
	if errors.Is(err, pgx.ErrNoRows) {
		return uuid.Nil, ErrExtractionNotReady
	}
	if err != nil {
		return uuid.Nil, err
	}
	batchID := uuid.New()
	_, err = tx.Exec(ctx, `INSERT INTO generation_batches(id,source_run_id,paper_id,user_id,formats,extracted_gcs_path)
        VALUES($1,$2,$3,$4,$5,$6)`, batchID, sourceID, paperID, userID, formats, extracted)
	if err != nil {
		return uuid.Nil, err
	}
	for _, format := range formats {
		if _, err := CreateGenerationOutput(ctx, tx, batchID, paperID, userID, format, extracted); err != nil {
			return uuid.Nil, err
		}
	}
	return batchID, tx.Commit(ctx)
}

func CreateGenerationOutput(ctx context.Context, tx pgx.Tx, batchID, paperID, userID uuid.UUID, format, extracted string) (uuid.UUID, error) {
	runID := uuid.New()
	if _, err := tx.Exec(ctx, `INSERT INTO pipeline_runs(id,paper_id,user_id,mode,generation_batch_id,checkpoint_data)
        VALUES($1,$2,$3,$4,$5,jsonb_build_object('pdf_extract_path',$6::text))`, runID, paperID, userID, format, batchID, extracted); err != nil {
		return uuid.Nil, err
	}
	// A reference to the completed shared extraction, never a parser job.
	if _, err := tx.Exec(ctx, `INSERT INTO pipeline_steps(run_id,step_name,status,gcs_output_path,completed_at)
        VALUES($1,'pdf_extract','completed',$2,NOW())`, runID, extracted); err != nil {
		return uuid.Nil, err
	}
	return runID, QueueGenerationScript(ctx, tx, runID, paperID, userID, format, extracted)
}

func QueueGenerationJob(ctx context.Context, tx pgx.Tx, runID uuid.UUID, key, stream string, payload map[string]string) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO generation_job_outbox(run_id,dedupe_key,stream,payload) VALUES($1,$2,$3,$4::jsonb)`, runID, key, stream, string(raw))
	return err
}

// All generators receive the exact same extracted_gcs_path. Format-specific
// prompts remain independent; neither PDFs nor images are parsed again.
func QueueGenerationScript(ctx context.Context, tx pgx.Tx, runID, paperID, userID uuid.UUID, format, extracted string) error {
	step, mode := "script_gen", "video"
	if format == "slides" {
		mode = "slides_deck"
	}
	if format == "podcast" {
		step, mode = "podcast_script_gen", "podcast"
	}
	stepID := uuid.New()
	if _, err := tx.Exec(ctx, `INSERT INTO pipeline_steps(id,run_id,step_name) VALUES($1,$2,$3::step_name_enum)`, stepID, runID, step); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE pipeline_runs SET status='processing',current_step=$2::step_name_enum,
        error_message=NULL,failed_step=NULL,completed_at=NULL,updated_at=NOW() WHERE id=$1`, runID, step); err != nil {
		return err
	}
	return QueueGenerationJob(ctx, tx, runID, stepID.String(), "saral:jobs:script", map[string]string{
		"run_id": runID.String(), "step_id": stepID.String(), "paper_id": paperID.String(), "user_id": userID.String(),
		"extracted_gcs_path": extracted, "mode": mode, "language": "en-IN", "host_a_gender": "female",
		"host_b_gender": "male", "render_video": "true", "audience_level": "intermediate", "tone": "formal",
	})
}
