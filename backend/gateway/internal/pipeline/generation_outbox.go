package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	goredis "github.com/redis/go-redis/v9"
	"github.com/saral/gateway/internal/db"
	redisx "github.com/saral/gateway/internal/redis"
)

// The Redis marker closes the XADD/SQL-commit crash window. No provider keys
// are persisted in the outbox; current user keys are resolved at dispatch.
var enqueueGenerationOnce = goredis.NewScript(`
local previous=redis.call('GET',KEYS[1])
if previous then return previous end
local id=redis.call('XADD',KEYS[2],'*',unpack(ARGV))
redis.call('SET',KEYS[1],id)
return id
`)

func DispatchGenerationJob(ctx context.Context, pool *pgxpool.Pool, rdb *goredis.Client) (bool, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	var id, userID uuid.UUID
	var stream string
	var raw []byte
	err = tx.QueryRow(ctx, `SELECT o.id,r.user_id,o.stream,o.payload FROM generation_job_outbox o
        JOIN pipeline_runs r ON r.id=o.run_id WHERE o.dispatched_at IS NULL AND r.generation_batch_id IS NOT NULL
        ORDER BY o.created_at,o.id LIMIT 1 FOR UPDATE OF o SKIP LOCKED`).Scan(&id, &userID, &stream, &raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	payload := redisx.JobData{}
	if err = json.Unmarshal(raw, &payload); err != nil {
		return false, err
	}
	gemini, sarvam, err := db.GetUserKeys(ctx, tx, userID)
	if err != nil {
		return false, err
	}
	payload["gemini_key"], payload["sarvam_key"] = gemini, sarvam
	args := []interface{}{}
	for key, value := range payload {
		args = append(args, key, value)
	}
	if err = enqueueGenerationOnce.Run(ctx, rdb, []string{"saral:generation-job:" + id.String(), stream}, args...).Err(); err != nil {
		return false, err
	}
	if _, err = tx.Exec(ctx, `UPDATE generation_job_outbox SET dispatched_at=NOW() WHERE id=$1`, id); err != nil {
		return false, err
	}
	return true, tx.Commit(ctx)
}

func StartGenerationDispatcher(ctx context.Context, pool *pgxpool.Pool, rdb *goredis.Client) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		// Bound each pass so cancellation and other gateway work stay responsive.
		for i := 0; i < 50; i++ {
			dispatchCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			dispatched, err := DispatchGenerationJob(dispatchCtx, pool, rdb)
			cancel()
			if err != nil {
				log.Printf("Generation dispatcher: %v", err)
				break
			}
			if !dispatched {
				break
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
