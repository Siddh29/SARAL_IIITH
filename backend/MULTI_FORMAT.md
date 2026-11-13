# Multi-format generation

Upload one paper, select **Video**, **Podcast**, and/or **Slides**, confirm the
paper details, and click **Generate outputs**. Selected outputs appear together
on the paper dashboard with independent progress, previews, downloads and retry
buttons. Leaving every option unchecked preserves the existing upload-only flow.

The first version uses the existing English/default voice settings. Existing
single-output configuration and edit/regenerate flows remain available.

## Architecture

```text
One PDF upload (or arXiv ingestion)
  └─ pdf_extract → immutable extracted.json + image references
       └─ one generation_batch
            ├─ video script → slides + narration → video
            ├─ podcast script → dialogue audio → waveform video
            └─ slides script → presentation PDF
```

A batch is the user-facing generation run. Its per-format `pipeline_runs` retain
the existing workers, output paths and preview/download APIs. All branches point
to the exact extraction captured from the upload's completed `pdf_extract` step.
The completed extraction rows on branches are **references**, not additional
parser jobs. Generation and branch retry never enqueue the PDF stream.

The batch, child runs and initial generator jobs commit in one Postgres
transaction. Jobs are stored in `generation_job_outbox`; the gateway dispatcher
forwards them to the existing Redis Streams. Redis delivery markers prevent
duplicate enqueueing if the gateway restarts between XADD and SQL acknowledgement.
Provider keys are resolved at dispatch time, not copied into the outbox.

Script completion webhooks automatically continue batch video/slides/podcast
branches on the server. Browser tabs do not need to confirm each branch or stay
open. The dashboard polls durable batch status and restores in-progress cards
after refresh. Existing standalone script review behavior stays unchanged.

One failed branch does not cancel siblings. Aggregate state is `processing`
until all branches finish; terminal aggregate states are `completed`, `failed`,
or `partial_failure`. Retry targets one failed format and uses the retained
extraction. Each retry gets a new child run while retaining the old failed attempt
for history, so late worker callbacks cannot complete the new attempt. Repeated
identical start requests return the same batch; changing
formats on an already-started upload returns 409 rather than creating duplicate
generators. Existing individual generation actions can create additional outputs.

## Database setup

Apply `migrations/010_multi_format.sql` after the existing numbered migrations
before starting the updated gateway. For example, from `backend/`:

```bash
psql "$DATABASE_URL" -f migrations/010_multi_format.sql
```

The gateway's normal startup starts the dispatcher. No new worker service or
external dependency is required for the feature. `miniredis` is a test dependency.

## API

All routes require the existing authentication middleware and enforce ownership.

```http
POST /api/papertovideo/:upload_run_id/generate-formats
Content-Type: application/json

{"formats":["video","podcast","slides"]}
```

202 returns `{success:true,data:{id,source_run_id,paper_id,formats,status,outputs}}`.
Each output contains `run_id`, `format`, `status`, `current_step`, `updated_at`
and, on failure, `error_message`. Empty/unsupported format selections return 400;
unfinished extraction and changed selections return 409; unowned runs return 404.

```http
GET /api/generations/:batch_id
GET /api/papers/:paper_id/generations
POST /api/generations/:batch_id/outputs/:format/retry
```

`format` is `video`, `podcast`, or `slides`. The list endpoint returns
`{success:true,data:{batches:[...]}}`. Retry returns 202; retrying a healthy or
already-retrying branch returns 409. Source extraction and successful siblings
are unchanged.

## Validation

```bash
# From frontend/
npm run test:generation
npx tsc --noEmit
npm run lint
npm run build

# From backend/gateway/
go test ./...

# Use an isolated database with all migrations applied for SQL/outbox coverage.
SARAL_TEST_DATABASE_URL='postgres://...' go test ./internal/pipeline -run TestMultiFormat -v -count=1
```

Tests cover selection validation, aggregate states, three generators sharing one
extraction path, request/callback deduplication, outbox crash recovery, ownership,
partial failure, branch-only retry, preview hydration and stale status recovery.
The SQL test skips unless `SARAL_TEST_DATABASE_URL` is set.

An opt-in browser fixture in `generation_browser_test.go` serves simulated auth,
ingestion and worker completions on **127.0.0.1:8080**, while using the real batch
handlers/database/outbox. It is excluded from production binaries:

```bash
SARAL_TEST_DATABASE_URL='postgres://...' SARAL_BROWSER_FIXTURE=1 \
  go test ./internal/pipeline -run '^TestGenerationBrowserFixture$' -v -timeout=25m
```

Use a disposable test database and test credentials. Do not run this fixture
alongside a real gateway on port 8080. It verifies UI wiring and orchestration;
real LLM/TTS quality and rendered media still require the normal configured
workers and provider credentials.
