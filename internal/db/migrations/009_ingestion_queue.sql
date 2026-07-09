-- +goose Up
-- Task 06 turns inbox_events into the durable ingestion work queue (PLAN
-- decision: postgres is the queue, no external broker). The dedupe-by-
-- activity-id contract from migration 004 is unchanged; these columns add
-- what the worker pool needs:
--   payload         the raw activity JSON as delivered (verified body)
--   actor_id        the signature-verified AP actor the activity binds to
--   ordering_key    per-community serialization key: events sharing a key
--                   are processed strictly in arrival (id) order
--   attempts        how many times a worker claimed the event
--   next_attempt_at retry-with-backoff schedule; claimable when <= now
--   claimed_until   worker lease; a crashed worker's claim expires
--   failed_at       poison marker: permanently failed events are skipped
--                   (error column keeps the reason) and stop blocking their
--                   ordering key
ALTER TABLE inbox_events
    ADD COLUMN payload BYTEA,
    ADD COLUMN actor_id TEXT NOT NULL DEFAULT '',
    ADD COLUMN ordering_key TEXT NOT NULL DEFAULT '',
    ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    ADD COLUMN claimed_until TIMESTAMPTZ,
    ADD COLUMN failed_at TIMESTAMPTZ;

-- The claim query scans pending events per ordering key in id order.
CREATE INDEX idx_inbox_events_queue
    ON inbox_events (ordering_key, id)
    WHERE processed_at IS NULL AND failed_at IS NULL;

-- ap_tombstones remembers Deletes that arrived for AP ids we had never
-- materialized. AP delivery is unordered: without this, a Create arriving
-- (or being re-delivered) after its Delete would happily materialize content
-- the origin already removed (the create-after-delete gap flagged in task
-- 05). Undo{Delete} removes the row.
CREATE TABLE ap_tombstones (
    ap_id TEXT PRIMARY KEY CHECK (ap_id <> ''),
    deleted_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- +goose Down
DROP TABLE IF EXISTS ap_tombstones;
DROP INDEX IF EXISTS idx_inbox_events_queue;
ALTER TABLE inbox_events
    DROP COLUMN IF EXISTS failed_at,
    DROP COLUMN IF EXISTS claimed_until,
    DROP COLUMN IF EXISTS next_attempt_at,
    DROP COLUMN IF EXISTS attempts,
    DROP COLUMN IF EXISTS ordering_key,
    DROP COLUMN IF EXISTS actor_id,
    DROP COLUMN IF EXISTS payload;
