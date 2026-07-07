-- +goose Up
-- inbox_events deduplicates inbound AP activities by id and records
-- processing outcomes. The unique activity_id is the dedupe key: a second
-- delivery of the same activity inserts nothing.
CREATE TABLE inbox_events (
    id BIGSERIAL PRIMARY KEY,
    activity_id TEXT NOT NULL CHECK (activity_id <> ''),        -- AP activity id (URL)
    type TEXT NOT NULL CHECK (type <> ''),                      -- AP activity type: Announce, Create, Like, ...
    received_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMPTZ,                                   -- NULL until successfully processed
    error TEXT,                                                 -- last processing error, if any
    CONSTRAINT inbox_events_activity_id_key UNIQUE (activity_id)
);

CREATE INDEX idx_inbox_events_unprocessed ON inbox_events (received_at) WHERE processed_at IS NULL;

-- +goose Down
DROP INDEX IF EXISTS idx_inbox_events_unprocessed;
DROP TABLE IF EXISTS inbox_events;
