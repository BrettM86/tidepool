-- +goose Up
-- Task 04: the firehose retention pruner (FIREHOSE_RETENTION, default 72h)
-- finds the newest expired seq by created_at before deleting the seq prefix;
-- this index keeps that scan off the whole table as the backlog grows.
CREATE INDEX idx_firehose_events_created_at ON firehose_events (created_at);

-- +goose Down
DROP INDEX IF EXISTS idx_firehose_events_created_at;
