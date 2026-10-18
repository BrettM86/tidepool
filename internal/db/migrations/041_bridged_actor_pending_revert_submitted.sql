-- +goose Up
-- Set right before a rollback POSTs a PLC update back to the current handle
-- while abandoning pending_handle. AbandonPendingHandle reads it to decide
-- whether PLC changed (and so whether to emit #identity) even when the POST
-- landed in an earlier attempt, and clears it.
ALTER TABLE bridged_actors ADD COLUMN pending_revert_submitted_at TIMESTAMPTZ;

-- +goose Down
ALTER TABLE bridged_actors DROP COLUMN pending_revert_submitted_at;
