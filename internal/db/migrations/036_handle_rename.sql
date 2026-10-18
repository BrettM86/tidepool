-- +goose Up
ALTER TABLE bridged_actors
    ADD COLUMN pending_handle TEXT,
    ADD COLUMN previous_handle TEXT,
    ADD COLUMN handle_migrated_at TIMESTAMPTZ,
    ADD CONSTRAINT bridged_actors_pending_handle_differs
        CHECK (pending_handle IS NULL OR handle IS NULL OR pending_handle <> handle);

CREATE UNIQUE INDEX bridged_actors_pending_handle_key
    ON bridged_actors (pending_handle) WHERE pending_handle IS NOT NULL;

-- +goose Down
DROP INDEX IF EXISTS bridged_actors_pending_handle_key;
ALTER TABLE bridged_actors
    DROP CONSTRAINT IF EXISTS bridged_actors_pending_handle_differs,
    DROP COLUMN handle_migrated_at,
    DROP COLUMN previous_handle,
    DROP COLUMN pending_handle;
