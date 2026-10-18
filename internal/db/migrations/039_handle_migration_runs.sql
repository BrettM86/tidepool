-- +goose Up
CREATE INDEX bridged_actors_forward_rename_scan_idx
    ON bridged_actors (id) INCLUDE (did, handle)
    WHERE consent_state <> 'deleted' AND handle IS NOT NULL;

CREATE TABLE handle_migration_runs (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    mode TEXT NOT NULL CHECK (mode IN ('forward')),
    dids TEXT[],
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ
);

CREATE UNIQUE INDEX handle_migration_runs_one_active_idx
    ON handle_migration_runs ((true)) WHERE finished_at IS NULL;

-- +goose Down
DROP TABLE IF EXISTS handle_migration_runs;
DROP INDEX IF EXISTS bridged_actors_forward_rename_scan_idx;
