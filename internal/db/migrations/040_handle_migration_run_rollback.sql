-- +goose Up
ALTER TABLE handle_migration_runs
    DROP CONSTRAINT handle_migration_runs_mode_check,
    ADD CONSTRAINT handle_migration_runs_mode_check CHECK (mode IN ('forward', 'rollback'));

-- +goose Down
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM handle_migration_runs WHERE mode = 'rollback') THEN
        RAISE EXCEPTION 'handle_migration_runs holds rollback runs; remove them before rolling back migration 040';
    END IF;
END $$;
-- +goose StatementEnd

ALTER TABLE handle_migration_runs
    DROP CONSTRAINT handle_migration_runs_mode_check,
    ADD CONSTRAINT handle_migration_runs_mode_check CHECK (mode IN ('forward'));
