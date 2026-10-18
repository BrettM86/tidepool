-- +goose Up
ALTER TABLE bridged_actors
    ADD COLUMN handle_migration_error TEXT,
    ADD COLUMN handle_migration_message TEXT,
    ADD CONSTRAINT bridged_actors_handle_migration_error_check
        CHECK (handle_migration_error IS NULL OR handle_migration_error IN
            ('plc_rejected', 'plc_not_controlled', 'handle_invalid'));

-- +goose Down
ALTER TABLE bridged_actors
    DROP CONSTRAINT IF EXISTS bridged_actors_handle_migration_error_check,
    DROP COLUMN handle_migration_message,
    DROP COLUMN handle_migration_error;
