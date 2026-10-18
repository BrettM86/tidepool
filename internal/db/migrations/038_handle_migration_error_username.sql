-- +goose Up
ALTER TABLE bridged_actors
    DROP CONSTRAINT bridged_actors_handle_migration_error_check,
    ADD CONSTRAINT bridged_actors_handle_migration_error_check
        CHECK (handle_migration_error IS NULL OR handle_migration_error IN
            ('plc_rejected', 'plc_not_controlled', 'handle_invalid', 'username_unavailable'));

-- +goose Down
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM bridged_actors WHERE handle_migration_error = 'username_unavailable') THEN
        RAISE EXCEPTION 'bridged_actors holds username_unavailable errors; resolve them before rolling back migration 038';
    END IF;
END $$;
-- +goose StatementEnd

ALTER TABLE bridged_actors
    DROP CONSTRAINT bridged_actors_handle_migration_error_check,
    ADD CONSTRAINT bridged_actors_handle_migration_error_check
        CHECK (handle_migration_error IS NULL OR handle_migration_error IN
            ('plc_rejected', 'plc_not_controlled', 'handle_invalid'));
