-- +goose Up
-- Existing actors acquire their original username on their next profile sync.
ALTER TABLE bridged_actors ADD COLUMN preferred_username TEXT;

-- +goose Down
ALTER TABLE bridged_actors DROP COLUMN IF EXISTS preferred_username;
