-- +goose Up
-- The Group's AP sensitive (NSFW) flag as last seen in its document.
ALTER TABLE communities ADD COLUMN sensitive boolean NOT NULL DEFAULT false;

-- +goose Down
ALTER TABLE communities DROP COLUMN IF EXISTS sensitive;
