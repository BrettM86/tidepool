-- +goose Up
-- Where a deferred row came from: 'backfill' (a walk skipped the item on a
-- dead host) or 'inbox' (a failing inbox event deferred its content).
ALTER TABLE backfill_deferred_items
    ADD COLUMN source TEXT NOT NULL DEFAULT 'backfill'
        CHECK (source IN ('backfill', 'inbox'));

-- +goose Down
ALTER TABLE backfill_deferred_items DROP COLUMN IF EXISTS source;
