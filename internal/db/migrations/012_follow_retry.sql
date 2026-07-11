-- +goose Up
-- Task 11: automatic Follow re-send. Lemmy initializes a newly-seen
-- instance's federation cursor at the current max activity id, so the
-- Accept answering the very FIRST Follow to a given Lemmy instance is
-- usually skipped (FOLLOWUPS "Lemmy first-contact Accept race"). The
-- follow retrier re-sends the Follow (fresh activity id → fresh Accept)
-- when a subscription stays pending past a threshold; these columns track
-- when the last Follow went out and how many have been sent, bounding the
-- retries.
ALTER TABLE communities
    ADD COLUMN follow_requested_at TIMESTAMPTZ,
    ADD COLUMN follow_attempts INTEGER NOT NULL DEFAULT 0;

-- +goose Down
ALTER TABLE communities
    DROP COLUMN IF EXISTS follow_attempts,
    DROP COLUMN IF EXISTS follow_requested_at;
