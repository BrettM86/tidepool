-- +goose Up
-- A bridged community whose removal a label negation cleared owes a
-- reconcile of its kept objects against their origins. reconcile_pending_at
-- records it in the negation's transaction and stays set until a reconcile
-- pass gives every kept object a final outcome; a newer negation during a
-- pass sets a newer value, which that pass does not clear. NULL is nothing
-- pending.
ALTER TABLE communities ADD COLUMN reconcile_pending_at TIMESTAMPTZ;

-- reconcile_first_attempt_at is when the first pass for the current
-- reconcile_pending_at started. Past the backfill's dead-host window from it,
-- a pass counts the kept objects whose origin still gives no answer
-- unreachable and clears the marker. A new marker resets it to NULL; the
-- clear resets it with the marker.
ALTER TABLE communities ADD COLUMN reconcile_first_attempt_at TIMESTAMPTZ;

CREATE INDEX communities_reconcile_pending
    ON communities (id) WHERE reconcile_pending_at IS NOT NULL;

-- +goose Down
DROP INDEX IF EXISTS communities_reconcile_pending;
ALTER TABLE communities DROP COLUMN IF EXISTS reconcile_first_attempt_at;
ALTER TABLE communities DROP COLUMN IF EXISTS reconcile_pending_at;
