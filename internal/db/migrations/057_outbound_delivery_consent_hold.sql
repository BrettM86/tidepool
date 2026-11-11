-- +goose Up
-- The consent hold (chunk 03). A takedown restore's Undo{Delete} whose author
-- is paused, disabled or opted out waits for consent off its ordering queue,
-- held with reason 'consent' on the restored object's at-uri, together with
-- that object's queued Create/Update rows, so it neither blocks every other
-- author's work to the community nor lets the object's edits overtake it. Only
-- the author's consent returning releases these rows, never a takedown release
-- on the subject.
ALTER TABLE outbound_deliveries DROP CONSTRAINT outbound_deliveries_held_reason_check;
ALTER TABLE outbound_deliveries ADD CONSTRAINT outbound_deliveries_held_reason_check
    CHECK (held_reason IN ('', 'object', 'parent', 'consent'));

-- +goose Down
UPDATE outbound_deliveries
SET state = 'pending', held_reason = '', held_subject = ''
WHERE state = 'held' AND held_reason = 'consent';
ALTER TABLE outbound_deliveries DROP CONSTRAINT outbound_deliveries_held_reason_check;
ALTER TABLE outbound_deliveries ADD CONSTRAINT outbound_deliveries_held_reason_check
    CHECK (held_reason IN ('', 'object', 'parent'));
