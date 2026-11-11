-- +goose Up
-- Account labels (chunk 04).
--
-- lapse_applied_at marks a label whose exp passed and whose lapse was applied
-- through the appliers as a negation, so the periodic lapse check applies each
-- lapse once. A newer label for the same (uri, val) replaces the expiry by
-- being the latest row.
ALTER TABLE coves_labels ADD COLUMN lapse_applied_at TIMESTAMPTZ NULL;

CREATE INDEX coves_labels_unlapsed_exp
    ON coves_labels (uri, val)
    WHERE exp IS NOT NULL AND NOT neg AND lapse_applied_at IS NULL;

-- 'pause': a claimed delivery of a persona paused by a takedown source (an
-- account label, or a takendown/suspended status) waits for the pause to
-- clear instead of being cancelled.
ALTER TABLE outbound_deliveries DROP CONSTRAINT outbound_deliveries_held_reason_check;
ALTER TABLE outbound_deliveries ADD CONSTRAINT outbound_deliveries_held_reason_check
    CHECK (held_reason IN ('', 'object', 'parent', 'consent', 'pause'));

-- An account-scope label withhold covers every blob its owner DID owns,
-- including blobs stored after the label: it names no CID.
ALTER TABLE blob_withholds ALTER COLUMN cid DROP NOT NULL;
ALTER TABLE blob_withholds ADD CONSTRAINT blob_withholds_cid_check
    CHECK (cid IS NOT NULL OR (source = 'label' AND scope = 'owner'));

CREATE UNIQUE INDEX blob_withholds_one_active_owner_label
    ON blob_withholds (owner_did)
    WHERE source = 'label' AND scope = 'owner' AND cid IS NULL AND lifted_at IS NULL;

-- +goose Down
DROP INDEX IF EXISTS blob_withholds_one_active_owner_label;
DELETE FROM blob_withholds WHERE cid IS NULL;
ALTER TABLE blob_withholds DROP CONSTRAINT IF EXISTS blob_withholds_cid_check;
ALTER TABLE blob_withholds ALTER COLUMN cid SET NOT NULL;
UPDATE outbound_deliveries
SET state = 'pending', held_reason = '', held_subject = ''
WHERE state = 'held' AND held_reason = 'pause';
ALTER TABLE outbound_deliveries DROP CONSTRAINT outbound_deliveries_held_reason_check;
ALTER TABLE outbound_deliveries ADD CONSTRAINT outbound_deliveries_held_reason_check
    CHECK (held_reason IN ('', 'object', 'parent', 'consent'));
DROP INDEX IF EXISTS coves_labels_unlapsed_exp;
ALTER TABLE coves_labels DROP COLUMN IF EXISTS lapse_applied_at;
