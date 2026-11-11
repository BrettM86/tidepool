-- +goose Up
-- Community label withholds (chunk 05). A Coves `!takedown` label on a
-- bridged community's bare DID withholds, for each record's owner, every blob
-- CID of every hosted bridged record bound to the community
-- (ap_objects.community_did): community scope, naming the community, the
-- owner and the CID. One active row per (community, owner, CID), so the label
-- applier and a commit of a record in the community write idempotently (ON
-- CONFLICT on this index). The negation lifts the community's label rows only.
CREATE UNIQUE INDEX blob_withholds_one_active_community_label
    ON blob_withholds (community_did, owner_did, cid)
    WHERE source = 'label' AND scope = 'community' AND lifted_at IS NULL;

-- A community label row refuses the CID for its owner only.
ALTER TABLE blob_withholds ADD CONSTRAINT blob_withholds_community_label_owner_check
    CHECK (source <> 'label' OR scope <> 'community' OR owner_did IS NOT NULL);

-- The label applier enumerates the records bound to a community.
CREATE INDEX idx_ap_objects_community_did
    ON ap_objects (community_did) WHERE community_did IS NOT NULL;

-- 'community': native work into a removed bridged community (a vote, or a
-- Create/Update with no takedown source of its own) waits for the community's
-- `!takedown` to be negated; the subject is the community DID.
ALTER TABLE outbound_deliveries DROP CONSTRAINT outbound_deliveries_held_reason_check;
ALTER TABLE outbound_deliveries ADD CONSTRAINT outbound_deliveries_held_reason_check
    CHECK (held_reason IN ('', 'object', 'parent', 'consent', 'pause', 'community'));

-- +goose Down
UPDATE outbound_deliveries
SET state = 'pending', held_reason = '', held_subject = ''
WHERE state = 'held' AND held_reason = 'community';
ALTER TABLE outbound_deliveries DROP CONSTRAINT outbound_deliveries_held_reason_check;
ALTER TABLE outbound_deliveries ADD CONSTRAINT outbound_deliveries_held_reason_check
    CHECK (held_reason IN ('', 'object', 'parent', 'consent', 'pause'));
DROP INDEX IF EXISTS idx_ap_objects_community_did;
DELETE FROM blob_withholds WHERE source = 'label' AND scope = 'community';
ALTER TABLE blob_withholds DROP CONSTRAINT IF EXISTS blob_withholds_community_label_owner_check;
DROP INDEX IF EXISTS blob_withholds_one_active_community_label;
