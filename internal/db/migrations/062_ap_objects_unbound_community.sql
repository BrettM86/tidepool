-- +goose Up
-- A community label resolves every live bridged record with no stored
-- community through its record (materialize.bindUnboundRecordsTx) while it
-- holds the repo commit lock. This partial index keeps that read to the
-- unbound rows instead of a scan of ap_objects.
CREATE INDEX idx_ap_objects_unbound_community
    ON ap_objects (at_uri)
    WHERE community_did IS NULL AND deleted_at IS NULL;

-- +goose Down
DROP INDEX IF EXISTS idx_ap_objects_unbound_community;
