-- +goose Up
-- Backfill items skipped because their origin host was dead, kept so a later
-- sweep retries them on a schedule measured from created_at (the first skip).
-- One row per object per community. object_type is the item's embedded AP
-- type ('' when unknown). A host's recovery (its host-gone streak cleared)
-- makes its rows due at once. No foreign key: a row outlives its community
-- until the sweep reads it and drops it.
CREATE TABLE backfill_deferred_items (
    object_iri      TEXT        NOT NULL,
    community_ap_id TEXT        NOT NULL,
    community_did   TEXT        NOT NULL,
    host            TEXT        NOT NULL,
    object_type     TEXT        NOT NULL DEFAULT '',
    attempts        INTEGER     NOT NULL DEFAULT 0,
    next_retry_at   TIMESTAMPTZ NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (object_iri, community_ap_id)
);
CREATE INDEX backfill_deferred_items_next_retry_at_idx ON backfill_deferred_items (next_retry_at);
CREATE INDEX backfill_deferred_items_host_idx ON backfill_deferred_items (host);

-- +goose Down
DROP TABLE IF EXISTS backfill_deferred_items;
