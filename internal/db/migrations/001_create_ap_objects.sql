-- +goose Up
-- ap_objects is the spine of the bridge: the bidirectional mapping between
-- ActivityPub object IDs and the atproto records they were materialized as.
-- Every materialization writes a row; every strongRef resolution reads one.
-- Unique constraints carry explicit names so the store layer can map
-- SQLSTATE 23505 violations to precise conflict errors.
CREATE TABLE ap_objects (
    id BIGSERIAL PRIMARY KEY,
    ap_id TEXT NOT NULL CHECK (ap_id <> ''),                  -- canonical AP object id (URL)
    ap_type TEXT NOT NULL CHECK (ap_type <> ''),              -- AP type: Page, Note, Group, Person, ...
    origin_instance TEXT NOT NULL CHECK (origin_instance <> ''), -- host the object originated from, e.g. lemmy.world
    origin TEXT NOT NULL DEFAULT 'fediverse'
        CHECK (origin IN ('fediverse', 'bridge')),            -- which side authored the object (echo suppression, task 06)
    did TEXT NOT NULL CHECK (did <> ''),                      -- repo the record was written into
    collection TEXT NOT NULL CHECK (collection <> ''),        -- record NSID, e.g. social.coves.community.post
    rkey TEXT NOT NULL CHECK (rkey <> ''),                    -- deterministic TID rkey
    at_uri TEXT NOT NULL CHECK (at_uri <> ''),                -- at://did/collection/rkey
    cid TEXT NOT NULL CHECK (cid <> ''),                      -- CID of the current record version
    ap_published_at TIMESTAMPTZ,                              -- AP `published` time (may be absent upstream)
    indexed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMPTZ,                                   -- soft delete (AP Delete / Tombstone)
    CONSTRAINT ap_objects_ap_id_key UNIQUE (ap_id),
    CONSTRAINT ap_objects_at_uri_key UNIQUE (at_uri)
);

CREATE INDEX idx_ap_objects_did_collection ON ap_objects (did, collection);
CREATE INDEX idx_ap_objects_origin_instance ON ap_objects (origin_instance);

-- +goose Down
DROP INDEX IF EXISTS idx_ap_objects_origin_instance;
DROP INDEX IF EXISTS idx_ap_objects_did_collection;
DROP TABLE IF EXISTS ap_objects;
