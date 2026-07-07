-- +goose Up
-- Task 05: blob storage + record-author tracking for the materializer.

-- blobs holds binary media (avatars, banners, post images) referenced by
-- records via atproto blob refs. Keyed per-DID like blocks: a blob belongs
-- to the repo whose records reference it, and com.atproto.sync.getBlob
-- serves it by (did, cid). Content-addressed and immutable; re-fetching the
-- same bytes is an idempotent no-op. No GC in v1 (same policy as blocks).
CREATE TABLE blobs (
    did TEXT NOT NULL CHECK (did <> ''),
    cid TEXT NOT NULL CHECK (cid <> ''),
    mime_type TEXT NOT NULL CHECK (mime_type <> ''),
    size BIGINT NOT NULL CHECK (size >= 0),
    bytes BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT blobs_pkey PRIMARY KEY (did, cid)
);

-- author_did records which bridged actor authored the materialized record.
-- It differs from `did` (the repo the record lives in) for posts, which are
-- written into the COMMUNITY's repo with author = the bridged user's DID.
-- Delete(Actor) enumerates rows by author_did OR did to tombstone
-- everything the actor authored (their comments and profile in their own
-- repo, their posts in community repos) — without it, a deleted user's
-- posts would be unfindable. Nullable: rows written before task 05 (none
-- in practice) and non-record mappings simply have no author.
ALTER TABLE ap_objects ADD COLUMN author_did TEXT;

CREATE INDEX idx_ap_objects_author_did ON ap_objects (author_did)
    WHERE author_did IS NOT NULL;

-- +goose Down
DROP INDEX IF EXISTS idx_ap_objects_author_did;
ALTER TABLE ap_objects DROP COLUMN IF EXISTS author_did;
DROP TABLE IF EXISTS blobs;
