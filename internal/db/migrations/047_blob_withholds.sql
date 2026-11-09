-- +goose Up
-- Blob withholds: per-source refusals to store or serve a blob CID. Every
-- source (operator now; community-removal and label later) writes rows here,
-- and the single "may (DID, CID) be stored or served" check reads every
-- active row (lifted_at IS NULL) by source and scope. cid is the canonical
-- CIDv1 base32 string, the same form the blobs table stores.
CREATE TABLE blob_withholds (
    id BIGSERIAL PRIMARY KEY,
    cid TEXT NOT NULL,
    source TEXT NOT NULL
        CHECK (source IN ('operator', 'community-removal', 'label')),
    -- every-owner: any DID; subject: owner_did, for the record subject_uri;
    -- owner: owner_did; community: blobs used under community_did.
    scope TEXT NOT NULL
        CHECK (scope IN ('every-owner', 'subject', 'owner', 'community')),
    owner_did TEXT NULL,
    subject_uri TEXT NULL,
    community_did TEXT NULL,
    reason_code TEXT NOT NULL
        CHECK (reason_code IN ('illegal-content', 'csam', 'operator-discretion', 'community-removal')),
    moderation_action_id TEXT NULL,
    csam_case_id TEXT NULL,
    -- Operator-supplied actor labels, each set only by its own action.
    created_by TEXT NULL,
    lifted_by TEXT NULL,
    bytes_deleted_by TEXT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    lifted_at TIMESTAMPTZ NULL,
    deleted_at TIMESTAMPTZ NULL,
    CHECK (scope NOT IN ('subject', 'owner') OR owner_did IS NOT NULL),
    CHECK (scope <> 'subject' OR subject_uri IS NOT NULL),
    CHECK (scope <> 'community' OR community_did IS NOT NULL),
    CHECK (source <> 'operator' OR reason_code IN ('illegal-content', 'csam', 'operator-discretion')),
    CHECK (lifted_at IS NULL OR lifted_by IS NOT NULL OR source <> 'operator'),
    CHECK (deleted_at IS NULL OR bytes_deleted_by IS NOT NULL OR source <> 'operator')
);

-- At most one active operator withhold per CID: withhold is idempotent.
CREATE UNIQUE INDEX blob_withholds_one_active_operator
    ON blob_withholds (cid) WHERE source = 'operator' AND lifted_at IS NULL;

-- The store/serve check looks up active rows by CID.
CREATE INDEX blob_withholds_active_cid
    ON blob_withholds (cid) WHERE lifted_at IS NULL;

-- +goose Down
DROP TABLE IF EXISTS blob_withholds;
