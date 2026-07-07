-- +goose Up
-- communities tracks the AP groups the bridge follows (community
-- subscriptions) and their backfill progress.
-- Unique constraints carry explicit names so the store layer can map
-- SQLSTATE 23505 violations to precise conflict errors.
CREATE TABLE communities (
    id BIGSERIAL PRIMARY KEY,
    ap_group_id TEXT NOT NULL CHECK (ap_group_id <> ''),        -- canonical AP Group actor id (URL)
    did TEXT NOT NULL CHECK (did <> ''),                        -- the community's bridged repo DID
    preferred_username TEXT NOT NULL CHECK (preferred_username <> ''),
    instance TEXT NOT NULL CHECK (instance <> ''),              -- host, e.g. lemmy.world
    follow_state TEXT NOT NULL DEFAULT 'none'
        CHECK (follow_state IN ('none', 'pending', 'accepted')),
    followed_at TIMESTAMPTZ,                                    -- when the Follow was accepted
    last_backfill_at TIMESTAMPTZ,                               -- last completed outbox backfill
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT communities_ap_group_id_key UNIQUE (ap_group_id),
    CONSTRAINT communities_did_key UNIQUE (did)
);

CREATE INDEX idx_communities_instance ON communities (instance);
CREATE INDEX idx_communities_follow_state ON communities (follow_state);

-- +goose Down
DROP INDEX IF EXISTS idx_communities_follow_state;
DROP INDEX IF EXISTS idx_communities_instance;
DROP TABLE IF EXISTS communities;
