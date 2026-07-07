-- +goose Up
-- bridged_actors registers every AP actor (person or group) that Tidepool
-- has minted an atproto identity for, including the escrowed signing key.
-- NOTE: signing_key holds AES-GCM-encrypted secp256k1 key material once
-- task 03 lands; the column is bytea so encryption needs no schema change.
-- Unique constraints carry explicit names so the store layer can map
-- SQLSTATE 23505 violations to precise conflict errors.
CREATE TABLE bridged_actors (
    id BIGSERIAL PRIMARY KEY,
    ap_actor_id TEXT NOT NULL CHECK (ap_actor_id <> ''),        -- canonical AP actor id (URL)
    actor_type TEXT NOT NULL CHECK (actor_type IN ('person', 'group')),
    did TEXT NOT NULL CHECK (did <> ''),
    handle TEXT,                                                -- bridged handle, e.g. user.lemmy-world.tidepool.example
    signing_key BYTEA,                                          -- escrowed signing key (AES-GCM ciphertext from task 03 on)
    consent_state TEXT NOT NULL DEFAULT 'ok'
        CHECK (consent_state IN ('ok', 'nobridge', 'deleted')),
    profile_synced_at TIMESTAMPTZ,                              -- last time the profile record was (re)materialized
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT bridged_actors_ap_actor_id_key UNIQUE (ap_actor_id),
    CONSTRAINT bridged_actors_did_key UNIQUE (did)
);

-- atproto handles are 1:1 with DIDs; NULL means "not yet assigned" and is
-- allowed to repeat, hence the partial index.
CREATE UNIQUE INDEX bridged_actors_handle_key ON bridged_actors (handle) WHERE handle IS NOT NULL;

CREATE INDEX idx_bridged_actors_consent_state ON bridged_actors (consent_state);

-- +goose Down
DROP INDEX IF EXISTS idx_bridged_actors_consent_state;
DROP INDEX IF EXISTS bridged_actors_handle_key;
DROP TABLE IF EXISTS bridged_actors;
