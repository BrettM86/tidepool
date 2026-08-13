-- +goose Up
-- ap_actors is the other direction from bridged_actors: not fediverse actors
-- given atproto identities, but COVES users given ActivityPub ones (task 13).
-- One Person actor per DID, so the DID is the primary key rather than a
-- surrogate id — there is no second row to point at.
--
-- actor_id stores the FULL actor URL, origin included, because the origin is
-- not a constant: vanity origins (decision 10) let a deployment serve actors
-- on hosts other than AP_USER_ORIGIN, and every derived URL — webfinger href,
-- HTTP-signature keyId, the signing identity itself — must come from what was
-- minted, not from whatever the config says at read time.
--
-- (normalized_origin, local_part) is the webfinger lookup key and is unique
-- TOGETHER, not globally: alice@coves.social and alice@vanity.example are two
-- different people, and a global unique on local_part would make the first
-- vanity origin to mint an alice permanently own the name everywhere. The
-- composite constraint is NAMED EXPLICITLY: postgres would default it to
-- ap_actors_normalized_origin_local_part_key, and the store's 23505 →
-- ConflictError mapping switches on the name.
--
-- rsa_key_sealed is the AP signing key sealed under BRIDGE_KEK and AAD-bound
-- to the DID (identity.Custodian's RSA surface). It is BYTEA and never PEM:
-- the private half exists in postgres only as ciphertext. rsa_key_version
-- makes rotation definable without a schema change. public_key_pem is the
-- published SPKI half, which is public by construction.
--
-- kind is CHECKed to person|group even though nothing mints a group yet:
-- group actors are reserved for Scope B, so until a code path exists the
-- constraint is the only thing between a typo and a garbage actor kind.
--
-- Lifecycle is three columns, not one state machine: enabled gates webfinger
-- resolution and is stamped on both transitions, while delivery_paused is the
-- transient #account state (decision 19) where delivery stops but the
-- identity survives. They are independent — a paused actor is still enabled.
-- Federation is default-on (decision 11), hence enabled DEFAULT TRUE.
--
-- display_name/summary/avatar_url are a cache of the user's atproto profile
-- (task 14 owns the sync), NOT identity: local_part is frozen at creation, so
-- a rename refreshes these and nothing else.
CREATE TABLE ap_actors (
    did TEXT PRIMARY KEY,
    kind TEXT NOT NULL CHECK (kind IN ('person', 'group')),
    actor_id TEXT NOT NULL,                                     -- full actor URL, origin included
    normalized_origin TEXT NOT NULL,                            -- scheme-less lowercase host, as Host routing yields
    local_part TEXT NOT NULL,                                   -- frozen at creation
    rsa_key_sealed BYTEA NOT NULL,                              -- KEK-sealed, AAD-bound to did
    rsa_key_version INT NOT NULL,
    public_key_pem TEXT NOT NULL,
    enabled BOOL NOT NULL DEFAULT TRUE,
    enabled_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    disabled_at TIMESTAMPTZ,
    delivery_paused BOOL NOT NULL DEFAULT FALSE,
    display_name TEXT NOT NULL DEFAULT '',
    summary TEXT NOT NULL DEFAULT '',
    avatar_url TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT ap_actors_actor_id_key UNIQUE (actor_id),
    CONSTRAINT ap_actors_origin_local_part_key UNIQUE (normalized_origin, local_part)
);

-- +goose Down
DROP TABLE IF EXISTS ap_actors;
