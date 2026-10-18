-- +goose Up
-- Reserve handles alongside the actor row that owns them. Existing handles
-- remain protected by bridged_actors_handle_key; they are not backfilled.
CREATE TABLE handle_claims (
    handle TEXT PRIMARY KEY,
    actor_id BIGINT NOT NULL REFERENCES bridged_actors(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- +goose Down
DROP TABLE IF EXISTS handle_claims;
