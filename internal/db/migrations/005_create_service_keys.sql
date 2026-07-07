-- +goose Up
-- service_keys persists the bridge's own long-lived key material, keyed by
-- purpose name. Today it holds one row: the service actor's AP-side RSA
-- private key (PEM, PKCS#8) under the name 'service-actor'. This is the
-- bridge's OWN key, not escrowed user key material (that lives on
-- bridged_actors.signing_key), so it is stored unencrypted like any other
-- service credential.
-- Unique constraints carry explicit names so the store layer can map
-- SQLSTATE 23505 violations to precise conflict errors.
CREATE TABLE service_keys (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL CHECK (name <> ''),                      -- key purpose, e.g. service-actor
    private_key_pem BYTEA NOT NULL CHECK (length(private_key_pem) > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT service_keys_name_key UNIQUE (name)
);

-- +goose Down
DROP TABLE IF EXISTS service_keys;
