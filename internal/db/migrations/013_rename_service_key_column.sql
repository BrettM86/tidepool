-- +goose Up
-- Task 11 housekeeping: private_key_pem lied for the "plc-rotation" row,
-- which holds AES-GCM sealed ciphertext (task 03), not PEM. key_material is
-- honest for both rows: plaintext PKCS#8 PEM for 'service-actor', sealed
-- ciphertext for 'plc-rotation' (the per-row encoding is documented on the
-- store model).
ALTER TABLE service_keys RENAME COLUMN private_key_pem TO key_material;

-- +goose Down
ALTER TABLE service_keys RENAME COLUMN key_material TO private_key_pem;
