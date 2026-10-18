-- +goose Up
-- Per-host nodeinfo discovery results: the software an instance reports
-- (lowercased, NULL when no discovery has succeeded) and when discovery was
-- last attempted. A failed attempt advances fetched_at and keeps the stored
-- name, so failures are cached for the refresh interval like successes.
CREATE TABLE instances (
    host TEXT PRIMARY KEY,
    software_name TEXT NULL,
    fetched_at TIMESTAMPTZ NOT NULL
);

-- +goose Down
DROP TABLE IF EXISTS instances;
