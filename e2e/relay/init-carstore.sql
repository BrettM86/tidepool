-- The BigSky relay wants TWO databases: its main BGS state (users, PDS
-- registry, slurp config) and the carstore's shard metadata. One postgres
-- container serves both; this init script (run once by the postgres image's
-- entrypoint on first boot) creates the second database next to the
-- POSTGRES_DB-created `bgs`. The CAR shard FILES live on the relay
-- container's own disk (DATA_DIR) — only the metadata is relational.
CREATE DATABASE carstore OWNER relay;
