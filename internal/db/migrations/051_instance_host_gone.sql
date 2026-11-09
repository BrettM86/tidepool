-- +goose Up
-- Per-host host-gone streaks for backfill: when the host's current run of
-- host-gone fetch failures (NXDOMAIN of its own name, or an off-host redirect
-- to a parked page) began, and when the latest one happened. Both NULL means
-- no streak. Backfill skips a gone host's items only once its streak is old
-- enough, so a briefly lapsed domain is retried instead of dropped.
--
-- A host-gone failure can be the first thing recorded for a host, before any
-- nodeinfo discovery. fetched_at becomes nullable so such a row carries no
-- discovery time: the nodeinfo cache reads a NULL fetched_at as never
-- discovered (stale) and runs discovery, rather than trusting a fake fetch.
ALTER TABLE instances ALTER COLUMN fetched_at DROP NOT NULL;
ALTER TABLE instances ADD COLUMN host_gone_since TIMESTAMPTZ NULL;
ALTER TABLE instances ADD COLUMN host_gone_last_at TIMESTAMPTZ NULL;

-- +goose Down
ALTER TABLE instances DROP COLUMN IF EXISTS host_gone_last_at;
ALTER TABLE instances DROP COLUMN IF EXISTS host_gone_since;
-- Rows that only ever held a host-gone streak have no discovery state to keep.
DELETE FROM instances WHERE fetched_at IS NULL;
ALTER TABLE instances ALTER COLUMN fetched_at SET NOT NULL;
