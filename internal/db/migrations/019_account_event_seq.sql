-- +goose Up
-- Task 14 (second-opinion C4): per-DID ordering guard for #account events.
--
-- #account and #identity frames go straight to their handlers UNGATED — the
-- rev gate (jetstream_record_revs) only covers commit events, which carry a
-- rev. But #account carries a monotonic per-DID `seq`, and the reconnect
-- rewind replays recent events on every reconnect. Without a guard a stale
-- pause (seq N) redelivered after a newer reactivation (seq N+1) flips a
-- recovered user back to paused and silently un-delivers them until the next
-- live event — the exact regression the rev gate prevents for commits.
--
-- The high-water mark lives as a COLUMN on ap_actors rather than a standalone
-- table: #account state is per-actor lifecycle (delivery_paused sits right
-- beside it), the gate only ever runs for a DID that already has an actor
-- (nothing was federated under one that does not, so there is nothing to
-- order), and keeping it on the actor row means the seq is dropped together
-- with the actor when a repo is scrubbed. A handler applies an event only when
-- its seq is strictly greater than last_account_seq; equal or lower is a
-- duplicate or a stale replay and is a no-op.
ALTER TABLE ap_actors ADD COLUMN last_account_seq BIGINT NOT NULL DEFAULT 0;

-- +goose Down
ALTER TABLE ap_actors DROP COLUMN IF EXISTS last_account_seq;
