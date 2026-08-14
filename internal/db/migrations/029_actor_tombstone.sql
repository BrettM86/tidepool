-- +goose Up
-- Task 17d: the DESTRUCTIVE tier's two reads.
--
-- 1. tombstoned_at — the actor document stops resolving.
--
-- ap_actors already had three lifecycle columns and none of them can express
-- this. `enabled` gates DISCOVERY: a disabled actor's local part stops resolving
-- through webfinger while its document keeps being served, deliberately, because
-- every Note and Page the bridge already delivered names that actor and a
-- dangling reference orphans the thread it hangs in. `delivery_paused` is about
-- outbound traffic and has no read-side meaning at all.
--
-- A withdrawal is the opposite decision, and it is the ONLY difference a peer
-- can observe between the two tiers: the document answers 410 GONE. Not 404 —
-- "never heard of them" reads as a lookup failure and invites the peer to try
-- again, while Gone is the statement that lets it stop asking and clean up.
--
-- It is a TIMESTAMP rather than a flag because when a withdrawal happened is the
-- question anyone asks afterwards, and because IRREVERSIBILITY is the point: no
-- code path clears this column. Peers that honour a Delete cannot restore what
-- they dropped, so nothing here promises resurrection.
ALTER TABLE ap_actors ADD COLUMN tombstoned_at TIMESTAMPTZ;

-- 2. The purge's vote query.
--
-- A purged actor's LIVE votes have to be retracted, and "live" is exactly
-- delivered_state = 'delivered' (task 17b: the reseed subtracts precisely those
-- from the origin's API tally, so a vote left standing for an actor that no
-- longer exists is a number the reseed keeps subtracting from a served score
-- forever). The lookup is by actor, which no index served: outbound_votes is
-- indexed for the subject-side reseed, not the actor-side purge.
--
-- PARTIAL on the same predicate the reader uses, so the index holds only the
-- rows a purge can act on — 17b's ruling that the subject index be partial, one
-- axis over.
CREATE INDEX outbound_votes_actor_delivered_idx
    ON outbound_votes (actor_did) WHERE delivered_state = 'delivered';

-- +goose Down
DROP INDEX IF EXISTS outbound_votes_actor_delivered_idx;
ALTER TABLE ap_actors DROP COLUMN IF EXISTS tombstoned_at;
