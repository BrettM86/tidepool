-- +goose Up
-- Task 17b: the served vote aggregate is the FEDIVERSE-ONLY tally.
--
-- Coves keeps native and bridged counts in separate columns, so the votes
-- Tidepool wrote back on behalf of native users must come OUT of what the
-- bridge serves — otherwise one person's single vote is counted twice in the
-- UI, once in each column. SeedAggregates now nets delivered outbound_votes out
-- of the baseline alongside live vote_events.
--
-- The index is PARTIAL on the predicate the seeder uses. Not
-- (subject_ap_id, delivered_state): task 17d wants (actor_did) WHERE
-- delivered_state = 'delivered', a different index, and a composite here would
-- serve neither well while costing writes on the delivery worker's hot update.
CREATE INDEX outbound_votes_subject_delivered_idx
    ON outbound_votes (subject_ap_id)
    WHERE delivered_state = 'delivered';

-- One-time cleanup: vote_events rows cast by OUR OWN personas, plus a recompute
-- of ONLY the subjects that cleanup touches.
--
-- Task 17a's voter probe makes such a row unwritable — Aggregator.ApplyVote
-- refuses a voter that resolves to an ap_actors persona before it touches any
-- table — so any row matching this predicate predates the guard and is
-- unconditionally garbage. Write-back has never been deployed, so production
-- has zero of them; this exists so a development database cannot carry one into
-- the new accounting.
--
-- It is deliberately NOT a filter in the seeder's live subquery. Any such
-- filter would have to be MIRRORED in recomputeAggregate, which reads the same
-- rows on every inbound vote: filtering in one place only yields
-- seeded = api − inbound_filtered and then served = seeded + inbound_unfiltered,
-- wrong by exactly the rows the filter excluded, on every subject those rows
-- touch.
--
-- The predicate is EXACT-ID equality, which is deliberately NARROWER than the
-- runtime probe it backfills: echo.identifyActor matches an actor route on a
-- normalized host plus scheme, so a legacy row spelled non-canonically
-- (explicit :443, trailing dot, differing case) is "ours" to the probe and
-- survives this DELETE — and would then be subtracted twice, once as a live
-- inbound row and once as a delivered outbound one. Accepted because the set is
-- empty in production and non-canonical ids were never written by any code path
-- here; a normalizing DELETE would have to re-implement the probe in SQL.
--
-- The recompute is SCOPED to the affected subjects, for two reasons beyond
-- cost: an unqualified UPDATE row-locks every aggregate (blocking vote
-- ingestion for its duration), and it stamps updated_at on every row — which
-- migration 014's stats watermark reads as "due" (updated_at > stats_emitted_at)
-- and turns into a full re-emit sweep of record rewrites and firehose traffic
-- for every voted-on subject in the system. Scoped, this is a true no-op when
-- there is nothing to clean, and re-running it re-triggers nothing.
--
-- The anti-join inside the counts is NOT redundant with the DELETE: every
-- sub-statement of a WITH sees the SAME snapshot, so the recompute cannot
-- observe the deletion above and must exclude the same rows itself.
WITH scrubbed AS (
    DELETE FROM vote_events
    WHERE voter_ap_id IN (SELECT actor_id FROM ap_actors)
    RETURNING subject_ap_id
)
UPDATE vote_aggregates a
SET upvotes = a.seeded_upvotes + (
        SELECT COUNT(*) FROM vote_events e
        WHERE e.subject_ap_id = a.subject_ap_id AND NOT e.undone AND e.direction = 'up'
          AND e.voter_ap_id NOT IN (SELECT actor_id FROM ap_actors)),
    downvotes = a.seeded_downvotes + (
        SELECT COUNT(*) FROM vote_events e
        WHERE e.subject_ap_id = a.subject_ap_id AND NOT e.undone AND e.direction = 'down'
          AND e.voter_ap_id NOT IN (SELECT actor_id FROM ap_actors)),
    updated_at = clock_timestamp()
WHERE a.subject_ap_id IN (SELECT subject_ap_id FROM scrubbed);

-- +goose Down
-- The deleted persona rows are not restorable (they were garbage), and the
-- scoped recompute is idempotent, so the index is all there is to drop.
--
-- But this Down is NOT a full reversal, and the residue is silent: every
-- baseline seeded while 023 was applied was stored NET of our delivered
-- outbound votes, and the pre-023 code does not re-derive baselines. Nothing
-- re-seeds periodically either (SeedAggregates' only caller is the backfill's
-- post walk, behind SEED_COUNTS_FROM_API and a freshness window), so after a
-- Down those subjects serve totals understated by our own votes INDEFINITELY —
-- not until the next backfill. Re-seeding the affected subjects with a forced
-- backfill is the only correction.
DROP INDEX IF EXISTS outbound_votes_subject_delivered_idx;
