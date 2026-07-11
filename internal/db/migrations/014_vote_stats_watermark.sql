-- +goose Up
-- Bridged-vote-stats emission (FOLLOWUPS "Votes-as-records", locked decision
-- 7 final direction): bridged vote counts ride the CONTENT record as an
-- optional bridgedStats field, refreshed by a debounced sweeper as
-- vote_aggregates rows change. stats_emitted_at is that sweeper's per-row
-- watermark: the vote_aggregates.updated_at value the refresher last emitted
-- onto the materialized record. A row is DUE when updated_at > stats_emitted_at
-- (a vote landed since the last emit) or stats_emitted_at IS NULL (never
-- emitted). Nullable — every existing aggregate is born un-emitted, so the
-- first sweep after this migration emits every row's current counts.
--
-- The watermark is advanced to the updated_at READ during the sweep (never
-- now()) on EMIT or on a deliberate PERMANENT SKIP (missing/soft-deleted
-- mapping, record gone, consent-frozen repo, persistently invalid record) — so
-- a subject that can never be stamped stops being reconsidered every sweep,
-- while a transient failure leaves the row dirty for retry. A vote landing
-- mid-sweep bumps updated_at past the watermark (via clock_timestamp() in the
-- recompute) and re-dirties the row for the next sweep — no lost update.
ALTER TABLE vote_aggregates ADD COLUMN stats_emitted_at TIMESTAMPTZ;

-- The sweep selects due rows ordered by updated_at; a partial index over the
-- un-emitted watermark keeps that scan bounded to the actually-dirty rows
-- rather than the whole table once steady state is reached.
CREATE INDEX idx_vote_aggregates_stats_due ON vote_aggregates (updated_at)
    WHERE stats_emitted_at IS NULL OR updated_at > stats_emitted_at;

-- +goose Down
DROP INDEX IF EXISTS idx_vote_aggregates_stats_due;
ALTER TABLE vote_aggregates DROP COLUMN IF EXISTS stats_emitted_at;
