-- +goose Up
-- Task 07: the vote side channel (PLAN.md locked decision 7 — votes never
-- become records). Lemmy Like/Dislike/Undo activities maintain bridge-side
-- aggregate counts served over social.coves.bridge.getVoteAggregates.
--
-- Two tables because AP vote delivery is a stream of per-voter state
-- changes, not increments: voters flip votes (Like → Undo{Like} → Dislike)
-- and instances re-deliver activities. Counts must reflect each distinct
-- voter's LATEST state, so vote_events tracks per-(voter, subject) state and
-- vote_aggregates holds the served totals, recomputed transactionally.

-- vote_aggregates: one row per voted-on bridged subject (post/comment).
-- upvotes/downvotes are the SERVED totals: seeded_* (a baseline imported
-- from the origin's public API during backfill — history whose individual
-- Like activities the bridge never saw) plus the live count of non-undone
-- vote_events. The aggregate row also serializes vote writes per subject:
-- every ApplyVote/RetractVote/Seed transaction locks it first, so the
-- recompute always sees a consistent event set.
CREATE TABLE vote_aggregates (
    subject_ap_id TEXT PRIMARY KEY CHECK (subject_ap_id <> ''),   -- canonical AP id of the voted-on object
    subject_at_uri TEXT NOT NULL CHECK (subject_at_uri <> ''),    -- the materialized record (ap_objects.at_uri)
    upvotes INTEGER NOT NULL DEFAULT 0 CHECK (upvotes >= 0),
    downvotes INTEGER NOT NULL DEFAULT 0 CHECK (downvotes >= 0),
    seeded_upvotes INTEGER NOT NULL DEFAULT 0 CHECK (seeded_upvotes >= 0),
    seeded_downvotes INTEGER NOT NULL DEFAULT 0 CHECK (seeded_downvotes >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- The XRPC queries by at-uri (that is what the AppView knows).
CREATE INDEX idx_vote_aggregates_at_uri ON vote_aggregates (subject_at_uri);

-- vote_events: one row per vote ACTIVITY (append-only). activity_id is the
-- dedupe key — a re-delivered Like is a no-op even if the queue's fencing
-- ever lets one through. Invariant maintained by the aggregator: at most one
-- non-undone row per (voter, subject) — that row IS the voter's current
-- vote; superseded and undone votes keep their rows with undone = TRUE, so
-- a stale activity id can never be re-applied.
CREATE TABLE vote_events (
    id BIGSERIAL PRIMARY KEY,
    activity_id TEXT NOT NULL CHECK (activity_id <> ''),          -- AP Like/Dislike activity id
    voter_ap_id TEXT NOT NULL CHECK (voter_ap_id <> ''),          -- AP actor id of the voter
    subject_ap_id TEXT NOT NULL CHECK (subject_ap_id <> ''),      -- AP id of the voted-on object
    direction TEXT NOT NULL CHECK (direction IN ('up', 'down')),
    undone BOOLEAN NOT NULL DEFAULT FALSE,                        -- superseded by a newer vote, or Undo{Like|Dislike}
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT vote_events_activity_id_key UNIQUE (activity_id)
);

-- Current-state lookup (supersede on flip, Undo targeting) and the
-- per-subject recompute both scan live events only.
CREATE INDEX idx_vote_events_live ON vote_events (subject_ap_id, voter_ap_id)
    WHERE NOT undone;

-- +goose Down
DROP INDEX IF EXISTS idx_vote_events_live;
DROP TABLE IF EXISTS vote_events;
DROP INDEX IF EXISTS idx_vote_aggregates_at_uri;
DROP TABLE IF EXISTS vote_aggregates;
