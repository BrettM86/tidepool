-- +goose Up
-- Task 17c-2: the moderation state THE BRIDGE OWNS.
--
-- A lock is the first moderation decision with no home in either repo. A
-- community's removal of a post is a record in that community's own repo
-- (social.coves.community.removal, written in ONE commit with the acceptance it
-- replaces); the post itself is the author's and is never touched; the
-- acceptance says only that the post was admitted. Lemmy keeps `locked` on the
-- post row, and atproto has no record whose meaning is "this thread is closed".
-- So the bridge holds it, here — and holding it is worth nothing unless
-- something READS it, which is the point: the next comment does not go out.
--
-- KEYED BY AT-URI, AND DELIBERATELY NOT A COLUMN ON ap_objects. putMapping's
-- ON CONFLICT DO UPDATE rewrites the whole mapping row, so any re-pin — a
-- re-materialization, a restore, an author edit — would silently CLEAR a lock,
-- and the symptom (comments start federating again) is indistinguishable from
-- the moderators having lifted it. A separate row survives everything that
-- rewrites a mapping.
--
-- The AT-URI is the key because it is the handle BOTH readers hold: the comment
-- consumer resolves a parent at-uri and never sees an AP id, and the announced
-- moderation path holds a mapping, which carries both. Locks apply to
-- FEDIVERSE-origin posts too — a Lemmy post a native user replies to can be
-- locked exactly like a native one — and those have an at-uri from the moment
-- they are materialized.
CREATE TABLE object_moderation (
    at_uri TEXT PRIMARY KEY,
    -- ap_id is the same object's fediverse id, denormalized so the row can be
    -- read back from either side of the bridge without a join through
    -- ap_objects (whose row is the very thing this table must not depend on).
    --
    -- FORWARD-LOOKING, and honestly so: every write supplies it and NOTHING
    -- reads it today. It is kept because the readers this table is heading
    -- towards arrive holding an AP id and not an at-uri — the admin moderation
    -- surface, and 17d's delivery-side ban checks, whose OrderingKey is an AP
    -- group id — and because a column backfilled later can only be backfilled
    -- from the mapping row this table exists to be independent of.
    ap_id TEXT NOT NULL,
    -- community_did BINDS the decision to the community that made it. Unbound,
    -- any co-hosted community's Undo{Lock} could clear a decision it did not
    -- make — and Lemmy hosts many communities per instance by design, so that
    -- is ordinary traffic rather than a threat model.
    community_did TEXT NOT NULL,
    -- locked_at IS the lock: NULL means open. Undo{Lock} clears it instead of
    -- deleting the row, because the removal state below shares the row.
    locked_at TIMESTAMPTZ,
    -- COMMENTS ONLY. A post's removal state lives in the community repo's
    -- removal record, which acceptrec writes atomically with the withdrawal of
    -- the acceptance; duplicating it here would create two sources of truth for
    -- one decision, and they would disagree the first time the commit succeeded
    -- and this write did not. Comments have no such record yet (the vendored
    -- removal lexicon is post-scoped), which is why the columns exist at all.
    removed_at TIMESTAMPTZ,
    removal_code TEXT NOT NULL DEFAULT '',
    removal_reason TEXT NOT NULL DEFAULT '',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- NO INDEX, deliberately, and here is the one query that will eventually want
-- one: CommunityHoldsAnyLock filters (community_did) WHERE locked_at IS NOT
-- NULL. It runs ONLY where a comment's thread could not be determined at all —
-- a cold branch over a table holding one row per moderated object — so a partial
-- index today would cost every lock write to serve a scan of a handful of rows.
-- If that branch ever warms (a backlog of pre-026 rows, a bulk repair), the
-- index to add is exactly:
--   CREATE INDEX object_moderation_locked_community_idx
--       ON object_moderation (community_did) WHERE locked_at IS NOT NULL;
-- Every other read is by at_uri, which the primary key already serves.

-- +goose Down
DROP TABLE IF EXISTS object_moderation;
