-- +goose Up
-- One post_uri, one community — enforced by the schema instead of by belief.
--
-- Migration 021 keys this table (community_did, post_uri), which says "one row
-- per post PER COMMUNITY". But a postv2 is bound to ONE community forever (the
-- lexicon marks `community` immutable), and the engine reads that binding back
-- by post_uri ALONE: GetByPostURI, boundCommunityOf, and Readmit are all
-- single-row queries over `WHERE post_uri = $1` with no ORDER BY. Two rows for
-- one post_uri does not surface as an error anywhere — it surfaces as postgres
-- picking a winner, which means the community a post is "bound" to can change
-- between two reads of the same ledger.
--
-- The invariant was asserted in the H3 tests and in this table's own comments
-- and enforced nowhere, so the one writer that violated it (decide() judging a
-- record's merit BEFORE refusing a community move, filing the resulting verdict
-- under the TARGET community) did so silently. That ordering is fixed in
-- internal/accept/engine.go; this index is what makes the next such writer fail
-- loudly at the INSERT instead of corrupting the binding.
--
-- It also makes the ledger's own upsert honest: record() says
-- ON CONFLICT (community_did, post_uri) DO UPDATE, which quietly turns a
-- cross-community duplicate into an INSERT. With this index that insert raises
-- a unique violation and the caller sees it.

-- Any pre-existing duplicate is the bug's residue, and it must be cleared before
-- the index can exist. The row KEPT is the one the post is actually bound to:
-- the community that holds the post's outbound_objects row (the accepted
-- binding) if there is one, else the oldest decision — which is the community
-- that decided first, and therefore the one a later moving edit was refused
-- into. The discarded rows are, by construction, decisions written about a
-- community that never accepted this post.
DELETE FROM admissions a
      WHERE a.community_did <> (
            SELECT keep.community_did
              FROM (
                    SELECT DISTINCT ON (candidate.post_uri)
                           candidate.post_uri, candidate.community_did
                      FROM admissions candidate
                      LEFT JOIN outbound_objects o
                             ON o.at_uri = candidate.post_uri
                            AND o.community_did = candidate.community_did
                     ORDER BY candidate.post_uri,
                              (o.at_uri IS NOT NULL) DESC,
                              candidate.created_at,
                              candidate.community_did
                   ) keep
             WHERE keep.post_uri = a.post_uri
      );

CREATE UNIQUE INDEX idx_admissions_post_uri ON admissions (post_uri);

-- +goose Down
-- The index goes; the rows deleted above do NOT come back. They were decisions
-- recorded about a community that never accepted the post, and re-inserting them
-- is neither possible nor desirable — the ledger is a debug/admin surface that
-- re-derives from a replay.
DROP INDEX IF EXISTS idx_admissions_post_uri;
