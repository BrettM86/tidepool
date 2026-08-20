-- +goose Up
-- Announced-delete authorization (ingest.authorizeDelete) and announced-vote
-- binding (votes.subjectBelongsToCommunity) both answer ONE question: which
-- community's content is this? Until the author-owned flip (PLAN.md decision
-- 20) the answer was free — posts committed into the community's own repo, so
-- the mapping's did WAS the community did, and a comment borrowed the answer
-- from its thread root's repo. After the flip a post is a postv2 in the
-- AUTHOR's repo and matches no community did at all, so both checks compare
-- unequal for every legitimate announce and fail in the quiet direction:
-- deletes refused, votes dropped, counts simply stop moving. Nothing errors.
--
-- community_did records the answer at materialization time instead of
-- re-deriving it from repo placement afterwards. Nullable because it cannot be
-- reconstructed for every historical row: legacy POSTS backfill exactly (their
-- repo is the community), but legacy COMMENTS never carried it and their
-- answer lives in the thread root's record, not in postgres. Those stay NULL
-- and the readers keep their existing record-walking derivation as the
-- fallback — which is also what covers postv2 rows written between the flip
-- landing and this migration, whose community is in the record's `community`
-- field where SQL cannot reach it.
--
-- No index: every reader arrives holding the mapping it already fetched by
-- ap_id and compares this column in memory. Nothing queries BY community_did.
ALTER TABLE ap_objects ADD COLUMN community_did TEXT;

UPDATE ap_objects
    SET community_did = did
    WHERE collection = 'social.coves.community.post';

-- +goose Down
ALTER TABLE ap_objects DROP COLUMN IF EXISTS community_did;
