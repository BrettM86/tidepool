-- +goose Up
-- Task 17c-1: bind bridge-origin objects to the community they were federated
-- into, so announced moderation of NATIVE content can be authorized at all.
--
-- materialize.CommunityDIDOf is the bridge-wide answer to "whose content is
-- this?", and for an author-owned postv2 it cannot be derived: the record lives
-- in the AUTHOR's repo, so `did` names the author, not the community. With the
-- column unset it answers "" and authorizeAnnouncedContentDelete refuses every
-- announced removal — the accident that has been standing in for authorization.
-- The enqueuer now carries community_did on the mapping it writes; this
-- backfills the rows written before it did.
--
-- The backfill is an EXACT JOIN, not a derivation. outbound_objects is the
-- bridge's own record of what it federated where, keyed by the same at-uri, and
-- it already carries community_did. Guessing (say, from the acceptance records
-- in each followed community) could bind a post to the wrong community, and a
-- wrong binding is worse than none: it would let one community's moderators
-- remove another community's content.
UPDATE ap_objects a
   SET community_did = o.community_did
  FROM outbound_objects o
 WHERE a.at_uri = o.at_uri
   AND a.origin = 'bridge'
   AND a.community_did IS NULL
   AND o.community_did <> '';

-- +goose Down
-- Deliberately NOT reversed. Clearing community_did would silently re-disable
-- moderation of native content — the refusal looks identical to "no moderator
-- acted" — and the values are recoverable from outbound_objects at any time by
-- re-running the statement above. A Down that re-creates a security-relevant
-- blind spot is worse than one that leaves a correct binding in place.
SELECT 1;
