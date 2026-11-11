-- +goose Up
-- Bridged community removal (chunk 05). A Coves `!takedown` label on a
-- bridged community's bare DID unfollows the community; while it stands no
-- follow path follows the community again. Each removal records the
-- community's follow state and follow provenance at the removal, which decide
-- whether the negation follows the community again. One removal is active per
-- community DID; the negation (or the label's lapse) clears it.
--
-- Keyed by the community DID with no reference to communities: a label can
-- name a community Tidepool has no row for yet, and a referencing table would
-- make every plain TRUNCATE of communities fail.
CREATE TABLE community_removal_suppressions (
    id BIGSERIAL PRIMARY KEY,
    community_did TEXT NOT NULL,
    labeler_did TEXT NOT NULL,
    follow_state_at_removal TEXT NOT NULL
        CHECK (follow_state_at_removal IN ('none', 'pending', 'accepted')),
    follow_provenance_at_removal TEXT
        CHECK (follow_provenance_at_removal IN ('follow-list', 'instance', 'post-walk', 'manual')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    cleared_at TIMESTAMPTZ
);

CREATE UNIQUE INDEX community_removal_suppressions_one_active
    ON community_removal_suppressions (community_did) WHERE cleared_at IS NULL;

-- How the community was last followed: an explicit follow-list entry, an
-- instance entry's listing, an instance entry's post walk, or a manual
-- POST /admin/communities. NULL (every row before this migration) is unknown
-- and treated as configured: the negation of a removal follows it again only
-- if the follow list still wants it.
ALTER TABLE communities ADD COLUMN follow_provenance TEXT
    CHECK (follow_provenance IN ('follow-list', 'instance', 'post-walk', 'manual'));

-- +goose Down
ALTER TABLE communities DROP COLUMN IF EXISTS follow_provenance;
DROP TABLE IF EXISTS community_removal_suppressions;
