-- +goose Up
-- Task 16: the acceptance engine's admission ledger.
--
-- Coves' post.getStatus reads a post's admission state from the
-- firehose-visible acceptance/removal records the engine writes — that is the
-- authoritative, cross-network answer. This table is the engine's OWN debug and
-- admin surface: it records, for every (community, post) the engine has decided
-- on, the machine-readable WHY of that decision (decision_code) and the state it
-- left the post in, so an operator can list pending/rejected admissions with
-- reasons and force a re-admit. It is NOT a watermark and it is NOT consulted on
-- the correctness path: losing it re-derives from a replay.
--
-- PK is (community_did, post_uri): one row per post per community. A post edited
-- and re-evaluated updates its row in place (evaluated_cid moves); it is not a
-- log.
CREATE TABLE admissions (
    community_did TEXT NOT NULL,                                 -- the bridged community the post was submitted to
    post_uri TEXT NOT NULL,                                      -- the postv2 at-uri (author repo)
    -- author_did is the postv2 author (the repo owner). It is what the
    -- per-author-per-community flood cap counts on: Tidepool must not let one
    -- native account flood a Lemmy community it vouches for, so the engine
    -- counts a (author_did, community_did) author's ACCEPTED rows against
    -- ADMISSION_MAX_PER_AUTHOR_PER_COMMUNITY. Denormalized from post_uri's
    -- authority segment so the cap is one indexed COUNT rather than a LIKE scan.
    author_did TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL
        CHECK (status IN ('pending', 'accepted', 'pending_reacceptance', 'rejected', 'removed')),
    -- decision_code is the machine-readable reason for the current status: ''
    -- for a clean accept, else the rejection/removal reason (opted-out,
    -- title-required, banned, community-gone, parent-locked, rate-limited,
    -- lexicon-invalid, …). Distinct codes are what the admin surface needs and
    -- the firehose records cannot carry.
    --
    -- 'accepted' WITH a code is legal, not a contradiction: a post accepted
    -- cleanly can later carry the cause of a refusal that deliberately left the
    -- acceptance standing — a banned author's edit is refused while the post the
    -- moderators chose not to remove stays up, and author-banned is recorded
    -- against a row that is still accepted (accept.RecordRefusal). status alone
    -- says whether the post is live; the code says what last happened to it.
    decision_code TEXT NOT NULL DEFAULT '',
    -- evaluated_cid is the post CID this decision was made AGAINST (decision 5.5:
    -- admission runs against the EVENT's CID). A later event with a different CID
    -- supersedes it — the digest rkey converges, and this column is how a replay
    -- or a concurrent engine tells "already decided this version" from "new
    -- content to re-run admission on".
    evaluated_cid TEXT NOT NULL DEFAULT '',
    -- acceptance_rkey / accepted_cid pin what the engine actually wrote when it
    -- accepted: the community-repo record key (SubjectRKey) and the CID the
    -- acceptance pinned. Empty on a rejection.
    acceptance_rkey TEXT NOT NULL DEFAULT '',
    accepted_cid TEXT NOT NULL DEFAULT '',
    -- redrivable marks a rejection an admin (or a transient-cause re-scan) may
    -- retry. A hard, permanent rejection (opted-out-history, lexicon-invalid) is
    -- NOT redrivable; a soft one (community temporarily gone) is.
    redrivable BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (community_did, post_uri)
);

-- The admin "list pending/rejected admissions" query filters by status; the
-- ledger is small (one row per bridged post) but the index keeps that listing
-- from scanning removed/accepted rows.
CREATE INDEX idx_admissions_status ON admissions (status) WHERE status IN ('pending', 'rejected');

-- The per-author-per-community flood cap counts accepted rows for one author in
-- one community. Leading (author_did, community_did) serves the WHERE; created_at
-- trails so a windowed variant of the cap stays index-only.
CREATE INDEX idx_admissions_author_community ON admissions (author_did, community_did, created_at);

-- +goose Down
DROP INDEX IF EXISTS idx_admissions_author_community;
DROP INDEX IF EXISTS idx_admissions_status;
DROP TABLE IF EXISTS admissions;
