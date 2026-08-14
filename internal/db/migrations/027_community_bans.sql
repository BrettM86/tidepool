-- +goose Up
-- Task 17c-3: a community's ban of one author.
--
-- A ban is an INTERSECTION, and the schema is that intersection: this author, in
-- this community. The two facts the codebase already had are each one dimension
-- short — CancelForActor stops an author everywhere (one community bans them and
-- every other community they write to stops receiving their posts), and
-- CancelForCommunity stops everyone (one user is banned and the community goes
-- dark) — so the primary key is the pair, and every read carries both halves.
CREATE TABLE community_bans (
    community_did TEXT NOT NULL,                                 -- the bridged community that issued the ban
    subject_did TEXT NOT NULL,                                   -- the native author it excludes
    -- community_ap_id is DENORMALIZED, and the honest status is: ONE reader
    -- today, and it is this table's own write path. Ban() cancels the author's
    -- pending deliveries, which are keyed by ordering_key — the AP group id, not
    -- the DID — so the column is what makes the cancellation expressible in the
    -- same statement as the row.
    --
    -- It is kept for a reader that does not exist yet, deliberately: a
    -- delivery-side ban recheck (claim time, where the worker holds ActorDID and
    -- OrderingKey and nothing else) is the last line of defence against work
    -- queued in the window before a ban lands. That reader would need an index on
    -- (subject_did, community_ap_id); none exists, because nothing reads it that
    -- way yet. It cannot drift meanwhile — the DID↔group-id mapping is immutable
    -- 1:1 (store.Communities rejects an upsert that moves either).
    community_ap_id TEXT NOT NULL,
    banned_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- expires_at is MANDATORY to honour, not optional to store. Lemmy's
    -- BlockUser carries `expires` for a temporary ban and then sends NOTHING
    -- when it lapses — no Undo, no second activity — because it expires locally
    -- on their side. Ignore this column and a moderator who chose three days has
    -- excluded that author forever, with no message that could ever clear it.
    -- NULL means permanent. EVERY read is:
    --     expires_at IS NULL OR expires_at > now()
    expires_at TIMESTAMPTZ,
    reason TEXT NOT NULL DEFAULT '',
    -- remove_data records what the moderator asked for, not what we did with it.
    -- The content removal happens once, when the ban lands; keeping the flag is
    -- what lets an operator answer "was their content purged too?" afterwards —
    -- and it is deliberately NOT read by the Undo, because Lemmy models
    -- restoration as a SEPARATE restore_data flag and an unban that quietly
    -- republished removed posts would reverse a decision nobody reversed.
    remove_data BOOLEAN NOT NULL DEFAULT false,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (community_did, subject_did)
);

-- No secondary index. Both readers arrive holding BOTH halves of the primary
-- key — the admission gate asks about one author in one community, and the ban
-- write scopes its delivery cancellation by (subject_did, community_ap_id) it
-- already has. Nothing lists bans by community or by author yet; when the admin
-- surface does, the index it wants is (subject_did) for "where is this user
-- banned?", which the PK's leading column cannot serve.

-- +goose Down
DROP TABLE IF EXISTS community_bans;
