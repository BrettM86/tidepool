-- +goose Up
-- ap_tombstones markers were keyed by ap_id alone, so every marker suppressed
-- its id GLOBALLY. Combined with authorizeDelete's deliberate allowance for an
-- UNMAPPED announced target (needed so the delete-before-create race can still
-- leave a marker), that made the table a cross-community suppression
-- primitive: any ONE followed community could announce Delete{<any IRI>} and
-- pre-suppress an id belonging to a DIFFERENT community for the whole
-- TOMBSTONE_RETENTION window (30d default), because materializeContent and the
-- backfill honoured the marker no matter who laid it.
--
-- announcer scopes the marker to the authority that laid it: the announcing
-- community's AP group id, or '' for an ORIGIN-authorized marker (a bare
-- same-authority Delete, or the admin sweep's origin-verified 410) which stays
-- global. Reads ask for markers visible in their own context — global, or laid
-- by the community whose announce is being processed — so community A's marker
-- can no longer suppress community B's content. The primary key moves to
-- (ap_id, announcer) so two communities' independent markers for the same id
-- coexist instead of the first one winning the ON CONFLICT.
--
-- Existing rows become announcer = '' (global). That is the conservative
-- reading of history: the old markers were laid by a mix of origin-authorized
-- and community-announced deletes that we can no longer tell apart, and
-- keeping them global preserves the suppression they already provide (a marker
-- that stops resurrecting deleted content is the safe direction to err in;
-- retention prunes them within 30 days anyway). It is also sound rather than
-- merely conservative: every row that exists in PRODUCTION was recorded under
-- the ORIGINAL same-authority-only delete rule, i.e. by the target id's own
-- host — the intermediate revision that let a followed community announce a
-- delete for an unmapped foreign id was never deployed. Promoting those rows
-- to global states what they already were.
ALTER TABLE ap_tombstones ADD COLUMN announcer TEXT NOT NULL DEFAULT '';

ALTER TABLE ap_tombstones DROP CONSTRAINT ap_tombstones_pkey;
ALTER TABLE ap_tombstones ADD PRIMARY KEY (ap_id, announcer);

-- +goose Down
-- Collapse back to one row per ap_id, keeping the OLDEST marker (its
-- deleted_at is what retention pruning reads, so keeping the oldest preserves
-- the original prune schedule rather than extending it).
DELETE FROM ap_tombstones a
    USING ap_tombstones b
    WHERE a.ap_id = b.ap_id
      AND (b.deleted_at, b.announcer) < (a.deleted_at, a.announcer);

ALTER TABLE ap_tombstones DROP CONSTRAINT ap_tombstones_pkey;
ALTER TABLE ap_tombstones ADD PRIMARY KEY (ap_id);
ALTER TABLE ap_tombstones DROP COLUMN IF EXISTS announcer;
