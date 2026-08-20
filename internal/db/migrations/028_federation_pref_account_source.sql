-- +goose Up
-- Task 17d: a third provenance for a federation preference.
--
-- federation_prefs.source answers "how do we know this?", and the column exists
-- precisely because the answers have different staleness (see migration 018).
-- Until now there were two: a social.coves.bridge.federation RECORD the consumer
-- saw, and a PROBE where the bridge fetched that record itself.
--
-- The terminal tier (decision 19) writes a third. When a #account event says a
-- repo is deleted, the bridge CONFIRMS it against the identity's own sources —
-- the PLC directory, then the PDS that document names — and records the result.
-- That is not a federation record at all: the user never wrote one, and there is
-- no record left to fetch. Filing it under 'probe' would claim we read a
-- preference they expressed, when what we read is that their account is gone —
-- and this column is the one place an operator can tell a user who OPTED OUT
-- from a user who was DELETED, which is the difference between a decision they
-- can reverse and one they cannot.
ALTER TABLE federation_prefs DROP CONSTRAINT IF EXISTS federation_prefs_source_check;
ALTER TABLE federation_prefs ADD CONSTRAINT federation_prefs_source_check
    CHECK (source IN ('record', 'probe', 'account'));

-- +goose Down
-- Rows written by the terminal tier are rewritten to 'probe' rather than
-- deleted: the PREFERENCE is what stops the bridge federating for a deleted
-- account, so dropping the rows to satisfy a narrower constraint would resume
-- federating on their behalf. Losing the provenance is recoverable; losing the
-- preference is not.
UPDATE federation_prefs SET source = 'probe' WHERE source = 'account';
ALTER TABLE federation_prefs DROP CONSTRAINT IF EXISTS federation_prefs_source_check;
ALTER TABLE federation_prefs ADD CONSTRAINT federation_prefs_source_check
    CHECK (source IN ('record', 'probe'));
