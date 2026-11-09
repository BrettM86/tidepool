-- +goose Up
-- Community-removal withholds: one active row per (removed subject, blob CID
-- its current record references). The partial unique index makes the
-- removal write idempotent (inserts are ON CONFLICT on this index), so a
-- redelivered removal or a re-sync of the same record adds no second row.
CREATE UNIQUE INDEX blob_withholds_one_active_community_removal
    ON blob_withholds (subject_uri, cid)
    WHERE source = 'community-removal' AND lifted_at IS NULL;

-- A community-removal row carries its removal record's code. The removal
-- lexicon's code is an open set (knownValues, maxLength 64 bytes), so these
-- rows take any non-empty code of at most 64 bytes; every other source keeps
-- 047's fixed codes, and the operator check from 047 still applies.
ALTER TABLE blob_withholds DROP CONSTRAINT blob_withholds_reason_code_check;
ALTER TABLE blob_withholds ADD CONSTRAINT blob_withholds_reason_code_check CHECK (
    CASE WHEN source = 'community-removal'
        THEN reason_code <> '' AND octet_length(reason_code) <= 64
        ELSE reason_code IN ('illegal-content', 'csam', 'operator-discretion', 'community-removal')
    END
);

-- +goose Down
-- 047's constraint admits only community-removal as a community-removal row's
-- code, so rows written under a removal code take it back first.
UPDATE blob_withholds SET reason_code = 'community-removal'
WHERE source = 'community-removal'
  AND reason_code NOT IN ('illegal-content', 'csam', 'operator-discretion', 'community-removal');
ALTER TABLE blob_withholds DROP CONSTRAINT blob_withholds_reason_code_check;
ALTER TABLE blob_withholds ADD CONSTRAINT blob_withholds_reason_code_check
    CHECK (reason_code IN ('illegal-content', 'csam', 'operator-discretion', 'community-removal'));
DROP INDEX IF EXISTS blob_withholds_one_active_community_removal;
