-- +goose Up
-- Label withholds: a Coves `!takedown` label on a bridged record this bridge
-- hosts withholds, for the record's repo DID, every blob CID the record
-- references: one active row per (subject, CID), so the label applier and a
-- re-materialization of the subject write idempotently (ON CONFLICT on this
-- index). A negation lifts the subject's label rows only.
CREATE UNIQUE INDEX blob_withholds_one_active_label
    ON blob_withholds (subject_uri, cid)
    WHERE source = 'label' AND lifted_at IS NULL;

-- Label rows carry the label-takedown code; community-removal rows keep 048's
-- open codes and every other source 047's fixed codes.
ALTER TABLE blob_withholds DROP CONSTRAINT blob_withholds_reason_code_check;
ALTER TABLE blob_withholds ADD CONSTRAINT blob_withholds_reason_code_check CHECK (
    CASE
        WHEN source = 'community-removal'
            THEN reason_code <> '' AND octet_length(reason_code) <= 64
        WHEN source = 'label'
            THEN reason_code = 'label-takedown'
        ELSE reason_code IN ('illegal-content', 'csam', 'operator-discretion', 'community-removal')
    END
);

-- +goose Down
DELETE FROM blob_withholds WHERE source = 'label';
ALTER TABLE blob_withholds DROP CONSTRAINT blob_withholds_reason_code_check;
ALTER TABLE blob_withholds ADD CONSTRAINT blob_withholds_reason_code_check CHECK (
    CASE WHEN source = 'community-removal'
        THEN reason_code <> '' AND octet_length(reason_code) <= 64
        ELSE reason_code IN ('illegal-content', 'csam', 'operator-discretion', 'community-removal')
    END
);
DROP INDEX IF EXISTS blob_withholds_one_active_label;
