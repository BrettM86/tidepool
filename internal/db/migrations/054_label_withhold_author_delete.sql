-- +goose Up
-- A label row whose subject record its author deleted while the label stood
-- carries the label-takedown-author-deleted code: the delete kept the bytes,
-- and a negation lifts every other label row of an absent subject but not
-- these, so the kept bytes stay refused until an operator's delete-bytes.
ALTER TABLE blob_withholds DROP CONSTRAINT blob_withholds_reason_code_check;
ALTER TABLE blob_withholds ADD CONSTRAINT blob_withholds_reason_code_check CHECK (
    CASE
        WHEN source = 'community-removal'
            THEN reason_code <> '' AND octet_length(reason_code) <= 64
        WHEN source = 'label'
            THEN reason_code IN ('label-takedown', 'label-takedown-author-deleted')
        ELSE reason_code IN ('illegal-content', 'csam', 'operator-discretion', 'community-removal')
    END
);

-- +goose Down
UPDATE blob_withholds SET reason_code = 'label-takedown'
WHERE source = 'label' AND reason_code = 'label-takedown-author-deleted';
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
