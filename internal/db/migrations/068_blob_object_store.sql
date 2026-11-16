-- +goose Up
-- In s3 mode a blob's bytes live in the bucket, so the row keeps its metadata
-- with bytes NULL.
ALTER TABLE blobs ALTER COLUMN bytes DROP NOT NULL;

-- Queue of bucket objects to delete in s3 mode: one row per blob whose row
-- was removed but whose object may still be in the bucket. A sweep deletes
-- the object and drops the row; attempts and last_error record failures.
CREATE TABLE blob_object_deletes (
    did        TEXT        NOT NULL CHECK (did <> ''),
    cid        TEXT        NOT NULL CHECK (cid <> ''),
    queued_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    attempts   INTEGER     NOT NULL DEFAULT 0,
    last_error TEXT,
    PRIMARY KEY (did, cid)
);

-- +goose Down
DROP TABLE IF EXISTS blob_object_deletes;
-- Fails while any row has NULL bytes, which is correct: those bytes are only
-- in the bucket.
ALTER TABLE blobs ALTER COLUMN bytes SET NOT NULL;
