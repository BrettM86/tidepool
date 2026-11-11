-- +goose Up
-- Coves labels: every verified label from the configured Coves labeler's
-- com.atproto.label.subscribeLabels stream, kept forever. A publisher reset
-- (a FutureCursor answer) restarts the stream's seq at 1, so a stream position
-- is (reset_epoch, seq): the replay's rows sit beside the earlier epoch's
-- rows instead of colliding with them. A label's current state per (uri, val)
-- is the row with the greatest (reset_epoch, seq); cts never orders labels.
CREATE TABLE coves_labels (
    reset_epoch BIGINT NOT NULL CHECK (reset_epoch >= 0),
    seq BIGINT NOT NULL CHECK (seq > 0),
    src TEXT NOT NULL,
    uri TEXT NOT NULL,
    cid TEXT NULL,
    val TEXT NOT NULL,
    neg BOOLEAN NOT NULL,
    cts TEXT NOT NULL,
    exp TEXT NULL,
    sig BYTEA NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (reset_epoch, seq)
);

-- The current-state read: latest row per (val, uri).
CREATE INDEX coves_labels_current
    ON coves_labels (val, uri, reset_epoch DESC, seq DESC);

-- The consumer's position: one row, (reset_epoch, seq) of the last committed
-- label. A missing row is (0, 0), the start of the stream.
CREATE TABLE coves_label_cursor (
    singleton BOOLEAN PRIMARY KEY DEFAULT true CHECK (singleton),
    reset_epoch BIGINT NOT NULL CHECK (reset_epoch >= 0),
    seq BIGINT NOT NULL CHECK (seq >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- +goose Down
DROP TABLE IF EXISTS coves_label_cursor;
DROP TABLE IF EXISTS coves_labels;
