-- +goose Up
-- The seq of the newest inactive #account frame whose unconfirmed pause
-- committed (phase one of the confirmed path) before its PDS confirmation.
-- last_account_seq advances only with the confirmed half, so without this an
-- older frame whose PDS read was in flight passes the watermark and clears
-- the newer frame's pause. A frame with a seq below pending_account_seq is
-- stale, as one at or below last_account_seq is.
ALTER TABLE ap_actors ADD COLUMN pending_account_seq BIGINT NOT NULL DEFAULT 0;

-- +goose Down
ALTER TABLE ap_actors DROP COLUMN IF EXISTS pending_account_seq;
