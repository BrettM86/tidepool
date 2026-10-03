-- +goose Up
-- How a fediverse object reached the bridge, recorded as a provenance code.
--
-- The delegation bar counts a post or comment only when the community itself
-- announced it. A bare signed Create naming a followed community in its
-- audience is still materialized, but anyone who controls a host can send one,
-- so it says nothing about whether a real community let the content stand.
--
--   not_announced        — every row that existed before this column, and any
--                          object that arrived bare, was fetched as a parent or
--                          thread root, or came in through a backfill.
--   community_announced  — the followed community's own Announce carried this
--                          object. Set once by ingest and never cleared: a
--                          later bare delivery of the same object does not
--                          downgrade it, and mapping upserts do not touch it.
ALTER TABLE ap_objects
    ADD COLUMN arrival TEXT NOT NULL DEFAULT 'not_announced'
        CHECK (arrival IN ('not_announced', 'community_announced'));

-- +goose Down
ALTER TABLE ap_objects DROP COLUMN IF EXISTS arrival;
