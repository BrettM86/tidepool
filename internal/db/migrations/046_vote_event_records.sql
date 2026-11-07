-- +goose Up
-- PLAN.md decision 21: an eligible bridged voter's Like/Dislike becomes a
-- social.coves.feed.vote record in the voter's own repo instead of an
-- aggregate count.
--
-- record_at_uri names the record (at://<voter did>/social.coves.feed.vote/<rkey>)
-- and record_state tracks it: pending = owed but not committed yet, written =
-- in the repo, removed = none exists and none is owed. The two are both NULL
-- or both set; 'removed' keeps the URI. A non-NULL record_at_uri takes the row
-- OUT of the served aggregate whatever its state: Coves counts the record in
-- its native column.
--
-- The seeder still subtracts recorded votes from the API total, which
-- contains them: served = api_total − ours − recorded.
ALTER TABLE vote_events
    ADD COLUMN record_at_uri TEXT,
    ADD COLUMN record_state TEXT,
    ADD CONSTRAINT vote_events_record_state_check
        CHECK (record_state IN ('pending', 'written', 'removed')),
    ADD CONSTRAINT vote_events_record_pair_check
        CHECK ((record_at_uri IS NULL) = (record_state IS NULL));

-- The record work set (rows owing a create or a delete), looked up by voter by
-- the writer, the repair sweep and the scrub.
CREATE INDEX idx_vote_events_record_work ON vote_events (voter_ap_id)
    WHERE record_state IN ('pending', 'written');

-- Catch-up conversion candidates: a voter's live votes that are not records.
CREATE INDEX idx_vote_events_live_unrecorded ON vote_events (voter_ap_id)
    WHERE NOT undone AND record_at_uri IS NULL;

-- +goose Down
-- Dropping the columns puts recorded votes back into the served aggregate on
-- the next recompute while their records stay in the voters' repos.
DROP INDEX IF EXISTS idx_vote_events_live_unrecorded;
DROP INDEX IF EXISTS idx_vote_events_record_work;
ALTER TABLE vote_events
    DROP CONSTRAINT IF EXISTS vote_events_record_pair_check,
    DROP CONSTRAINT IF EXISTS vote_events_record_state_check,
    DROP COLUMN IF EXISTS record_state,
    DROP COLUMN IF EXISTS record_at_uri;
