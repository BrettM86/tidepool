-- +goose Up
-- The persona pause source set (chunk 04). A native persona's delivery is
-- paused while at least one source stands: its account `!takedown` label
-- (key 'account-label:<labeler DID>') or its #account status (key
-- 'account-status:<DID>'). Each key maps to the #account status that source
-- last applied ('' for a label). Neither source clears the other.
-- ap_actors.delivery_paused stays as the derived value (the set is not empty),
-- so its readers keep working; every source write sets both in one statement.
--
-- The set lives on the actor row rather than in its own table, so it goes
-- with the row: a referencing table would make every plain TRUNCATE of
-- ap_actors fail.
ALTER TABLE ap_actors ADD COLUMN pause_sources JSONB NOT NULL DEFAULT '{}'::jsonb
    CHECK (jsonb_typeof(pause_sources) = 'object');

-- Every pause written before this migration came from #account.
UPDATE ap_actors SET pause_sources = jsonb_build_object('account-status:' || did, '')
WHERE delivery_paused;

-- +goose Down
ALTER TABLE ap_actors DROP COLUMN IF EXISTS pause_sources;
