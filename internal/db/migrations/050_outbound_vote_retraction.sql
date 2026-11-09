-- +goose Up
-- A vote flip arrives as a delete of vote A plus a create of vote B under a
-- new rkey. A's row must outlive its Undo (the delivery worker clears it on
-- success), but it must stop holding the actor's one LIVE slot on the subject,
-- or B is refused until the Undo lands — and forever if the Undo poisons.
-- retraction_activity_id names the Undo withdrawing the row; NULL means live,
-- and only live rows are unique per (actor, subject).
ALTER TABLE outbound_votes ADD COLUMN retraction_activity_id TEXT;

-- Rows already withdrawn by a queued Undo come up retracting.
UPDATE outbound_votes v
   SET retraction_activity_id = (
        SELECT a.activity_id FROM outbound_activities a
         WHERE a.kind = 'Undo'
           AND a.actor_did = v.actor_did
           AND a.payload->'object'->>'id' = v.current_activity_id
         ORDER BY a.created_at DESC, a.activity_id DESC
         LIMIT 1)
 WHERE v.delivered_state <> 'undone'
   AND EXISTS (
        SELECT 1 FROM outbound_activities a
         WHERE a.kind = 'Undo'
           AND a.actor_did = v.actor_did
           AND a.payload->'object'->>'id' = v.current_activity_id);

ALTER TABLE outbound_votes DROP CONSTRAINT outbound_votes_actor_subject_key;
CREATE UNIQUE INDEX outbound_votes_actor_subject_key
    ON outbound_votes (actor_did, subject_at_uri)
    WHERE retraction_activity_id IS NULL;

-- +goose Down
-- Restoring the full unique constraint would have to drop a vote row; resolving that is left to an operator.
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM outbound_votes
         GROUP BY actor_did, subject_at_uri
        HAVING COUNT(*) > 1) THEN
        RAISE EXCEPTION 'outbound_votes holds more than one row for an (actor_did, subject_at_uri) pair; resolve them before rolling back migration 050';
    END IF;
END $$;
-- +goose StatementEnd

DROP INDEX outbound_votes_actor_subject_key;
ALTER TABLE outbound_votes
    ADD CONSTRAINT outbound_votes_actor_subject_key UNIQUE (actor_did, subject_at_uri),
    DROP COLUMN retraction_activity_id;
