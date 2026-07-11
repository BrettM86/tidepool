-- +goose Up
-- Task 11: the firehose grows a second event kind. Until now every
-- firehose_events row was a #commit; consent revocation (Delete(Actor))
-- now appends an #account{active:false} row through the same durable log —
-- same seq space, same advisory-lock serialization, same retention — so
-- relays learn the repo is gone instead of inferring it from scrub
-- delete-commits (bigsky only learns account state from #account frames).
--
-- Account rows carry no commit payload, so the commit columns loosen to
-- nullable with a CHECK that keeps them REQUIRED for commit rows: the old
-- invariant is not weakened, it is scoped to the kind it belongs to.
ALTER TABLE firehose_events
    ADD COLUMN kind TEXT NOT NULL DEFAULT 'commit' CHECK (kind IN ('commit', 'account')),
    ADD COLUMN account_active BOOLEAN,
    ADD COLUMN account_status TEXT;

ALTER TABLE firehose_events
    ALTER COLUMN commit_cid DROP NOT NULL,
    ALTER COLUMN rev DROP NOT NULL,
    ALTER COLUMN ops DROP NOT NULL,
    ALTER COLUMN car DROP NOT NULL;

ALTER TABLE firehose_events
    DROP CONSTRAINT IF EXISTS firehose_events_commit_cid_check,
    DROP CONSTRAINT IF EXISTS firehose_events_rev_check;

ALTER TABLE firehose_events
    ADD CONSTRAINT firehose_events_kind_shape_check CHECK (
        (kind = 'commit'
            AND commit_cid IS NOT NULL AND commit_cid <> ''
            AND rev IS NOT NULL AND rev <> ''
            AND ops IS NOT NULL AND car IS NOT NULL
            AND account_active IS NULL AND account_status IS NULL)
        OR
        (kind = 'account'
            AND account_active IS NOT NULL
            AND commit_cid IS NULL AND rev IS NULL AND ops IS NULL AND car IS NULL
            -- status is inactive-only: the model/emitter document a status
            -- (e.g. 'deleted') solely for active=false frames, so an active
            -- account frame must carry a NULL status.
            AND (NOT account_active OR account_status IS NULL))
    );

-- +goose Down
DELETE FROM firehose_events WHERE kind <> 'commit';
ALTER TABLE firehose_events
    DROP CONSTRAINT IF EXISTS firehose_events_kind_shape_check;
ALTER TABLE firehose_events
    ALTER COLUMN commit_cid SET NOT NULL,
    ALTER COLUMN rev SET NOT NULL,
    ALTER COLUMN ops SET NOT NULL,
    ALTER COLUMN car SET NOT NULL;
ALTER TABLE firehose_events
    ADD CONSTRAINT firehose_events_commit_cid_check CHECK (commit_cid <> ''),
    ADD CONSTRAINT firehose_events_rev_check CHECK (rev <> '');
ALTER TABLE firehose_events
    DROP COLUMN IF EXISTS account_status,
    DROP COLUMN IF EXISTS account_active,
    DROP COLUMN IF EXISTS kind;
