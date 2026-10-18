-- +goose Up
-- Identity changes share the commit cursor and the account event backlog.
ALTER TABLE firehose_events
    DROP CONSTRAINT firehose_events_kind_check,
    DROP CONSTRAINT firehose_events_kind_shape_check,
    ADD COLUMN identity_handle TEXT;

ALTER TABLE firehose_events
    ADD CONSTRAINT firehose_events_kind_check CHECK (kind IN ('commit', 'account', 'identity')),
    ADD CONSTRAINT firehose_events_kind_shape_check CHECK (
        (kind = 'commit'
            AND commit_cid IS NOT NULL AND commit_cid <> ''
            AND rev IS NOT NULL AND rev <> ''
            AND ops IS NOT NULL AND car IS NOT NULL
            AND account_active IS NULL AND account_status IS NULL
            AND identity_handle IS NULL)
        OR
        (kind = 'account'
            AND account_active IS NOT NULL
            AND commit_cid IS NULL AND rev IS NULL AND ops IS NULL AND car IS NULL
            AND (NOT account_active OR account_status IS NULL)
            AND identity_handle IS NULL)
        OR
        (kind = 'identity'
            AND did <> ''
            AND identity_handle IS NOT NULL AND identity_handle <> ''
            AND commit_cid IS NULL AND prev_data_cid IS NULL
            AND since_rev IS NULL AND rev IS NULL AND ops IS NULL AND car IS NULL
            AND account_active IS NULL AND account_status IS NULL)
    );

-- +goose Down
-- Restoring 011's two-kind CHECK is only lossless with no identity rows; deleting them is left to an operator.
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM firehose_events WHERE kind = 'identity') THEN
        RAISE EXCEPTION 'firehose_events holds identity events; delete them before rolling back migration 035';
    END IF;
END $$;
-- +goose StatementEnd

ALTER TABLE firehose_events
    DROP CONSTRAINT firehose_events_kind_shape_check,
    DROP CONSTRAINT firehose_events_kind_check,
    DROP COLUMN identity_handle;

ALTER TABLE firehose_events
    ADD CONSTRAINT firehose_events_kind_check CHECK (kind IN ('commit', 'account')),
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
            AND (NOT account_active OR account_status IS NULL))
    );
