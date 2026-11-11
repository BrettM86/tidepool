-- +goose Up
-- The held delivery state (chunk 03). A held row is off its ordering queue
-- while its own object, or the parent it replies to, is taken down. It is not
-- 'pending', so it leaves idx_outbound_deliveries_queue: ClaimNext never sees
-- it, it no longer blocks its key, and every cancellation (all of which match
-- state = 'pending') leaves it alone.
--
-- held_reason says which blocker held it (its own object, or its parent) and
-- held_subject is that blocker's at-uri, the key a release looks rows up by.
-- A release rechecks every blocker, not only the recorded one.
ALTER TABLE outbound_deliveries DROP CONSTRAINT outbound_deliveries_state_check;
ALTER TABLE outbound_deliveries ADD CONSTRAINT outbound_deliveries_state_check
    CHECK (state IN ('pending', 'delivered', 'poisoned', 'cancelled', 'held'));

ALTER TABLE outbound_deliveries
    ADD COLUMN held_reason TEXT NOT NULL DEFAULT ''
        CHECK (held_reason IN ('', 'object', 'parent')),
    ADD COLUMN held_subject TEXT NOT NULL DEFAULT '',
    ADD CONSTRAINT outbound_deliveries_held_check CHECK (
        CASE WHEN state = 'held' THEN held_reason <> '' AND held_subject <> ''
             ELSE held_reason = '' AND held_subject = ''
        END
    );

CREATE INDEX idx_outbound_deliveries_held
    ON outbound_deliveries (held_subject)
    WHERE state = 'held';

-- +goose Down
DROP INDEX IF EXISTS idx_outbound_deliveries_held;
ALTER TABLE outbound_deliveries DROP CONSTRAINT IF EXISTS outbound_deliveries_held_check;
UPDATE outbound_deliveries SET state = 'pending' WHERE state = 'held';
ALTER TABLE outbound_deliveries
    DROP COLUMN IF EXISTS held_subject,
    DROP COLUMN IF EXISTS held_reason;
ALTER TABLE outbound_deliveries DROP CONSTRAINT outbound_deliveries_state_check;
ALTER TABLE outbound_deliveries ADD CONSTRAINT outbound_deliveries_state_check
    CHECK (state IN ('pending', 'delivered', 'poisoned', 'cancelled'));
