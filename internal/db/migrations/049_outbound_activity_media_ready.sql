-- +goose Up
-- When the outbound media gate last found every proxy image URL in the
-- activity's payload served (or gave up waiting past its budget). NULL means
-- the gate has not settled yet, so a delivery of the activity still probes;
-- once set, fan-out deliveries and retries of the same activity skip the probe.
ALTER TABLE outbound_activities ADD COLUMN media_ready_at TIMESTAMPTZ NULL;

-- +goose Down
ALTER TABLE outbound_activities DROP COLUMN IF EXISTS media_ready_at;
