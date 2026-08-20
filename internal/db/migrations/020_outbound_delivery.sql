-- +goose Up
-- Task 15: the outbound delivery pipe, split into canonical ACTIVITIES and
-- per-inbox DELIVERIES (decision 15), plus the causal-gating marker on
-- outbound_objects.
--
-- WHY THE SPLIT: a single globally-unique activity row cannot fan out. Task
-- 17's Delete{Person} is ONE activity delivered to MANY community inboxes, and
-- an already-delivered Create must keep serving its ORIGINAL payload after a
-- later edit (activities are immutable, objects are current). So the canonical
-- activity — the byte-stable wire payload a peer re-fetches by id — lives once
-- in outbound_activities, and every (activity, inbox) delivery attempt is a row
-- in outbound_deliveries.

-- outbound_activities is the canonical, IMMUTABLE wire payload for one activity
-- id. It is what GET /ap/activity/{hash} serves and what a redelivery re-sends
-- verbatim, so a peer that already has the activity dedupes it on our stable
-- id. Rows are inserted ON CONFLICT DO NOTHING: the payload of an activity a
-- peer may already hold must never change under it.
CREATE TABLE outbound_activities (
    activity_id TEXT PRIMARY KEY,                                -- deterministic id (consume.ActivityID)
    actor_did TEXT NOT NULL,                                     -- the persona that signs the delivery
    kind TEXT NOT NULL,                                          -- Create/Update/Delete/Like/Dislike/Undo
    payload JSONB NOT NULL,                                      -- the canonical wire activity, byte-stable
    parent_at_uri TEXT NOT NULL DEFAULT '',                      -- causal dependency ('' = none)
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Supports CancelForActor's actor_did filter (the consent-withdrawal /
-- kill-switch sweep that parks a disabled or paused actor's pending outbound
-- work): it scans activities by actor to cancel their deliveries.
CREATE INDEX idx_outbound_activities_actor ON outbound_activities (actor_did);

-- outbound_deliveries is one delivery attempt per (activity, target inbox). It
-- GENERALIZES inbox_events: the same claimed_until fencing token, the same
-- loose-index-scan per-ordering-key serialization (task 12's lesson), the same
-- SKIP LOCKED concurrency. ordering_key is the community AP id, so every
-- activity bound for one community delivers in a single serial line.
--
-- seq is the monotonic ordering column the loose index scan descends: "the
-- head of an ordering key" is its min-seq pending row. It is a BIGSERIAL and
-- NOT part of the PK, because the PK is the natural (activity_id, target_inbox)
-- fan-out key.
CREATE TABLE outbound_deliveries (
    seq BIGSERIAL NOT NULL,
    activity_id TEXT NOT NULL REFERENCES outbound_activities (activity_id) ON DELETE CASCADE,
    target_inbox TEXT NOT NULL,
    ordering_key TEXT NOT NULL,                                  -- community AP id: per-community serial line
    state TEXT NOT NULL DEFAULT 'pending'
        CHECK (state IN ('pending', 'delivered', 'poisoned', 'cancelled')),
    attempts INT NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    claimed_until TIMESTAMPTZ,                                   -- the fencing/lease token (see inbox_events)
    delivered_at TIMESTAMPTZ,
    last_status_code INT,
    last_error_class TEXT NOT NULL DEFAULT '',
    response_excerpt TEXT NOT NULL DEFAULT '',                   -- bounded body sample for triage
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (activity_id, target_inbox)
);

-- The loose-index-scan support index (task 12): one index descent per DISTINCT
-- pending ordering key jumps to each key's head, so ClaimNext is O(pending
-- keys × log N) regardless of any one community's backlog depth. Partial on
-- pending so delivered/poisoned/cancelled rows never bloat it.
CREATE INDEX idx_outbound_deliveries_queue
    ON outbound_deliveries (ordering_key, seq)
    WHERE state = 'pending';

-- CancelForActor sweeps a disabled/deleted actor's pending deliveries; it joins
-- through outbound_activities.actor_did, so the FK side is indexed above.
CREATE INDEX idx_outbound_deliveries_activity ON outbound_deliveries (activity_id);

-- The causal marker (decision 15). accepted_at is stamped by delivery success:
-- NULL means "not yet delivered to its community", which is what gates a
-- bridge-origin child from delivering before its parent. Fediverse-origin
-- parents (no outbound_objects row) are always eligible.
ALTER TABLE outbound_objects ADD COLUMN accepted_at TIMESTAMPTZ;

-- +goose Down
ALTER TABLE outbound_objects DROP COLUMN IF EXISTS accepted_at;
DROP INDEX IF EXISTS idx_outbound_deliveries_activity;
DROP INDEX IF EXISTS idx_outbound_deliveries_queue;
DROP TABLE IF EXISTS outbound_deliveries;
DROP INDEX IF EXISTS idx_outbound_activities_actor;
DROP TABLE IF EXISTS outbound_activities;
