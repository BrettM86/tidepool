-- +goose Up
-- Task 14: the Jetstream consumer's own state, plus the durable OUTBOUND
-- state every later Delete and Undo is rebuilt from.
--
-- The consumer half (consumer_cursors, jetstream_record_revs,
-- jetstream_dead_letters) is a PORT of the Coves AppView's own Jetstream
-- tables (their migrations 032/033). Same discipline, same failure taxonomy,
-- one deliberate divergence — stated on consumer_cursors below.

-- Cursor persistence. Each consumer stores the time_us of the last event it
-- fully processed (handled or safely dead-lettered) and resumes there instead
-- of at the live tail, so restarts, deploys and crashes stop losing the events
-- that happened during the gap.
--
-- DIVERGENCE FROM COVES: the key is (consumer_name, schema_version), not
-- consumer_name alone. A future incompatible handler must be able to replay
-- the whole retained store from scratch, and it cannot do that if its replay
-- cursor overwrites the production one — the two rows coexist and neither
-- version can see the other's progress. Learned the hard way (FOLLOWUPS): a
-- cursor sitting between Jetstream's newest stored event and now replays the
-- ENTIRE retained store, so every path downstream of a cursor must be
-- idempotent and a wall-clock "now" cursor is never a dedupe boundary.
CREATE TABLE consumer_cursors (
    consumer_name TEXT NOT NULL,
    schema_version INT NOT NULL,                                -- the HANDLER contract version, not the DB schema
    cursor_time_us BIGINT NOT NULL CHECK (cursor_time_us >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer_name, schema_version)
);

-- Per-record rev gate: the ordering guard that makes replay safe.
--
-- Every commit event carries `rev`, the repo's monotonic TID — a fixed-length
-- base32-sortable string, so plain lexicographic comparison IS commit order
-- within one repo. Handlers record the rev of the last APPLIED event per
-- record URI here and apply an incoming create/update/delete only when its rev
-- is strictly greater. Equal rev means the same event replayed (reconnect
-- rewind, redrive) and is a no-op; smaller means a stale copy and is skipped.
--
-- WHY A SEPARATE TABLE rather than a rev column on outbound_objects: the row
-- must SURVIVE the record it describes, and it must gate record types that
-- have no outbound row at all (federation prefs, profiles, votes). It doubles
-- as the tombstone that rejects the stale create which would otherwise
-- resurrect a deleted record — stable activity ids alone cannot prevent that.
CREATE TABLE jetstream_record_revs (
    record_uri TEXT PRIMARY KEY,
    -- COLLATE "C" pins the comparison to bytewise order (for TIDs, bytewise IS
    -- commit order) regardless of the database's default collation.
    rev TEXT COLLATE "C" NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Dead letter queue for events that failed all in-line retries. Malformed
-- records go HERE, not to a log line: a lexicon rollout mistake must be
-- recoverable, not silent permanent loss.
--
-- event_data is BYTEA, not TEXT/JSONB, because byte-corrupt frames (NUL bytes,
-- invalid UTF-8) must also be capturable. A TEXT column would reject them, the
-- failed dead-letter write would tear the connection down without advancing
-- the cursor, and the consumer would replay the same frame forever.
CREATE TABLE jetstream_dead_letters (
    id BIGSERIAL PRIMARY KEY,
    consumer_name TEXT NOT NULL,
    event_time_us BIGINT NOT NULL DEFAULT 0,
    event_data BYTEA NOT NULL,
    last_error TEXT NOT NULL,
    attempts INT NOT NULL DEFAULT 0,                            -- redrive attempts, not in-line retries
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- The redriver scans per consumer, oldest first, skipping rows that exhausted
-- their budget.
CREATE INDEX idx_jetstream_dead_letters_redrive
    ON jetstream_dead_letters (consumer_name, attempts, id);

-- Dedup. A poison event inside the reconnect rewind window (or an unparseable
-- frame, which never advances the cursor) would otherwise insert a fresh row —
-- each with its own redrive budget — on every reconnect. AddDeadLetter inserts
-- ON CONFLICT DO NOTHING, so an already-captured event counts as success and
-- the cursor may advance past it. The name is the Coves original's: the store
-- and its tests both name it.
CREATE UNIQUE INDEX idx_jetstream_dead_letters_dedup
    ON jetstream_dead_letters (consumer_name, event_time_us, md5(event_data));

-- outbound_objects is the state a Delete is rebuilt from (decision 14).
--
-- WHY IT EXISTS: a Jetstream DELETE commit carries the DID, the collection and
-- the rkey and NOTHING else — no record body, no CID (verified against the
-- event model). So every fact a Delete{Note} needs — which AP id it addresses,
-- which community it is addressed to, what the object looked like — must
-- already be at rest here before the delete arrives.
--
-- Rows are TOMBSTONED, never deleted: the row is what a late replay of the
-- create is rejected against, and task 17 restores content from the snapshot.
--
-- last_cid/last_rev are PROVENANCE ONLY. The ordering gate is
-- jetstream_record_revs; reading a rev back from here to decide whether to
-- apply an event would be a check→write race by construction.
--
-- last_activity_seq is the counter behind the deterministic activity id
-- (decision 12: sha256(at_uri + op + seq)). It is a seq and not the CID
-- because deletes have no CID. A create is seq 0; every APPLIED write bumps
-- it, so each operation gets its own stable id — stable because the rev gate
-- runs first and a replayed commit never reaches the bump.
CREATE TABLE outbound_objects (
    at_uri TEXT PRIMARY KEY,
    ap_object_id TEXT NOT NULL,                                 -- the AP id this record federates as
    last_cid TEXT NOT NULL DEFAULT '',                           -- provenance only
    last_rev TEXT NOT NULL DEFAULT '',                           -- provenance only
    community_did TEXT NOT NULL,
    community_ap_id TEXT NOT NULL,
    translated_snapshot JSONB NOT NULL,                          -- what task 15 serves and task 17 restores
    last_activity_seq INT NOT NULL DEFAULT 0,
    depth INT NOT NULL DEFAULT 0,                                -- reply depth; Lemmy caps comments at 50
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    tombstoned_at TIMESTAMPTZ
);

-- Task 15 serves an actor's outbound work and task 17 sweeps a community's.
CREATE INDEX idx_outbound_objects_community ON outbound_objects (community_did);

-- outbound_votes is the state an Undo is rebuilt from (decision 16).
--
-- The PRIMARY KEY is the VOTE RECORD's at-uri because that is the only thing a
-- vote delete commit carries: direction and the activity id the Like/Dislike
-- went out under have to be readable back from that one key alone.
--
-- (actor_did, subject_at_uri) is a SECOND unique constraint, named EXPLICITLY
-- (postgres would default it to outbound_votes_actor_did_subject_at_uri_key)
-- because the store's 23505 → ConflictError mapping switches on the name. One
-- actor holds at most one LIVE vote per subject: letting a second vote record
-- clobber the first would strand an Undo that is still owed to the peer, so
-- the constraint refuses the write instead.
--
-- delivered_state is the consumer's INTENT ledger. The consumer only ever
-- writes 'pending'; task 15 flips it on DELIVERY SUCCESS, never on enqueue —
-- a row claiming delivery the wire never confirmed makes the Undo unsendable.
CREATE TABLE outbound_votes (
    vote_at_uri TEXT PRIMARY KEY,
    actor_did TEXT NOT NULL,
    subject_at_uri TEXT NOT NULL,
    subject_ap_id TEXT NOT NULL,
    community_did TEXT NOT NULL,
    direction TEXT NOT NULL CHECK (direction IN ('up', 'down')),
    current_activity_id TEXT NOT NULL,
    delivered_state TEXT NOT NULL DEFAULT 'pending'
        CHECK (delivered_state IN ('pending', 'delivered', 'undone')),
    activity_seq INT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT outbound_votes_actor_subject_key UNIQUE (actor_did, subject_at_uri)
);

-- federation_prefs holds Coves users' federation preferences (decision 11).
--
-- Federation is DEFAULT-ON and social.coves.bridge.federation is an OPT-OUT
-- record, so ABSENCE IS THE DEFAULT-ON STATE: this table only ever holds rows
-- for users who said something, and the record-delete path removes the row
-- rather than writing enabled = true. Nothing may read a missing row as
-- "unknown" — a caller that cannot tell "opted in" from "never spoke" cannot
-- tell a re-enable from a first sighting either.
--
-- source is CHECKed and has no default: "we read this from a record" and "we
-- went and asked" have different staleness, and a defaulted source hides which
-- one applied.
CREATE TABLE federation_prefs (
    did TEXT PRIMARY KEY,
    enabled BOOL NOT NULL,
    delete_remote BOOL NOT NULL DEFAULT FALSE,                   -- the destructive tier (task 17)
    source TEXT NOT NULL CHECK (source IN ('record', 'probe')),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- +goose Down
DROP TABLE IF EXISTS federation_prefs;
DROP TABLE IF EXISTS outbound_votes;
DROP INDEX IF EXISTS idx_outbound_objects_community;
DROP TABLE IF EXISTS outbound_objects;
DROP INDEX IF EXISTS idx_jetstream_dead_letters_dedup;
DROP INDEX IF EXISTS idx_jetstream_dead_letters_redrive;
DROP TABLE IF EXISTS jetstream_dead_letters;
DROP TABLE IF EXISTS jetstream_record_revs;
DROP TABLE IF EXISTS consumer_cursors;
