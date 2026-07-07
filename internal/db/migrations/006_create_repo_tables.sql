-- +goose Up
-- The virtual-PDS storage spine (task 03): every bridged DID gets a real
-- signed MST repo whose blocks and head pointer live here, plus a durable
-- firehose event log that task 04 serves over com.atproto.sync.subscribeRepos.

-- blocks holds every IPLD block (records, MST nodes, commits) for every
-- bridged repo, keyed per-DID so repos stay independently exportable.
-- Blocks are content-addressed and therefore immutable; superseded MST
-- nodes are kept (cheap, and historical CAR slices in firehose_events
-- reference them). Garbage collection of unreachable blocks is a later
-- optimization, deliberately not v1.
CREATE TABLE blocks (
    did TEXT NOT NULL CHECK (did <> ''),
    cid TEXT NOT NULL CHECK (cid <> ''),
    bytes BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT blocks_pkey PRIMARY KEY (did, cid)
);

-- repo_state is the mutable head pointer per repo: the latest signed commit
-- CID and its rev TID. Cross-process serialization comes from a GLOBAL
-- advisory transaction lock every commit takes before reading this table
-- (internal/repo commitAdvisoryLockKey) — NOT from the row lock: SELECT ...
-- FOR UPDATE on a missing row locks nothing, so the row lock alone cannot
-- make genesis commits race-free. The row lock is kept only as a backstop.
CREATE TABLE repo_state (
    did TEXT PRIMARY KEY CHECK (did <> ''),
    head_cid TEXT NOT NULL CHECK (head_cid <> ''),
    rev TEXT NOT NULL CHECK (rev <> ''),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- firehose_events is the durable subscribeRepos backlog: one row per commit,
-- appended in the SAME transaction as the commit itself so the stream can
-- never miss a write. seq is the firehose cursor: because every commit
-- transaction holds the global advisory lock until it commits, seq order
-- equals commit-visibility order, so a tailer doing `WHERE seq > cursor`
-- never permanently skips an event. car holds the CAR slice for the
-- #commit message (root = commit CID, written first; contains the commit
-- block plus the MST and record blocks new in this commit). ops is the
-- JSON op list ({action, path, cid, prev}); prev_data_cid is the MST root
-- before this commit (sync v1.1 prevData, NULL on a repo's genesis commit);
-- since_rev is the previous commit's rev (the #commit frame's `since`
-- field, NULL on genesis) — persisted because task 04 cannot reconstruct
-- it once older events are pruned.
CREATE TABLE firehose_events (
    seq BIGSERIAL PRIMARY KEY,
    did TEXT NOT NULL CHECK (did <> ''),
    commit_cid TEXT NOT NULL CHECK (commit_cid <> ''),
    prev_data_cid TEXT,
    since_rev TEXT,
    rev TEXT NOT NULL CHECK (rev <> ''),
    ops JSONB NOT NULL,
    car BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Task 04 serves per-DID catch-up (getRepo diff-since) and global cursor
-- scans; both want these indexes.
CREATE INDEX idx_firehose_events_did_seq ON firehose_events (did, seq);

-- +goose Down
DROP TABLE IF EXISTS firehose_events;
DROP TABLE IF EXISTS repo_state;
DROP TABLE IF EXISTS blocks;
