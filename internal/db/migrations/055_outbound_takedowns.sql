-- +goose Up
-- Native record takedowns (chunk 03). A native object is removed while its
-- takedown source set is non-empty: a record label, an account label, an
-- account status or a community label each add one source, and the object is
-- restored only when the last one clears.
--
-- No FK to outbound_objects: a label can name a native record before Tidepool
-- has seen it, and the source row is kept so the object is held when it is
-- created.
CREATE TABLE outbound_takedown_sources (
    at_uri TEXT NOT NULL,
    source_kind TEXT NOT NULL
        CHECK (source_kind IN ('record-label', 'account-label', 'account-status', 'community-label')),
    source_key TEXT NOT NULL CHECK (source_key <> ''),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (at_uri, source_kind, source_key)
);

-- One row per object that has ever had a source. It is the row source-set
-- changes lock, so concurrent changes to one object serialize. episode counts
-- removals (each empty-to-non-empty transition starts a new one), and
-- delete_activity_id is the takedown Delete sent in the current episode, if
-- any: the restore's Undo embeds that Delete, and settlement reads it to know
-- whether a Delete is still owed.
CREATE TABLE outbound_takedowns (
    at_uri TEXT PRIMARY KEY,
    episode INT NOT NULL DEFAULT 0 CHECK (episode >= 0),
    delete_activity_id TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- +goose Down
DROP TABLE IF EXISTS outbound_takedowns;
DROP TABLE IF EXISTS outbound_takedown_sources;
