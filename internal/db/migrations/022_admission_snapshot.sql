-- +goose Up
-- Task 16 admin surface (readmit recoverability).
--
-- POST /admin/admissions/readmit re-runs admission for ONE post. The primary
-- use case is re-admitting a post that was REJECTED (opted-out, paused, …) once
-- the cause clears. But a rejection writes NO outbound_objects row — that state
-- is created only on ACCEPT — so the postv2 record body a rejected post was
-- decided against does not survive anywhere. The author's repo is a native PDS
-- Tidepool does not host, so the body cannot be read back locally.
--
-- evaluated_snapshot stores the record (plus resolved context) the engine
-- decided against, on EVERY decision (accept AND reject), so readmit can re-run
-- from stored state alone. A fresh com.atproto.repo.getRecord fetch from the
-- author's PDS (fresher content) is deferred to task 18; this column is the
-- task-16 baseline. Empty ('{}') marks a decision made before this column
-- existed, or a legacy row — readmit surfaces that as an unrecoverable readmit
-- rather than silently re-admitting nothing.
ALTER TABLE admissions ADD COLUMN evaluated_snapshot JSONB NOT NULL DEFAULT '{}'::jsonb;

-- +goose Down
ALTER TABLE admissions DROP COLUMN IF EXISTS evaluated_snapshot;
