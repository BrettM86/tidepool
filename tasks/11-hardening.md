# Task 11 — Hardening: the pre-internet-facing FOLLOWUPS (~1000 LOC)

## Goal
Work the correctness/security backlog in FOLLOWUPS.md so the bridge can
face the open internet: admission control on every public surface, the
missing firehose account signal, ordering/tombstone gaps, unbounded table
growth, and the small housekeeping items. Verified under the task 09/10
harness — the strictest pipeline we have.

## Deliverables (FOLLOWUPS is the checklist; this is the triage)
Security / admission control:
- **Per-signer AND per-IP rate limit on `/inbox`** (top item:
  queue-flood DoS via many self-signed identities). Token-bucket,
  fail-closed cap, mirroring the votes XRPC limiter's discipline.
- **Connection cap + per-IP rate limit on the public sync surface**
  (subscribeRepos, getRepo and friends).
- Seeded-count upper sanity cap (a hostile origin API can't inject
  absurd baselines).

Protocol correctness:
- **`#account{active:false}` firehose frame** on `Delete(Actor)` /
  consent revocation, so subscribers purge instead of relying on scrub
  delete-commits. Assert it in an e2e scenario (task 10's Delete(Actor)
  scenario gains the frame check).
- **Delete-before-Create**: reconcile the README claim ("remembered via
  `ap_tombstones`") against the FOLLOWUPS gap ("a Delete arriving before
  its object was ever materialized leaves nothing to tombstone") —
  verify which is true, close the gap, kill the false doc either way.
- **Automatic Follow re-send** when a subscription stays `pending` past
  a threshold (Lemmy first-contact Accept race — currently only the test
  harness retries; production operators shouldn't have to).
- Actor-delete / consent revocation scrubs that actor's `vote_events`
  rows (consistency with scrub posture elsewhere).

Unbounded growth / housekeeping:
- Pruners for `ap_tombstones` and superseded/undone `vote_events` rows
  (mirror `FIREHOSE_RETENTION` treatment); batch the `PruneEvents`
  DELETE while there.
- `MAX_BLOB_BYTES` above 5 MiB: wire the AP client's response cap to the
  config value instead of silently clamping.
- Transient media-fetch failure on profile refresh carries forward
  existing blobs instead of dropping them; `DeleteActor` scrubs blobs
  stored under community DIDs.
- `commitRecord` PutRecord→PutMapping in one tx.
- Delete dead `ingest.NewNoopVotes`; rename
  `service_keys.private_key_pem` (it holds sealed ciphertext for the
  plc-rotation row) via migration.
- Strict-validation failure metric (production logs-and-writes today —
  make it observable; strict-first rollout stays deferred).

## Definition of done
- Full unit suite + `make e2e` green.
- Every FOLLOWUPS item this task closes is deleted from FOLLOWUPS.md;
  anything triaged out is annotated with why.
- New public-surface limits have tests proving both enforcement and
  non-interference with legitimate load (the e2e suite itself is the
  canary — it must pass under the new limits).

## References
- FOLLOWUPS.md (Ingestion, Sync surface, Votes, Materializer,
  Storage/housekeeping sections).
- `internal/votes/xrpc.go` limiter (pattern to reuse), LOOP_STATE task
  06/07 notes (queue fencing, outcome contract — do not violate).
