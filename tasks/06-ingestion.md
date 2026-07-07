# Task 06 — Ingestion pipeline: inbox, follows, backfill, consent (~1200 LOC)

## Goal
Wire the fediverse to the materializer: subscribe to communities, receive
pushed activities, verify them, unwrap FEP-1b12 Announces, keep ordering
and idempotency, and backfill history. After this task the bridge is
functionally complete end-to-end (minus votes).

## Deliverables
- `internal/ingest/inbox.go` — POST `/inbox` (shared inbox) + per-actor
  inboxes: verify HTTP signature (task 02 `Verify`, actor key fetch with
  cache + re-fetch-on-fail once), check `inbox_events` dedupe by activity
  id, enqueue. Reject unsigned/expired (>1h skew). Serve the service actor
  document at `/actor` + WebFinger for it at `/.well-known/webfinger`
  (needed so Lemmy accepts our Follow), and `/.well-known/nodeinfo`
  (minimal; Lemmy checks software names for allowlists).
- `internal/ingest/queue.go` — durable work queue on postgres
  (`inbox_events` as the queue; worker pool, per-community serial ordering
  key so a community's events apply in order, retry w/ backoff, poison
  → error column + skip). No external queue dependency.
- `internal/ingest/handler.go` — activity dispatch:
  - `Announce{Create|Update{Page|Note}}` (FEP-1b12 group fan-out — the
    normal Lemmy path) → materializer.
  - Bare `Create/Update/Delete` from user actors (some arrive direct) →
    verify the object belongs to a followed community, then materialize.
  - `Announce{Like|Dislike}` → hand to vote aggregator (task 07 stub:
    define the interface now, no-op impl until 07).
  - `Accept{Follow}` → mark community follow_state accepted.
  - `Undo`, `Delete(Actor)`, `Update{Group|Person}` → materializer updates.
  - **Echo suppression**: drop any activity whose object's ap_id maps to a
    record the bridge itself created (future-proofing for write-side; cheap
    check against ap_objects origin flag — add `origin` column
    (fediverse|bridge) to ap_objects if not present).
- `internal/ingest/follow.go` — community subscription lifecycle: admin/API
  trigger `POST /admin/communities {"community":"!tech@lemmy.world"}` →
  WebFinger resolve → fetch Group → materialize community → signed
  `Follow` from service actor → await Accept; `DELETE` → `Undo{Follow}`.
  Simple bearer-token admin auth (`ADMIN_TOKEN` env).
- `internal/ingest/backfill.go` — on follow-accept (and on demand): page
  the Group outbox (task 02 FetchCollection), materialize newest→oldest up
  to `BACKFILL_MAX_POSTS` (default 100) + each post's replies collection
  if advertised; rate-limited, resumable via last_backfill_at.
- `internal/ingest/consent.go` — scan actor summary/tags for
  `#nobridge`/`#nobot` on first sight and on profile Update → set
  consent_state, tombstone existing content if switching to nobridge;
  `Delete(Actor)` handling already in materializer — wire it. Document the
  policy in README (mirrors Bridgy Fed norms).
- Tests: signature verify against fixtures (valid, bad digest, expired,
  key rotation re-fetch); dedupe; ordering (two comments same community
  arrive out of order → both land, parent-fetch path exercised); follow
  state machine; echo suppression; consent transitions. Use a fake Lemmy
  HTTP server (httptest) serving task 02 fixtures.

## Definition of done
- With the fake Lemmy server: subscribe → Accept → Announce{Create{Page}}
  → post record visible via task 04 firehose; backfill produces mapped
  history; nobridge author's comment dropped with logged reason.
- `go test ./...` green.

## References
- `~/Code/bridgy-fed/activitypub.py` (inbox verification, Announce
  unwrapping, quirks), `ids.py` (echo/copies logic), FEP-1b12.
- Lemmy federation docs for Follow/Accept + outbox shapes.
