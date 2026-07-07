# Task 08 — E2E harness: real Lemmy → Tidepool → Jetstream (~1000 LOC)

## Goal
Prove the whole read path against real infrastructure, Coves-style ("E2E
tests must test REAL infrastructure - not mocks"): a real Lemmy container
federating with Tidepool, records flowing out our firehose, decoded by a
real Jetstream, validating against real Coves lexicons.

## Deliverables
- `docker-compose.e2e.yml` (or profile in dev compose): postgres, PLC
  directory, Tidepool (built from Dockerfile — write the Dockerfile,
  multi-stage, distroless-ish), Jetstream pointed at Tidepool, **Lemmy**
  (lemmy + lemmy postgres + pictrs; use dessalines/lemmy image with
  federation enabled, allowlist tidepool host; both services on one
  compose network with hostnames — Lemmy requires HTTPS in prod mode but
  supports a debug/local federation mode; if TLS is unavoidable, add a
  caddy with internal CA like Coves' Caddyfile.dev pattern). Getting
  Lemmy↔bridge federation working in compose is the hard 60% of this task
  — budget accordingly; crib from Lemmy's own `docker/federation` compose
  in github.com/LemmyNet/lemmy which runs multi-instance federation
  locally over HTTP.
- `tests/e2e/helpers.go` — Lemmy API client for test setup only (create
  site/admin, community, post, comment, vote via Lemmy's HTTP API);
  Jetstream WebSocket listener with per-collection matchers + timeouts;
  Tidepool admin client (subscribe community).
- `tests/e2e/bridge_test.go` — the scenarios:
  1. Subscribe `!testing@lemmy` → community.profile appears on firehose,
     validates against Coves lexicon.
  2. Lemmy user posts → actor.profile then community.post (in the
     community DID's repo, author = user DID) appear, in that order.
  3. Comment + nested reply → comment records with correct root/parent
     strongRefs (resolve them: parent uri/cid match earlier events).
  4. Edit post → update event; delete comment → delete op on firehose.
  5. Votes → getVoteAggregates XRPC reflects them; no vote records on
     the firehose.
  6. Backfill: pre-existing posts appear after subscribe.
  7. Idempotency/restart: restart Tidepool container mid-test, replay
     causes no duplicate rkeys and Jetstream cursor resume works.
- Lexicon conformance suite: every record type Tidepool emits validated
  against `~/Code/coves` lexicons synced by `scripts/sync-lexicons.sh`
  (already vendored in task 05 — add a CI check that they're in sync).
- Makefile: `make e2e` (compose up, wait-for-healthy, run
  `go test ./tests/e2e/... -tags e2e`, compose down), `make e2e-logs`.
- CI workflow file (GitHub Actions) running unit tests always, e2e on
  demand/label (it's heavy).
- README "Running the stack" section + architecture diagram refresh.

## Definition of done
- `make e2e` passes from a clean checkout on this machine.
- A written FOLLOWUPS.md capturing everything discovered but deferred
  (PieFed quirks, relay requestCrawl in prod, key claiming, write-side).

## References
- `~/Code/coves/docker-compose.dev.yml`, `Caddyfile.dev`, `Makefile`,
  `tests/integration/helpers.go` (harness conventions).
- LemmyNet/lemmy `docker/federation/` (local federation compose),
  `api_tests/` (their own federation test patterns).
