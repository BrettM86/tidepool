# Task 09 — E2E relay: full pipeline Lemmy → Tidepool → relay → Jetstream (~600 LOC)

## Goal
Put a real atproto relay (indigo BigSky) between the bridge and Jetstream
in the e2e stack, so every scenario's records transit the strictest
consumer that exists: DID resolution against the local PLC, per-commit
signature verification, and initial `getRepo` crawls. Closes the FOLLOWUPS
item "relay `requestCrawl` has never been exercised against a real relay"
and turns the sync surface's correctness claims (CAR exports, proofs,
cursor semantics) into things a hostile consumer verifies on every run.

## Locked decisions
- **One pipeline, through the relay.** Jetstream consumes from the relay,
  not the bridge, so every existing and future scenario exercises relay
  validation for free (locked requirement: every state has a full
  Lemmy → PDS record → firehose pipeline where applicable). Keep the
  direct bridge→Jetstream wiring available behind a compose profile for
  debugging, not as the tested path.
- **The bridge's own `RequestCrawlAll` must send the real request.** The
  e2e stack runs `ENVIRONMENT=development`, where `RELAY_HOSTS` is
  log-only. Add a narrowly-scoped dev override (mirror the
  `ALLOW_PRIVATE_FETCH` pattern: dev-only, refused/ignored in production
  where sending is already the behavior) so the harness drives the real
  code path. The relay's admin API (`POST /admin/pds/requestCrawl`,
  Coves' pattern) is an acceptable suite *fallback/bootstrap*, but the
  DoD is bridge-originated crawl.
- Local-only invariant holds: loopback-only host binds, nothing ever
  contacts a public relay or plc.directory.

## Deliverables
- `docker-compose.e2e.yml`: relay service — the pinned bigsky image Coves
  uses (`ghcr.io/bluesky-social/indigo:bigsky-0a2d4173e6e89e49b448f6bb0a6e1ab58d12b385`,
  bump deliberately if needed), its own postgres (or a second database in
  an existing one — decide and comment), `BGS_CRAWL_INSECURE_WS=true`
  (ws:// upstream) *[verified outcome: this env var does not exist in the
  pinned image — `--crawl-insecure-ws` is arg-only and is passed as a
  command argument instead; see FOLLOWUPS.md]*, admin key, healthcheck,
  loopback-only host port.
  **Identity resolution must point at the compose `plc` service** — find
  bigsky's actual PLC-host flag/env in the indigo source (verify, don't
  guess) or the relay will try the public directory and fail closed.
- Jetstream re-pointed at the relay's firehose; existing scenarios pass
  unchanged through the longer pipeline (expect timing budgets to need
  loosening — crawl + validation adds latency).
- Bridge sends `requestCrawl` to the relay on startup via the real
  `RequestCrawlAll` path (`RELAY_HOSTS=<relay>` + the new dev override).
- `tests/e2e`: relay-specific assertions — the bridge host appears in the
  relay's crawled-PDS state; repos are listed/crawled; the restart/replay
  scenario still proves cursor resume *through the relay*; tombstoned
  (`active:false`) repo status observed through the relay where its API
  exposes it.
- **Known rock, investigate and document:** bridged handles
  (`alice.lemmy.tidepool`) do not resolve in compose DNS — determine
  whether bigsky's handle verification failure is non-fatal (likely:
  marks handle invalid, keeps repo) and document the posture; add
  network aliases only if actually required.
- README stack diagram + runbook update; FOLLOWUPS updates for anything
  discovered and deferred.

## Definition of done
- `make e2e` green from a clean checkout with all scenarios transiting
  Lemmy → tidepool → relay → Jetstream.
- Bridge-originated `requestCrawl` observed in relay state/logs (asserted,
  not eyeballed).
- Unit suite still green; nothing contacts public infrastructure.

## References
- `~/Code/coves/docker-compose.dev.yml` relay stanza (lines ~248–296):
  image pin, `BGS_CRAWL_INSECURE_WS`, admin requestCrawl curl.
- indigo source (`cmd/bigsky`) for flags/env: PLC host, admin API routes.
- `internal/sync/crawl.go` `RequestCrawlAll`; `cmd/tidepool/main.go`
  dev-mode skip.
