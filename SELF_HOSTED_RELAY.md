# Self-hosted relay + Jetstream (the app's ingest path)

## Why this exists

Diagnosed 2026-07-16: bsky.network caps a new PDS at **100 accounts**
(plus ~50 ev/s per-host stream consumption). tdpl.io mints one repo per
bridged fediverse user and was at ~950 repos; every account past the
quota reports `{"active":false,"status":"throttled"}` from the public
relay and its commits are **silently dropped** — they never reach the
Bluesky-run Jetstream instances the Coves AppView consumes. Symptom
that surfaced it: a bridged thread showing "1 comment" (an orphaned
reply from one of the ~90 active accounts) with an empty comment tree —
28 of 29 commenters were throttled. Even active accounts lagged ~1.3 h
behind the PDS due to the consumption throttle.

The fix: Coves' completeness must not depend on a third party's quota.
The `relay` and `jetstream` services in `docker-compose.prod.yml` form a
private ingest path that carries ONLY our two PDS hosts:

```
tdpl.io ──────────┐
                  ├─► relay (:2470, host-local) ─► jetstream (:8080, internal) ─► Coves @self consumers
pds.coves.me ─────┘

tdpl.io / pds.coves.me ─► bsky.network ─► jetstream2.us-east ─► Coves @bsky consumers   (unchanged; public visibility + third-party PDS records)
```

`pds.coves.me` is included deliberately: it has its own 100-account cap
at bsky.network, so native Coves signup #101 would silently vanish from
the app exactly like the bridged commenters did.

Design decisions and their reasons live as comments on the compose
services; this file is the runbook.

## Not in scope here (Phase 2+, Coves repo)

- Pointing Coves' consumers at `ws://tidepool-prod-jetstream:8080/subscribe`
  (second consumer set, distinct consumer names).
- **Rev-gating in the consumers must land first** — dual feeds break the
  per-repo ordering the handlers assume; without gating on the commit
  `rev`, a stale cross-feed create resurrects deleted comments and a
  stale update clobbers newer content.
- `POST /admin/reemit` backfill of records the public relay discarded.

## Deploy (first time)

On the box (`/opt/tidepool`), after `git pull`:

1. Add to `/opt/tidepool/.env`:

   ```
   RELAY_ADMIN_PASSWORD=<openssl rand -hex 32>
   ```

2. Build + start (targeted services only — never bare `up -d`):

   ```
   docker compose -f docker-compose.prod.yml up -d --build relay
   ```

   The build context is a pinned upstream git SHA; docker fetches it
   directly. Wait for healthy: `docker inspect -f '{{.State.Health.Status}}' tidepool-prod-relay`

3. One-time host bootstrap (this replaces a RELAY_HOSTS announcement —
   Tidepool's production requestCrawl egress is SSRF-guarded and
   correctly refuses the relay's private address):

   ```
   curl -s -X POST localhost:2470/xrpc/com.atproto.sync.requestCrawl \
     -H 'Content-Type: application/json' -d '{"hostname":"tdpl.io"}'
   curl -s -X POST localhost:2470/xrpc/com.atproto.sync.requestCrawl \
     -H 'Content-Type: application/json' -d '{"hostname":"pds.coves.me"}'
   ```

   The relay validates each host by calling back into its public
   `describeServer` over https (hairpin through Caddy), then subscribes.
   The host table persists in the `relay-data` volume — restarts do NOT
   need re-announcement, only a volume wipe does.

4. Verify the relay is consuming both hosts:

   ```
   curl -s 'localhost:2470/xrpc/com.atproto.sync.listHosts' | jq .
   curl -s 'localhost:2470/xrpc/com.atproto.sync.getHostStatus?hostname=tdpl.io' | jq .
   ```

   Expect both hosts `active` with advancing `seq`, and accountCount
   ≈ the PDS's repo count (NOT capped at 100 — `RELAY_DEFAULT_ACCOUNT_LIMIT`
   is set effectively unlimited; only our own hosts can ever be added).

5. Start Jetstream:

   ```
   docker compose -f docker-compose.prod.yml up -d jetstream
   ```

   First boot bootstrap-backfills every repo from the relay (minutes at
   our scale). Watch `docker logs -f tidepool-prod-jetstream` until it
   settles into live tailing.

6. End-to-end proof (verified at deploy, 2026-07-17). Jetstream is not
   on a host port; probe from a throwaway container on the internal
   network. Two gotchas learned the hard way: websocat's `-E` exits on
   stdin EOF — over non-interactive SSH that's instantly, making a
   healthy stream look empty (use `-U`); and a `cursor` older than the
   stored window replays nothing (pick one inside it).

   ```
   # replay recent events (cursor is µs since epoch)
   cursor=$((($(date +%s) - 900) * 1000000))
   docker run --rm --network tidepool-prod-internal solsson/websocat \
     -U "ws://jetstream:8080/subscribe?cursor=$cursor" | head -c 2000

   # relay-side truth: events received from PDSs vs sent to jetstream
   docker run --rm --network tidepool-prod-internal curlimages/curl -s \
     http://relay:2471/metrics | grep -E "events_(received|sent)_counter"

   # jetstream-side: subscriber gauge + its own metrics
   docker run --rm --network tidepool-prod-internal curlimages/curl -s \
     http://jetstream:6060/metrics | grep jetstream_subscribe
   ```

   The clincher: pick a `did` from the replayed events and check
   `bsky.network/xrpc/com.atproto.sync.getRepoStatus?did=...` — for a
   throttled account, that event is provably absent from the public
   path, so seeing it here proves the quota bypass works. (At deploy,
   a live bridged comment from a throttled account came through within
   minutes of starting the tap.)

   Note the relay live-tails the PDSs from "now" — it does NOT replay
   history that predates its first subscription (the same cold-start
   property bsky.network has). Pre-existing records reach Coves via
   `POST /admin/reemit` in Phase 3, not via this pipeline's backlog.

## Ongoing ops

- **Never** expose 2470/8080 publicly; never run `relay pull-hosts`.
  The relay having only our hosts is a property the whole design leans
  on (unlimited account quota is safe ONLY because of it).
- Upgrades: bump the pinned SHA in the compose `build.context`, then
  `up -d --build relay` (or `jetstream`). Treat like any dependency bump.
- Disk: relay keeps a 72 h replay window under `relay-data`; jetstream
  keeps its own store under `jetstream-data`. Both are trivial at two
  hosts' volume.
- Debug surface: `localhost:2470` also serves the relay admin web UI
  (login with `RELAY_ADMIN_PASSWORD`) and `/metrics` on the internal
  `:2471`.
