# Self-hosted relay and Jetstream

Production runs its own indigo relay and Jetstream so that every commit from
our two PDS hosts reaches Coves, whatever quota bsky.network applies. This
file is the runbook for that ingest path. The full deploy (config gate,
`CONSUMER_ENABLED`/`OUTBOUND_WORKERS` staging order, coves.social Caddy
change, rollout and rollback) is in [`DEPLOY.md`](DEPLOY.md). The reasons
behind individual settings are comments on the compose services.

## Why

bsky.network caps a new PDS at 100 accounts and about 50 events per second
of stream consumption (diagnosed 2026-07-16). tdpl.io mints one repo per
bridged fediverse user and had about 950 repos. Every account past the quota
reports `{"active":false,"status":"throttled"}` on the public relay, and its
commits never reach the Bluesky-run Jetstream instances. The symptom was a
bridged thread showing "1 comment" with an empty comment tree: 28 of its 29
commenters were throttled. Active accounts also lagged about 1.3 hours behind
the PDS because of the consumption throttle.

`pds.coves.me` is included because it has its own 100-account cap at
bsky.network. Without it, native Coves signup number 101 would disappear from
the app the same way.

## Layout

The `relay` and `jetstream` services in `docker-compose.prod.yml` carry only
our two PDS hosts:

```
tdpl.io ──────────┐
                  ├─► relay (:2470, host-local) ─► jetstream (:8080, internal) ─► Coves "self" feed
pds.coves.me ─────┘

tdpl.io / pds.coves.me ─► bsky.network ─► jetstream2.us-east ─► Coves "bsky" feed
whole network ─► relay.fire.hose.cam ─► jetstream.fire.hose.cam ─► Coves "fire" feed
```

Coves reads all three feeds through `JETSTREAM_FEEDS` in its
`docker-compose.prod.yml`
(`self=ws://tidepool-prod-jetstream:8080` over `coves-prod-network`). Coves
rev-gates each repo's commits, so the overlapping feeds do not reorder or
resurrect records. Expect `rev-gate: skipped stale` lines in Coves logs for
whichever copy arrives later.

Tidepool's own consumer reads the same Jetstream over `JETSTREAM_URL`
(`ws://tidepool-prod-jetstream:8080/subscribe`). Port 8080 is
`JETSTREAM_ADDR`. Port 6060 is the debug listener and serves no `/subscribe`.

| Service | Container | Ports | Volume |
| --- | --- | --- | --- |
| `relay` | `tidepool-prod-relay` | `127.0.0.1:2470` (API, admin UI), `:2471` metrics on the internal network | `relay-data` |
| `jetstream-init` | `tidepool-prod-jetstream-init` | none; one-shot `chown` of the volume to uid 65532 | `jetstream-data` |
| `jetstream` | `tidepool-prod-jetstream` | `:8080` `/subscribe`, `:6060` debug, both internal only | `jetstream-data` |

## First deploy

On the box (`/opt/tidepool`), after `git pull`:

1. Add to `/opt/tidepool/.env`:

   ```
   RELAY_ADMIN_PASSWORD=<openssl rand -hex 32>
   ```

2. Build and start the relay. Start only the named service. Never run a bare
   `up -d`.

   ```
   docker compose -f docker-compose.prod.yml up -d --build relay
   ```

   The build context is a pinned upstream git SHA, which docker fetches
   directly. Wait until it is healthy:
   `docker inspect -f '{{.State.Health.Status}}' tidepool-prod-relay`

3. Add the two hosts by hand. Tidepool's `requestCrawl` announcement cannot
   do this, because its SSRF guard refuses the relay's private address.

   ```
   curl -s -X POST localhost:2470/xrpc/com.atproto.sync.requestCrawl \
     -H 'Content-Type: application/json' -d '{"hostname":"tdpl.io"}'
   curl -s -X POST localhost:2470/xrpc/com.atproto.sync.requestCrawl \
     -H 'Content-Type: application/json' -d '{"hostname":"pds.coves.me"}'
   ```

   The relay checks each host by calling its public `describeServer` over
   https (back through Caddy), then subscribes. The host table lives in the
   `relay-data` volume. Restarts do not need this step again. A volume wipe
   does.

4. Check that the relay is consuming both hosts:

   ```
   curl -s 'localhost:2470/xrpc/com.atproto.sync.listHosts' | jq .
   curl -s 'localhost:2470/xrpc/com.atproto.sync.getHostStatus?hostname=tdpl.io' | jq .
   ```

   Both hosts should be `active` with an advancing `seq`, and `accountCount`
   should be close to the PDS's repo count, not capped at 100.
   `RELAY_DEFAULT_ACCOUNT_LIMIT` is 10000000. That is safe only because the
   relay holds no hosts but ours.

5. Start Jetstream:

   ```
   docker compose -f docker-compose.prod.yml up -d jetstream
   ```

   On an empty volume, Jetstream first backfills. It calls `listHosts` on
   the relay, then `listRepos` and `getRepo` against each PDS over https
   (public addresses only). `/subscribe` answers 503 until the backfill
   finishes, which takes minutes at our scale. Watch
   `docker logs -f tidepool-prod-jetstream` until it switches to live
   tailing.

6. Check end to end (done at the first deploy, 2026-07-17). Jetstream has no
   host port, so probe it from a throwaway container on the internal
   network. Use websocat's `-U`. Its `-E` exits on stdin EOF, which over
   non-interactive SSH happens at once and makes a healthy stream look
   empty. Pick a `cursor` inside the stored window. An older cursor replays
   nothing.

   ```
   # replay recent events (cursor is µs since epoch)
   cursor=$((($(date +%s) - 900) * 1000000))
   docker run --rm --network tidepool-prod-internal solsson/websocat \
     -U "ws://jetstream:8080/subscribe?cursor=$cursor" | head -c 2000

   # relay side: events received from PDSs vs sent to jetstream
   docker run --rm --network tidepool-prod-internal curlimages/curl -s \
     http://relay:2471/metrics | grep -E "events_(received|sent)_counter"

   # jetstream side: subscriber gauge and its own metrics
   docker run --rm --network tidepool-prod-internal curlimages/curl -s \
     http://jetstream:6060/metrics | grep jetstream_subscribe
   ```

   To prove the quota bypass, take a `did` from the replayed events and
   check `bsky.network/xrpc/com.atproto.sync.getRepoStatus?did=...`. If that
   account is throttled there, its event is missing from the public path,
   so seeing it here shows this path works. At the first deploy, a bridged
   comment from a throttled account came through within minutes.

   The relay tails the PDSs from the moment it subscribes. It does not
   replay older history, the same as bsky.network on a cold start. Records
   that predate the subscription reach Coves through `POST /admin/reemit`
   (see the [admin API](docs/ADMIN_API.md)), not through this path.

## Ongoing ops

- Never expose 2470 or 8080 publicly. Never run `relay pull-hosts`. The
  unlimited account quota is safe only while the relay holds just our hosts.
- Upgrades: bump the pinned SHA in the service's `build.context`, then run
  `docker compose -f docker-compose.prod.yml build relay` and
  `docker compose -f docker-compose.prod.yml up -d --no-deps relay` (or the
  same for `jetstream`). Before a new Jetstream version first starts, stop
  Jetstream and copy the `jetstream-data` volume. A newer Jetstream can
  change the data dir so the old version no longer resumes from it, and the
  copy is the only rollback.
- Disk: the relay keeps a 72-hour replay window (`RELAY_REPLAY_WINDOW`) in
  `relay-data`. Jetstream keeps its own store in `jetstream-data`. Both are
  small at two hosts' volume.
- Debugging: `localhost:2470` also serves the relay admin web UI (log in
  with `RELAY_ADMIN_PASSWORD`). Relay metrics are on `:2471` inside the
  internal network.
