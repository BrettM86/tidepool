# Configuration

Tidepool reads every setting from environment variables once, at process
start (`config.Load`). There is no reload signal. To change a value, recreate
the container.

## Rules that apply to every variable

- `ENVIRONMENT` is `development` (the default) or `production`. Any other value
  stops the process at boot.
- Seven variables are required in production: `DATABASE_URL`, `LISTEN_ADDR`,
  `BRIDGE_HOSTNAME`, `PLC_DIRECTORY_URL`, `BRIDGE_KEK`, `ADMIN_TOKEN` and
  `AP_USER_ORIGIN`. Development fills them with fixed dev defaults. In
  production an unset one stops the process with
  `config: <NAME> is required in production`. Several of these defaults are
  public (the KEK, the admin token), so never run production on them.
- Production refuses to start when any of these is set: `BRIDGE_SCHEME=http`,
  an `http://` `AP_USER_ORIGIN`, `ALLOW_PRIVATE_FETCH`,
  `ALLOW_DEV_REQUEST_CRAWL`, `AP_HOST_FALLTHROUGH_DEV`.
- Every other variable has the same default in every environment. Each applied
  default is logged at startup.
- Integer and duration settings must be positive. `0` or a negative value stops
  the process at boot. The exceptions are `OUTBOUND_WORKERS` and
  `ADMISSION_MAX_PER_AUTHOR_PER_COMMUNITY`, where `0` is allowed. Durations use
  Go syntax (`30s`, `15m`, `72h`).
- Boolean settings accept `1/0`, `true/false`, `yes/no` and `on/off`, case
  insensitive. An unrecognized value stops the process at boot. The two dev
  opt-ins, `ALLOW_PRIVATE_FETCH` and `ALLOW_DEV_REQUEST_CRAWL`, read any
  unrecognized value as off.
- The `tidepool migrate` and `tidepool rotate-kek` subcommands do not call
  `config.Load`. `migrate` reads only `DATABASE_URL`. `rotate-kek` reads
  `DATABASE_URL`, `BRIDGE_KEK` and `BRIDGE_KEK_PREVIOUS`, and requires all three.

## Core and identity

| Variable | Default | What it does |
|---|---|---|
| `ENVIRONMENT` | `development` | `development` or `production`. Development runs migrations on start, applies dev defaults, and fails writes on lexicon validation errors. Production logs those errors and writes anyway. |
| `DATABASE_URL` | local dev Postgres on port 5442 | Postgres connection string for bridge state. Required in production. |
| `LISTEN_ADDR` | `:8091` | HTTP bind address. Required in production. |
| `BRIDGE_HOSTNAME` | `localhost` | Public domain of the bridge. Bridged handles and the PDS endpoint in minted DID documents are built from it. Required in production. |
| `BRIDGE_SCHEME` | `https` | Scheme of the bridge's own ActivityPub URLs (actor ids, inbox, activity ids). `http` exists for the local e2e stack and is refused in production. |
| `PLC_DIRECTORY_URL` | `http://localhost:3002` | did:plc directory. The dev default is the local PLC from `make plc-up`. Production uses `https://plc.directory`. Required in production. |
| `BRIDGE_KEK` | fixed public dev key | 32-byte key-encryption key, as 64 hex characters or base64. Seals per-actor signing keys and the escrow rotation key at rest (AES-256-GCM). Required in production. |
| `BRIDGE_KEK_PREVIOUS` | unset | The old KEK during a rotation, same encodings. Must decode to a different key than `BRIDGE_KEK`. See the note below. |
| `BRIDGE_SERVICE_DID` | unset | Pre-provisioned DID for the bridge's own service actor. When unset, the bridge uses `did:web:<BRIDGE_HOSTNAME>` and serves it at `/.well-known/did.json`; nothing is registered with PLC. |
| `AP_USER_ORIGIN` | `http://localhost:8091` | Origin that Coves users' ActivityPub actors live under, for example `https://coves.social`. Must be `https` in production. Its host must not be `BRIDGE_HOSTNAME` or a subdomain of it. Required in production. |
| `AP_HOST_FALLTHROUGH_DEV` | on in development, off in production | Sends requests with an unknown `Host` to the bridge instead of answering 421. Refused in production. |
| `ADMIN_TOKEN` | `dev-admin-token` | Bearer token for the `/admin` API. Required in production. |
| `USER_AGENT` | `tidepool/0.1 (Coves bridge; +https://<BRIDGE_HOSTNAME>)` | User agent for bridge traffic: signed fetches, media, relay `requestCrawl`, PLC. Deliveries sent as Coves users always use `coves/0.1 (+<AP_USER_ORIGIN>)`, and this setting does not change that. |
| `ALLOW_PRIVATE_FETCH` | off | Dev only. Turns off the SSRF egress guard for ActivityPub fetches and PLC requests so localhost targets work. Refused in production. |

`AP_USER_ORIGIN` is written into every `actor_id` minted under it. Changing it
later does not move existing actors, because serving reads the stored row.
Pick the final value before the first Coves user federates.

To rotate the KEK, set the old key as `BRIDGE_KEK_PREVIOUS` and the new one as
`BRIDGE_KEK`. While both are set, sealed material opens under either key and
new material is sealed only under `BRIDGE_KEK`. Run `tidepool rotate-kek` to
re-seal every blob under `BRIDGE_KEK`, then unset `BRIDGE_KEK_PREVIOUS`. The
runbook is [DEPLOY.md](../DEPLOY.md) §6.

## DNS and handles

| Variable | Default | What it does |
|---|---|---|
| `HANDLE_ZONE_ROOT` | `ap.<BRIDGE_HOSTNAME>` | DNS zone for new-format bridged handles. Must be a valid hostname strictly under `BRIDGE_HOSTNAME`. |
| `HANDLE_MIGRATION_RATE` | `0.1` | PLC handle updates per second sent by the admin handle migration job. Any positive number. See the note below. |
| `DNS_LISTEN` | empty (DNS off) | UDP and TCP listen address for the built-in DNS server, for example `:53`. Serves the handle zone and per-instance subzones (TXT, SOA, NS, A, AAAA). Zone transfers are refused. When set, `BRIDGE_HOSTNAME` must be a domain name with no port. |
| `DNS_UDP_RESPONSE_RATE` | `200` | UDP responses per second per client IPv4 /24 or IPv6 /56. Over the limit, every second suppressed response is an empty truncated reply and the rest are dropped. TCP is not limited. |
| `DNS_PUBLIC_IPV4` | unset | IPv4 address returned in A answers. Required when `DNS_LISTEN` is set. |
| `DNS_PUBLIC_IPV6` | unset | IPv6 address returned in AAAA answers. Optional. |
| `DNS_NAMESERVERS` | `ns1.<BRIDGE_HOSTNAME>,ns2.<BRIDGE_HOSTNAME>` | Comma-separated nameserver hostnames for apex NS answers. The first is the SOA primary. |
| `CLOUDFLARE_API_TOKEN` | empty (delegation off) | Cloudflare API token with DNS edit rights on the zone. Turns on NS delegation, described below. Requires `DNS_LISTEN`, `CLOUDFLARE_ZONE_ID` and `DELEGATION_MAX_NS_RECORDS`. |
| `CLOUDFLARE_ZONE_ID` | unset | Zone ID of the Cloudflare zone holding `BRIDGE_HOSTNAME`. Required when `CLOUDFLARE_API_TOKEN` is set. |
| `DELEGATION_MAX_NS_RECORDS` | unset | Ceiling on NS records pointing at the configured nameservers in the zone. Required when `CLOUDFLARE_API_TOKEN` is set. Use 3,000 on Cloudflare Pro and 150 on Free. |

`DNS_PUBLIC_IPV4`, `DNS_PUBLIC_IPV6` and `DNS_NAMESERVERS` are read only when
`DNS_LISTEN` is set. `DELEGATION_MAX_NS_RECORDS` is read only when
`CLOUDFLARE_API_TOKEN` is set.

Handle migration: each PLC request, retries included, is sent at least
1/rate seconds after the previous one. A PLC 429 pauses the whole run. See
[HANDLE_SCHEME.md](../HANDLE_SCHEME.md).

Delegation runs one pass at startup and one every 15 minutes. Each pass creates
`<label>.<BRIDGE_HOSTNAME> NS <first DNS_NAMESERVERS entry>` records, oldest
qualified label first. Creation is capped at 20 records per rolling hour across
the whole zone, counted from Cloudflare's `created_on`. A pass also stops at
`DELEGATION_MAX_NS_RECORDS`. Labels it skips wait for a later pass.

## Ingest and rate limits

| Variable | Default | What it does |
|---|---|---|
| `INGEST_WORKERS` | `4` | Inbox queue worker count. |
| `BACKFILL_MAX_POSTS` | `100` | Posts materialized per community backfill run. |
| `MINT_RATE_PER_MINUTE` / `MINT_BURST` | `60` / `120` | Rate limit on minting DIDs for inbound authors. Unseen authors in delivered content trigger mints, and PLC registrations are permanent. |
| `SEED_COUNTS_FROM_API` | on | Seeds backfilled posts' vote counts from the community's host through Lemmy's public API. Uses `/api/v3/post` for posts on that host and anonymous `/api/v3/resolve_object` for posts from other instances. Never queries the post's own host. Set `0` to turn off. |
| `MAX_BLOB_BYTES` | `5242880` (5 MiB) | Largest remote media file (avatar, banner, post image) the materializer downloads. Larger files are dropped, never truncated. Lexicon slots set tighter limits of their own, for example 1 MB for avatars. |
| `PROFILE_REFRESH_TTL` | `24h` | How old a bridged actor's profile may get before it is fetched again. `Update{Person}` and `Update{Group}` refresh it immediately. The TTL catches what Lemmy does not federate, such as bio edits and the `#nobridge` marker. |
| `INBOX_IP_RATE_PER_SECOND` / `INBOX_IP_RATE_BURST` | `50` / `200` | Per-client-IP limit on `POST /inbox`. Refusals are 503, which federation queues retry. |
| `INBOX_SIGNER_RATE_PER_SECOND` / `INBOX_SIGNER_RATE_BURST` | `20` / `100` | Per-verified-signer limit on `POST /inbox`. |
| `INBOX_TOMBSTONE_CONFIRMS_PER_MINUTE` / `INBOX_TOMBSTONE_CONFIRM_BURST` | `6` / `10` | Per-IP limit on the unauthenticated path that confirms a tombstoned actor's self-delete. Each request costs an outbound fetch and database writes. Over the limit the delivery gets 503 so the sender retries the deletion. |
| `SYNC_RATE_PER_SECOND` / `SYNC_RATE_BURST` | `25` / `200` | Per-client-IP limit on the public `com.atproto.sync.*` endpoints. Refusals are 429. `_health` is exempt. |
| `SYNC_MAX_SUBSCRIBERS` | `100` | Maximum concurrent `subscribeRepos` connections. |
| `RELAY_HOSTS` | unset | Comma-separated relays sent `com.atproto.sync.requestCrawl` at startup, each retried for a bounded time. In development the request is logged and not sent unless `ALLOW_DEV_REQUEST_CRAWL` is on. |
| `ALLOW_DEV_REQUEST_CRAWL` | off | Dev only. Sends `requestCrawl` to `RELAY_HOSTS` in development, for the e2e stack's local relay. Refused in production, which always sends. |
| `FOLLOW_LIST_PATH` | unset | Path to the declarative follow list. See [ADMIN_API.md](ADMIN_API.md#declarative-follow-list-follow_list_path). When unset, the `/admin` API is the only way to manage subscriptions. |
| `FOLLOW_LIST_INTERVAL` | `15m` | How often the follow-list reconciler runs. |

## Outbound federation

Outbound federation sends native Coves users' opt-outs, profiles, posts,
comments and votes to ActivityPub peers. Delivery runs only when
`CONSUMER_ENABLED` is on and `OUTBOUND_WORKERS` is above 0. There is no
`OUTBOUND_ENABLED` variable.

| Variable | Default | What it does |
|---|---|---|
| `CONSUMER_ENABLED` | off | Starts the Jetstream consumer. It writes durable outbound state: queued deliveries, community-signed acceptances for postv2 posts, and opt-out and account-deletion purges at peers. Also enables `POST /admin/actors/purge` and the `/admin/admissions` routes. Requires `JETSTREAM_URL`. |
| `JETSTREAM_URL` | unset | Self-hosted Jetstream to subscribe to (`ws://` or `wss://`). Validated whenever set, so it can be staged before `CONSUMER_ENABLED` and a typo fails at boot. |
| `OUTBOUND_WORKERS` | `0` | Number of delivery workers. With the consumer on and this at `0`, deliveries queue up and nothing is sent. Raising it sends the whole backlog right away. |
| `OUTBOUND_DISABLED` | off | Global delivery kill switch. Blocked deliveries are parked: they stay `pending` and resume when the switch is cleared. |
| `OUTBOUND_DISABLED_HOSTS` | empty | Comma-separated inbox hosts to park. Case-insensitive. |
| `OUTBOUND_DISABLED_COMMUNITIES` | empty | Comma-separated community ActivityPub ids to park, for example `https://lemmy.world/c/comicstrips`. Exact, case-sensitive match. |
| `OUTBOUND_DISABLED_ACTORS` | empty | Comma-separated actor DIDs to park. Exact match. |
| `OUTBOUND_DRY_RUN` | off | Logs each claimed delivery and sends nothing. The worker parks before signing, so deliveries resume when the flag is cleared. |
| `ADMISSION_MAX_PER_AUTHOR_PER_COMMUNITY` | `50` | Most posts one native author may have accepted into one bridged community. Tidepool signs the community's acceptance, so this caps what it vouches for. `0` means no limit. |

Notes on the kill switches and dry run:

- A park does not use up a retry attempt and never poisons or cancels a
  delivery. No `redrive` is needed afterward.
- While a switch is engaged and workers are running, each ordering key's head
  delivery is claimed and parked again about every 5 seconds. That is a
  steady stream of database writes. To stop all delivery, `OUTBOUND_WORKERS=0`
  is cheaper, because no worker is built.
- The switches and dry run have no effect while `OUTBOUND_WORKERS=0`.
- There is no allowlist. To canary one community, disable every other
  community.
- Dry run does not test the translator. Translation happens when a delivery is
  queued, inside the consumer, and the worker sends the stored payload as is.
  The translator has already run on everything once `CONSUMER_ENABLED` is on.
- Do not use `redrive` to resume after a dry run. It matches only `poisoned`
  rows, and an unscoped `{"all":true}` redrive would replay unrelated poisoned
  deliveries.

See [DEPLOY.md](../DEPLOY.md) §2 for how these flags fit together and §5 for the rollout order.

## Retention and tuning

| Variable | Default | What it does |
|---|---|---|
| `FIREHOSE_RETENTION` | `72h` | How long `firehose_events` rows are kept for `subscribeRepos` cursor replay. Pruned hourly. |
| `TOMBSTONE_RETENTION` | `720h` | How long `ap_tombstones` markers are kept. They stop a Create that arrives after its Delete from recreating the object. Pruned hourly. |
| `VOTE_EVENT_RETENTION` | `2160h` | How long undone (superseded or retracted) `vote_events` rows are kept. Live rows are the counts and are never pruned. |
| `BLOCKS_GC_RETENTION` | `72h` | How long unreachable repo blocks are kept before the 6-hourly GC sweep deletes them. Keep it far above the sweep's run time and any clock skew between the app and the database. See the note below. |
| `MST_CACHE_SIZE` | `512` | Number of repos held as decoded MST trees in memory. Memory use grows with the size of the cached repos. Lower it when bridging many very large communities. |
| `STATS_REFRESH_INTERVAL` | `30s` | How often bridged vote counts are written onto post and comment records (the `bridgedStats` field). Votes on one subject within an interval become one record update. Longer means staler counts and fewer firehose events. |
| `STATS_REFRESH_BATCH` | `200` | Most vote aggregates processed per refresh. The rest wait for the next one. Keeps one refresh from holding the commit lock for too long. |
| `DIVERGENCE_INTERVAL` | `15m` | How often the reconciliation sweep compares atproto state with outbound state and updates the `tidepool_divergence_*` gauges. The sweep always runs, once at startup and then on this interval. It only reports and never repairs. `GET /admin/divergence` runs one on demand. |
| `DIVERGENCE_ACCEPTANCE_STALE_AFTER` | `12h` | How long a pending delivery may wait before the sweep reports its acceptance as stale. Shorter flags every in-flight post. Longer hides a queue that stopped hours ago. |

`BLOCKS_GC_RETENTION`: the retention window also protects a block that a
concurrent commit makes reachable again. The cutoff uses the app clock while
the commit's `created_at` refresh uses the database clock, so the window must
exceed clock skew plus the gap between the sweep selecting and deleting blocks.
See `internal/repo/gc.go`.

`DIVERGENCE_ACCEPTANCE_STALE_AFTER`: a failing delivery is poisoned after 8
attempts with backoff starting at 30 seconds and doubling, about 63 minutes in
all. 12 hours sits well past that.
