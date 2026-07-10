# Tidepool

Tidepool is a read-only ActivityPub → atproto bridge for the threadiverse.
It follows Lemmy/PieFed/Mbin communities (FEP-1b12 group federation),
materializes their posts, comments, and profiles as `social.coves.*` records
in a virtual PDS it operates itself, and serves them over
`com.atproto.sync.*` so the [Coves](https://github.com/coves-social) AppView
indexes fediverse communities exactly as it indexes native ones. Votes stay
bridge-side as aggregates behind one sanctioned XRPC.

See **[PLAN.md](PLAN.md)** for the architecture, locked design decisions,
and the task-by-task build plan (`tasks/`).

## Quick start

```sh
make dev-up      # start the dev postgres (localhost:5442)
make run         # run the bridge (migrations apply on start in dev)
make test        # start the test postgres (localhost:5443) and run the suite
```

Requires Go 1.25+, Docker, and (for `make db-migrate` / `make lint`) the
`goose` and `golangci-lint` CLIs. Store tests need a real postgres: they
skip with a clear message when `TIDEPOOL_TEST_DATABASE_URL` is unset.

The identity-minting tests additionally need a **local** PLC directory
(`make plc-up`, port 3002; the first start clones and builds
[did-method-plc](https://github.com/did-method-plc/did-method-plc), which
takes several minutes). They skip when it is unreachable and under
`go test -short`, and hard-refuse to run against any non-loopback
directory — the test suite can never create DIDs on the public
`plc.directory`. Point `TIDEPOOL_TEST_PLC_URL` elsewhere-on-localhost if
your directory is on a different port.

## Running the stack (e2e harness)

The end-to-end harness runs the whole read path against **real
infrastructure** — a real Lemmy federating with the bridge, a real did:plc
directory backing DID minting, a real atproto relay (indigo **BigSky**)
crawling the bridge, and a real Jetstream decoding the **relay's** firehose
— in one compose network:

```
                        docker-compose.e2e.yml
 ┌───────────────────────────────────────────────────────────────────────┐
 │  http://lemmy (debug build)          http://tidepool (this repo)      │
 │ ┌───────────────────────┐  Follow ◀─ ┌───────────────────────────┐    │
 │ │ lemmy + lemmy-postgres│  Accept ─▶ │ tidepool + its postgres   │    │
 │ │ + pictrs              │ Announce ─▶│  inbox → queue → material │    │
 │ └───────────────────────┘  (plain    │  izer → virtual PDS       │    │
 │                             HTTP)    └─────┬──────────────┬──────┘    │
 │                                  mint DIDs │   CBOR frames│ ▲         │
 │                              ┌─────────────▼──┐           │ │request  │
 │                              │ plc (did:plc   │◀────┐     │ │Crawl    │
 │                              │ + plc-postgres)│     │  ┌──▼─┴───────┐ │
 │                              └────────────────┘ DID │  │ relay      │ │
 │                                          resolution └──│ (BigSky)   │ │
 │                                                        │ + postgres │ │
 │                                                        └──────┬─────┘ │
 │                                                    CBOR frames│       │
 │                                                       ┌───────▼─────┐ │
 │                                                       │ jetstream   │ │
 │                                                       │ (JSON)      │ │
 │                                                       └───────┬─────┘ │
 └───────────────────────────────────────────────────────────────┼───────┘
      host (127.0.0.1 only): tidepool :8092, lemmy :8541,        │
                             relay :2480, jetstream :6028 ◀──────┘
                    tests/e2e (go test -tags e2e)
```

Every event the suite consumes has therefore survived the strictest consumer
that exists: the relay resolves each repo's DID against the local PLC,
verifies every commit signature, and crawls repos over the bridge's
`getRepo`. The bridge announces itself to the relay on startup via the
**real** `RequestCrawlAll` code path (`RELAY_HOSTS=http://relay:2470` plus
the dev-only `ALLOW_DEV_REQUEST_CRAWL=1` — production always sends and
refuses the flag), retrying internally because the relay validates the
hostname by calling back into the bridge's `describeServer`. A fresh
relay's new-PDS-per-day limit is 0 (refuses all non-admin `requestCrawl`),
so a one-shot `relay-bootstrap` service raises the limit over the admin API
before the bridge starts — the announcement itself is still
bridge-originated, and the suite asserts it landed (`relay_test.go`).
Bridged handles verify for real too: `HANDLE_RESOLVER_HOSTS=tidepool`
points bigsky's trial-host resolver at the bridge's Host-header-keyed
`/.well-known/atproto-did` (compose-DNS-invisible names like
`alice.lemmy.tidepool` would otherwise fail handle verification, which
bigsky treats as non-fatal). For debugging, a direct bridge→Jetstream tap
exists behind the `direct` compose profile (`jetstream-direct`, host port
6038) — it is not the tested path.

One ordering consequence worth knowing: bigsky indexes its inbound firehose
with a parallel scheduler keyed by repo DID, so **per-repo event order
survives the relay but cross-repo order does not** — an author's
`actor.profile` (author repo) and their post (community repo) may swap on
the relay's output, and any AppView consuming through relay infrastructure
must tolerate that (see FOLLOWUPS.md).

The host ports bind **loopback-only** (`127.0.0.1:8092/8541/2480/6028`):
the stack carries admin tokens and runs with `ALLOW_PRIVATE_FETCH=1`, so it
must not be reachable from the local network.

```sh
make e2e        # build + start the stack, run tests/e2e, tear down
make e2e-up     # start and leave running (iterate with make e2e-test)
make e2e-test   # run the suite against the running stack
make e2e-logs   # tail everything
make e2e-down   # tear down (removes volumes)
```

The first `make e2e` builds Lemmy **from source in debug mode** (a full Rust
compile; cached afterwards). That is deliberate: whether Lemmy accepts
`http://` federation URLs is a compile-time property (`cfg!(debug_assertions)`
→ the AP crate's `allow_http_urls`), and every published Lemmy image is a
release build that refuses plain HTTP, explicit ports, and private addresses.
A debug build — the same thing LemmyNet's own `docker/federation` compose
uses — federates happily over `http://lemmy` ↔ `http://tidepool` inside the
network, with `BRIDGE_SCHEME=http` (dev-only) making the bridge emit
plain-HTTP AP ids to match. See the header comments in
`docker-compose.e2e.yml` and `e2e/lemmy/Dockerfile` for the full story.

The suite (`tests/e2e/`, build tag `e2e`: `bridge_test.go`,
`lifecycle_test.go`, `media_test.go`, `relay_test.go`,
`votes_hammer_test.go`, `zz_sweep_test.go`)
covers: subscribe → `community.profile` on the firehose; a link post →
`actor.profile` and `community.post` (presence + author linkage; arrival
order across the two repos is relay-dependent, see above), with the shared
url crossing the wire as `embed.external`; comment threads in the authors'
repos with resolving strongRefs; edits/deletes as update/delete ops; post
**and** comment votes reaching `getVoteAggregates` — upvote, flip, retract —
while **never** appearing as records; pre-subscribe backfill with every
post's author profile arriving and Lemmy's pre-existing vote counts seeded
into the aggregates; a mid-test container restart proving deterministic-rkey
replay idempotency (the forced backfill redo is confirmed complete via the
admin API's `last_backfill_at` before asserting it emitted nothing) plus
exactly-once delivery of a post created in the recovery window — now proven
**through the relay**, whose slurper reconnects to the bounced bridge with
its stored cursor; a concurrent-ingestion burst accounted per
(did, collection/rkey); and relay-state assertions — the bridge registered
and actively subscribed in the relay's PDS registry (the bridge-originated
`requestCrawl`, asserted rather than eyeballed), and bridged repos listed
and served by the relay's own `listRepos`/`getLatestCommit`.

The task-10 lifecycle scenarios extend that: an **image post** — a real
pictrs upload, the blob fetched and stored by the bridge, `embed.images`
plus the `nsfw` self-label crossing the wire and served back through
`getBlob`; the full **`#nobridge` consent lifecycle** — a marked user's
posts never materialize (no DID minted), removing the marker resumes
bridging on the next post, re-adding it scrubs every record with delete
commits while the repo stays active (reversible; discovery rides the
`PROFILE_REFRESH_TTL` re-fetch because Lemmy 0.19 never federates
`Update{Person}` on bio edits); **`Delete(Actor)`** — account deletion
scrubs the author's post/comment/profile and terminally tombstones the repo
(`getRepoStatus`/`listRepos` report `active:false`, the handle stops
resolving, content endpoints refuse — asserted on the bridge's sync surface
because the relay cannot observe tombstones until task 11's `#account`
frame); **unsubscribe** — `DELETE /admin/communities` sends `Undo{Follow}`
and new posts in that community produce no bridge output while a
still-subscribed control keeps flowing; a **community profile update**
federating as an `Announce{Update{Group}}` → `community.profile` update on
rkey `self`; a **vote-concurrency hammer** — ten real voters in parallel
bursts (votes, flips, clears) with exactly-correct final aggregates; and a
**suite-end sweep** that replays the entire firehose from cursor 1 (a zero
or negative cursor omits the param and live-tails — see the `newListener`
doc in `tests/e2e/helpers.go`) on a fresh unfiltered listener, re-vetting
every retained event (collection whitelist, lexicon validation, per-DID rev
monotonicity), with a replay floor so a truncated replay cannot pass on the
sentinel alone. Every negative
assertion ("nothing bridged") is bounded by a positive control in the same
window — never a bare sleep-and-assert-nothing.

Every
create/update the tests consume from Jetstream has passed the relay's
signature verification AND is validated against the vendored Coves lexicons
on the consumer side of the wire, and any collection outside the four the
bridge emits fails the suite immediately (votes must never become records). Two scripts keep the vendored
lexicons honest: `scripts/sync-lexicons.sh` copies them from a Coves
checkout; `scripts/check-lexicons.sh` verifies the committed manifest (the
layer that runs in CI) and additionally byte-compares against
`~/Code/coves` when that checkout exists (local only).

Everything is local-only: the harness never contacts `plc.directory`, public
relays, or public Lemmy instances.

## Configuration

Environment variables with logged dev defaults (see
`internal/config/config.go`); everything below is **required in
production**:

| Variable | Dev default | Meaning |
|---|---|---|
| `DATABASE_URL` | local dev postgres | bridge state |
| `LISTEN_ADDR` | `:8091` | HTTP bind address |
| `BRIDGE_HOSTNAME` | `localhost` | public domain of the bridge; anchors handles and the PDS endpoint in minted DID docs |
| `BRIDGE_SCHEME` | `https` | scheme of the bridge's own AP URLs (actor id, inbox, activity ids). `http` is dev-only — the e2e harness federates with a debug-mode Lemmy over plain HTTP |
| `PLC_DIRECTORY_URL` | `http://localhost:3002` (local, `make plc-up`) | did:plc directory; production uses `https://plc.directory` |
| `BRIDGE_KEK` | fixed public dev key | 32-byte key-encryption key (64 hex chars or base64) sealing per-actor signing keys and the escrow rotation key at rest (AES-256-GCM) |
| `BRIDGE_SERVICE_DID` | *(optional)* | pre-provisioned service DID for the bridge's own actor |
| `USER_AGENT` | derived | outbound HTTP user agent |
| `ALLOW_PRIVATE_FETCH` | off | dev-only: disables the SSRF egress guard (AP fetches **and** PLC directory requests) so localhost targets work |
| `FIREHOSE_RETENTION` | `72h` | how long `firehose_events` rows are kept for `subscribeRepos` cursor replay (Go duration; a background pruner trims older events hourly) |
| `RELAY_HOSTS` | *(optional)* | comma-separated relays to send `com.atproto.sync.requestCrawl` to on startup (each retried on a bounded budget — the relay calls back into `describeServer` before subscribing, which can race process start); in development the request is logged, never sent, unless `ALLOW_DEV_REQUEST_CRAWL` opts in |
| `ALLOW_DEV_REQUEST_CRAWL` | off | dev-only: actually SEND `requestCrawl` to `RELAY_HOSTS` in development (exists for the e2e stack's local BigSky); refused in production, where sending is already the behavior |
| `ADMIN_TOKEN` | `dev-admin-token` | bearer token protecting the `/admin` API |
| `BACKFILL_MAX_POSTS` | `100` | posts materialized per community backfill run |
| `MINT_RATE_PER_MINUTE` / `MINT_BURST` | `60` / `120` | rate gate on inbound DID minting (PLC registrations are forever; unseen authors in delivered content trigger mints) |
| `INGEST_WORKERS` | `4` | inbox queue worker-pool size |
| `SEED_COUNTS_FROM_API` | on | seed backfilled posts' vote aggregates from the origin instance's public API (`/api/v3/post` `counts`); set `0` to disable |

## Subscribing to communities (admin API)

Community subscriptions are operator-driven, over bearer-token-protected
endpoints (`Authorization: Bearer $ADMIN_TOKEN`):

```sh
# follow: WebFinger → fetch Group → materialize community → signed Follow
curl -X POST localhost:8091/admin/communities \
  -H "Authorization: Bearer dev-admin-token" \
  -d '{"community":"!technology@lemmy.world"}'

# state is `pending` until the community's Accept arrives at /inbox, which
# flips it to `accepted` and triggers an outbox backfill automatically.

curl localhost:8091/admin/communities \
  -H "Authorization: Bearer dev-admin-token"          # list
curl -X POST localhost:8091/admin/communities/backfill \
  -H "Authorization: Bearer dev-admin-token" \
  -d '{"community":"!technology@lemmy.world"}'        # on-demand backfill
curl -X DELETE localhost:8091/admin/communities \
  -H "Authorization: Bearer dev-admin-token" \
  -d '{"community":"!technology@lemmy.world"}'        # Undo{Follow}
```

The bridge's AP face lives next to the inbox: the service actor document at
`/actor`, an **instance actor at the origin apex** (`GET /`, type
`Application` — Lemmy resolves every peer's "Site" actor there and delivers
its send-to-all-instances activities, account deletions above all, ONLY to
the inbox that document advertises; without it those activities are
silently skipped), WebFinger for the service actor at
`/.well-known/webfinger`, and a minimal nodeinfo 2.0 (`software.name:
"tidepool"` — what Lemmy instance allowlists match against).

## Consent policy (#nobridge / #nobot)

Tidepool mirrors the [Bridgy Fed](https://fed.brid.gy/docs#opt-out) opt-out
norms, enforced fail-closed in the materializer and wired to live AP
activity by the ingestion layer:

- An actor whose profile summary or hashtags carry **`#nobridge`** or
  **`#nobot`** is never bridged: no DID is minted, and every post or comment
  they author is dropped with the reason logged.
- If a **previously bridged** actor adds the marker (seen on a profile
  `Update` or any profile re-fetch), every record they authored is deleted
  from the bridged repos and new materialization stops. This state is
  **reversible**: removing the marker upstream restores bridging on the next
  profile refresh.
- **`Delete(Actor)`** (account deletion upstream) scrubs all their records
  and tombstones the bridged repo **terminally**; the sync surface reports
  the repo `active: false`.
- Object-level `Delete`s tombstone the mapped record; a `Delete` arriving
  before its object was ever seen is remembered (`ap_tombstones`), so an
  out-of-order or re-delivered `Create` cannot resurrect deleted content.
  `Undo{Delete}` restores by re-fetching the object from its origin.
- Bridged profiles are visibly labeled: bios/descriptions end with a
  "bridged from … by Tidepool" provenance line, and community profiles set
  `hostedBy` to the bridge's service DID.

Deliveries are accepted only over valid draft-cavage HTTP signatures
(rsa-sha256, 1h date-skew window, digest required); content is accepted only
from communities the operator subscribed to, and embedded objects are
re-fetched from their origin instance whenever the delivering signer lacks
authority over the object's id.

## Sync surface (what relays and Jetstream consume)

Tidepool serves the `com.atproto.sync.*` XRPC surface a relay needs to treat
it as a `subscribeRepos` upstream:

- `GET /xrpc/com.atproto.sync.subscribeRepos` (WebSocket; `?cursor=N` replays
  from the durable event log, then tails live; slow consumers are evicted
  and resume by reconnecting with their last cursor; a cursor older than
  retention gets an `#info OutdatedCursor` frame first)
- `getRepo` (full CAR), `getLatestCommit`, `getRecord` (proof CAR),
  `listRepos` (paginated), `getRepoStatus`
- `com.atproto.server.describeServer`, `/xrpc/_health`
- `com.atproto.identity.resolveHandle` + `/.well-known/atproto-did` (task 03)

Repos whose actor revoked consent (tombstoned) report `RepoDeactivated` /
`active: false` and their content endpoints stop serving.

## Vote aggregates (the AppView integration point)

Votes never become records (nothing may strongRef a vote): Lemmy
`Like`/`Dislike`/`Undo` activities maintain bridge-side aggregate counts,
served over **one sanctioned side-channel XRPC** the Coves AppView polls:

```
GET /xrpc/social.coves.bridge.getVoteAggregates?uris=at://…&uris=at://…
```

- `uris`: at-uris of bridged posts/comments — repeated `uris` params (the
  atproto convention; comma-separated values are also accepted), at most
  **100** per call (`InvalidRequest` beyond that).
- Response: `{"aggregates":[{"uri","upvotes","downvotes","updatedAt"}]}` in
  request order. Unknown or never-voted uris are **omitted**, not an error.
- Public read, `Cache-Control: public, max-age=30`, rate limited per client
  IP (token bucket; deployments behind a proxy should rate-limit the real
  client at the edge — the bridge deliberately ignores `X-Forwarded-For`).

The contract is the lexicon at
[`lexicons/social/coves/bridge/getVoteAggregates.json`](lexicons/social/coves/bridge/getVoteAggregates.json)
and is versioned by nsid: breaking changes ship under a new name.

Counts reflect each distinct voter's **latest** state — flips
(`Like` → `Dislike`) and `Undo`s are folded in, re-delivered activities are
deduplicated by activity id. Votes on content the bridge never materialized
are dropped (logged at debug). Known limitation: AP delivers votes only
going forward, and Lemmy outboxes announce historical Likes sparsely — so
backfilled posts would start near zero. `SEED_COUNTS_FROM_API` (default on)
compensates by seeding a baseline from the origin's public API during
backfill; live votes stack on top, and an undo of a vote that only exists in
the baseline is a no-op (accepted drift, refreshed on re-seed). Comment
scores are not seeded in v1 — comments accumulate live votes only.

## Verifying with Jetstream

**Automated:** the e2e harness (`make e2e`, above) runs a real Jetstream
against the bridge and asserts on the decoded events — this manual runbook
survives for ad-hoc poking at a dev bridge:

Until the materializer (task 05) generates organic writes, the repo test
suite is the write driver — so point the bridge at the **test** database and
let the tests produce commits:

```sh
make test-db-up      # test postgres on :5443
TEST_DB='postgres://tidepool_test:tidepool_test@localhost:5443/tidepool_test?sslmode=disable'

# serve the test DB (leave running):
DATABASE_URL="$TEST_DB" make run

make jetstream-up    # Jetstream container → ws://host.docker.internal:8091

# watch Jetstream re-emit our records as JSON:
websocat 'ws://localhost:6018/subscribe?wantedCollections=social.coves.*'

# in another shell, generate commits (any repo test that writes):
TIDEPOOL_TEST_DATABASE_URL="$TEST_DB" go test -run TestPutRecord -count=1 ./internal/repo/
```

Each commit appears on the Jetstream socket as decoded JSON with the repo
DID, collection, rkey, and record body. `make jetstream-down` stops it.
(Tests truncate repo tables when they start, so Jetstream cursors don't
survive a rerun — fine for a smoke check, which is all this is.)

## Handle resolution & DNS (wildcard requirement)

Every bridged actor's atproto handle is a subdomain of `BRIDGE_HOSTNAME`:
communities get `technology.lemmy-world.<BRIDGE_HOSTNAME>`, users get
`alice.lemmy-world.<BRIDGE_HOSTNAME>` (dots replace the fediverse `!`/`@`
separators; dots inside the instance hostname become dashes; colliding
handles get a `-2`, `-3`, … suffix).

For those handles to resolve, the operator **must configure wildcard DNS**:
`*.<BRIDGE_HOSTNAME>` → the bridge (a DNS wildcard matches multiple label
levels, so one record covers `*.lemmy-world.<BRIDGE_HOSTNAME>` and every
other bridged instance). The bridge then answers both resolution paths:

- `GET /xrpc/com.atproto.identity.resolveHandle?handle=…` — the XRPC query;
- `GET https://<handle>/.well-known/atproto-did` — the HTTPS well-known
  method; the wildcard DNS routes every bridged subdomain to the bridge,
  which answers from the `Host` header.

Note TLS: a single wildcard certificate only covers one label level, while
bridged handles sit two levels below `BRIDGE_HOSTNAME` — terminate TLS with
on-demand certificate issuance (e.g. Caddy) or per-instance wildcard certs.
