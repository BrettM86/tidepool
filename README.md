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

## Configuration

Environment variables with logged dev defaults (see
`internal/config/config.go`); everything below is **required in
production**:

| Variable | Dev default | Meaning |
|---|---|---|
| `DATABASE_URL` | local dev postgres | bridge state |
| `LISTEN_ADDR` | `:8091` | HTTP bind address |
| `BRIDGE_HOSTNAME` | `localhost` | public domain of the bridge; anchors handles and the PDS endpoint in minted DID docs |
| `PLC_DIRECTORY_URL` | `http://localhost:3002` (local, `make plc-up`) | did:plc directory; production uses `https://plc.directory` |
| `BRIDGE_KEK` | fixed public dev key | 32-byte key-encryption key (64 hex chars or base64) sealing per-actor signing keys and the escrow rotation key at rest (AES-256-GCM) |
| `BRIDGE_SERVICE_DID` | *(optional)* | pre-provisioned service DID for the bridge's own actor |
| `USER_AGENT` | derived | outbound HTTP user agent |
| `ALLOW_PRIVATE_FETCH` | off | dev-only: disables the SSRF egress guard (AP fetches **and** PLC directory requests) so localhost targets work |
| `FIREHOSE_RETENTION` | `72h` | how long `firehose_events` rows are kept for `subscribeRepos` cursor replay (Go duration; a background pruner trims older events hourly) |
| `RELAY_HOSTS` | *(optional)* | comma-separated relays to send `com.atproto.sync.requestCrawl` to on startup; in development the request is logged, never sent |
| `ADMIN_TOKEN` | `dev-admin-token` | bearer token protecting the `/admin` API |
| `BACKFILL_MAX_POSTS` | `100` | posts materialized per community backfill run |
| `MINT_RATE_PER_MINUTE` / `MINT_BURST` | `60` / `120` | rate gate on inbound DID minting (PLC registrations are forever; unseen authors in delivered content trigger mints) |
| `INGEST_WORKERS` | `4` | inbox queue worker-pool size |

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
`/actor`, WebFinger for it at `/.well-known/webfinger`, and a minimal
nodeinfo 2.0 (`software.name: "tidepool"` — what Lemmy instance allowlists
match against).

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

## Verifying with Jetstream

The task-04 integration proof: run a real Jetstream against the bridge and
watch it re-emit our records as JSON. Not automated in CI (it needs Docker
networking to the host); the manual runbook is:

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
