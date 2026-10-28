# End-to-end harness

The e2e suite runs the bridge against real services in one compose network
(`docker-compose.e2e.yml`). Nothing is mocked:

- Lemmy (a debug build) and pictrs federate with the bridge over plain HTTP.
- A did:plc directory (`plc`) backs DID minting and resolution.
- An atproto relay (indigo BigSky) crawls the bridge.
- A reference PDS (`ghcr.io/bluesky-social/pds`, pinned by digest) hosts one
  native account.
- Jetstream decodes the relay's firehose. The tests read from it, and the
  bridge's own Jetstream consumer reads from it to deliver native posts to
  Lemmy.

```
┌────────────────┐   AP, plain HTTP ┌──────────────────────┐ mint DIDs  ┌─────┐
│ lemmy + pictrs │◀────────────────▶│       tidepool       │───────────▶│ plc │
│ (debug build)  │                  └──────────────────────┘            └─────┘
└────────────────┘                        │              ▲                 ▲
                                 firehose │              │ native          │
                                          ▼              │ records         │
┌────────────────┐     firehose     ┌────────────────┐   │   resolve DIDs  │
│ pds (reference)│─────────────────▶│ relay (BigSky) │───┼─────────────────┘
└────────────────┘                  └────────────────┘   │
                                          │              │
                                 firehose │              │
                                          ▼              │
                                    ┌────────────────────┴─┐
                                    │      jetstream       │──────────▶ tests/e2e
                                    └──────────────────────┘
```

Supporting services:

- `dns` (CoreDNS) is the bridge's only upstream resolver. It serves the
  `.test` zone and answers every other name NXDOMAIN, so no query leaves the
  stack. Config is in `tests/e2e/dns/`.
- `handle-tls` (Caddy with its own local CA) serves
  `https://native-alice.pds.test/.well-known/atproto-did` so the bridge can
  verify the native author's handle. The bridge trusts only that CA.
- Each service has its own Postgres. One-shot services `tidepool-migrate`,
  `relay-bootstrap` and `pds-bootstrap` run before the services that need
  them.

## Running it

```sh
make e2e        # build + start the stack, run tests/e2e, tear down
make e2e-up     # start and leave running (iterate with make e2e-test)
make e2e-test   # run the suite against the running stack
make e2e-logs   # tail everything
make e2e-down   # tear down (removes volumes)
```

`make e2e-test` runs `go test -tags e2e,nodynamic -count=1 -v -timeout 20m
./tests/e2e/...`.

Host ports bind to loopback only:

| Port | Service |
|---|---|
| 127.0.0.1:8092 | Tidepool HTTP (admin API, XRPC, healthz) |
| 127.0.0.1:5380 | Tidepool DNS, UDP and TCP (`TIDEPOOL_E2E_DNS_PORT` overrides) |
| 127.0.0.1:8541 | Lemmy HTTP API |
| 127.0.0.1:2480 | Relay XRPC and admin API |
| 127.0.0.1:3081 | Reference PDS (published on the `relay` service) |
| 127.0.0.1:6028 | Jetstream WebSocket |

The stack carries admin tokens and runs with `ALLOW_PRIVATE_FETCH=1`, so it
must not be reachable from the local network. The harness never contacts
`plc.directory`, public relays, or public Lemmy instances.

### Why Lemmy is built from source

The first `make e2e` builds Lemmy from source in debug mode. This is a full
Rust compile, cached afterwards. Lemmy accepts `http://` federation URLs only
in debug builds (`cfg!(debug_assertions)` sets the AP crate's
`allow_http_urls`). Every published Lemmy image is a release build that
refuses plain HTTP, explicit ports, and private addresses. LemmyNet's own
`docker/federation` compose uses a debug build for the same reason. The
bridge runs with `BRIDGE_SCHEME=http` (dev-only) so its AP ids are plain HTTP
too. The header comments in `docker-compose.e2e.yml` and
`e2e/lemmy/Dockerfile` have the details.

## How the relay is wired

Every event the tests read has passed through the relay. The relay resolves
each repo's DID against the local PLC, verifies every commit signature, and
crawls repos over the bridge's `getRepo`.

- The bridge announces itself to the relay on startup through the production
  `RequestCrawlAll` code path (`RELAY_HOSTS=http://relay:2470` plus the
  dev-only `ALLOW_DEV_REQUEST_CRAWL=1`). It retries, because the relay calls
  back into the bridge's `describeServer` to validate the hostname.
- A fresh relay allows 0 new PDSes per day and refuses every non-admin
  `requestCrawl`. The `relay-bootstrap` one-shot raises that limit over the
  admin API before the bridge starts. The announcement still comes from the
  bridge, and `relay_test.go` asserts it landed.
- `HANDLE_RESOLVER_HOSTS=tidepool,localhost:3001` points BigSky's handle
  resolver at the bridge's Host-keyed `/.well-known/atproto-did` and at the
  reference PDS. Without it, names like `alice.lemmy.tidepool` would fail
  handle verification, which BigSky treats as non-fatal.
- For debugging, the `direct` compose profile starts `jetstream-direct` (host
  port 6038), which reads the bridge's firehose without the relay. The tests
  do not use it.

## Why a reference PDS

Every other repo on the firehose is written by the bridge and checked by
test code written against the same reading of the atproto spec. A misreading
shared by both would pass. The reference PDS is a repo host this
project did not write, so it cannot share that misreading.

The `pds` container runs in the relay's network namespace
(`network_mode: "service:relay"`). `@atproto/pds` builds its public URL from
`PDS_HOSTNAME`: `localhost` becomes `http://localhost:$PDS_PORT` and any other
hostname becomes `https://`, which this plain-HTTP harness cannot serve.
BigSky contacts a PDS at the host in its DID document, from inside its own
namespace. Sharing the namespace makes `http://localhost:3001` correct for
both. As a result:

- The PDS's host port is published on the `relay` service, because a service
  using another container's network cannot publish its own ports.
- The host port is 3081, because the Coves dev stack already uses 3001.

The `pds-bootstrap` one-shot creates `native-alice.pds.test` with the public
`createAccount` (invites are off, no admin API). It treats an already-taken
handle as success, waits for `createSession` to return 200, then calls
`requestCrawl` for `localhost:3001` on the relay.

## Event ordering through the relay

BigSky indexes its inbound firehose with a parallel scheduler keyed by repo
DID. Order within one repo survives the relay. Order across repos does not.

A post is a `community.postv2` in the author's repo plus a
`community.acceptance` in the community's repo. Those two events can arrive
in either order. An AppView can see the acceptance before the post, or the
post before it is visible in the community. The same holds for an author's
`actor.profile` and their content. Any AppView reading through a relay must
handle both orders (see [FOLLOWUPS.md](../FOLLOWUPS.md)).

Older `community.post` records, written before posts moved to author repos,
live only in the community's repo, so they have no cross-repo pair to
reorder. Those records still exist and are not migrated.

## What the suite covers

The suite is `tests/e2e/`, build tag `e2e`. Shared helpers are in
`helpers.go`.

### Lemmy to Coves (`bridge_test.go`)

- Subscribing to a community emits `community.profile` on the firehose.
- A link post emits the author's `actor.profile`, a `community.postv2` in the
  author's repo, and a `community.acceptance` in the community's repo. The
  shared URL crosses as `embed.external`.
- Comment threads land in the authors' repos with strongRefs that resolve.
- Lemmy edits and deletes arrive as update and delete ops.
- Votes on posts and comments (upvote, flip, retract) reach
  `getVoteAggregates` and the `bridgedStats` field, and never appear as
  records.
- Pre-subscribe backfill brings in every post and its author's profile, and
  seeds Lemmy's existing vote counts into the aggregates.
- A container restart mid-test replays with deterministic rkeys and emits
  nothing new. The test confirms the forced backfill redo finished (via the
  admin API's `last_backfill_at`) before asserting that. A post created
  during recovery is delivered exactly once, through the relay, which
  reconnects to the restarted bridge with its stored cursor.
- A concurrent-ingestion burst is accounted for per (DID, collection/rkey).

### Lifecycle and consent (`lifecycle_test.go`)

- `#nobridge`: a marked user's posts never materialize and no DID is minted.
  Removing the marker resumes bridging on the next post. Adding it back
  deletes every record they authored while the repo stays active. The bridge
  learns of bio changes through the `PROFILE_REFRESH_TTL` re-fetch, because
  Lemmy 0.19 never federates `Update{Person}` on bio edits.
- `Delete(Actor)`: the author's post, comment, profile and the post's
  acceptance are deleted, and the repo is tombstoned for good.
  `getRepoStatus` and `listRepos` report `active:false` with status
  `deleted`, the handle stops resolving, and content endpoints refuse. The
  `#account{active:false, status:"deleted"}` frame appears on the bridge's
  firehose, and the repo disappears from the relay's `listRepos`.
- Unsubscribe: `DELETE /admin/communities` sends `Undo{Follow}`. New posts in
  that community produce nothing, while a still-subscribed control community
  keeps flowing.
- A community profile edit federates as `Announce{Update{Group}}` and becomes
  a `community.profile` update on rkey `self`.

### Other Lemmy-side scenarios

- `media_test.go`: an image post uploaded to pictrs. The bridge fetches and
  stores the blob, `embed.images` and the `nsfw` self-label cross the wire,
  and `getBlob` serves the image.
- `moderation_test.go`: a moderator removal with a reason, a restore, and an
  author's own delete. A removal replaces the acceptance with a removal record
  in the community's repo in one commit. A restore writes a fresh acceptance
  pinned to the post's current CID. An author delete removes the acceptance
  and writes no removal. Both the firehose and the final repo state are
  checked.
- `upstream_ban_test.go`: a Lemmy site ban with "remove content" (a direct
  `Block` with `removeData`) takes down the user's bridged post and comment.
  A Lemmy post purge (an announced moderator `Delete`) withdraws the
  acceptance and writes a removal record, while the author's `postv2` stays.
- `votes_hammer_test.go`: ten real voters vote, flip and clear in parallel
  bursts, and the final aggregates are exact.

### Identity and DNS

- `identity_test.go`: a new bridged actor's `#identity` frame comes before
  its first `#commit` on the bridge's firehose.
- `dns_test.go`: the bridge's DNS server answers `_atproto.<handle>` TXT with
  the actor's DID and an A record with `DNS_PUBLIC_IPV4`, and the TXT answer
  goes away after the actor is tombstoned.
- `rename_test.go`: `POST /admin/handles/migrate` moves old-format handles
  (`<name>.lemmy.tidepool`) to the current scheme for a user and a community.
  The tests cover `#identity` frames on the bridge, relay and Jetstream
  streams, the old handle no longer resolving, rollback, recovery after PLC
  accepted an update that was not finalized, fetching a missing username from
  the live Lemmy actor, and resuming after a restart mid-run.

### Relay (`relay_test.go`)

- The bridge's own `requestCrawl` registers it in the relay's PDS registry
  with a live subscription.
- Bridged repos appear in the relay's `listRepos` and are served by its
  `getLatestCommit`.

### Native Coves content (`native_pds_test.go`, `native_facets_test.go`)

- `native_pds_test.go` is a smoke test of the wire. It logs in to the
  reference PDS, writes one `community.postv2` into the native account's
  repo, and follows it through the relay to Jetstream (with the CID the PDS
  assigned) and into the relay's `getLatestCommit`.
- `native_facets_test.go` covers the outbound path. A `postv2` with richtext
  facets, written to the reference PDS and targeting a bridged community,
  crosses the relay and Jetstream. The bridge's consumer and acceptance
  engine admit it, and it is delivered to Lemmy as `Create{Page}`. The test
  asserts the markdown Lemmy stored as the post body against literal
  expected strings: spoilers, a code block in a spoiler, headings and
  spoilers inside quotes, an escaped `!` before a link, bold, and plain
  links.

No e2e test covers native comments, votes, edits or deletes going to Lemmy.
Those are covered by unit tests in `internal/outbound`, `internal/consume`
and `internal/accept`.

### Suite-end sweep (`zz_sweep_test.go`)

The last test replays the whole Jetstream history from cursor 1 on a fresh
unfiltered listener. It re-checks every retained event: collection allowlist,
lexicon validation, and per-DID rev order. A replay floor (more than 30 commit
events, at least one post create of either collection, and at least one
delete) makes a truncated replay fail. A zero or negative cursor live-tails instead of
replaying; see `newListener` in `helpers.go`. The test runs last only
because of alphabetical ordering within one `go test` run. Running it alone
with `-run` checks only what is still in Jetstream's 24-hour store.

### Not run

`moderation_test.go`'s `TestMixedEra_LegacyPostDispatch` always skips. The
stack cannot produce a pre-postv2 legacy post without writing to the
database directly. That dispatch is covered by unit tests in
`internal/materialize`, `internal/ingest` and `internal/votes`.

### Compose check without Docker

`compose_oneshots_test.go` has no build tag and runs in a plain
`go test ./...`. It checks that every one-shot service in
`docker-compose.e2e.yml` has a dependent with
`condition: service_completed_successfully`, so `docker compose up --wait`
does not fail when a one-shot has already exited.

### Checks on every event

- Every negative assertion ("nothing bridged") is paired with a positive
  control in the same window, never a bare sleep.
- Every create and update read from Jetstream has passed the relay's
  signature check and is validated against the vendored Coves lexicons.
- Any collection outside the `expectedCollections` allowlist in `helpers.go`
  fails the suite immediately, in any repo. Votes must never become records.

The allowlist does not know which repo a record came from. It fails closed
on a collection outside the list: a vote record written into the native repo
fails its own test and the sweep. It fails open on one inside it: an
`actor.profile` in the native repo passes, indistinguishable from one the
bridge wrote. Scoping the allowlist by repo class is tracked in
[FOLLOWUPS.md](../FOLLOWUPS.md). Keep this in mind before extending the
native-PDS tests.

### Vendored lexicons

- `scripts/sync-lexicons.sh` copies the lexicons from a Coves checkout.
- `scripts/check-lexicons.sh` (`make check-lexicons`) verifies the committed
  manifest and byte-compares against the current Coves checkout. CI clones
  the Coves repository for this. Locally it uses `~/Code/coves` by default.

## Manual Jetstream check

The harness covers Jetstream automatically. For an ad-hoc check against a
dev bridge, point the bridge at the test database and use the repo tests to
write commits:

```sh
make test-db-up      # test postgres on :5443
TEST_DB='postgres://tidepool_test:tidepool_test@localhost:5443/tidepool_test?sslmode=disable'

# serve the test DB (leave running):
DATABASE_URL="$TEST_DB" make run

make jetstream-up    # Jetstream container → ws://host.docker.internal:8091

# watch Jetstream re-emit our records as JSON:
websocat 'ws://localhost:6018/subscribe?wantedCollections=social.coves.*'

# in another shell, generate commits (any repo test that writes):
TIDEPOOL_TEST_DATABASE_URL="$TEST_DB" go test -tags nodynamic -run TestPutRecord -count=1 ./internal/repo/
```

Each commit appears on the socket as JSON with the repo DID, collection,
rkey and record body. `make jetstream-down` stops it. Tests truncate the repo
tables when they start, so Jetstream cursors do not survive a rerun.
