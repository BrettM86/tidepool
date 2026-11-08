# Tidepool

Tidepool is a two-way bridge between the threadiverse and
[Coves](https://github.com/BrettM86/coves), over ActivityPub and atproto.

Inbound, it follows Lemmy/PieFed/Mbin communities (FEP-1b12 group
federation), materializes their posts, comments, and profiles as
`social.coves.*` records in a virtual PDS it operates itself, and serves them
over `com.atproto.sync.*` so the Coves AppView indexes fediverse communities
exactly as it indexes native ones. Fediverse votes stay bridge-side as
aggregates behind one sanctioned XRPC, except that `BRIDGED_VOTE_RECORDS`
(default off) can write votes by bridged users who already have a repo as
`feed.vote` records in that repo.

Outbound, it consumes Jetstream so native Coves users can take part in
bridged communities: their posts, comments, votes, edits, and deletes are
delivered to the fediverse, where those users appear as
`@name@coves.social`. Federation is on by default and users can opt out.

Communities created natively on Coves do not federate yet. Native users can
only reach the fediverse through bridged communities. Making native
communities followable ActivityPub Groups is planned (Scope B in
[PLAN.md](PLAN.md)).

## Supported peers

| Peer | Communities | Posts | Comments | Votes\* | Users | Outbound |
|---|---|---|---|---|---|---|
| **Lemmy 0.19.20** | Tested | Tested | Tested | Tested | Tested | Posts tested; comments, votes, edits, deletes unit-tested |
| **PieFed** | Untested | Untested | Untested | Untested | Untested | Untested |
| **Mbin** | Backfill unit-tested | Link posts unit-tested | Untested | Untested | Untested | Untested |
| **Pylova** | Untested | Untested | Untested | Untested | Untested | Untested |
| **Lemmy 1.0-beta** | Untested | Untested | Untested | Known gap | Untested | Untested |
| **Mastodon** | n/a | Untested | Untested | Untested | Untested | n/a |

Tested means covered by the e2e suite against a real Lemmy instance. Untested means
the peer speaks FEP-1b12 group federation and is expected to work, but
nothing exercises it.

\* Inbound votes are bridged as tallies per post and comment. With
`BRIDGED_VOTE_RECORDS` on (default off), votes by bridged users who already
have a repo become per-voter `feed.vote` records instead. PieFed sends votes from anonymous per-user actors, which
is fine for tallies. Lemmy 1.0 changes how votes federate, which Tidepool
does not handle yet.

Outbound profile changes are not sent: Lemmy rejects `Update{Person}`, so
peers pick up a native user's new name or avatar on their own actor refetch,
within about a day.

## How it works

**Inbound.** The bridge follows a community with a signed `Follow` from its
service actor, then receives the community's `Announce`s at its inbox.
Content is materialized only from the subscribed community's own `Announce`,
over a valid HTTP signature. Each fediverse author and community gets a
did:plc identity. Fediverse users who mark their profile `#nobridge` or
`#nobot` are never bridged.

**Votes.** The bridge keeps per-subject aggregates and folds the counts onto
post and comment records as a `bridgedStats` field, so they reach the AppView
on the same firehose. `social.coves.bridge.getVoteAggregates` serves the same
totals directly. With `BRIDGED_VOTE_RECORDS=up|both` (default off), a vote by
a bridged user who already has a repo is written as a
`social.coves.feed.vote` record in that repo and leaves the aggregate, so
Coves counts it natively (PLAN.md decision 21). A vote becomes a record only
after the bridge fetches its activity from the voter's own instance and it
matches; the `Announce` signs only the community. Votes older than
`BRIDGED_VOTE_VERIFY_WINDOW` (default 144h, under 168h) are not checked and
stay in the aggregate. Fetches are paced by `BRIDGED_VOTE_VERIFY_HOST_RPS`
(default 1) and `BRIDGED_VOTE_VERIFY_GLOBAL_RPS` (default 5), and each sweep
reads up to `BRIDGED_VOTE_VERIFY_BATCH` (default 300) unchecked or retryable
votes, about 48 per voter host and 240 in all per 1m sweep. Dry-run does not
verify, so it previews only already-verified votes.

**Outbound.** With the consumer on, Tidepool reads native Coves activity from
Jetstream, translates it to ActivityPub, and queues deliveries to the
bridged communities' instances. Coves users get `Person` actors on a second
origin (`AP_USER_ORIGIN`, `coves.social` in production). Posts into a bridged
community are admitted by an acceptance engine that writes a
community-signed acceptance record. Users opt out with the
`social.coves.bridge.federation` record.

**Handles.** Tidepool serves bridged handles from its own DNS server.

| Direction | From | To |
|---|---|---|
| Inbound user | `@alice@lemmy.world` | `alice.lemmy.world.ap.tdpl.io` |
| Inbound community | `!technology@lemmy.world` | `c-technology.lemmy.world.ap.tdpl.io` |
| Outbound user, Coves handle | `alice.coves.social` | `@alice@coves.social` |
| Outbound user, own domain | `bretton.dev` | `@bretton.dev@coves.social` |

A native user's fediverse address is fixed the first time they federate; a
later handle change does not change it. See [HANDLE_SCHEME.md](HANDLE_SCHEME.md)
for collisions and the full naming rules.

[PLAN.md](PLAN.md) has the architecture and the design decisions behind it.

## Quick start

Requires Go 1.26+, Docker, and (for `make db-migrate`) the `goose` CLI.

```sh
make dev-up      # start the dev postgres (localhost:5442)
make run         # run the bridge (migrations apply on start in dev)
make test        # start the test postgres (localhost:5443) and run the suite
make lint        # pinned golangci-lint, including the format check
```

Builds and tests use `-tags nodynamic` (the Makefile passes it). The tag keeps
AVIF decoding inside the WASM sandbox; the server refuses to start if a
system libavif was loaded.

Store tests need a real postgres and skip when `TIDEPOOL_TEST_DATABASE_URL`
is unset. Identity-minting tests need a local PLC directory (`make plc-up`,
port 3002; the first start builds did-method-plc and takes several minutes).
They refuse to run against a non-loopback directory, so tests never create
DIDs on the public `plc.directory`.

To try the DNS server locally, use a free port (5353 is usually taken by
mDNS). The dev handle zone is `ap.localhost`:

```sh
DNS_LISTEN=127.0.0.1:5300 DNS_PUBLIC_IPV4=127.0.0.1 make run
dig @127.0.0.1 -p 5300 TXT _atproto.<name>.<instance>.ap.localhost
```

## End-to-end tests

The e2e harness runs the bridge against a real Lemmy, a local PLC directory,
an indigo relay, a reference PDS, and Jetstream, all in one compose network.
It never contacts public PLC, relays, or Lemmy instances.

```sh
make e2e        # build + start the stack, run tests/e2e, tear down
make e2e-up     # start and leave running (iterate with make e2e-test)
make e2e-test   # run the suite against the running stack
make e2e-logs   # tail everything
make e2e-down   # tear down (removes volumes)
```

The first run builds Lemmy from source in debug mode, which takes a while.
[docs/E2E.md](docs/E2E.md) describes the stack and what the suite covers.

## Operating

- [DEPLOY.md](DEPLOY.md): production runbook, including the outbound flag
  order, canary, rollback, KEK rotation, and backups.
- [docs/CONFIGURATION.md](docs/CONFIGURATION.md): every environment variable.
- [docs/ADMIN_API.md](docs/ADMIN_API.md): the admin routes and the
  declarative follow list (`communities.yaml`).
- [docs/PROTOCOL.md](docs/PROTOCOL.md): what the bridge serves and accepts:
  consent rules, the sync surface, rate limits, vote aggregates, and the
  Coves user origin.
- [SELF_HOSTED_RELAY.md](SELF_HOSTED_RELAY.md): the relay and Jetstream
  ingest path.
- [FOLLOWUPS.md](FOLLOWUPS.md): known open items.

## License

Tidepool is licensed under the [GNU Affero General Public License v3.0](LICENSE)
(AGPL-3.0), the same license as [Coves](https://github.com/BrettM86/coves).
