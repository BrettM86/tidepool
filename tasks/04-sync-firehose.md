# Task 04 — com.atproto.sync.* serving + firehose (~1000 LOC)

## Goal
Make Tidepool a valid `subscribeRepos` upstream so Jetstream (and any relay)
can consume it. After this task, records written by task 03's repo layer are
visible to the Coves AppView with zero Coves changes.

## Deliverables
- `internal/sync/server.go` — chi routes for the sync XRPC surface Jetstream
  and relays actually use:
  - `com.atproto.sync.subscribeRepos` — WebSocket, DAG-CBOR frames
    (header `{op:1, t:"#commit"}` + body per the sync spec: seq, rebase,
    tooBig, repo, commit, rev, since, blocks (CAR slice), ops, time).
    Cursor support: `?cursor=N` replays from `firehose_events` then goes
    live; per-connection outbox goroutine with slow-consumer disconnect;
    `#info` frame `OutdatedCursor` when cursor < oldest retained seq.
  - `com.atproto.sync.getRepo` (full CAR export), `getLatestCommit`,
    `getRecord` (proof CAR), `listRepos` (paginated), `getRepoStatus`.
  - `com.atproto.server.describeServer` + `_health` — enough for crawlers.
  - `com.atproto.identity.resolveHandle` (from task 03, mounted here).
- `internal/sync/broadcast.go` — fan-out: repo layer signals new seq;
  broadcaster wakes subscriber outboxes; each reads sequentially from
  `firehose_events` (DB-backed, so restarts and slow consumers are safe).
- Event retention: config `FIREHOSE_RETENTION` (default 72h), pruning job.
- Optional-but-cheap: `com.atproto.sync.requestCrawl` client helper to ask
  a relay to crawl us (used in prod; log-only in dev).
- Tests: end-to-end within the package — write records via task 03 API,
  connect a real WebSocket client, decode frames with indigo's
  `events`/`repo` packages, assert ops + CAR blocks verify against the MST;
  cursor replay from mid-stream; slow-consumer eviction.
- **Integration proof (the money test, may live in compose profile):**
  run Jetstream container pointed at Tidepool
  (`--ws-url ws://tidepool:PORT/xrpc/com.atproto.sync.subscribeRepos` per
  Coves' jetstream service config), write a `social.coves.community.post`,
  assert Jetstream emits the decoded JSON commit with correct collection,
  repo DID, and record body.

## Definition of done
- Jetstream consumes Tidepool without errors and re-emits our records.
- Cursor replay is gapless and ordered (test writes 100 records, connects
  at cursor 50, sees exactly 51..100 then live events).
- `go test ./...` green.

## References
- atproto sync spec (event stream framing): atproto.com/specs/event-stream
  and /specs/sync.
- `~/Code/arroba/arroba/xrpc_sync.py` — the reference implementation of
  exactly this surface, including subscribeRepos framing.
- indigo `events` package (frame encoding), Jetstream config in
  `~/Code/coves/docker-compose.dev.yml`.
