# Follow-ups

Everything known-deferred at the end of the v1 build loop (tasks 01–08),
collected from `LOOP_STATE.md`'s cross-task notes plus discoveries made
while building the e2e harness, updated as v1.1 tasks land. Organized by
area; items marked **(e2e)** were discovered or confirmed by the task-08
harness, **(relay)** by task 09's relay pipeline.

## Relay pipeline (task 09 discoveries)

- **Cross-repo event ordering does NOT survive a relay (relay).** bigsky
  indexes its inbound firehose with a parallel scheduler keyed by repo DID
  (indigo `events/schedulers/parallel`, 100 workers): per-repo order is
  preserved, cross-repo order is not. The bridge's profile-before-post
  emission discipline (locked decision 3's "author indexed before post")
  therefore only holds on the bridge's OWN firehose — through a relay, a
  post in the community repo routinely overtakes its author's first-ever
  `actor.profile` (the profile costs the relay a PLC resolution + handle
  verification; the community DID is already cached). **Carry to Coves:**
  the AppView consumes through relay infrastructure, so its "author must be
  indexed before the post" validation needs a retry/park-and-reprocess
  path, or Coves consumes the bridge firehose directly. The e2e suite's
  listener buffers out-of-order events (`tests/e2e/helpers.go` `pending`)
  and asserts presence + linkage instead of cross-repo arrival order.
- ~~Tombstoned (`active:false`) repo status is not observable through
  bigsky (no `#account` frame emitted).~~ **Closed by task 11**: the bridge
  emits `#account{active:false, status:"deleted"}` through the durable
  firehose log on `Delete(Actor)` (after the scrub delete-commits, same
  advisory-lock seq ordering), and `TestDeleteActor_ScrubsAndTombstones`
  now asserts the frame on the bridge's own firehose AND the repo
  disappearing from the relay's `listRepos`. Wire fact (source-verified
  against the pinned bigsky): on `status:"deleted"` bigsky re-resolves the
  DID doc (PDS-authority check), marks the account `tombstoned` (the
  `listRepos` filter), and PURGES the repo's carstore data — exactly the
  downstream purge consent revocation wants. bigsky's `listRepos` filter
  also excludes `deactivated`/`suspended`/`takendown`, but only `deleted`
  purges. nobridge (reversible) deliberately emits NO frame — the repo
  stays active.
- **A fresh bigsky refuses all non-admin `requestCrawl`**: the new-PDS
  per-day limit defaults to 0 and is checked BEFORE the trusted-domain
  list. Any deployment announcing to a self-hosted relay needs the
  `setPerDayLimit` admin bootstrap (compose does it in `relay-bootstrap`).
- **Verify env vars against `--help`, not other people's compose files.**
  Coves' relay stanza sets `BGS_CRAWL_INSECURE_WS`, `BGS_PORT`, and
  `LOG_LEVEL` — none of which exist in the pinned image
  (`--crawl-insecure-ws` has no env binding and must be a command arg; the
  log knob is `BSKYLOG_LOG_LEVEL`). Also local-only traps: bigsky defaults
  to the public `plc.directory` (`ATP_PLC_HOST`) AND the public `1.1.1.1`
  DNS resolver (`RESOLVE_ADDRESS`).
- The pinned bigsky image has **no arm64 manifest** — Apple Silicon runs it
  under emulation (`platform: linux/amd64`), fine at e2e volume; a future
  image bump could revisit.
- **Jetstream cursor semantics have a full-replay middle band (task 10,
  measured live).** The pinned Jetstream replays precisely (µs-exact,
  inclusive) for cursors at or before its newest stored event and
  live-tails for cursors beyond server-now — but a cursor BETWEEN the
  newest stored event and now (i.e. any "subscribe from now" against a
  quiet stream) replays the ENTIRE retained store. Every `cursorNow()`
  e2e listener has therefore been receiving full-history replays without
  anyone noticing (scoped awaits absorb it); negative assertions must be
  bounded by an observed event's `time_us`, never by wall-clock cursors
  (`tests/e2e/helpers.go` cursorNow doc, unsubscribe scenario). Carry to
  Coves: an AppView consumer that reconnects "from now" after downtime on
  a quiet Jetstream may get a surprise full replay — idempotent indexing
  handles it, but don't treat cursor=now as a dedupe boundary.
- indigo's slurper reconnect backoff is effectively sub-second for the
  first 10 attempts (`sleepForBackoff` multiplies by 2 **nanoseconds**, so
  only the additive +0–1s jitter — rand milliseconds — matters), then 30s;
  on the 16th consecutive dial failure (backoff > 15) it marks the PDS
  `registered=false` and STOPS retrying — a
  bridge outage longer than ~3 minutes needs a fresh `requestCrawl` (ours
  re-announces on every startup, which covers the bridge's own restarts but
  not a long bridge outage while the relay stays up).

## Design revisits (decide before/with the write side)

- **Votes-as-records (2026-07-10 discussion).** The aggregate side channel
  is a v1 decision worth re-opening when write-back is designed, not a law
  of nature. Facts to carry into that decision:
  - A faithful **going-forward** per-voter record stream is buildable
    today: every live Like/Dislike arrives with the voter's AP identity
    (that's how `vote_events` dedupes). Flips/undos are already solved
    state-tracking.
  - **History is counts-only forever**: Lemmy's per-voter list endpoints
    (`listPostLikes`/`listCommentLikes`) are origin-instance admin/mod
    APIs — the bridge is a federated peer, not an admin. Any records
    design still needs an aggregate baseline for pre-subscribe history.
  - Naïve design (vote record in the voter's repo) mints a permanent
    public did:plc per drive-by voter — a global-registry externality,
    not a compute cost. **Leading alternative:** votes in the
    *community's* repo with a `voter` field + deterministic rkey
    (voter+subject hash), mirroring how posts already live in the
    community repo with an `author` field. Zero new DIDs, votes on the
    firehose. Requires a new lexicon + Coves AppView consumer — decide
    WITH Coves.
  - ~~Write amplification (one commit + firehose event per vote) is only
    viable after the perf items land: per-DID MST cache, block GC,
    batched pruning (tasks 11–12). Hardening first is a prerequisite,
    not a competing priority.~~ **The perf prerequisites now hold (task
    12)**: per-DID MST cache (steady-state commit 3.3 ms on a 2k-record
    repo, 42.6x over the full-reload path), blocks GC (superseded blocks
    reclaimed after `BLOCKS_GC_RETENTION`), batched firehose pruning
    (task 11), streaming reachable-set getRepo, and a bounded ClaimNext
    scan. What is still NOT solved for votes-as-records: commits remain
    globally serialized (the advisory lock — ~300/s ceiling at the
    benchmarked commit cost, shared across ALL writes), and every vote
    would still be a firehose event relays and the AppView must chew
    through. The decision is now genuinely open on design grounds
    (lexicon + Coves consumer + DID externality), not blocked on storage
    perf.
  - Write-back symmetry favors records: Coves users' votes on bridged
    posts are already native `social.coves.feed.vote` records; outbound
    translation is records→Like. Symmetric records would let frontends
    drop the `getVoteAggregates` XRPC for everything except historical
    baselines.

## Federation & interop

- **Lemmy first-contact Accept race (e2e).** Lemmy's federation queue
  initializes a newly-seen instance's cursor at the *current* max activity
  id ("skip all past activities", `crates/federate/src/worker.rs`), so the
  Accept answering the very first Follow to a given Lemmy instance is
  usually skipped: the instance row is created by that same Follow, and the
  per-instance worker spawns after the Accept is queued. Re-sending the
  Follow (fresh activity id → fresh Accept) recovers. ~~Consider an
  automatic Follow re-send when a subscription stays `pending` past a
  threshold.~~ **Closed by task 11**: `ingest.FollowRetrier` re-sends the
  Follow (fresh activity id) for subscriptions pending past 2m, bounded to
  5 total sends (`communities.follow_requested_at`/`follow_attempts`,
  migration 012; unsubscribe resets the budget). The harness's own retry in
  `subscribeCommunity` stays — the suite must not depend on the 2m
  threshold.
- **Author auto-upvotes do not federate (e2e).** Lemmy casts a local Like
  by the post author but never announces it, so live-bridged posts read one
  upvote lower than Lemmy's UI until a backfill re-seed. Accepted drift;
  document for the AppView (the vote-hammer scenario's expectations bake
  this in).
- **Lemmy 0.19.x never federates `Update{Person}` on profile edits (task
  10, source-verified).** `SaveUserSettings` submits no activity and
  `SendActivityData` has no Update-Person variant at the pinned tag — bio
  changes reach remote instances only when they re-fetch the actor.
  Production consequence: a bridged actor adding `#nobridge` to their bio
  is discovered only on the next TTL-stale actor re-fetch (default
  `PROFILE_REFRESH_TTL` 24h) *triggered by their next activity* — an
  inactive actor's opt-out is never discovered. Consider a periodic consent
  re-scan of active bridged actors. (The e2e compose sets
  `PROFILE_REFRESH_TTL=2s` to drive this path; when Lemmy grows
  Update/Person federation, the consent scenario should also assert the
  immediate-refresh path.)
- **Account deletion federates ONE `Delete{Person}` (task 10,
  source-verified).** `delete_content` rides as a nonstandard `removeData`
  flag; there are no per-object Delete activities for the user's posts and
  comments. The bridge's actor-scrub already deletes all authored records
  regardless of the flag's value.
- **Lemmy delivers send-to-all-instances activities (account deletions!)
  ONLY to peers with a stored Site actor row (task 10, observed live +
  source-verified).** The federate worker resolves `to_all_instances()`
  targets to the remote instance's Site row inbox and silently skips
  (`no inboxes`, `was_skipped: true`) when none exists — the bridge
  received posts and votes happily while every `Delete{Person}` was
  dropped unsent. Lemmy creates the site row by fetching the peer's ORIGIN
  APEX (`fetch_instance_actor_for_object`, on every Person/Community
  `from_json`), so the bridge now serves an instance actor at `GET /`
  (`ap.ServiceActor.InstanceDocumentJSON`; type must be exactly
  `Application` — the OPPOSITE of the `/actor` rule, where Lemmy's Person
  enum rejects Application and needs `Service`). **Operational note for
  existing deployments:** Lemmy's per-instance worker caches "no site row"
  for its lifetime and actor re-fetches are 24h-TTL'd — a peer Lemmy that
  federated with the bridge before this change needs an actor re-fetch AND
  a worker restart before deletions start flowing.
- **A federated `Delete{Person}` is signed by an actor its origin already
  serves as 410 Gone (task 10, observed live).** Unless that user's key
  happens to sit in the bridge's cache from an earlier direct delivery,
  the signature can never verify — rejecting it would mean Lemmy account
  deletions never tombstone bridged repos (a consent failure). The inbox
  now accepts a bare SELF-referential Delete when verification failed on a
  tombstone AND an independent SSRF-guarded fetch of the claimed actor's
  own IRI confirms the origin serves it Gone: the origin's word for its
  own actor is the same host-granularity trust unit bare-Delete
  authorization already uses, so a forger can only "forge" a deletion
  that is already true at the origin. Any OTHER payload with a tombstoned
  signer is a definitive 401 (a 5xx would head-of-line block Lemmy's
  per-instance retry queue, which retries forever). See
  `internal/ingest/inbox.go` `tombstonedSelfDelete` + 3 regression tests.
- **The tombstone confirmation is Lemmy-scoped by design: a 404-on-deleted
  origin can never pass it.** `tombstonedSelfDelete` accepts only
  independently-confirmed tombstone evidence — a 410 Gone or a 200 whose
  body is a Tombstone object (`internal/ap/client.go`); a plain 404 maps to
  IsNotFound and the Delete stays a definitive 401. Platforms that serve
  404 for deleted actors (Mastodon-style behavior in some configurations)
  would therefore never get their self-Deletes honored via this path. Fine
  while the bridge federates with Lemmy only (Lemmy serves 410/Tombstone);
  the PieFed/Mbin follow-up below must revisit this alongside the `/c/`
  heuristic.
- **Lemmy's AP `Image` attachment drops `mediaType`** (task 10): an image
  post's attachment serializes as `{"type":"Image","url":…,"name":<alt>}` —
  the `type` field (plus the materializer's extension fallback) is the only
  image discriminator on the wire; `Link` attachments carry `mediaType` but
  no `name`, so alt text does not survive the metadata-fetch-failure
  fallback.
- **PieFed / Mbin untested** (PLAN.md deferred: verify against Lemmy only).
  Known gap: `communityRef` uses a Lemmy `/c/` heuristic — Mbin uses `/m/`.
- **Lemmy 1.0 / API v4.** Everything (harness helpers, seeder's
  `/api/v3/post`, webfinger expectations) targets Lemmy 0.19.x. 1.0 moves to
  `/api/v4` and changes some protocol structs; revisit `e2e/lemmy/Dockerfile`'s
  pinned tag and `tests/e2e/helpers.go` together.
- **Service actor type is `Service`** (changed in task 08): Lemmy's Person
  protocol enum accepts only `Person|Service|Organization` — an
  `Application` actor fails deserialization and the Follow is dropped. If
  another platform chokes on `Service`, this needs per-platform content
  negotiation (unlikely; Mastodon et al. accept it).
- **WebFinger https→http fallback** exists only when the SSRF guard is
  relaxed (`ALLOW_PRIVATE_FETCH`, dev/e2e). Production is https-only by
  construction — fine until someone runs a bridge against an http-only
  internal instance on purpose.

## Write side & production rollout (PLAN.md §9, explicitly out of v1)

- Outbound Coves→Lemmy direction; AP actors for Coves users.
- Key claiming/migration for bridged users (keys are escrowed under the
  rotation key precisely to enable this later). On handle-collision retry,
  callers should eventually REUSE an orphaned minted DID via a PLC
  updateHandle op instead of re-minting (task 03 note).
- Moderation federation, DMs.
- ~~Relay `requestCrawl` is wired but has never been exercised against a
  real relay (dev logs instead of sending).~~ **Closed by task 09**: the
  e2e stack runs a real BigSky; the bridge announces itself through the
  production `RequestCrawlAll` path (now with a bounded retry — the relay
  calls back into `describeServer`, racing process start) under the
  dev-only `ALLOW_DEV_REQUEST_CRAWL` override, and the suite asserts the
  bridge is registered + actively subscribed in the relay's PDS registry.
- `ENVIRONMENT=production` has never been end-to-end tested (the harness
  runs development mode for migrations-on-start, strict validation, http
  scheme, private fetch).
- Strict lexicon validation is dev/test-only; production logs-and-writes.
  ~~Wire a metric on validation failures~~ **(task 11:
  `tidepool_lexicon_validation_failures` expvar counter, served on the
  bearer-protected `GET /admin/metrics`)**; the strict-first rollout stays
  deferred — that counter sitting at zero in production is its
  precondition.

## Sync surface (task 04 notes)

- ~~No `#account{active:false}` frame on consent revocation.~~ **Closed by
  task 11** (see the Relay pipeline entry: durable-log `account` event
  kind, migration 011, emitted by `DeleteActor`, status `deleted` — which
  also became the bridge's own `getRepoStatus`/`listRepos` status token,
  replacing `deactivated`). Tombstoned actors' HISTORICAL events still
  replay until retention expires — deliberate: replay windows are
  advertised complete, and the trailing #account frame is the purge signal.
- ~~No connection cap / per-IP rate limit on the public sync surface.~~
  **Closed by task 11**: per-client-IP token bucket over every
  `com.atproto.sync.*` endpoint (`SYNC_RATE_PER_SECOND`/`SYNC_RATE_BURST`,
  429 `RateLimitExceeded`; `_health` exempt — container healthchecks must
  not flap) + a concurrent `subscribeRepos` connection cap
  (`SYNC_MAX_SUBSCRIBERS`, reserve-then-check, 429
  `SubscriberLimitExceeded`).
- ~~`getRepo` buffers full CARs in memory; `ExportCAR` includes unreachable
  historical blocks (consider reachable-set-only).~~ **Closed by task 12**:
  `repo.ExportCARTo` streams the CAR block-by-block (memory bounded by one
  fetch batch, `walkBatchSize` = 256 blocks, plus a CID-string seen-set that
  grows with the reachable set) and exports only the reachable
  set from the current head (10.9x smaller on the 2k-record benchmark
  fixture; correct CAR consumers traverse from the root, so omitting
  superseded blocks is transparent — verified against indigo's
  `LoadRepoFromCAR`). The sync handler streams; a pre-first-byte failure
  still returns a clean 500, a mid-stream failure can only truncate
  (logged).
- ~~`PruneEvents` is one unbatched DELETE per hourly sweep.~~ **Closed by
  task 11**: batched (1000/statement) from the oldest seq up, so a partial
  sweep still leaves a contiguous retained suffix.
- ~~MST loads are full-tree (one SELECT per node) → `PutRecord` is O(repo
  size); needs a per-DID tree cache before big-community scale.~~ **Closed
  by task 12**: per-DID LRU tree cache (`internal/repo/treecache.go`,
  `MST_CACHE_SIZE`, default 512 repos) — steady-state commit is an
  in-memory mutation + diff write (42.6x faster on the 2k-record fixture).
  Coherence model documented in that file: take() detaches (a failed commit
  can never leave a mutated tree cached), put() only under a
  durably-committed head, head validated against the value read under the
  commit locks (a cross-process commit makes the entry stale, not wrong).
  A cold first commit still pays one full-tree load.
- `SigningKeys` could become a `SignCommit` capability (keeps key plaintext
  inside identity; enables KMS later) — revisit before the interface
  calcifies.
- `getRepo`'s optional `since` parameter (diff export) is not implemented —
  a `since` request gets the full reachable-set CAR, which the spec permits
  (extra blocks are legal); consumers needing incremental sync use
  subscribeRepos (`internal/sync/server.go`). Deliberately left that way in
  task 12: a real diff export would have to read historical blocks, which
  the blocks GC invariant (internal/repo/gc.go) explicitly does not
  guarantee — implementing it means revisiting that invariant, not just
  adding a query.

## Ingestion (task 06 notes)

- ~~No per-signer/per-IP rate limit on `/inbox`.~~ **Closed by task 11**:
  two token-bucket layers (shared `internal/ratelimit`, the votes-limiter
  discipline — sweep throttle, 50k fail-closed cap): per client IP before
  the body is read, per verified signer after verification, plus a
  DEDICATED much tighter per-IP cap on the `tombstonedSelfDelete`
  confirmation branch (checked after the shape checks, BEFORE the outbound
  confirmation fetch — the unauthenticated fetch+2-durable-writes
  amplification task 10 flagged). All refusals are **503, never 4xx**:
  Lemmy's federation crate retries server errors but drops 4xx permanently,
  and rate-limited legitimate deliveries must delay, not vanish. Config
  `INBOX_IP_RATE_PER_SECOND`/`INBOX_SIGNER_RATE_PER_SECOND`/
  `INBOX_TOMBSTONE_CONFIRMS_PER_MINUTE` (+ bursts).
- Every in-process per-IP limiter (inbox, the tombstone-confirmation cap, and
  the `com.atproto.sync.*` bucket) keys on `RemoteAddr` and IGNORES
  `X-Forwarded-For` — a deliberate non-goal (XFF is spoofable without a
  trusted-proxy allowlist). A proxied/LB deployment MUST rate-limit at the
  edge (see the README operations note), or every client shares the proxy's
  one bucket and the tombstone-confirmation cap becomes GLOBAL. Clean future
  fix if a real deployment needs it: an opt-in `TRUSTED_PROXY` config that
  parses `X-Forwarded-For` only from an allowlisted hop; RemoteAddr-only stays
  the safe default.
- ~~`ap_tombstones` grows unbounded.~~ **Closed by task 11**: batched
  pruner (`TOMBSTONE_RETENTION`, default 30d, shared `internal/prune`
  runner — fail-closed on non-positive retention). Accepted trade-off: a
  pruned marker re-opens delete-before-create for that id, but redelivery
  horizons are hours-to-days.
- ~~A `Delete` arriving before its object was ever materialized leaves
  nothing to tombstone → a later `Create` still materializes.~~ **Stale —
  closed by task 06 itself and verified in task 11**: `handleDelete`
  records the `ap_tombstones` marker BEFORE `HandleDelete`, and
  `materializeContent` checks it (`TestCreateAfterDeleteTombstone` pins the
  whole ordering, restore included). The README's claim was correct; this
  entry was the false doc.
- ~~`ClaimNext` does an O(N) row scan when one community's queue backs up
  behind a failing event (per-key serialization cost; revisit at scale).~~
  **Closed by task 12**: the claim query is now a recursive loose index
  scan over `idx_inbox_events_queue` — one index descent per distinct
  pending ordering key, O(pending keys × log N) regardless of any key's
  backlog depth (measured: 162.6 ms → 0.05 ms with a 50k-deep backed-up
  key). Same semantics, same fencing contract, no schema change;
  `TestInboxEvents_DeepBacklogDoesNotBlockOtherKeys` pins it.
- A shutdown-interrupted attempt still consumes its ClaimNext attempt
  increment (cosmetic).
- ~~`MAX_BLOB_BYTES` above 5 MiB is a silent no-op.~~ **Closed by task
  11**: the AP client grew a dedicated media cap
  (`ClientOptions.MaxMediaBytes`, wired from `MAX_BLOB_BYTES`) so raising
  the blob budget does not also raise the JSON-object response cap.
- Mint-gate ("retry via queue backoff") is verified at unit level only —
  the harness never drives minting into the rate limiter (a low
  `MINT_RATE_PER_MINUTE` stack variant would need its own compose profile).
- The `activityID` rand-failure path is guarded but unit-untestable
  (Go 1.24+ makes a `crypto/rand` failure a fatal crash, not a returnable
  error) — permanent test gap unless the reader is injected.
- ~~`ingest.NewNoopVotes` is dead code.~~ **Deleted in task 11.**

## Votes (task 07 notes)

- **Lemmy vote-clear Undo carries a RECONSTRUCTED inner vote (e2e,
  fixed).** Measured on the wire against Lemmy 0.19: a flip federates as a
  bare opposite vote (no Undo at all), and a clear as Undo{Like} with a
  freshly generated activity id, typed Like even when the live vote is a
  Dislike — task 07's "Lemmy inlines the original Like" was false. The
  original id-targeted-only retraction therefore made EVERY Lemmy
  vote-clear a silent no-op; caught by the e2e retract-to-zero assertion
  and fixed in `internal/votes/aggregator.go` (id-targeted update first,
  known-id replay guard, then direction-agnostic live-vote fallback).
- ~~`vote_events` grows unbounded (no pruning of superseded/undone rows).~~
  **Closed by task 11**: batched pruner over UNDONE rows only
  (`VOTE_EVENT_RETENTION`, default 90d) — live rows are the counts and are
  never pruned. Accepted trade-off (documented on `PruneUndoneEvents`): an
  undone row is also its activity id's dedupe record, so replay protection
  now has the retention as its horizon.
- ~~Actor-delete / consent revocation does **not** scrub that actor's
  `vote_events` rows.~~ **Closed by task 11**: `votes.Aggregator.ScrubVoter`
  (deletes ALL of the voter's rows, recomputes every affected aggregate
  under ordered row locks; seeded baselines untouched), called from both
  `DeleteActor` and the reversible nobridge `SuppressActor` via the
  materializer's `VoteScrubber` hook. Residual: `ScrubVoter` takes the
  affected aggregate row locks in a deterministic subject order to avoid
  deadlock across the multiple subjects one voter touched, but that lock
  ordering has no concurrency test — deterministic deadlock tests are flaky,
  so it stays asserted by construction / at unit level only.
- ~~No true concurrency stress on the aggregate-row locking claim — a
  many-voters-one-post hammer is still missing.~~ **Closed by task 10**:
  `TestVoteHammer_ConcurrentVotersExactAggregates` fires ten real Lemmy
  voters in parallel bursts (votes, flips, clears) at one post and asserts
  exactly-correct final aggregates. Honest caveat: Lemmy's per-instance
  federation worker delivers sequentially and the bridge's queue serializes
  per community ordering key, so this proves burst correctness end-to-end
  rather than true same-row lock contention (which stays unit-level).
- Subject resolution happens outside the mutation tx (narrow TOCTOU with a
  racing Delete, documented in task 07).
- ~~No upper sanity cap on seeded counts~~ **(task 11:
  `votes.MaxSeededCount` = 1,000,000 — a hostile origin's absurd baseline
  is a validation error the seeder logs; the previous baseline survives)**;
  comment count seeding stays skipped (per-comment API calls would triple
  backfill egress).
- Baseline-voter drift: a voter counted only in the seeded baseline who
  later flips federates a bare Dislike (no Undo), leaving the baseline
  upvote next to the new live downvote; a clear sends an Undo the live-vote
  fallback cannot act on (no live row). Counts stale until re-seed.

## Materializer (task 05 notes)

- ~~Transient media-fetch failure on profile refresh drops existing blobs
  (no carry-forward).~~ **Closed by task 11**: a failed avatar/banner fetch
  while the actor still ADVERTISES the image carries the stored blob
  forward; a removed image still drops it. The related "stale actor behind
  a 403-ing instance drops content instead of serving stale" remains open
  (that is the actor DOCUMENT, not its media).
- ~~`commitRecord`'s PutRecord→PutMapping is not one tx.~~ **Closed by
  task 11**: `repo.PutRecordTx` runs a `TxSideEffect` inside the commit
  transaction (also on the idempotent NoOp path) and the materializer
  writes the mapping through `store.PutMappingTx` there — record and
  mapping land together or not at all, deterministic-rkey collisions now
  roll the record back too.
- ~~`DeleteActor` scrubs records but not blobs stored under community
  DIDs.~~ **Closed by task 11**: the scrub reads each record before its
  delete commit and deletes the blobs it referenced (`atdata.ExtractBlobs`
  — post thumbnails/images under COMMUNITY DIDs included); `DeleteActor`
  additionally drops every remaining blob under the actor's own terminally
  frozen DID. Accepted edge (commented on `repo.DeleteBlob`): blobs are
  content-addressed per DID (`blobs` PK `(did, cid)`), so two records in one
  repo embedding byte-identical media share a single blob row, and scrubbing
  one record can orphan the other's image. Accepted at bridge scale; a blob
  refcount / junction table is the fix if this ever bites.
- ~~Test gap: `embed.images` arm + nsfw label shapes never appear on the
  wire (needs pictrs-backed image upload in the harness).~~ **Closed by
  task 10**: `TestImagePost_EmbedImagesAndNSFWLabel` uploads through
  Lemmy's pictrs proxy, posts it NSFW with alt text, and asserts the
  `embed.images` blob ref + `nsfw` self-label lexicon-validate on the
  relay-fed wire and the blob serves back byte-consistent via `getBlob`.
  (Unit-level lexicon fixtures for those arms are still absent — the e2e
  path is the coverage.)
- Residual TOCTOU (task 03): a consent flip racing an in-flight commit can
  let that one commit land (consent read is outside the commit tx; fine for
  single-writer v1).

## Storage / housekeeping

- ~~`service_keys.private_key_pem` column name lies for the "plc-rotation"
  row.~~ **Closed by task 11**: renamed to `key_material` (migration 013;
  the per-row encoding — plaintext PEM vs sealed ciphertext — is documented
  on `store.ServiceKey`).
- ~~`blocks` is append-only with no GC (load-bearing for GetRecord read
  consistency; revisit together with the getRepo memory item).~~ **Closed
  by task 12**: `repo.GCBlocks` (shared `internal/prune` runner, 6h sweeps,
  `BLOCKS_GC_RETENTION` default 72h) deletes blocks that are BOTH
  unreachable from their repo's current head AND older than the retention
  cutoff. The full invariant + the audit of every consumer is written at
  the top of `internal/repo/gc.go` — the load-bearing parts are that
  readers moved to REPEATABLE READ snapshots, firehose replay reads the
  self-contained `firehose_events.car` (never `blocks`), and the commit
  path's `ON CONFLICT ... DO UPDATE SET created_at = clock_timestamp()`
  refresh makes the retention window the compute→delete race guard. If a
  real `since` diff export ever gets served from `blocks` history, it must
  revisit that invariant first (the fallback stays: `since` requests get
  the full reachable-set CAR, which the spec permits).

## E2E harness itself (task 08)

- **PLC directory image is pinned** to a did-method-plc commit
  (`PLC_COMMIT` in `e2e/plc/Dockerfile`); bump it deliberately via
  `git ls-remote` when upstream fixes/features are needed.
- Jetstream **exits** when its upstream drops; `restart: unless-stopped`
  papers over it. Since task 09 its upstream is the relay (which stays up
  across the bridge-restart scenario — the relay's own slurper reconnects),
  so the policy only matters if the relay itself dies. If Jetstream grows
  reconnect logic upstream, drop the policy.
- Unexpected-collection enforcement runs on **every** commit event any
  listener consumes (await and drain both fail fast on a collection outside
  the four emitted ones), and the vote scenarios watch the firehose
  unfiltered while votes flow. ~~Remaining gap: events emitted while no
  unfiltered listener is subscribed go unchecked.~~ **Closed by task 10**:
  `TestZZ_SuiteEndSweep` (alphabetically-last file, runs after every
  scenario) replays the entire firehose from cursor 1 (a zero/negative
  cursor omits the param and live-tails — `tests/e2e/helpers.go`
  newListener) on a fresh unfiltered listener and re-vets every retained
  event — collection whitelist, lexicon validation of every create/update,
  per-DID rev monotonicity from the beginning of retained history —
  terminated by a fresh-subscription sentinel, not a bare quiet window,
  with a replay floor (min commit count + at least one post create and one
  delete op) so a truncated replay cannot pass on the sentinel alone.
- **The sweep's replay depth is Jetstream's event TTL** (24h in the pinned
  image): a stack left up longer than the TTL under-vets — history older
  than the TTL is gone from Jetstream's store and nothing can re-check it —
  unless the stack (or at least the Jetstream container + its volume) is
  recreated per run, as `make e2e` does. The sweep's replay floor catches
  the degenerate cases (near-empty store, live-tail regression) but cannot
  recover expired events.
- ~~Scenario ideas not yet covered: image post, consent, `Delete(Actor)`,
  unsubscribe, community profile update.~~ **Closed by task 10**
  (`tests/e2e/media_test.go`, `lifecycle_test.go`, `votes_hammer_test.go`,
  `zz_sweep_test.go`). The stretch idea from the task — a low
  `MINT_RATE_PER_MINUTE` stack variant driving the mint gate end-to-end —
  was skipped as specced (it needs its own compose profile; see the
  mint-gate item under Ingestion).
- The e2e compose now sets `PROFILE_REFRESH_TTL=2s` (consent scenario
  driver — see the Federation note on Update{Person}). Side effect: every
  materialization re-fetches sub-2s-stale actor profiles; unchanged
  profiles are idempotent no-op re-puts, so no extra firehose traffic — but
  a future scenario asserting fetch COUNTS would need to account for it.

## CI

- **The e2e job has never run on GitHub Actions**, and it cold-builds
  **Lemmy from source every run.** BuildKit cache
  mounts (the cargo target-dir cache in `e2e/lemmy/Dockerfile`) do not
  persist on GitHub-hosted runners, so each CI run pays the full debug
  Rust compile — plausibly 30–60+ min on a 2-core runner, with disk
  pressure to match (the workflow prunes preinstalled images up front as a
  stopgap). Mitigations, in preference order: build the lemmy-debug image
  once, push it to GHCR, and have the compose file pull it (rebuild only on
  version bumps); or wire buildx's `gha` cache backend via
  docker/build-push-action. Neither is verifiable locally, so this stays a
  follow-up until the job has run on Actions at least once.
