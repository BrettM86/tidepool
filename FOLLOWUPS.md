# Follow-ups

Everything known-deferred at the end of the v1 build loop (tasks 01–08),
collected from `LOOP_STATE.md`'s cross-task notes plus discoveries made
while building the e2e harness. Organized by area; items marked **(e2e)**
were discovered or confirmed by the task-08 harness.

## Federation & interop

- **Lemmy first-contact Accept race (e2e).** Lemmy's federation queue
  initializes a newly-seen instance's cursor at the *current* max activity
  id ("skip all past activities", `crates/federate/src/worker.rs`), so the
  Accept answering the very first Follow to a given Lemmy instance is
  usually skipped: the instance row is created by that same Follow, and the
  per-instance worker spawns after the Accept is queued. Re-sending the
  Follow (fresh activity id → fresh Accept) recovers. The harness retries in
  `subscribeCommunity`; production operators hit this at most once (usually)
  per peer instance. Consider an automatic Follow re-send in the follow
  lifecycle when a subscription stays `pending` past a threshold.
- **Author auto-upvotes do not federate (e2e).** Lemmy casts a local Like
  by the post author but never announces it, so live-bridged posts read one
  upvote lower than Lemmy's UI until a backfill re-seed. Accepted drift;
  document for the AppView.
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
- Relay `requestCrawl` is wired but has never been exercised against a real
  relay (dev logs instead of sending).
- `ENVIRONMENT=production` has never been end-to-end tested (the harness
  runs development mode for migrations-on-start, strict validation, http
  scheme, private fetch).
- Strict lexicon validation is dev/test-only; production logs-and-writes.
  Wire a metric on validation failures and consider a strict-first rollout
  (task 05 note).

## Sync surface (task 04 notes)

- **No `#account{active:false}` frame on consent revocation** — subscribers
  currently rely on the scrub delete-commits; tombstoned actors' historical
  events stay replayable until retention expires.
- No connection cap / per-IP rate limit on the public sync surface
  (pre-internet-facing hardening).
- `getRepo` buffers full CARs in memory; `ExportCAR` includes unreachable
  historical blocks (consider reachable-set-only).
- `PruneEvents` is one unbatched DELETE per hourly sweep.
- MST loads are full-tree (one SELECT per node) → `PutRecord` is O(repo
  size); needs a per-DID tree cache before big-community scale.
- `SigningKeys` could become a `SignCommit` capability (keeps key plaintext
  inside identity; enables KMS later) — revisit before the interface
  calcifies.

## Ingestion (task 06 notes)

- **No per-signer/per-IP rate limit on `/inbox`** — queue-flood DoS via
  many self-signed identities remains the top hardening item.
- `ap_tombstones` grows unbounded (no pruner — mirror `FIREHOSE_RETENTION`
  treatment).
- A `Delete` arriving before its object was ever materialized leaves
  nothing to tombstone → a later `Create` still materializes (needs dedup /
  tombstone-of-unseen-ids).
- `ClaimNext` does an O(N) row scan when one community's queue backs up
  behind a failing event (per-key serialization cost; revisit at scale).
- A shutdown-interrupted attempt still consumes its ClaimNext attempt
  increment (cosmetic).
- `MAX_BLOB_BYTES` above 5 MiB is a silent no-op (clamped by the AP
  client's fixed `maxResponseBytes`).
- Mint-gate ("retry via queue backoff") is verified at unit level only —
  the harness never drives minting into the rate limiter (a low
  `MINT_RATE_PER_MINUTE` stack variant would need its own compose profile).

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
- `vote_events` grows unbounded (no pruning of superseded/undone rows).
- Actor-delete / consent revocation does **not** scrub that actor's
  `vote_events` rows (inconsistent with the scrub posture elsewhere; counts
  are anonymous on the wire, so exposure is low).
- No true concurrency stress on the aggregate-row locking claim — the e2e
  burst scenario exercises concurrent queue workers across communities but
  contains **no votes at all**, so the vote-concurrency path is untested
  beyond unit level; a many-voters-one-post hammer is still missing.
- Subject resolution happens outside the mutation tx (narrow TOCTOU with a
  racing Delete, documented in task 07).
- No upper sanity cap on seeded counts; comment count seeding skipped
  (per-comment API calls would triple backfill egress).
- Baseline-voter drift: a voter counted only in the seeded baseline who
  later flips federates a bare Dislike (no Undo), leaving the baseline
  upvote next to the new live downvote; a clear sends an Undo the live-vote
  fallback cannot act on (no live row). Counts stale until re-seed.

## Materializer (task 05 notes)

- Transient media-fetch failure on profile refresh drops existing blobs
  (no carry-forward); a stale actor behind a 403-ing instance drops content
  instead of serving stale.
- `commitRecord`'s PutRecord→PutMapping is not one tx (self-heals on retry;
  a Delete landing in the crash window logs Warn).
- `DeleteActor` scrubs records but not blobs stored under community DIDs.
- Test gap: `embed.images` arm + nsfw label shapes are never
  lexicon-validated by unit tests (only the external-embed arm is). The e2e
  suite lexicon-validates every create/update its listeners consume — the
  external-embed arm crosses the wire via scenario 2's link post — but no
  scenario posts an image, so `embed.images` never appears on the wire
  (needs pictrs-backed image upload in the harness).
- Residual TOCTOU (task 03): a consent flip racing an in-flight commit can
  let that one commit land (consent read is outside the commit tx; fine for
  single-writer v1).

## Storage / housekeeping

- `service_keys.private_key_pem` column name lies for the "plc-rotation"
  row (it holds sealed ciphertext) — rename candidate (task 03 note).
- `blocks` is append-only with no GC (load-bearing for GetRecord read
  consistency; revisit together with the getRepo memory item).

## E2E harness itself (task 08)

- **PLC directory image is pinned** to a did-method-plc commit
  (`PLC_COMMIT` in `e2e/plc/Dockerfile`); bump it deliberately via
  `git ls-remote` when upstream fixes/features are needed.
- Jetstream **exits** when its upstream drops; `restart: unless-stopped`
  papers over it. If Jetstream grows reconnect logic upstream, drop the
  policy.
- Unexpected-collection enforcement runs on **every** commit event any
  listener consumes (await and drain both fail fast on a collection outside
  the four emitted ones), and the vote scenario watches the firehose
  unfiltered while votes flow. Remaining gap: events emitted while no
  unfiltered listener is subscribed go unchecked — a stack-wide
  "nothing else ever appeared" sweep at suite end would close it.
- Scenario ideas not yet covered: image post (pictrs → blob → embed.images
  lexicon validation), consent (`#nobridge` in a Lemmy bio → suppression),
  `Delete(Actor)` tombstone flow, unsubscribe (Undo{Follow}) stopping
  announces, community profile *update* propagation.

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
