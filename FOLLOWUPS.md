# Follow-ups

Open work, accepted limitations, and operational caveats remaining after the
v1.1 hardening and performance passes. Completed task history belongs in the
task documents and git history rather than this list.

## Relay pipeline

- A fresh self-hosted BigSky refuses non-admin `requestCrawl` calls because
  its new-PDS daily limit defaults to zero. Bootstrap it with the admin
  `setPerDayLimit` call (as `relay-bootstrap` does in the e2e compose stack).
- Verify BigSky configuration against `--help`: the pinned image does not
  recognize `BGS_CRAWL_INSECURE_WS`, `BGS_PORT`, or `LOG_LEVEL`.
  `--crawl-insecure-ws` is a command argument and the log variable is
  `BSKYLOG_LOG_LEVEL`. Also set `ATP_PLC_HOST` and `RESOLVE_ADDRESS` when the
  public PLC directory and `1.1.1.1` resolver are inappropriate.
- The pinned BigSky image has no arm64 manifest. Apple Silicon uses amd64
  emulation; revisit when bumping the image.
- **Jetstream has surprising quiet-stream cursor behavior.** A cursor between
  its newest stored event and server-now replays the full retained store.
  Consumers must be idempotent and must not treat a wall-clock "now" cursor as
  a dedupe boundary.
- Indigo's slurper stops retrying after roughly three minutes of continuous
  dial failure and marks the PDS unregistered. Tidepool re-announces on its own
  startup, but a long Tidepool outage while the relay remains up requires a
  fresh `requestCrawl`.

## Federation and interop

- Lemmy's local author auto-upvote is not federated, so a newly bridged post
  can show one fewer upvote than Lemmy until a backfill re-seed.
- Lemmy 0.19.x does not federate profile edits. A new `#nobridge` marker is
  discovered only after `PROFILE_REFRESH_TTL` expires and another activity
  triggers an actor fetch; an inactive actor's opt-out is not discovered.
  Add a periodic consent re-scan of active bridged actors.
- Existing Lemmy peers that federated before Tidepool served its instance
  actor at `GET /` may need an actor re-fetch and Lemmy federation-worker
  restart before account deletions are delivered.
- Tombstoned self-delete confirmation accepts a 410 response or ActivityPub
  Tombstone, not a plain 404. This is correct for the currently supported
  Lemmy target but must be revisited for platforms that return 404 on actor
  deletion.
- Lemmy's AP `Image` attachment omits `mediaType`; only its `type` and the URL
  extension identify it as an image. `Link` attachments omit alt text, so alt
  text is lost if metadata fetching fails.
- PieFed and Mbin are untested. `communityRef` currently uses Lemmy's `/c/`
  URL heuristic; Mbin uses `/m/`.
- Lemmy 1.0/API v4 is untested. The harness, seeder, and WebFinger assumptions
  target pinned Lemmy 0.19.x/API v3.
- The ActivityPub service actor uses type `Service` because Lemmy rejects
  `Application` for Person actors. Revisit only if another supported platform
  proves incompatible.
- Production WebFinger is HTTPS-only by design. HTTP fallback exists only with
  the dev/e2e SSRF relaxation.

## Write side and product scope

- Outbound Coves-to-Lemmy federation and ActivityPub actors for Coves users.
- Key claiming/migration for bridged users. Handle-collision recovery should
  eventually reuse an orphaned minted DID via a PLC `updateHandle` operation
  rather than minting another DID.
- If vote write-back is added, suppress echoes of Tidepool-managed voters and
  subtract Tidepool's written-back tally during subsequent Lemmy re-seeds.
- Moderation federation and DMs.

## Outbound delivery (task 15)

- **outbound_deliveries.ClaimNext lacks a standalone `seq` index.** The
  loose-scan CTE builds the head set via the `(ordering_key, seq)` partial
  index, but the outer `c.seq = ANY(ARRAY(...)) FOR UPDATE` re-check has no
  index on `seq` alone (`seq` is BIGSERIAL, not the PK `(activity_id,
  target_inbox)`). Unlike inbox_events (where `id` IS the PK), this is not a
  point-fetch. A dedicated `UNIQUE INDEX (seq)` in its OWN migration (the next
  free number — 023 as of this writing; amending an already-applied migration
  is a silent no-op under goose) would
  restore O(keys × log N); add it if the claim path profiles hot.
- **OutboundDeliveries is a 12-method interface** (go-proverbs SHOULD). It
  is one cohesive repository seam but the worker uses only the
  claim/mark subset and the admin API only inspect/redrive/cancel. If it
  grows, split into a `deliveryClaimer` (worker) + `deliveryAdmin`
  (admin) at the consumer packages.
- **DeliveredStateUndone is reserved but never written** — task 15 deletes
  the outbound_votes row on Undo-delivery success rather than transitioning
  to 'undone'. Kept in the enum + CHECK against a future keep-the-record
  policy; nothing consumes it today.
- **Image embeds don't federate outbound yet** — `social.coves.embed.images`
  → `attachment [{Image,url}]` and external-embed thumbnails need a
  blob→author-PDS-getBlob-URL seam not yet designed (decision 13: native
  blobs live on the author's PDS, never Tidepool). Link embeds + text +
  nsfw + name + content/source all federate; images are dropped with a
  documented gap in apobject/translator.
- **Enqueuer resolves the inbox (a network fetch on cache miss) inside the
  consumer's rev-gate tx** — a slow/hanging community Group doc head-of-line
  blocks the consumer's hot path (gate row locks held for fetch latency).
  Rare (1h TTL), but consider pre-resolving before the gate tx or a short
  distinct resolve timeout.

## Postv2 flip (task 19)

- **Legacy-set migration is deferred deliberately** (product decision
  2026-08-11): pre-flip bridged posts stay on the deprecated
  `social.coves.community.post` collection in community repos forever;
  every era-sensitive path dispatches on the mapping's collection. A
  future migration needs Coves' PRD §11 remap decision (comment/vote
  refs point at the old URIs) plus an old→new ledger. Recorded
  consequence: Coves' legacy-drain gate never fires while bridged
  legacy posts exist.
- **postv2 provenance costs one extra actor fetch per post
  create/edit**: `originalAuthor.displayName` is read from the fetched,
  authority-bound actor document on every materialization (deterministic
  records; inline `attributedTo` is never trusted). The zero-extra-fetch
  alternative is persisting the display name on `bridged_actors`,
  refreshed with the profile — a migration plus plumbing. Revisit if
  actor-fetch egress on post paths matters at scale.
- **`SetBridgedStats`' NoOp result carries `mapping.CID`**, which can be
  stale relative to the record the unchanged-counts read just observed.
  Callers currently ignore it; worth aligning with the read's CID.
- **No standalone acceptance-reconcile sweep**: every acceptance heal is
  driven by an event on the post (redelivery, edit, stats sweep). A
  quiet community's crash-window acceptance gap persists until any such
  event. The decision-19 reconciliation job (task 18) is the natural
  home for a periodic acceptance-vs-record pin audit.

## Production rollout

- Bluesky's public relay accepts new PDS hosts, but its documented default
  allowance is only 100 accounts, 50 repo-stream events/second, 2,600/hour,
  and 21,000/day. Tidepool mints one repo DID per bridged actor/community and
  will exceed the account cap quickly. Arrange a relay limit increase before
  broad subscriptions, or operate a suitably bootstrapped relay.
- `ENVIRONMENT=production` has not been exercised end-to-end. The harness uses
  development mode for migrations-on-start, HTTP/private fetching, and strict
  lexicon validation.
- Production lexicon validation currently records a metric and writes the
  record instead of failing it. A strict-first rollout should happen only
  after `tidepool_lexicon_validation_failures` remains zero in production.

## Sync surface

- `SigningKeys` could become a `SignCommit` capability so private key material
  stays inside the identity package and a future KMS implementation is easier.
- `getRepo` ignores the optional `since` optimization and returns a full
  reachable-set CAR, which the spec permits. A real diff export would require
  retaining historical blocks and revisiting the blocks-GC invariant.

## Ingestion and rate limiting

- All in-process IP limiters use `RemoteAddr` and ignore
  `X-Forwarded-For`. A deployment behind a proxy/load balancer must rate-limit
  at the edge or all clients share the proxy's bucket. A future opt-in
  `TRUSTED_PROXY` setting should trust forwarded addresses only from an
  allowlisted hop.
- A shutdown-interrupted queue attempt still consumes its `ClaimNext` attempt
  increment (cosmetic).
- The mint gate is covered only by unit tests; the e2e harness has no
  low-`MINT_RATE_PER_MINUTE` compose variant.
- The `activityID` random-source failure path is untestable without injecting
  the reader (Go 1.24+ treats `crypto/rand` failure as fatal).

## Votes

- `ScrubVoter` takes aggregate locks in deterministic subject order, but this
  deadlock-avoidance property has no true concurrency test.
- Vote subject resolution occurs outside the mutation transaction, leaving a
  narrow race with deletion.
- Baseline-only voters can temporarily drift: a later flip or clear lacks a
  per-voter baseline row to retract. A re-seed heals the aggregate.

## Materializer and storage

- A stale actor document behind a temporarily forbidden/unavailable instance
  can drop content instead of serving the last known actor state. Media blobs
  already carry forward on transient fetch failure; actor documents do not.
- Blobs are content-addressed per DID without refcounts. Two records in one
  repo can share a blob row, so scrubbing one record can remove media still
  referenced by another. Add a refcount/junction table if this occurs in
  practice.
- A consent change racing an in-flight commit can allow that one commit to
  land because consent is read outside the commit transaction.

## E2E harness and CI

- The PLC directory image is pinned by commit in `e2e/plc/Dockerfile`; bump it
  deliberately when upstream fixes are needed.
- Jetstream exits when its upstream disconnects; compose's
  `restart: unless-stopped` supplies recovery. Remove the workaround if
  Jetstream gains reconnect support.
- The suite-end replay sweep is bounded by Jetstream's 24-hour event TTL.
  Recreate the stack/volume per run, as `make e2e` does, or old events cannot
  be revalidated.
- The e2e suite has not yet completed on GitHub Actions. It cold-builds Lemmy
  from source and may exceed practical runner time/disk limits. Prefer a
  prebuilt pinned Lemmy debug image in GHCR, or persist a buildx GHA cache.
- The declarative follow list (FOLLOW_LIST_PATH) has no e2e coverage: an
  end-to-end convergence test would mount a follow-list YAML into the
  tidepool compose service, restart it, and poll GET /admin/communities
  until the listed community turns `accepted`, then remove the entry and
  assert an Undo lands at Lemmy. The reconciler is unit-tested against the
  ingest harness (real Postgres + fake Lemmy inbox) and was smoke-tested
  live (startup fail-fast, sweep, POST /admin/communities/reconcile);
  compose plumbing for the file mount is the missing piece.
- Cold-start visibility gap (observed at the 2026-07-13 production launch):
  records committed before a relay's first subscription never re-emit as
  live commit frames, so a Jetstream-fed AppView misses them — the initial
  community backfill ran ~25 minutes before bsky.network accepted the
  crawl, and none of those posts indexed in Coves despite the relay
  serving the repos (crawl imports state, not per-op events). Live records
  heal organically (any bridgedStats vote sweep or upstream edit re-emits
  the record as an update commit, which the AppView upserts), but quiet
  posts stay invisible. RESOLVED: `POST /admin/reemit` walks a repo (or
  all repos) and re-emits every record as a delete+create commit pair —
  honest diffs, unchanged at-uris/CIDs (a same-value "touch" update was
  rejected as the design: its op list would not match the empty MST diff,
  which sync-v1.1-validating relays may refuse). Remaining follow-up: the
  e2e harness has no scenario covering it.
