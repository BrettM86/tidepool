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
- Moderation federation and DMs.

## Outbound delivery (task 15)

- **Parks are attempt-neutral — the design that was chosen, and the two limits
  it leaves open.** `ClaimNext` charges an attempt to every claim
  (`internal/store/outbound_deliveries.go:143`), which is right for a delivery
  that was TRIED and wrong for one that was HELD. The fix is a SIBLING of
  `Release`, not a flag on it: `ReleaseParked` (`:276`) runs the same fenced
  statement from the same template (`releaseStatement`, `:224`) with one slot
  filled, `attempts = GREATEST(attempts - 1, 0)` (`:243-244`), and `park` /
  `parkCausal` settle through it (`internal/outbound/worker.go:572`, `:592`).
  The fence — `state = 'pending' AND claimed_until = $token` — is what makes the
  give-back safe: only the holder of the claim that added an increment can
  subtract one, so a park can never un-count an attempt another claim spent.

  *The rejected candidate, recorded because it reads cheaper than it is.*
  "Reset on unpark" — leave the increment, clear `attempts` when the delivery
  next passes the switch gate — keeps `Release` single-purpose but cannot tell a
  park's attempts from real failures that preceded the park, so it erases
  genuine failure history instead of handing back a hold; the only signal
  available to it is `last_error_class`, which would make an error-class string
  load-bearing for a correctness decision; and it repairs late, so `attempts`
  reads inflated on the admin surface for the whole duration of the park.

  *Two residual limits, both pre-existing and neither closed by this.*
  - **An abandoned claim still leaks its `+1` forever.** A lapsed lease, or a
    crash between `ClaimNext` and any settle, leaves an increment with nobody
    holding the fence to hand it back — so `attempts` counts claims CHARGED AND
    NEVER HANDED BACK: deliveries genuinely tried, plus abandoned claims. Each
    leak permanently costs the delivery one retry of real poison budget; the
    lease bounds only how soon the row is re-claimable, not the loss. Rare and
    bounded at **at most one lost retry per abandoned claim**, and shared with
    the real-failure path, so it is not a park defect — but it is the reason a
    poisoned row can read above `maxAttempts`. A park that the fence REFUSED now
    logs at Warn (`park did not apply: claim lost or row terminal`,
    `internal/outbound/worker.go`) and is not counted in
    `tidepool_outbound_parked`, which is what makes such a leak diagnosable
    rather than merely visible in the column.
  - **A park writes `last_status_code = 0`,** clobbering a real status a PRIOR
    failed attempt recorded. The divergence sweep reads that column as its
    answered-or-silent discriminator (`COALESCE(d.last_status_code, 0) > 0`,
    `internal/store/divergence.go:791`), where 0 means the peer never answered —
    so a 502 overwritten by a later park leaves no trace of the peer having
    spoken. Unchanged by this fix (a park has no status to write) and worth
    revisiting only if the refused/unanswered split has to be trusted per-row.

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

## Community bans (task 17c-3)

- **The in-transaction ban re-check NARROWS the race, it does not close
  it.** `accept()`'s side effect asks `StandingTx` on the acceptance
  transaction before writing, so the common ordering is covered — but
  under READ COMMITTED a ban committing between that read and the commit
  is still possible. Closing it needs SERIALIZABLE or a keyed lock shared
  with the ban path, which is a heavier change than the exposure
  warrants; the code says so where it happens.
- **A delivery claimed a moment before the ban commits can still be
  POSTed.** Cancellation reaches the row, not the socket: the worker's
  claim transaction has already committed, and the fencing correctly
  discards its settlement afterwards. So the row can read `cancelled`
  for something the peer accepted — and the vote reseed subtracts
  exactly the `delivered` state. A worker-side ban re-check immediately
  before `SendActivityAs` would shrink the window to the send itself; it
  was deliberately NOT added, because the escape it was meant to close
  (new comments and votes) is now closed at the source, and it would be
  defence-in-depth with no test behind it plus a new index. 17e's
  reconciliation is the natural place to detect the divergence.
- **A temporary ban that lapses leaves state divergent.** Posts accepted
  before the ban keep their acceptance records in the community repo,
  but their deliveries were cancelled, and `cancelled` is terminal
  (`RedrivePoisoned` revives only poisoned rows). So the community
  renders content on Coves that Lemmy will never receive, and nothing
  re-queues it when the exclusion ends. Exactly the atproto-vs-outbound
  divergence 17e is meant to report.
- **No e2e Block coverage.** The wire keys are hand-written in unit
  fixtures, so nothing exercises a real Lemmy's spelling. Both `expires`
  and AS2 `endTime` are read, but a third spelling — or a shape
  difference in `target` — would be invisible until production. Task 18's
  moderation scenario is the place for a captured Block.
- **A ban is recorded only in a private table.** A
  `social.coves.moderation.ban` lexicon exists, and every other
  moderation decision here is published into the community repo
  (acceptance, removal) where Coves' surfaces read it. As landed, a
  banned native author sees their posts rejected and the community's own
  moderation surface shows no ban. Deliberate scope cut for Scope A;
  recorded so it is a decision rather than an omission.

## Moderation state (task 17c-2)

- **Bridge-side comment removal is WRITE-ONLY.** Nothing reads
  `object_moderation.removed_at`, so a moderator-removed native comment
  still federates the author's later edits and still accepts native
  replies beneath it. That is the deliberate consequence of the unlanded
  comment-subject removal lexicon (the state is recorded so it can be
  honored the moment there is somewhere to publish it), but it is the
  same asymmetry the lock's own doc warns about: holding state is worth
  nothing unless something reads it. Closing it means gating child
  enqueues and edit federation on the parent's removal state, the way
  the lock already gates them.
- **The re-materialization test pins a COMPOSITE invariant.** Biting
  `storedThreadRoot` to `""` leaves it green, because `carryForward`
  restores the comment's original reply refs so the record still names
  the right thread and the mapping follows. `storedThreadRoot` is
  therefore single-covered, not double-covered — isolating it needs a
  stored record with no reply refs at all, which is repo-record surgery.
  Recorded so nobody reads that green as proof of the belt when it is
  proof of the brace.

## Thread locks (task 17c-2)

- **Comments materialized before migration 026 carry no
  `thread_root_at_uri`**, so they resolve to themselves and a lock on the
  post above them will not refuse native replies hanging under them. It
  degrades to the pre-lock behaviour rather than to the community-wide
  park, and it heals the moment each comment is next materialized — an
  admin backfill of the affected communities is the deliberate fix. A
  record-read fallback (reading `reply.root` back through `RecordGetter`
  when the column is empty) would close it for the existing corpus, but
  the consumer holds no repo manager and wiring one to read a fact the
  materializer already computes is the wrong trade on the hot path.
- **The lock read is per-object, keyed by at-uri.** A lock recorded on a
  Lemmy post reaches native replies anywhere in that thread; it does NOT
  reach replies whose chain the bridge never materialized. That is the
  boundary of what the bridge can know, not a policy choice.
- **`walkThreadRoot`'s dead-end branch remains reachable for native
  content** whose outbound state predates the root column (legacy rows
  only, and self-healing on the next successful edit). When it fires and
  the community holds a standing lock, the event is refused retryably and
  the error names the at-uri the chain stopped at. If dead ends ever
  become common, that branch parks traffic — the two fediverse controls in
  `moderation_lock_test.go` go red the moment it becomes reachable from
  fediverse resolution, which is what keeps it honest.

## Vote accounting (task 17b)

- **The re-seed baseline clamp is a FLOOR BREACH, not a discard
  detector.** `GREATEST(0, …)` fires only where the deficit exceeds a
  subject's entire fediverse tally, so on any post with a real score a
  Lemmy `FederationMode` silently discarding our written-back votes
  understates the served total with the raw baseline still positive and
  the counter at zero. `tidepool_vote_seed_ours_subtracted` is the
  signal that advances on healthy subjects; comparing it against a
  Lemmy-side sample is the actual audit.
- **Nothing re-seeds periodically.** `SeedPostCounts` has one caller,
  behind `SEED_COUNTS_FROM_API`, skipped inside a 1h window unless an
  admin forces a backfill. Any claim that drift "heals on the next
  re-seed" may mean never for a quiet community.
- **Migration 023's cleanup DELETE is narrower than the runtime guard
  it backfills.** It matches `voter_ap_id` by exact string equality
  while `echo.identifyActor` matches normalized host + scheme, so a
  legacy row carrying a non-canonical spelling of one of our actor ids
  would survive and then be counted in BOTH subtrahends — the one
  reachable way they double-subtract the same human. Accepted because
  the set is empty in production (write-back never shipped) and no code
  path here ever wrote non-canonical ids; a normalizing DELETE would
  mean re-implementing the probe in SQL.
- **A `Down` of migration 023 leaves baselines understated
  indefinitely.** Anything seeded while 023 was applied is net of our
  delivered votes, the pre-023 code does not re-derive it, and with no
  periodic re-seed only a forced backfill corrects it.

## Echo suppression (task 17a)

- **`ClassAncestorShortCircuit` counts ordinary threading, not a
  suppression.** It fires on the branch where `ResolveStrongRef` already
  anchored — no fetch was going to happen, identical control flow to a
  fediverse parent — so it rides inbound-comment volume while the other
  three classes are rare by construction, and it sits in the
  `tidepool_echo_drops_*` family though nothing is dropped. Flagged by
  three reviewers. Either rename it out of the drops family (an
  ancestor-anchor gauge) or move the counter to a site where a fetch was
  actually declined. It also costs a second point read of the row
  `ResolveStrongRef` just read, on the hot comment path.
- **Echo classification adds up to 4 indexed point reads per node to the
  highest-volume inbound path.** The 17a plan ruling asked for a cheap
  authority short-circuit before entity existence; the route-prefix test
  substitutes for it and is vanity-origin-safe, but an envelope crafted
  with `/ap/object/`-shaped ids on foreign hosts still forces reads at
  every probe, `MaxDepth` nodes deep. The inbox is signature-gated and
  rate-limited, so this is recorded rather than fixed.
- **No end-to-end vanity-origin announce test.** `Identify` is unit-covered
  in both directions (a positive case on the vanity origin, negatives for
  a DID whose `NormalizedOrigin` differs), but no inbound announce is
  driven for a vanity-origin persona through the full path.
- **`identifyObject`/`identifyActor` corroborate nothing.** Only
  `identifyActivity` binds the id to the activity we stored (verb + carried
  object). Object and actor ids are public and derivable, so a followed
  community can wear one to have content dropped as our echo. Bounded by
  the followed-community gate, and the announcer could simply not announce
  the content instead — but there is no per-entity payload to corroborate
  against, so closing it needs a different mechanism.
- **`materializeContent`'s legacy `origin=bridge` check is now a subset of
  the classifier** for announced traffic, and its only unique coverage is
  the bare Create/Update branch — the one dispatch branch that
  deliberately does not call `suppressEcho`, unlike Delete/Undo. The
  asymmetry is undocumented and the legacy check misses ids the classifier
  would catch (outbound_objects-only ids, activity ids, our own actor).
- **`Drops()` is exported with no production caller** and the tests keep a
  parallel class list; adding a fifth class would silently under-assert
  every "only this class moved" test. Export the class list instead.
- **`echo.normalizeHost` is a hand-copy of `personas.normalizeHost`.** The
  classifier is deliberately the read-side mirror of the serving surface,
  so divergence between the two copies is the named risk. Now covered by
  its own tests on both sides, but not by a test asserting the two agree.

## Production rollout

RESOLVED — Bluesky's public-relay account cap (100 accounts, 50 ev/s) is no
longer load-bearing: the self-hosted relay + Jetstream in
`docker-compose.prod.yml` are the app's ingest path and carry only our two PDS
hosts with an effectively unlimited account limit. `bsky.network` remains the
wider-visibility path only. Runbook: `SELF_HOSTED_RELAY.md`.

DOCUMENTED, not resolved — the v2 deploy gaps below now have a written home in
`DEPLOY.md` (§6 "Not implemented") with their blast radius. Writing them down
is not building them; they stay open here:

- **No `BRIDGE_KEK` / per-actor RSA rotation path.** Nothing re-seals existing
  ciphertext under a new KEK, and the binary's only subcommand is `migrate`
  (no args = serve). Changing the KEK orphans sealed key material in **three**
  tables, not two: `bridged_actors.signing_key` (~950 bridged identities'
  escrowed secp256k1 repo keys), `service_keys.key_material` row `plc-rotation`
  (the PLC escrow key, the only DID recovery path), and — added by v2, and the
  one a pre-v2 plan omits — `ap_actors.rsa_key_sealed`, **every native Coves
  user's AP signing key** (`internal/db/migrations/017_ap_actors.sql:46`). No
  recovery for any of it. Would need a key-version selector on each sealed
  blob, a dual-read custodian, an online re-seal pass over all three, and a
  cutover. Partial credit on the first: `ap_actors.rsa_key_version` already
  exists (`017_ap_actors.sql:47`, stamped from `currentRSAKeyVersion = 1` at
  `internal/personas/personas.go:25`) and is deliberately there so rotation is
  definable without a schema change — but nothing reads it as a selector, and
  the other two tables have no version column at all.
- **No backup or restore procedure.** `docker-compose.prod.yml` mounts
  `./backups` into the Postgres container and nothing writes to it. Note
  `BRIDGE_KEK` lives in `.env` and is not covered by any database backup at
  all. Whatever is built needs a restore *drill* — an unverified backup is a
  claim, not a capability.
- **No divergence off switch.** The sweep is wired unconditionally, sweeps once
  at startup, and `DIVERGENCE_INTERVAL=0` is refused by `durationVar`. The only
  lever is a large interval. Defensible while the sweep stays read-only; revisit
  if it is ever implicated in lock pressure.
- **No outbound announcement rate limiter.** Decision 19 asks for "deliberate
  throttling of initial actor announcements"; `internal/outbound` has no rate
  limiter, and `MINT_RATE_PER_MINUTE`/`MINT_BURST` gate the **inbound** mint
  path only (`ingest.NewMintGate`, materializer-only consumer). Today the
  throttle is the canary itself: `OUTBOUND_WORKERS=1` plus a one-community
  scope.
- **Kill switches and every other knob are boot-time only.** Config is read once
  at `cmd/tidepool/main.go:109` with no reload signal, so engaging a kill switch
  during an incident requires a container recreate. A SIGHUP reload (or an
  admin-write switch table) would cut that latency.
- **Scoped kill switches are denylist-only.** There is no allowlist form, so a
  one-community canary must enumerate every *other* subscribed community — and
  adding a community to `communities.yaml` silently escapes an existing canary.

Still open, unchanged:

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
- Baseline-only voters can drift: a later flip or clear lacks a per-voter
  baseline row to retract. A re-seed heals the aggregate — but nothing
  re-seeds on a schedule (see "Nothing re-seeds periodically" above), so for a
  quiet community that is never backfilled again the drift is permanent and
  unreported. "Temporarily" was the wrong word.

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

## Opt-out lifecycle (17d)

- **The withdrawal's 410 is observable before the withdrawal is delivered.**
  The destructive tier commits the actor tombstone together with the
  `Delete{Person}` enqueues, but the worker POSTs minutes later. A peer that
  re-dereferences the actor to verify the signature on that Delete therefore
  gets `410 Gone` — for the message announcing the very deletion it is trying
  to authenticate. The tier can revoke its own precondition.

  MITIGATED, not closed: both actor routes (and `handleOutbox`, which would
  otherwise invite the re-fetch loop the 410 exists to end) now answer 410
  with an AS2 `Tombstone` carrying `formerType: Person`, `deleted`, and **the
  public key** — so a peer that reads the body can still verify.

  RULED against the "complete" fix of keeping the actor document served until
  every person-delete delivery is terminal. That keeps an erased user's
  document published for as long as any peer is down — potentially forever —
  which is a worse failure for an erasure tier than the one it fixes. The
  Tombstone hands over verification material and none of the profile, which is
  the better trade, and it is why the committed test pinning "410 immediately,
  with no worker run" was deliberately left standing.

  RESIDUAL, and it is genuinely not ours to close: a peer that branches on the
  status code alone, without reading the body, still cannot verify the
  withdrawal. Revisit only if a real implementation (Lemmy specifically) is
  observed dropping our person-deletes for this reason — the fix would then be
  peer-shaped (retry after the delete, cached-key acceptance), not a change to
  when we serve the tombstone.

- **A delivery claimed and mid-POST at purge time is indistinguishable from
  one that will never be sent.** The purge enumerates what the peer holds
  (delivered, plus held-for-settlement), but a delivery the worker has claimed
  and is POSTing right now is neither. Detecting it needs the peer's state,
  not ours. Belongs to 17e (reconciliation).

## Deferred by 17e (reconciliation scoped to detect-only)

17e reports divergence and never repairs it (decision 19). These are the repairs
and the comparisons it deliberately did not build.

- **The re-cast race, leg 1 — `upsert` clobbers a delivered vote.**
  `internal/consume/votes.go` hardcodes `pending` on the vote write and
  `internal/store/outbound_votes.go` `Upsert` sets
  `delivered_state = EXCLUDED.delivered_state`, so re-casting a DELIVERED vote
  resets the row to pending while Lemmy still holds the OLD vote in the OLD
  direction: we subtract nothing and keep our stale vote. Transient normally,
  PERMANENT if that delivery poisons. THE FIX, and it already has a model in
  the tree: make the upsert refuse to write `pending` over `delivered` exactly
  the way `SetDeliveredState` now refuses to write over `undone` (17d), so a
  re-cast leaves a row that still owes an Undo. Vote-accounting change with its
  own RED test — 17e's report is its regression oracle, which is why the report
  ships first.

- **The re-cast race, leg 2 — the settlement silently forgets the old vote.
  Recorded nowhere before now.** `internal/outbound/worker.go` `voteCallback`
  resolves via `GetByActivityID(activity.ActivityID)` and returns `nil` on
  NotFound. After a re-cast the OLD Like's activity id no longer matches
  `current_activity_id`, so when that in-flight old delivery lands the callback
  no-ops and the delivery is marked delivered. The peer then demonstrably holds
  a vote that NO `outbound_votes` column records at all. This is why 17e reads
  divergence out of the delivery ledger (`outbound_activities` joined to
  `outbound_deliveries`) rather than out of the vote row: the vote row is
  exactly what the bug erases.

- **REJECTED, with reasons, so it is not re-proposed: the "what the peer holds"
  vs "what the user wants" column pair.** It changes the write path that feeds
  a number users read — 17b's binding ruling pins `delivered_state = 'delivered'`
  as a positive equality in `SeedAggregates`, and 17d made that correct only
  because `undone` is terminal; a second pair forces the seeder, the purge's
  `ListStandingForActor`, `voteCallback` and `CancelOutwardForActorTx` to each
  re-decide which column they meant. It also relocates the uncertainty rather
  than removing it: the pair's only honest maintainer is the delivery callback,
  which is where leg 2 already lives — two things that can be wrong instead of
  one, and then reconciliation has to reconcile *them*. Detection needs none of
  it; `outbound_activities` is append-only and already durable.

- **Instances that learned of an actor via search/WebFinger are unreachable by
  any local comparison.** `DistinctInboxesForActor` is, in its own words, the
  only record of which instances hold a user's content. Building the other side
  would mean logging the requesting host of every WebFinger and actor-document
  GET — a new surveillance log built to serve a deletion, which is the wrong
  trade for an erasure feature. 17e's report states its own coverage bound
  instead: the fan-out set is the delivery history. A reconciler reporting zero
  here would be asserting completeness it cannot have.

- **A delivery cancelled while mid-POST cannot be distinguished locally, and
  the evidence is erased on purpose.** Every cancel statement writes
  `claimed_until = NULL`, so after the cancel commits a row cancelled mid-flight
  is byte-identical to one cancelled while idle. Recovering it needs a new
  column plus edits to the five most safety-critical cancel statements in the
  outbound package — to record a fact that still would not say whether the POST
  landed. If ever wanted, the cheap version is `RETURNING` a count of rows with
  `claimed_until > now()` at each cancel site, bumping a counter where the
  decision is actually taken rather than reconstructing it later.

- **A ban-caused cancellation is indistinguishable from a consent cancellation**
  in `outbound_deliveries` — `cancelForActor`, the community cancel, the ban's
  intersection cancel and the consent cancel all write the same `cancelled`
  with no reason column. 17e reports the count rather than inventing the column.
  `community_bans` is joinable on `(community_did, subject_did)` with
  `expires_at IS NULL OR expires_at > now()`, so the reason is expressible
  read-side today if an operator needs it.

- **`bindFetch` own-id belt (noted in 17a, still unimplemented).** A typed
  "refusing to fetch our own id" error in `internal/ingest/handler.go` would
  cover the ancestor walk, bare-IRI announce, `resolveDelivered`, and
  `handleUndoDelete`'s restore in ONE place. Explicitly NOT 17e's: it changes
  what the inbound path processes, and a sub-run whose constraint is "report,
  never act" should not ship a new drop site — on the highest-volume inbound
  path, which already carries a recorded perf concern.

- **TWO legs of the divergence sweep scan `outbound_activities` in full, not
  one.** This entry previously said the re-cast driver was the only unindexed
  leg and that "every other leg is index-served". THAT WAS WRONG, and wrong in
  the direction that stops the next person looking: the undelivered-acceptance
  leg joins `outbound_activities` on a JSONB EXPRESSION, and no index in
  `internal/db/migrations/` can serve it. `outbound_activities` carries exactly
  two indexes — `outbound_activities_pkey (activity_id)` and
  `idx_outbound_activities_actor (actor_did)` — and neither is on
  `payload -> 'object' ->> 'id'`.

  Re-confirmed by `EXPLAIN` against the migrated test schema. The
  acceptance leg:

      ->  Hash Right Join
            Hash Cond: (((a.payload -> 'object'::text) ->> 'id'::text) = o.ap_object_id)
            ->  Seq Scan on outbound_activities a
            ->  Hash
                  ->  Seq Scan on outbound_objects o
                        Filter: (accepted_at IS NULL)

  and the proof that it is unindexed rather than merely cheap here: with
  `SET enable_seqscan = off` — which prices a sequential scan at 1e10 — the
  planner STILL chooses `Seq Scan on outbound_activities a` for that condition,
  because there is nothing else it could use.

  The re-cast leg's original finding stands: its three exclusion subqueries are
  index-served (the Undo and superseding-vote subqueries ride
  `idx_outbound_activities_actor`, the delivered-delivery joins ride
  `idx_outbound_deliveries_activity`, the ledger exclusion rides
  `outbound_votes_actor_delivered_idx`), while the outer driver is a full scan
  filtered on `kind IN ('Like','Dislike') AND parent_at_uri <> ''`.

  COST NOTE, so the next profile is not a surprise: bounding the sweep put an
  exact `COUNT(*)` beside each limited read, so the acceptance leg's full scan
  of `outbound_activities` now happens TWICE per sweep rather than once. That is
  the price of a count an operator can size an incident from; it is also the
  strongest argument for the expression index below, since indexing that join
  fixes both statements at once.

  Deliberately NOT fixed here — an index is a migration whose write cost lands
  on the delivery worker's hot path, and the sweep runs every 15 minutes against
  tables that are currently small. The two candidates, when it is needed:

      -- serves the re-cast driver, and tightens the Undo subquery
      CREATE INDEX outbound_activities_vote_subject_idx
          ON outbound_activities (actor_did, parent_at_uri, created_at)
          WHERE kind IN ('Like','Dislike');

      -- serves the undelivered-acceptance join, which has nothing today
      CREATE INDEX outbound_activities_object_id_idx
          ON outbound_activities ((payload -> 'object' ->> 'id'))
          WHERE payload -> 'object' ->> 'id' IS NOT NULL;

  The first also tightens the Undo subquery, which today index-scans by
  `actor_did` and applies kind/parent_at_uri/created_at as residuals; a sibling
  partial index on `kind = 'Undo'` finishes that half. The second is an
  EXPRESSION index and must be spelled with exactly the operators the query uses
  (`->` then `->>`) or the planner will not match it — and it is the one whose
  write cost is least predictable, because every outbound activity has a payload
  and the extraction runs on every insert.

  ROW ESTIMATES FROM A NEAR-EMPTY TEST DATABASE ARE MEANINGLESS. Every cost and
  row number in the plans above is noise; the plan SHAPE — a full scan of
  `outbound_activities` per sweep leg, per statement — is the entire finding.
  Re-profile against production volumes before choosing either index.

- **An acceptance with NO delivery row at all is not reported by the
  acceptance-undelivered classes**, and this is stated in the query rather than
  swallowed. All three classes are delivery states, and a row with no delivery
  has none; folding it into the nearest class would misdescribe it. It should
  not be reachable going forward — the acceptance record, the
  `outbound_objects` row, the delivery enqueue and the `admissions` ledger row
  all land in ONE transaction — so a test asserting it today would be vacuous.
  If it is ever observed, it wants its own class, not a widened existing one.

## Found while building 17e's fixtures (NOT a 17e defect)

- **Re-using a deleted vote record's rkey silently never federates.** The
  outbound activity id derives from (at-uri, "create", seq). Delete a vote row
  and the seq restarts at 1, so a vote re-created under the SAME rkey
  reproduces the FIRST vote's activity id. `InsertTx` then no-ops on the
  existing id, `EnqueueTx` reads back the standing **delivered** row (correctly
  — that is the idempotency that makes the fan-out safe), and nothing goes out.
  Observed on the wire as `["Dislike","Undo"]` with the third vote simply
  missing, no error anywhere.

  Whether this is reachable depends on Coves' rkey policy for re-created votes.
  If rkeys are ever reused after a delete, a user's vote silently does not
  federate and no signal is produced. Worth confirming against Coves before
  deciding whether it needs a fix here; the candidate fix is to derive the
  activity id from something that does not restart (the row's create timestamp,
  or a monotonic per-actor counter that survives deletion).
