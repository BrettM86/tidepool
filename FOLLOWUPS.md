# Follow-ups

Known open work, accepted limits, and operational caveats. Finished work is in
git history, not here.

## Not built yet

- Communities created on Coves do not federate to the fediverse. This is the
  planned "Scope B". Today only fediverse communities are bridged, and native Coves
  users federate only into those.
- Image embeds on native posts do not federate. `social.coves.embed.images`
  is dropped in `internal/apobject/object.go` (`linkAttachment`). The blobs live
  on the author's PDS, and there is no design yet for turning a blob into a
  public URL for the `attachment [{type: Image, url}]` Lemmy expects. Text,
  links, NSFW, titles and facets do federate.
- Bridged fediverse users cannot claim their DID keys or migrate their account.
- When a PLC mint fails after submission, the DID can be left registered with no
  `bridged_actors` row (`internal/identity/minter.go` logs it at error level).
  The next attempt mints another DID. The fix is to reuse the orphaned DID with
  a PLC `updateHandle` operation.
- DMs are not bridged. Moderation decisions made on Coves do not federate to
  Lemmy; only Lemmy's removals, locks and bans flow in.
- There is no rate limit on outbound actor announcements.
  `PLC_WRITES_PER_MINUTE` limits PLC POSTs only. The only levers are
  `OUTBOUND_WORKERS` and the scoped kill switches.

## Other platforms

- PieFed, Mbin and Pylova are untested. Mbin is partly handled: `communityRef`
  (`internal/materialize/posts.go`) reads `audience` first, which Mbin sets, and
  Mbin's bare-URL `source` parses. If `audience` is missing, the `to`/`cc`
  fallback only recognises Lemmy's `/c/` paths (`containsCommunityPath` in
  `internal/ingest/handler.go` has the same rule), so Mbin's `/m/` groups are
  missed. PieFed and Pylova have no captured fixtures.
- Lemmy 1.0 (API v4) is untested. The harness, seeder and WebFinger code target
  Lemmy 0.19.x (API v3), pinned at 0.19.20.
- Lemmy does not federate a post author's own automatic upvote. A newly bridged
  post can show one upvote fewer than Lemmy until a backfill re-seed.
- Lemmy 0.19.x does not federate profile edits. A user who adds `#nobridge` is
  noticed only when `PROFILE_REFRESH_TTL` has expired and another activity
  makes Tidepool re-fetch the actor. An inactive user's opt-out is never noticed
  unless an operator runs `POST /admin/users/refresh-profile {"all":true}`. Add
  a periodic consent re-scan.
- Lemmy peers that federated with Tidepool before it served its instance actor
  at `GET /` may need to re-fetch the actor, and restart their federation
  worker, before they accept account deletions.
- Self-delete confirmation accepts a 410 or an ActivityPub `Tombstone`, not a
  plain 404. That fits Lemmy. A platform that answers 404 for a deleted actor
  will need a change.
- Lemmy's `Image` attachments have no `mediaType`; only `type` and the URL
  extension mark them as images. `Link` attachments carry no alt text, so alt
  text is lost when the metadata fetch fails.
- The service actor uses type `Service` because Lemmy rejects `Application`.
  Revisit only if another platform rejects `Service`.
- Production WebFinger is HTTPS only. HTTP fallback exists only with the dev/e2e
  SSRF relaxation.

## Relay and Jetstream

- A new self-hosted BigSky refuses non-admin `requestCrawl` calls because its
  daily new-PDS limit defaults to zero. Raise it with the admin
  `setPerDayLimit` call, as `relay-bootstrap` does in the e2e stack.
- The pinned BigSky image ignores `BGS_CRAWL_INSECURE_WS`, `BGS_PORT` and
  `LOG_LEVEL`. Use the `--crawl-insecure-ws` argument and `BSKYLOG_LOG_LEVEL`.
  Set `ATP_PLC_HOST` and `RESOLVE_ADDRESS` when the public PLC directory and the
  `1.1.1.1` resolver are wrong for the deployment. Check `--help` when bumping
  the image.
- The pinned BigSky image has no arm64 build. Apple Silicon runs it under amd64
  emulation.
- Jetstream replays its whole retained store when given a cursor between its
  newest stored event and the current time. Consumers must be idempotent and
  must not use a wall-clock cursor as a dedupe boundary.
- Indigo's slurper stops retrying a PDS after about three minutes of failed
  dials and marks it unregistered. Tidepool re-announces itself on startup, but
  a long Tidepool outage with the relay still up needs a manual `requestCrawl`.

## Outbound delivery

- A claim that is abandoned (lease lapses, or a crash between `ClaimNext` and
  settling) keeps the attempt it was charged. Each one costs the delivery one
  real retry, and is why a poisoned row can show more than `maxAttempts`. A park
  the fence refused logs `park did not apply: claim lost or row terminal` at
  Warn in `internal/outbound/worker.go`.
- A park writes `last_status_code = 0`, overwriting a real status from an
  earlier attempt. The divergence sweep reads 0 as "the peer never answered"
  (`internal/store/divergence.go`), so a 502 followed by a park reads as
  silence. Fix only if the per-row answered/silent split needs to be exact.
- `ClaimNext` on `outbound_deliveries` re-checks rows by `seq`, which has no
  index of its own (the primary key is `(activity_id, target_inbox)`). If the
  claim path shows up in profiles, add `UNIQUE INDEX (seq)` in a new migration
  (next free number is 043). Editing an applied migration does nothing under
  goose.
- The `OutboundDeliveries` interface has 17 methods. The worker uses the
  claim/mark subset and the admin API uses inspect/redrive/cancel. If it grows,
  split it at the consumer packages.
- The enqueuer resolves a community's inbox inside the consumer's gate
  transaction. On a cache miss (1h TTL) that is a network fetch, so a slow
  Group document holds gate row locks and blocks the consumer. Resolve before
  the transaction, or give the resolve a short timeout.
- If a vote record is deleted and re-created with the same rkey, the vote never
  federates and nothing is logged. The activity id is derived from
  (at-uri, "create", seq), and seq restarts at 1, so the new vote reproduces the
  first vote's id and `InsertTx` no-ops on it. Coves' own vote service mints a
  fresh TID for each vote (`coves/internal/core/votes/service_impl.go`), so this
  needs a client that writes vote records directly with a reused rkey. Fix by
  deriving the id from something that does not restart.
- The operator purge (`POST /admin/actors/purge`) has two known gaps: two
  concurrent purges of one actor can each send an Undo for its votes (the
  runbook says run one at a time), and a real run restamps `disabled_at` on an
  actor that was already disabled.

## Community bans and moderation

- The ban re-check inside the acceptance transaction narrows the race with a
  new ban but does not close it. Under READ COMMITTED a ban can commit between
  the check and the acceptance commit. Closing it needs SERIALIZABLE or a lock
  shared with the ban path.
- A delivery claimed just before a ban commits can still be sent. The row then
  reads `cancelled` for content the peer accepted, and the vote re-seed (which
  subtracts only `delivered`) misses it.
- When a temporary ban expires, nothing re-queues the deliveries it cancelled.
  The posts stay accepted on Coves, and Lemmy never receives them. `cancelled`
  is terminal; `RedrivePoisoned` revives only poisoned rows.
- `outbound_deliveries` does not record why a delivery was cancelled. Ban,
  consent, community and actor cancels all write `cancelled`. An operator can
  get the ban reason by joining `community_bans` on
  `(community_did, subject_did)` with `expires_at IS NULL OR expires_at > now()`.
- Bans on native authors are stored only in a private table. A
  `social.coves.moderation.ban` lexicon exists, but nothing publishes to it, so
  Coves' moderation surface shows no ban while the author's posts are refused.
- The community `Block` that bans a native author is only covered by
  hand-written unit fixtures. Tidepool reads both `expires` and `endTime`; a
  different spelling or `target` shape from real Lemmy would go unnoticed. The
  site-ban `Block` is captured from real Lemmy in
  `tests/e2e/upstream_ban_test.go`.
- Comments materialized before migration 026 have no `thread_root_at_uri`, so
  a lock on the post above them does not refuse native replies under them. Each
  heals when re-materialized; an admin backfill of the affected communities
  fixes them all.
- A lock reaches native replies only through reply chains the bridge
  materialized.
- `walkThreadRoot` can still hit a dead end for native content whose outbound
  state predates the root column. If the community has a lock, the event is
  refused as retryable and the error names the at-uri where the chain stopped.
- The re-materialization test does not cover `storedThreadRoot` on its own:
  setting it to `""` still passes because `carryForward` restores the reply
  refs. Isolating it needs a stored record with no reply refs.

## Votes and counts

- Nothing re-seeds vote counts on a schedule. `SeedPostCounts` runs only behind
  `SEED_COUNTS_FROM_API`, and is skipped within a 1h window unless an admin
  forces a backfill. Drift that "heals on the next re-seed" may never heal in a
  quiet community.
- The re-seed baseline clamp (`GREATEST(0, …)`) fires only when the deficit is
  larger than the subject's whole fediverse tally. It does not detect Lemmy
  discarding our votes. Compare `tidepool_vote_seed_ours_subtracted` with a
  sample from Lemmy to audit that.
- Voters known only from the baseline can drift: a later flip or clear has no
  per-voter row to retract. A re-seed fixes the total, but see above.
- Migration 023's cleanup `DELETE` matches `voter_ap_id` by exact string, while
  `echo.identifyActor` normalises host and scheme. A legacy row with a
  non-canonical spelling of one of our actor ids would be subtracted twice.
  Accepted: no such rows exist in production.
- Rolling back migration 023 leaves baselines understated until a forced
  backfill.
- While a re-cast vote is in flight, `delivered_state` is still `delivered` but
  `direction` is already the new one. A re-seed in that window subtracts the
  wrong direction. It shows up only if it pushes a baseline below zero
  (`SeedBaselineClamped` and a Warn). If the new delivery poisons, the error
  repeats on every re-seed until the vote is undone; `RecastDivergence` reports
  it. `TestReseedDuringARecastWindowMisreadsBothDirections` pins the cost.
- When an old vote's delivery lands after a re-cast, `voteCallback`
  (`internal/outbound/worker.go`) finds no row for the old activity id and does
  nothing. The peer then holds a vote no `outbound_votes` row records. The
  divergence sweep reads the delivery ledger for this reason. The same path
  explains the next item.
- Deleting a vote and re-casting it with the same rkey before the Undo settles
  leaves the row `delivered` for a vote the peer no longer holds, until the next
  delivery or undo.
- An update that points an existing vote rkey at a different subject overwrites
  `subject_at_uri`. The peer keeps the vote on the old subject. Coves never does
  this. Fix: refuse a subject change for a live vote in `applyVoteWrite`.
- Deleting a vote after a poisoned re-cast sends an Undo naming the new,
  never-delivered activity. This relies on Lemmy matching Undo by
  `(actor, object)`, as the purge path also does. Verify with an outbound-vote
  e2e test against real Lemmy.
- A stale queued vote event can race an actor purge and overwrite `undone` with
  `pending`, sending new work after the purge. Not reachable at current scale.
  Candidate fix: make `undone` terminal in the upsert, test-first, since it
  reverses an earlier decision.
- `ScrubVoter` takes aggregate locks in a fixed subject order to avoid
  deadlocks, but no concurrency test checks it.
- Vote subject resolution runs outside the mutation transaction, leaving a
  small race with deletion.

## Echo suppression

- `ClassAncestorShortCircuit` counts normal threading, not a dropped echo. It
  fires on every comment whose ancestor `ResolveStrongRef` already anchored, yet
  it sits in the `tidepool_echo_drops_*` metrics. Rename it out of that family or
  move it to a place where a fetch was actually skipped. It also re-reads the
  row `ResolveStrongRef` just read.
- Echo classification adds up to 4 indexed reads per node on the busiest
  inbound path. An envelope with `/ap/object/`-shaped ids on foreign hosts
  forces reads at every probe, up to `MaxDepth` deep. Accepted because the inbox
  is signature-checked and rate-limited.
- No e2e test drives an inbound announce for a vanity-origin persona. `Identify`
  is unit-tested both ways.
- `identifyObject` and `identifyActor` check only the id. A followed community
  can send one of our public object or actor ids to get content dropped as an
  echo. Limited to followed communities, and closing it needs a different
  mechanism.
- `materializeContent`'s legacy `mapping.Origin == store.OriginBridge` check is
  now covered entirely by `suppressEcho`. Decide whether to delete it.
- `echo.Drops()` is exported but has no production caller, and the tests keep
  their own copy of the class list. Adding a class would weaken every "only
  this class moved" test. Export the class list instead.
- A typed "refusing to fetch our own id" error in `bindFetch`
  (`internal/ingest/handler.go`) would cover the ancestor walk, bare-IRI
  announce, `resolveDelivered` and `handleUndoDelete`'s restore in one place.

## Opt-out and erasure

- After an opt-out, the actor answers 410 before the `Delete{Person}` has been
  delivered. A peer that re-fetches the actor to check the Delete's signature
  gets 410. The 410 body is a `Tombstone` that carries the public key, so a
  peer that reads the body can still verify. A peer that looks only at the
  status code cannot. Act only if Lemmy is seen dropping our person-deletes.
- A delivery mid-POST at purge time is neither delivered nor held, so the purge
  cannot tell whether the peer has it.
- Instances that found a user through search or WebFinger, and never received a
  delivery, are not reached by an erasure. The delivery history is the only
  record of who holds a user's content. Accepted: logging every requester would
  be a surveillance log.

## Divergence sweep

The sweep reports divergence and never repairs it.

- There is no off switch. It always runs at startup, and
  `DIVERGENCE_INTERVAL=0` is refused. The only lever is a long interval.
- Two legs scan `outbound_activities` in full. The undelivered-acceptance leg
  joins on `payload -> 'object' ->> 'id'`, which no index serves; `EXPLAIN` with
  `enable_seqscan = off` still picks a sequential scan. The re-cast leg's outer
  driver also scans in full. Each leg also runs an exact `COUNT(*)`, so the
  acceptance scan runs twice per sweep. Candidate indexes, once production
  volume justifies the write cost:

      CREATE INDEX outbound_activities_vote_subject_idx
          ON outbound_activities (actor_did, parent_at_uri, created_at)
          WHERE kind IN ('Like','Dislike');

      CREATE INDEX outbound_activities_object_id_idx
          ON outbound_activities ((payload -> 'object' ->> 'id'))
          WHERE payload -> 'object' ->> 'id' IS NOT NULL;

  The expression index must use exactly `->` then `->>` or the planner will not
  use it. Profile against production volumes first; test-database row counts
  mean nothing.
- An acceptance with no delivery row at all is not reported. It should be
  unreachable because all four writes share one transaction. If it is ever
  seen, give it its own class.
- A delivery cancelled mid-POST looks the same as one cancelled while idle,
  because every cancel clears `claimed_until`. If needed, return a count of rows
  with `claimed_until > now()` from each cancel statement and bump a counter.
- There is no periodic sweep that checks acceptance records against the posts
  they pin. Acceptance gaps from a crash heal only when another event touches
  the post, which in a quiet community may not happen.

## Keys and identity

- A missing `plc-rotation` row on a populated database boots cleanly and mints
  a new escrow key. Every existing did:plc document still names the old key, so
  the bridge can no longer rotate any of them. This happens if a backup without
  the row is restored, or `bridged_actors` is restored without `service_keys`,
  or `DATABASE_URL` points at the wrong database. The KEK canary
  (`VerifyKEKAgainstSealedMaterial`) checks only that the KEK opens existing
  blobs. Fix: in `LoadOrCreateRotationKey`, refuse to create the row when
  `bridged_actors` is non-empty, and name the likely bad restore in the error.
- Per-actor RSA key rotation is not built. `ap_actors.rsa_key_version` is
  written and never read, and there is no two-key overlap for peers to
  re-fetch.
- The service actor's RSA key is stored as plaintext PEM in `service_keys`,
  outside the KEK. `rotate-kek` deliberately skips it, and there is no procedure
  to rotate it.
- `rotate-kek` reports success with exit code 0 even when it re-sealed rows.
  The runbook requires a second run that re-seals nothing before unsetting
  `BRIDGE_KEK_PREVIOUS`. Before anyone scripts this, add a `Quiescent()` check
  on the report or a distinct exit code for "moved rows".
- `internal/identity/reseal.go` has two near-identical pagination loops
  (`bridgedActors` and `apActors`). Merge them the next time either changes.
- `SigningKeys` (`internal/repo/repo.go`) hands out key material. A
  `SignCommit` method would keep private keys inside the identity package and
  make a KMS backend easier.

## Operations and configuration

- Config is read once at startup, so changing `OUTBOUND_DISABLED`,
  `OUTBOUND_DISABLED_ACTORS` or `OUTBOUND_DRY_RUN` during an incident means
  recreating the container. A SIGHUP reload or an admin-writable switch table
  would fix this. The host and community switches live in the follow list and
  need no restart.
- Scoped kill switches are deny lists only. A one-community canary has to list
  every other community, and adding a community to `communities.yaml` silently
  puts it outside the canary.
- Backups (`scripts/pg-backup.sh`, nightly systemd timer) stay on the host they
  protect. There is no offsite copy, no WAL/point-in-time recovery, a 24h
  recovery point, and nothing runs `scripts/pg-restore-drill.sh` automatically.
  `BRIDGE_KEK` and `.env` are outside the database backup; the backup script
  warns when `.env` is newer than its offsite marker.
- No CI job runs the backup and restore-drill scripts. A migration that renames
  a table the drill checks would break the drill unnoticed until the next
  manual run. Add a job that dumps the migrated test database, runs both
  scripts, and checks one induced failure.
- Production lexicon validation records `tidepool_lexicon_validation_failures`
  and writes the record anyway. Make it strict only after the counter has stayed
  at zero in production.
- The e2e harness runs in development mode (migrations on start, HTTP and
  private fetching, strict lexicon validation). `ENVIRONMENT=production` is
  exercised only by the production deployment.
- All in-process IP rate limiters use `RemoteAddr` and ignore
  `X-Forwarded-For`. Behind a proxy every client shares the proxy's bucket, so
  rate-limit at the edge. A future opt-in `TRUSTED_PROXY` setting could trust
  forwarded addresses from listed hops.

## Materializer and storage

- An actor document behind an instance that is temporarily forbidden or down
  can cause content to be dropped instead of using the last known actor. Media
  blobs already keep their last copy on transient failure; actors do not.
- Blobs are stored per DID with no reference counts. Ban removal
  (`internal/materialize/banremoval.go`) checks for other live references before
  deleting an image, but the general record scrub
  (`internal/materialize/updates.go`) does not, so scrubbing one record can delete
  media another record in the same repo still uses.
- Consent is read outside the commit transaction, so a consent change racing an
  in-flight commit can let that one commit land.
- Bridged posts written before the postv2 switch stay on the deprecated
  `social.coves.community.post` collection permanently (product decision,
  2026-08-11). Every era-sensitive path dispatches on the mapping's collection.
  As a result, Coves' legacy-drain gate never fires while those posts exist. A
  migration would need Coves to decide how comment and vote refs to the old URIs
  are remapped.
- Every postv2 create or edit fetches the author's actor document once more to
  read `originalAuthor.displayName`. Storing the display name on
  `bridged_actors` would remove the fetch. Revisit if actor-fetch traffic on
  post paths matters.
- `SetBridgedStats`' NoOp result returns `mapping.CID`, which can be older than
  the record the read just saw. Callers ignore it today.

## Sync and ingest

- `getRepo` ignores `since` and always returns the full repo CAR, which the
  spec allows. A real diff would need historical blocks and a change to the
  block GC rule.
- A queue attempt interrupted by shutdown still uses up its `ClaimNext` attempt.
  Cosmetic.
- The failure path for the `activityID` random source cannot be tested without
  injecting the reader, because Go 1.24+ treats a `crypto/rand` failure as fatal.

## E2E harness and CI

- Native e2e coverage is thin. `tests/e2e/native_facets_test.go` sends a
  native post through the reference PDS, relay, Jetstream and acceptance engine
  to real Lemmy. There are no e2e tests yet for native comments, native votes
  reaching Lemmy, or Lemmy votes on native posts.
- `vetEvent` (`tests/e2e/helpers.go`) checks every firehose event against one
  `expectedCollections` list without knowing which repo it came from. A
  collection the bridge may write passes even when it appears in the native
  repo. Give each repo class (bridged authors, bridged communities, native PDS)
  its own allowlist and rev tracking.
- Recreating the `relay` service alone breaks it: its disk persister keeps file
  refs in `relay-postgres` while the files live in the container layer, so
  bigsky exits with `open /data/persister/evts-0: no such file or directory`.
  Any edit to the `relay` service triggers this; run `make e2e-down` first. A
  named volume for `/data` would remove the trap.
- Jetstream exits when its upstream disconnects, and compose's
  `restart: unless-stopped` brings it back. Remove the workaround if Jetstream
  learns to reconnect.
- The suite-end replay sweep only sees Jetstream's last 24 hours. Recreate the
  stack per run, as `make e2e` does.
- The e2e job on GitHub Actions compiles Lemmy from source on every run and may
  exceed runner time or disk. Publish a prebuilt pinned Lemmy debug image to
  GHCR, or use the buildx `gha` cache.
- The e2e suite covers the follow list's host switch and an `instances` entry.
  An entry under `communities` is not subscribed from the file in any e2e test.
  The `instances` test reaches `accepted` through the suite's own
  `POST /admin/communities`, and its cleanup checks a removed entry against
  `GET /admin/communities` only. `installFollowList` in
  `tests/e2e/follow_list_test.go` mounts the file, so the missing piece is
  checking that a sweep's add alone turns `accepted` and that Lemmy receives the
  Undo.
- `POST /admin/reemit` has no e2e test.
- The PLC directory image is pinned by commit in `e2e/plc/Dockerfile`. Bump it
  on purpose when an upstream fix is needed.
