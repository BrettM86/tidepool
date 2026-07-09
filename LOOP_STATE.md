# Tidepool build-loop state

Protocol: PLAN.md §Loop protocol. One task per iteration:
implement (Fable agent) → /second-opinion review → fix → verify → commit →
update this file → schedule next. Stop the loop when every task is `done`.

| # | Task | Status | Commit | Notes |
|---|------|--------|--------|-------|
| 1 | 01-scaffold-storage | done | (see git log) | reviewed by 7 reviewers, 18 fixes applied |
| 2 | 02-ap-protocol | done | (see git log) | 5 reviewers incl. security; 14 fixes (critical: actor-id binding; high: SSRF, webfinger host confusion) |
| 3 | 03-identity-repos | done | (see git log) | 8 reviewers (5 Claude + codex/gemini/glm); 16 fixes (genesis race, seq ordering, MST-corruption-as-NotFound, KeyUse deletes, TID micro-fill) + 7 new tests |
| 4 | 04-sync-firehose | done | (see git log) | 7/8 reviewers (glm watchdog-killed); fixes: ping starvation, prune-mid-replay OutdatedCursor, broadcaster closed-channel, SeqBounds dirty-read, pruner fail-closed; consent-on-firehose deferred to 06 |
| 5 | 05-materializer | done | (see git log) | 8 reviewers; fixes: id-authority binding, Note-root panic, create-after-delete, nobridge scrub, embedded-actor trust, byte caps, uri scheme, Group-type check + 9 regression tests |
| 6 | 06-ingestion | done | (see git log) | 8 reviewers (5 Claude + codex/gemini; glm wandered, no JSON); fixes: announced-Delete/Undo scoped to announcer authority (+actor-delete only self), bare Update{Person/Group} no-mint gate, announce content community-authority check, Undo{Delete} restore compensation, handleAccept pending-only, queue lease fencing token + shutdown-cancel handling + processed/poisoned exclusivity, backfillReplies tombstone check, truncation leaves resumable, activityID rand-fail propagates + 14 regression tests |
| 7 | 07-vote-aggregates | pending | | |
| 8 | 08-e2e-harness | pending | | |

Statuses: pending → in-progress → review → done (or blocked: <reason>).

## Cross-task notes for future iterations
(implementation agents & reviewers append surprises, interface changes,
and deferred TODOs here)

- Reference clones live at ~/Code/bridgy-fed, ~/Code/granary,
  ~/Code/arroba (CC0). Coves AppView at ~/Code/coves.
- Coves post consumer requires: repo DID == record.community, community
  indexed before post, author user indexed before post.

### From task 01 (storage layer semantics later tasks MUST know)
- Ports: dev postgres 5442, test postgres 5443, HTTP :8091. Test DB URL:
  postgres://tidepool_test:tidepool_test@localhost:5443/tidepool_test.
  Containers tidepool-dev-postgres / tidepool-test-postgres.
- `PutMapping` derives at_uri itself — callers supply (DID, collection,
  rkey, CID) only. Second ap_id claiming same at_uri → IsAlreadyExists
  (deterministic-rkey collision = bug signal). ap_objects has an `origin`
  column (fediverse|bridge) for task 06 echo suppression.
- `ResolveStrongRef` has THREE outcomes: found; IsNotFound (missing →
  task 05 fetches ancestor chain); IsTombstoned (deleted → task 05 drops
  the subtree, consent-relevant). Tombstoned does NOT satisfy IsNotFound.
- `UpsertActor`/`UpsertCommunity`: identity drift (same AP id, different
  DID/type/instance) → ConflictError, row untouched. Tombstoned actors
  (consent_state=deleted, terminal) are fully frozen — upserts no-op and
  return the stored row. Handle/signing key are sticky: omitted values
  never clobber stored ones; key mutation is deliberate-only.
- ConsentState zero value ("") is INVALID by design (consent must be
  stated explicitly — fails closed). Model field is SigningKeyEncrypted
  (task 03 stores AES-GCM ciphertext there, column `signing_key`).
- ap_objects timestamp is `PublishedAt` (AP published), NOT created_at.
- `followed_at` stamps only on transition into accepted; AP-driven
  arbitrary transitions otherwise legal. inbox_events queue-consumption
  API (ListPending/ordering/attempts) deliberately deferred to task 06.
- ENVIRONMENT=production disables migrations-on-start and dev defaults.
- indigo pinned to pseudo-version v0.0.0-20260202181658-ea3d39eec464
  (same as Coves; @latest needs Go 1.26). Unique constraints have
  explicit names, mapped via pq.Error.Constraint in uniqueViolation().
- pr-review-toolkit plugin agents unavailable in this session — the loop
  emulates them with general-purpose agents (works fine; keep doing it).

### From task 02 (internal/ap protocol layer — tasks 05/06 consume this)
- FetchObject/FetchActor error branches mirror ResolveStrongRef: IsNotFound
  (404/401/403) → task 05 fetches ancestor chain; IsTombstoned (410 or
  Tombstone body) → drop subtree (consent). SignatureError → IsValidation.
- ap.Object is one universal struct. ap.Time has .OK()/.Valid — a present
  but malformed `published` is non-nil but OK()==false; task 05 MUST call
  OK() before deriving rkeys/TIDs (zero Time would collide/mis-sort).
- FetchCollection signals ErrCollectionTruncated when it hits the page cap
  or a next-loop — task 06 backfill must treat that as "resume needed",
  NOT complete. Bare-IRI collection items come back with only ID set
  (Type==""); re-fetch them.
- SSRF egress guard is ON by default; config.AllowPrivateAddresses /
  ClientOptions.AllowPrivateAddresses (env ALLOW_PRIVATE_FETCH, dev-only)
  disables it. ANY test/consumer hitting 127.0.0.1 httptest servers must
  set AllowPrivateAddresses=true or fetches are blocked at dial time.
- Task 06 inbox wiring: Verifier.Verify(ctx, req, body) returns the signing
  actor id, enforces same-authority binding (actor.ID host == keyId host)
  and requires host+date+(request-target)+digest signed. It does ONE
  fresh-key refetch on verify failure (key rotation) — task 06 should gate
  that retry (only when key came from cache) to bound forgery amplification.
  ServiceActor.DocumentJSON() ready to serve at /actor; inbox convention
  https://{host}/inbox. service_keys table (migration 005) holds the
  bridge's RSA key UNENCRYPTED (documented tradeoff; not user key material).
- Lemmy HTTP-sig facts (activitypub-federation-rust): Digest required on
  EVERY request incl. GET; keyId is {actorID}#main-key; hs2019 treated as
  rsa-sha256; 1h date-skew window.
- .claude/ is gitignored (session/tooling state, incl. scheduled_tasks.lock).

### From task 03 (identity + virtual repo layer — tasks 04/05/06 consume this)
- internal/repo.Manager is the ONLY write path into repos. PutRecord/
  DeleteRecord return (*repo.CommitResult, error) — {RecordCID (empty for
  deletes), CommitCID, Rev, Seq, NoOp}. NewManager returns (*Manager,
  error) (nil db/keys rejected). Records must carry non-empty `$type`.
  Identical re-put = idempotent NO-OP: NoOp=true, Seq=0, same cid+rev,
  NO new commit/firehose event (deterministic rkeys rely on this).
- repo.DeterministicTID(published time.Time, canonicalAPID string)
  (syntax.TID, error) is task 05's rkey function. FAILS CLOSED on zero/
  pre-epoch published (callers still gate on ap.Time.OK()). For second-
  precision inputs the microsecond field is filled from sha256(ap_id) —
  same-second bulk imports don't birthday-collide the 10 clock-ID bits;
  within-second sort order is hash order. GOLDEN-VALUE TESTS pin the
  algorithm (tid_test.go) — changing it breaks every persisted at-uri.
  Commit revs come from repo_state via NextRev — monotonic per repo
  across restarts. ops use typed repo.OpAction consts.
- firehose_events schema for task 04: seq bigserial, did, commit_cid,
  prev_data_cid (MST root before commit, NULL on genesis — the sync v1.1
  prevData), since_rev (previous commit's rev, NULL on genesis — the
  #commit `since` field), rev, ops jsonb ([{action,path,cid,prev}]),
  car bytea (CARv1, ROOT/COMMIT BLOCK FIRST, contains commit + MST-diff +
  record blocks), created_at. Appended in the SAME tx as the commit.
  Commits are v3, Prev always null.
- Commit serialization: every commitWrite tx takes GLOBAL
  pg_advisory_xact_lock(0x7469646570636d) (distinct from testutil's
  session lock 0x7469646570 — keep them distinct). This guarantees
  seq order == commit-visibility order, so task 04 may tail with naive
  `WHERE seq > cursor` — any future writer bypassing repo.Manager breaks
  that. Per-DID mutex + repo_state row lock remain as backstops. blocks
  keeps superseded blocks (no GC; append-only is load-bearing for
  GetRecord's read consistency); ExportCAR includes unreachable
  historical blocks — task 04's getRepo may want reachable-set-only.
- identity.Minter.MintActor mints did:plc via MODERN plc_operation genesis
  ops (indigo's plc package only has the deprecated legacy `create` op —
  don't use it): rotationKeys=[bridge escrow key], verificationMethods.
  atproto=per-actor key, signed by the escrow rotation key (enables later
  claiming). Minter does NOT write bridged_actors — callers (05/06) upsert
  the returned Identity{DID, Handle, DIDKey, SigningKeyEncrypted}; handle
  uniqueness race is caught by the bridged_actors_handle_key index.
- Minting failure semantics (tasks 05/06): a failed mint can leave a
  registered DID on the directory (PLC ops are forever); orphans are
  slog.Error'd with did+handle. On handle-collision retry, callers should
  eventually REUSE the minted DID via a PLC updateHandle op, not re-mint
  (deferred). Unrepresentable usernames (all-CJK/emoji) get deterministic
  u<10-hex-of-sha256> labels; collision suffixes shorten the base so the
  63-char DNS label limit holds. No mint rate limiting yet — REVISIT in
  task 06 when inbound AP activity can trigger minting (abuse vector).
- Key custody: identity.Custodian (AES-256-GCM under 32-byte BRIDGE_KEK,
  new env var, dev default is a fixed public key, required in prod).
  Ciphertexts are AAD-bound to the DID — copying signing_key between rows
  breaks decryption. Escrow rotation key lives ENCRYPTED in service_keys
  row "plc-rotation" (unlike the plaintext RSA service key; NOTE the
  column is named private_key_pem but holds sealed ciphertext — rename
  candidate). identity.ActorKeys implements repo.SigningKeys —
  SigningKey(ctx, did, use repo.KeyUse): tombstoned actor + KeyUseWrite →
  IsTombstoned (frozen); tombstoned + KeyUseDelete → key RELEASED, so
  task 05's Delete(Actor) → scrub-records flow works regardless of
  consent-flip ordering. Residual TOCTOU: a consent flip racing an
  in-flight commit can let that ONE commit land (consent read is outside
  the commit tx — full fix deferred; fine for single-writer v1).
- store.BridgedActors grew GetByHandle (minting collision-suffix +
  resolveHandle). Handle scheme: name.instance-with-dashes.BRIDGE_HOSTNAME,
  lowercased, non-[a-z0-9-] runs → single dash, collisions get -2/-3/….
  Tombstoned actors' handles do NOT resolve.
- Wired in main.go: GET /xrpc/com.atproto.identity.resolveHandle and
  GET /.well-known/atproto-did (resolves from Host header; wildcard DNS
  requirement documented in README). Task 04 mounts sync endpoints next to
  them.
- PLC egress uses ap.NewGuardedHTTPClient (same SSRF guard as the AP
  client; ALLOW_PRIVATE_FETCH relaxes it in dev/tests only).
- Testing: internal/testutil.DB(t) is the shared pg harness — it holds a
  postgres advisory lock per test process because store/repo/identity
  packages share the test DB and `go test ./...` runs packages in parallel.
  New pg-using packages MUST use it. PLC tests hit a LOCAL directory only
  (default http://localhost:3002, env TIDEPOOL_TEST_PLC_URL, `make plc-up`
  or the running Coves dev PLC); they hard-fail on non-loopback URLs and
  skip under -short/unreachable. NEVER point tests at https://plc.directory.
- go.mod grew direct deps: go-cid, go-block-format, go-ipld-format,
  go-car (v0, indigo's pinned pseudo-version), go-multihash.
- Test DB URL needs ?sslmode=disable (the bare URL earlier in this file
  fails with "SSL is not enabled"); Makefile's TEST_DATABASE_URL has it.

### From task 04 (sync surface — tasks 05/06/08 consume this)
- internal/sync serves the full relay-facing surface: subscribeRepos WS
  (cursor replay from firehose_events then live tail; per-conn outbox reads
  the DB log — no in-memory queue; slow consumers evicted by write deadline
  and resume by reconnecting with their last cursor), getRepo,
  getLatestCommit, getRecord (proof CAR), listRepos, getRepoStatus,
  describeServer, /xrpc/_health. Protocol tokens exported: sync.
  InfoOutdatedCursor / sync.ErrorFutureCursor.
- repo.Manager owns ALL sync reads (ListEvents, SeqBounds, PruneEvents,
  GetRecordProof, ListRepos, GetRepoInfo) — internal/sync has zero SQL.
  Broadcaster wakes on pg_notify (emitted IN the commit tx, repo.
  FirehoseNotifyChannel) with a poll fallback; wake = "rescan the log",
  payloads are never trusted. Pings live in a dedicated per-conn goroutine
  (pingPump) — replay stretches must never starve liveness (was a real bug:
  healthy consumers evicted every 3×pingInterval during deep backfills).
- Mid-replay pruning is SIGNALED: on a seq gap the outbox re-checks
  SeqBounds and emits #info OutdatedCursor if the consumer's position fell
  off the retained window (benign nextval gaps stay silent). Deactivated
  (consent=deleted) repos refuse getRepo/getRecord/getLatestCommit
  (RepoDeactivated) and report active:false in listRepos/getRepoStatus.
- DEFERRED to task 06 (documented in streamEvents): the firehose itself
  carries only #commit — consent revocation must eventually emit an
  #account{active:false} frame (+ scrub delete commits) so subscribers
  purge; tombstoned actors' historical events stay replayable for the
  retention window until then. Task 06 must also DECIDE: does nobridge
  (vs deleted) deactivate the read surface? (bridgy-fed deletes bridged
  content on nobridge discovery; we currently only stop new
  materialization.)
- FIREHOSE_RETENTION (Go duration, default 72h) drives RunPruner (hourly;
  fails closed on retention<=0). RELAY_HOSTS drives requestCrawl on start
  (log-only in dev, SSRF-guarded client in prod). created_at on
  firehose_events is clock_timestamp() (commit-visible time), not tx start.
- Jetstream verification is MANUAL (compose profile `jetstream`, port 6018
  + README runbook "Verifying with Jetstream") — the DoD line "Jetstream
  consumes without errors" is verified by hand, not CI. Task 08's harness
  should automate it.
- Not yet done (pre-internet-facing hardening, flagged by security review):
  no connection cap / per-IP rate limit on the public surface; getRepo
  buffers full CARs in memory (revisit with block GC / tree cache);
  PruneEvents is one unbatched DELETE per sweep.
- Deferred design notes for task 04: repo package should own the sync
  read API (GetRecordProof for sync.getRecord, ListEvents(sinceSeq,
  limit)) rather than task 04 issuing raw SQL against repo tables —
  decide there. MST loads are full-tree, one SELECT per node → PutRecord
  is O(repo size); fine now, needs a per-DID tree cache before big
  community backfills (task 05). SigningKeys could become a SignCommit
  capability (keeps key plaintext inside identity; enables KMS later) —
  revisit before the interface calcifies. No OnCommit hook yet: task 04's
  broadcaster should LISTEN/NOTIFY or poll seq (CommitResult.Seq exists).

### From task 05 (materializer — task 06 is THE consumer)
- Entry points task 06 drives: MaterializePost(Page/Article),
  MaterializeComment(Note), HandleUpdate(obj) [Update; also the Create
  dispatcher — an Update for an unseen object just materializes it],
  HandleDelete(apID) [object OR actor], DeleteActor (terminal, sets
  consent=deleted), SuppressActor (reversible nobridge scrub),
  Ensure/RefreshActor, Ensure/RefreshCommunity. All return *Result
  (DID/ATURI/CID/NoOp) or a typed error. IsSkip(err) = log-and-never-retry
  (consent/tombstone/cycle/unusable input); any other error = retryable.
- SECURITY CONTRACT task 06 MUST honor: RefreshActor/RefreshCommunity (and
  HandleUpdate on Person/Group) TRUST an embedded actor doc — call them
  ONLY after verifying the activity signature (signer == actor). Content
  paths (Ensure*) always re-fetch by IRI and bind the fetched id to the
  fetch authority (ap.SameAuthority), so an inline/forged attributedTo
  can't mint under a victim's id. Fetched-object ids are authority-bound at
  the materializer boundary (comments.go, actors.go), NOT inside
  ap.FetchObject (kept generic; ap.SameAuthority is exported for reuse).
- Consent: nobridge on a previously-bridged actor now SCRUBS existing
  records (scrubActorRecords) then sets NoBridge (reversible). deleted is
  terminal. commitRecord refuses to resurrect an object whose own mapping
  is soft-deleted (unordered create-after-delete). KNOWN GAP for task 06:
  a Delete arriving BEFORE the object was ever materialized leaves no
  mapping to tombstone → a later Create still materializes. Task 06's inbox
  needs dedup/ordering (or a tombstone-of-unseen-ids table) to close it.
  Also: Undo(Delete)/restore must explicitly clear the soft-delete.
- Lexicon validation: every record validated against vendored lexicons/
  (gojsonschema-equivalent indigo validator). StrictValidation=true in
  dev/tests (fail closed); production logs+writes on failure — task 06
  should wire a metric on that and consider strict-first rollout. Text
  fields are capped to BOTH maxGraphemes and maxLength bytes (truncateText).
- Blob store: migration 008 blobs(did,cid,bytes,mime); repo.PutBlob/GetBlob
  (content-addressed, CID computed server-side); sync getBlob serves with
  nosniff + sandbox CSP; image fetches go through the SSRF-guarded ap
  client with per-slot size/type caps. MAX_BLOB_BYTES config exists but is
  clamped by ap client's maxResponseBytes (5MiB, no knob) — raising it
  above 5MiB is currently a no-op (wire it in task 06/07 if needed).
- DID-MINT AMPLIFICATION (task 06 MUST address when wiring the inbox): a
  crafted deep comment thread with a distinct fake author per level mints
  up to maxAncestorDepth(50) DIDs per delivered object. No rate limiting in
  identity.Minter or materialize yet. Gate inbound minting in task 06.
- Deferred (LOW, noted for later): transient media-fetch failure on refresh
  drops existing blobs (no carry-forward); a stale actor behind a 403ing
  instance drops content instead of serving stale; commitRecord's
  PutRecord→PutMapping isn't one tx (self-heals on retry; a Delete landing
  in the crash window logs Warn); DeleteActor scrubs records but not blobs
  under community DIDs; communityRef uses a Lemmy /c/ heuristic (Mbin /m/
  later). Test gaps still open: embed.images arm + nsfw label shapes are
  never lexicon-validated (only external embed is) — add before trusting
  "all records validate" for image posts.

### From task 06 (ingestion — tasks 07/08 consume this)
- internal/ingest is the inbox→queue→dispatch→materializer pipeline. Entry:
  POST /inbox (+/actor/inbox alias) verify-sig → authority-bind signer →
  dedupe by activity id → Enqueue. GET /actor, /.well-known/webfinger
  (service actor only), /.well-known/nodeinfo + /nodeinfo/2.0
  (software.name "tidepool"). Admin (bearer ADMIN_TOKEN, constant-time):
  POST/DELETE/GET /admin/communities, POST /admin/communities/backfill.
- TASK 07 SEAM: implement ingest.VoteAggregator — ApplyVote/RetractVote(ctx,
  vote *ap.Object, communityIRI string). Wired as NewNoopVotes in main.go;
  swap it. Announce{Like|Dislike}→ApplyVote, Announce{Undo{Like|Dislike}}→
  RetractVote; communityIRI is "" for bare (non-announced) votes. RetractVote
  MUST treat nil/bare-IRI vote.Object/vote.Actor as no-ops (don't error/
  retry). Aggregate COUNT SEEDING is NOT expressible via per-vote ApplyVote —
  if task 07 seeds historical counts from Lemmy's API, add a separate
  SeedAggregates method + Backfill wiring (additive, not breaking).
- QUEUE (store.InboxEvents.Enqueue/ClaimNext/Release/MarkPoisoned/
  MarkProcessed): durable pg work queue, per-community serial ordering key,
  FOR UPDATE SKIP LOCKED + NOT-EXISTS older-sibling gate. Outcome contract
  (identical on Handler.Process, Processor, queue dispatch): nil/IsSkip →
  processed; IsValidation → poison; else retry w/ exp backoff (cap 1h);
  attempt-cap → poison. CRITICAL for task 07: the queue is now FENCED —
  ClaimNext returns claimed_until as a fencing token; Release/MarkPoisoned/
  MarkProcessed take that token + return (applied bool). A stale worker's
  write is a silent no-op (applied=false). This exists SO task 07's vote
  counter arithmetic isn't double-applied — do NOT bypass repo.Manager-style
  or write vote state outside the queue's outcome path without the same
  fencing discipline. HEAD-OF-LINE BLOCKING is intentional: a retrying event
  blocks its whole ordering key (community) until success or poison.
- Shutdown: processNext no longer classifies ctx-cancellation as a failure
  (lease lapses → redeliver); outcome writes use context.WithoutCancel so
  completed work is recorded during shutdown. Residual: a shutdown-
  interrupted attempt still consumes its ClaimNext attempt increment (not
  decremented; harmful poison/error-record behavior is gone).
- AUTHORIZATION MODEL (task 07/08 must preserve): announced (announcer!="")
  Delete/Undo require ap.SameAuthority(target, announcer) AND for actor
  targets require target==announcer (a community may delete only itself, not
  a co-hosted other actor). Announced CONTENT requires communityIRIFrom(obj)
  == announcer (no bridging a non-subscribed same-host community). Bare
  (announcer=="") Update{Person|Group} is REFRESH-ONLY: never mints/bridges
  an unknown actor (was an SSRF+mint-budget vector); embedded actor doc is
  trusted only when signer==actor, else forced re-fetch. Bare Delete still
  uses SameAuthority(target, signer) (host-granularity — instance is the
  trust unit; documented). Undo{Delete} restores mapping BEFORE re-
  materialize (commitRecord won't resurrect a soft-deleted mapping) and
  COMPENSATES (re-soft-delete + re-tombstone) if HandleUpdate skips/invalid.
- Consent: nobridge scrubs but repo stays ACTIVE (reversible, firehose-
  visible delete commits); only `deleted` deactivates the sync read surface.
  Still DEFERRED (task 07/08): firehose #account{active:false} frame on
  consent revocation (subscribers currently rely on scrub delete-commits);
  ap_tombstones grows unbounded (no pruner — mirror FIREHOSE_RETENTION);
  NO per-signer/per-IP rate limit on /inbox (queue-flood DoS via many self-
  signed identities — coarse admission limit is the hardening item);
  ClaimNext does an O(N) row scan when a community's queue backs up behind a
  failing event (per-key serialization cost; revisit at scale). Nudge now
  cascades on successful claim.
- Backfill: outbox newest→oldest to BACKFILL_MAX_POSTS (100), replies walk
  when advertised, tombstone-checked on all three paths now. RESUMABLE-BY-
  REDO: truncation (ErrCollectionTruncated) or failures>0 leave
  last_backfill_at UNSET so an un-forced re-trigger re-walks; deterministic
  rkeys make redo idempotent. main.go drains backfill.Wait() on shutdown
  (bounded); TriggerAsync runs on the root ctx (observes shutdown).
- New env: ADMIN_TOKEN (required in prod, dev default dev-admin-token),
  BACKFILL_MAX_POSTS (100), MINT_RATE_PER_MINUTE (60), MINT_BURST (120),
  INGEST_WORKERS (4). Migration 009: queue columns on inbox_events
  (payload, actor_id, ordering_key, attempts, next_attempt_at, claimed_until,
  failed_at) + ap_tombstones table. store grew Tombstones (Record/Exists/
  Remove), APObjects.Restore, and the fenced InboxEvents queue API.
- ap.Client.ResolveKeyDetailed exported + Object.Replies field added. Verify
  fresh-key refetch is now cache-gated (fires only when the failing key came
  from cache) — bounds forgery amplification.
- Deferred TEST gaps (task 08 harness): concurrent-worker queue stress
  (SKIP-LOCKED ordering only tested sequentially); mint-gate "retry via queue
  backoff" only asserted at unit level, not end-to-end; webfinger/nodeinfo
  vs what real Lemmy actually queries (needs live Lemmy). activityID rand-
  fail path is guarded but unit-untestable (Go 1.24+ crypto/rand failure is
  a fatal crash, not a returnable error).
