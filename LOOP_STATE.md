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
| 7 | 07-vote-aggregates | done | (see git log) | 6/8 reviewers (Gemini perm-denied, glm watchdog-killed); fixes: announced-vote subject↔community binding (post mapping-DID / comment reply.root), bare Undo{Like} signer binding, RetractVote id-targeted undo, dup-id 0/0 aggregate-row leak, seeder zero-clobber presence check, limiter sweep-throttle + 50k fail-closed cap, at-uri validation + ~20 regression tests |
| 8 | 08-e2e-harness (infra: Dockerfile, compose, Lemmy federation, Makefile, CI, lexicon-sync) | done | (see git log) | 7/7 reviewers (4 Claude + codex/gemini/glm, first full external panel since 03); fixes: PRODUCTION https→http redirect-downgrade guard (codex unique catch), webfinger fallback narrowed to transport failures + both-legs errors + 4 tests, minter PDS-endpoint scheme threading, PLC image commit-pin, --wait-timeout + CI logs if:always() + Makefile up-failure cleanup/teardown-status, loopback-only host binds, check-lexicons fail-open holes, sync-lexicons bridge-nesting guard, 2 false compose-header claims rewritten (invented env var, wrong --wait semantics) |
| 9 | 08-e2e-harness (tests: tests/e2e helpers + 8 scenarios, FOLLOWUPS.md, README) | done | (see git log) | 8/8 reviewers (5 Claude + codex/gemini/glm; gemini zero-issue "excellent", codex sharpest); fixes: drain() dead-listener vacuous-pass (5/8 flagged), centralized vetEvent (unknown-collection Fatalf + lexicon-validate every consumed create/update, suite-wide locked-decision-7 enforcement), scenario-7 backfill-completion poll + gap-post cursor-resume proof + op-agnostic dup keys, readLoop goroutine join, subscribe fail-fast on explicit reject, embed.external e2e coverage, seeder e2e assertion. NEW ASSERTION CAUGHT REAL BUG: Lemmy vote-clear federates Undo with RECONSTRUCTED inner vote (fresh id, type Like even for live dislike; flips are bare opposite votes, no Undo) → id-targeted RetractVote no-oped every production vote-clear; fixed with known-id replay probe + live-vote fallback + 3 unit tests |

v1 loop (tasks 01–08) COMPLETE — `make e2e` green (8/8 scenarios, ~100s), full unit suite green.

## v1.1 loop (tasks 09–12) — added 2026-07-10

Goal: work the FOLLOWUPS.md backlog and put a real relay in the e2e
pipeline. Locked requirement: every state has a full e2e pipeline of
Lemmy → PDS record → firehose ingestion (where applicable) — task 09
re-points Jetstream through the relay so every scenario transits it.
Same protocol: implement (Fable agent) → /second-opinion → fix → verify →
commit → update this file → next task. Votes-as-records was deliberately
NOT scheduled — see FOLLOWUPS.md "Design revisits" (decide with the
write-back design; tasks 11–12 are its prerequisites).

| # | Task | Status | Commit | Notes |
|---|------|--------|--------|-------|
| 10 | 09-e2e-relay | done | (see git log) | 7/7 reviewers (4 Claude emulated + codex/gemini/glm); fixes: dev requestCrawl PUBLIC-relay dial guard (codex unique catch — NewPrivateOnlyHTTPClient, inverse SSRF guard), terminal-error classification made pre-flight-only (whole-chain IsValidation was abandoning a relay on attempt 1 for transient DNS), 10s per-attempt timeout (budget arithmetic was 14min worst-case, not 2min), vacuous validation-no-retry test rewritten + 400-is-retried pin, vetEvent per-DID rev-monotonicity (restores per-repo ordering assertion suite-wide), drain() returns+clears pending (closes task-10 vacuous-pass trap), relay poll robustness + pagination cap, doc corrections (RESOLVE_ADDRESS overstatement, spec BGS_CRAWL_INSECURE_WS annotation, FOLLOWUPS 16th-failure off-by-one). KEPT DELIBERATE over 3 reviewers' objection: all wire errors incl. 4xx retried — bigsky answers the describeServer callback race with HTTP 400 (comment + test pin it). Final clean make e2e: 10/10, 96.7s |
| 11 | 10-e2e-scenarios | done | (see git log) | 6/7 reviewers (glm watchdog-killed); UNANIMOUS 6/6 finding: tombstone confirm-fetch transient failure → definitive 401 permanently lost legitimate account deletions → fixed with three-way taxonomy (tombstone→202, alive/validation/404→401, transport/5xx→503 defer) + test; codex unique: confirmation fetch followed cross-authority redirects (open-redirect → forged 410) → FetchActorSameAuthority pins every hop; security: unauthenticated durable-write path flagged → encoded into task 11 rate-limit spec; also: zz-sweep replay floor + honest bounds (sentinel-only pass was vacuous), Delete(Actor) over-scrub drain, actor!=object + Announce{Delete} 401 pins, GET / route-level test, cursor 0→1 doc fixes, vote-hammer header de-overclaimed. TASK ITSELF: 7 scenarios + 2 PRODUCTION fixes (apex instance actor — Lemmy silently never delivers Delete{Person} without a Site actor row; tombstone-verified self-delete acceptance). Final clean make e2e: 17/17, 239s |
| 12 | 11-hardening | done | (see git log) | 6/7 reviewers (glm watchdog-killed on 5k-line diff); NO high-sev confirmed (gemini's "carry-forward type assertion always fails" was a FALSE POSITIVE — GetRecord returns typed atdata.Blob, test green). Fixes: FollowRetrier atomic UPDATE...RETURNING claim (list-then-update raced Accept + burned attempts on transient failure + silent exhaustion), rate-limit refusal observability (expvar counters + sampled Warn — mistuned limit silently dropped all traffic), community-DID blob orphan now retryable (was swallowed → served forever; required delete-before-soft-delete reorder), ScrubVoter DELETE...RETURNING recompute (phantom-count lost update), carry-forward drops on permanent 404/410 vs carries on transient, DeleteActor terminal-state fixpoint (no double #account), migration-011 CHECK tightened + raw-insert test, /admin/metrics scoped expvar, internal/prune fail-closed test, proxy XFF ops note. TASK: inbox+sync admission control, #account{active:false,status:deleted} frame verified purging repo from bigsky, follow auto-retry, 3 pruners, one-tx record+mapping, blob/vote scrubs, service_keys rename. delete-before-create: README was RIGHT, FOLLOWUPS stale (task 06 already closed it). Final clean make e2e: 17/17, 232s |
| 13 | 12-perf-scale | in-progress | | MST cache, getRepo streaming/reachable-set, blocks GC, ClaimNext scan |

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

### From task 07 (vote aggregates — task 08 consumes this)
- internal/votes: Aggregator implements ingest.VoteAggregator.
  NewAggregator(db, apObjects, communities, records, logger) — `records` is
  a narrow votes.RecordReader (satisfied by repo.Manager, wired in main.go).
  XRPC: GET /xrpc/social.coves.bridge.getVoteAggregates (lexicon at
  lexicons/social/coves/bridge/, README documents it as the AppView
  integration point). ≤100 uris counted PRE-dedupe; malformed at-uri →
  InvalidRequest 400 (validated via syntax.ParseATURI); unknown uris
  omitted; response preserves request order; Cache-Control public/max-age=30;
  per-IP token bucket on RemoteAddr only (XFF deliberately ignored),
  FAIL-CLOSED at 50k buckets (unknown IPs get 429 during rotation floods),
  sweep throttled to 1/min, injectable clock for tests.
- Vote state machine: append-only vote_events, invariant ≤1 non-undone row
  per (voter,subject) is APP-enforced only — every mutation MUST take the
  vote_aggregates row lock FIRST (lockAggregate upsert / SELECT FOR UPDATE),
  then recompute-in-tx. Dedupe by activity_id (unique, first-writer-wins —
  forged-id squatting narrowed by authority binding but ids remain
  unauthenticated). RetractVote targets the undone activity's own id when
  vote.ID != "" (replay-proof), direction-match fallback for id-less undos.
  Dup activity id is probed BEFORE creating an aggregate row (no 0/0 rows).
- AUTHORITY MODEL for votes: announced votes require the announcer to
  resolve to a followed community AND subject∈that community — posts via
  mapping.DID == community DID (NOT SameAuthority: Lemmy hosts post objects
  on the AUTHOR's instance), comments via one GetRecord read of reply.root's
  DID. Bare Like/Dislike: inbox already binds actor↔signer (403).
  Bare Undo{vote}: Handler.authorizeBareVote (consent.go, host-granularity
  SameAuthority like bare Delete). All mismatches drop at debug as nil —
  queue outcome contract (nil=processed) preserved everywhere; only real DB
  errors are retryable.
- Seeding: seeded_upvotes/seeded_downvotes baseline columns (beyond spec —
  deliberate: served = seeded + live, re-seed idempotent, never clobbers
  live votes). LemmySeeder.SeedPostCounts posts-only via SSRF-guarded
  client, presence-checked decode (missing post_view/counts → error, old
  baseline survives; a wrong-shape 200 can NOT zero-clobber). Wired as
  optional ingest.CountSeeder in backfill (best-effort, Warn on failure,
  never affects run outcome). SEED_COUNTS_FROM_API (default on; strict bool
  parse rejects typos). Known accepted drift: a baseline voter who flips
  sends Undo for a like we never saw (no-op) — stale until re-seed.
### From task 09 (relay pipeline — tasks 10/11 MUST know)
- The e2e pipeline is now Lemmy → tidepool → BigSky relay → Jetstream:
  EVERY scenario's events transit relay validation (DID resolution against
  the local PLC, per-commit sig verification). jetstream-direct (compose
  profile `direct`, host :6038) is a debug tap only. Relay on host :2480
  (admin key e2e-relay-admin-key); helpers: relayPDSList/relayListRepos/
  relayGetLatestCommit in tests/e2e/helpers.go.
- CRITICAL for scenario design: bigsky's parallel indexer (100 workers
  keyed by repo DID) preserves per-repo order but NOT cross-repo order —
  author profiles and community posts routinely swap. The e2e listener now
  BUFFERS consumed-but-unmatched commit events and rescans them on later
  awaits; await predicates MUST be PURE (side-effecting accounting belongs
  in drain loops — scenario 8 was rewritten that way). Cross-repo ordering
  assertions were removed from scenarios 2/3/6; presence + linkage is what
  survives a relay. Carry to Coves AppView: it cannot rely on
  profile-before-post through relay infra (FOLLOWUPS "Relay pipeline").
- New config: ALLOW_DEV_REQUEST_CRAWL (dev-only, refused in production)
  makes dev actually send requestCrawl; RequestCrawlAll now retries per
  relay (5s × 24, vars compressible in tests) because bigsky's requestCrawl
  handler calls BACK into the announcing host's describeServer before
  subscribing — the first attempt races the bridge's own listener (observed
  live: attempt 1 fails, attempt 2 lands).
- bigsky env facts (verified against image --help + source; Coves' compose
  stanza is wrong): ATP_PLC_HOST (plc), RELAY_ADMIN_KEY, DATA_DIR,
  RESOLVE_ADDRESS (defaults to PUBLIC 1.1.1.1 — set 127.0.0.11),
  HANDLE_RESOLVER_HOSTS=tidepool (trial-host resolver GETs
  /.well-known/atproto-did with the handle as Host header — handles VERIFY,
  0 failures in a full run), BSKYLOG_LOG_LEVEL; --crawl-insecure-ws has NO
  env binding (command arg). Fresh relay refuses ALL non-admin requestCrawl
  (new-PDS-per-day limit 0, checked before trusted domains) → one-shot
  relay-bootstrap service raises it; tidepool depends_on its completion.
  Image has no arm64 manifest (platform: linux/amd64, emulated on Apple
  Silicon). Relay gets its own postgres with TWO dbs (bgs + carstore).
- Tombstone visibility through the relay is a DEAD END until task 11:
  bigsky has no getRepoStatus, filters tombstoned repos from listRepos, and
  only learns account state from #account frames the bridge doesn't emit
  yet. When task 11 adds the #account frame, add the relay-side assertion
  (repo disappears from relay listRepos after consent revocation).
- Timing budgets: eventTimeout 90s→120s (relay hop + first-sight DID work +
  amd64 emulation), burstTimeout 3m (scenario 8 is drain-based now),
  crawlTimeout 3m (covers the announce retry window). Suite runs ~114s.
- DEFERRED (task 08+): vote_events grows unbounded (no pruning of
  superseded/undone rows — mirror FIREHOSE_RETENTION treatment alongside
  ap_tombstones); actor-delete/consent revocation does NOT scrub that
  actor's vote_events rows (inconsistent with scrub posture elsewhere;
  counts are anonymous on the wire so exposure is low); no concurrency
  stress test of the aggregate-row locking claim (sequential tests only);
  subject resolution happens outside the mutation tx (narrow TOCTOU with a
  racing Delete, documented); no upper sanity cap on seeded counts;
  comment count seeding skipped (per-comment API calls would triple
  backfill egress).

### From task 10 (e2e scenario completion — tasks 11/12 MUST know)
- The task shipped TWO BRIDGE-CODE changes, not just tests (both were
  prerequisites for the Delete(Actor) scenario to be real; both are
  review-worthy):
  1. **Instance (Site) actor at the origin apex** (`GET /`, ingest
     inbox.go handleInstanceActor + ap.ServiceActor.InstanceDocumentJSON).
     Lemmy's federate worker resolves send-to-all-instances targets
     (Delete{Person}!) to the stored remote SITE row's inbox and silently
     skips (`no inboxes`) peers without one; Lemmy creates the row by
     fetching the peer's origin apex on every Person/Community from_json.
     Wire trap: the Instance protocol enum accepts ONLY `Application` —
     the OPPOSITE of /actor, where Lemmy's Person enum needs `Service`.
     Existing peer Lemmys need an actor re-fetch (24h TTL) + worker
     restart before deletions start flowing (worker caches site=None for
     its lifetime).
  2. **Tombstone-verified self-Delete acceptance** (ingest inbox.go
     tombstonedSelfDelete + ActorFetcher option, 3 regression tests).
     Lemmy signs the account-deletion Delete{Person} with a key whose
     actor doc its origin ALREADY serves as 410 Gone — unverifiable
     unless cached. The inbox accepts a bare self-referential Delete when
     verification failed on IsTombstoned AND an independent SSRF-guarded
     fetch of the claimed actor's own IRI confirms Gone (origin word =
     same host-granularity trust as bare-Delete SameAuthority; forgery ≡
     truth). Any other tombstoned-signer payload → definitive 401 (5xx
     would head-of-line block Lemmy's forever-retrying per-instance
     queue). Review fixes (post-7-model review): the confirmation is
     three-way — origin says Gone → accept; origin answers definitively
     otherwise (live actor / not-an-actor / 404) → 401; transport/5xx/
     timeout → 503 so the sender redelivers (a blip must never
     permanently drop a deletion) — and the confirmation fetch pins
     every redirect hop to the actor IRI's own authority
     (ap.FetchActorSameAuthority), so an open redirect on the origin
     cannot bounce it to an attacker host serving 410.
- **Task 11 rate limit MUST cover the tombstonedSelfDelete branch:** it is an unauthenticated POST → confirmation fetch → TWO durable writes (inbox_events + ap_tombstones) reachable with a cheap 410-serving endpoint; consider a dedicated per-IP cap (flagged in tasks/11-hardening.md).
- **Jetstream cursor full-replay middle band** (measured with a ws probe):
  cursor ≤ newest stored event → precise µs replay; cursor > server-now →
  live-tail; cursor BETWEEN newest event and now (any "from now" subscribe
  on a quiet stream) → replays the ENTIRE store. Every cursorNow()
  listener gets full-history replays; negative assertions must be bounded
  by an observed event's time_us (see unsubscribe scenario + cursorNow
  doc). Carry to Coves AppView: cursor=now is not a dedupe boundary.
- **Consumer-side lexicon validation of blobs requires
  atdata.UnmarshalJSON**, not encoding/json — indigo's SchemaBlob.Validate
  demands a typed atdata.Blob ("expected a blob" on raw maps). The e2e
  validateLexicon now mirrors the materializer's write-side decode; any
  JSON Jetstream consumer that validates records with blobs (Coves!) must
  do the same. Found by the image-post scenario — the first blob ever to
  cross the suite's validation.
- Lemmy 0.19.19 wire facts (source-verified + observed): save_user_settings
  federates NOTHING (no Update{Person} variant exists) — consent marker
  changes are discoverable only via TTL re-fetch on the actor's next
  activity (e2e compose sets PROFILE_REFRESH_TTL=2s to drive it; an
  inactive actor's opt-out is never discovered — task 11 candidate:
  periodic consent re-scan); delete_account is POST with required
  delete_content and federates ONE Delete{Person} with nonstandard
  removeData (no per-object deletes); image-post attachment is
  {"type":"Image","url",…,"name":alt} with mediaType DROPPED (type field +
  extension fallback are the only discriminators; alt text doesn't survive
  the Link fallback); pictrs upload is POST /pictrs/image multipart
  "images[]", Bearer jwt.
- GET /admin/communities lists accepted/pending ONLY — an unsubscribed
  community disappears from it (don't poll it for follow_state=none).
- e2e suite is now 17 scenarios, ~230s full run, re-runnable against an
  accumulated stack. TestZZ_SuiteEndSweep (zz_ file = alphabetically last
  = runs last) replays the whole firehose from cursor 1 unfiltered and
  re-vets everything incl. per-DID rev monotonicity from history start.
  TestDeleteActor pins "relay still lists tombstoned repo" — task 11's
  #account frame must FLIP that assertion.
- Task-10 stretch (low MINT_RATE_PER_MINUTE compose variant) skipped as
  specced — needs its own compose profile (FOLLOWUPS, Ingestion).

### From task 11 (hardening — task 12 MUST know)
- **firehose_events now carries TWO event kinds** (migration 011): `commit`
  and `account` (nullable commit columns + a shape CHECK per kind).
  repo.Event grew Kind/AccountActive/AccountStatus; any code iterating
  events must switch on Kind (sync/subscribe.go does; e2e vetEvent skips
  non-commit Jetstream kinds already). `repo.AppendAccountEvent` takes the
  SAME global commit advisory lock as record commits — seq order still ==
  visibility order; task 12's perf work must preserve that for both kinds.
- **#account status token is "deleted", and so is the bridge's own
  getRepoStatus/listRepos status** (was "deactivated"). Wire facts
  (source-verified, pinned bigsky): on #account it re-resolves the DID doc
  and REQUIRES the sender to be the DID's authoritative PDS; "deleted" →
  tombstoned=true (listRepos filters `NOT tombstoned`) + carstore purge;
  "deactivated"/"suspended"/"takendown" filter from listRepos but do NOT
  purge. Emitted ONLY by DeleteActor (terminal); nobridge stays frameless
  (repo active, reversible).
- **repo.PutRecordTx(… TxSideEffect)** is the new atomic seam: the hook
  runs inside the commit tx (also on the NoOp re-put, with res.NoOp=true)
  and an error rolls the RECORD back too. The materializer's ap_objects
  mapping now rides it (store.APObjects.PutMappingTx). Hooks run under the
  global advisory lock — keep them tiny; task 12's MST cache must not
  change hook semantics.
- **internal/ratelimit** is the shared keyed token-bucket (extracted from
  votes; sweep throttle + 50k fail-closed cap). Consumers: votes XRPC
  (429), sync surface (429, _health exempt; subscribeRepos also has a
  reserve-then-check connection cap), inbox (per-IP pre-body + per-signer
  post-verify + a dedicated tombstone-confirmation cap INSIDE
  tombstonedSelfDelete, before the confirmation fetch). INBOX REFUSALS ARE
  503, NEVER 4xx — Lemmy's federation crate retries 5xx but permanently
  drops 4xx; the tombstone-cap refusal is a DEFER for the same reason.
  Defaults are deliberately generous (suite = canary, ran green on
  defaults); all envs in README's config table.
- **internal/prune.Run** is the shared retention loop (fail-closed on
  retention<=0). Pruners: firehose (batched now, 1000/statement, oldest-up
  so a partial sweep keeps the retained suffix contiguous), ap_tombstones
  (30d), undone vote_events (90d — an undone row is also its activity id's
  dedupe record, so replay protection now has that horizon; live rows
  never pruned).
- **Blob scrub caveat**: blobs are content-addressed per (did,cid) with no
  reference tracking — scrubbing a deleted actor's record deletes blob rows
  other records in the same repo could share (byte-identical media).
  Accepted + commented on repo.DeleteBlob; a blob-refcount would close it
  if it ever matters. repo grew DeleteBlob/DeleteBlobsForDID.
- **votes.Aggregator.ScrubVoter** locks ALL affected aggregates in
  deterministic subject order (ORDER BY + FOR UPDATE) before deleting —
  any future multi-subject vote mutation must lock in the same order or
  risk deadlock against it.
- **Follow retrier** (ingest.FollowRetrier, migration 012): every
  set-to-pending stamps follow_requested_at + increments follow_attempts
  (SetFollowState does it); resend consumes the attempt BEFORE sending so
  a hanging peer can't get an unbounded budget; none resets. Default:
  pending >2m → resend, 5 total sends, 1m sweep.
- **service_keys.private_key_pem → key_material** (migration 013);
  store.ServiceKey.PrivateKeyPEM → KeyMaterial (identity/ap callers
  updated).
- **ap.ClientOptions.MaxMediaBytes**: FetchMedia's outer clamp, wired from
  MAX_BLOB_BYTES; the JSON-object cap (MaxResponseBytes) stays independent
  at 5MiB.
- **Delete-before-create verdict**: README was RIGHT, FOLLOWUPS was stale —
  task 06's handleDelete records the ap_tombstones marker before
  HandleDelete and materializeContent checks it;
  TestCreateAfterDeleteTombstone pins the whole ordering. No code change
  needed; the false FOLLOWUPS entry is annotated.
- **Validation-failure metric**: materialize.ValidationFailures (expvar,
  "tidepool_lexicon_validation_failures", counted in strict AND
  log-and-write modes), served on bearer-protected GET /admin/metrics.
  Strict-first production rollout still deferred; this counter is its
  precondition.
- e2e: TestDeleteActor's task-09/10 pins FLIPPED — it now asserts the
  #account frame on the bridge's own firehose (raw CBOR ws helper
  dialBridgeFirehose/readBridgeAccountFrame in tests/e2e/helpers.go) and
  the repo DISAPPEARING from the relay's listRepos (polled; bigsky
  processes the frame async).
