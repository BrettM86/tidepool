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
| 5 | 05-materializer | pending | | |
| 6 | 06-ingestion | pending | | |
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
