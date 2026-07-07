# Tidepool build-loop state

Protocol: PLAN.md §Loop protocol. One task per iteration:
implement (Fable agent) → /second-opinion review → fix → verify → commit →
update this file → schedule next. Stop the loop when every task is `done`.

| # | Task | Status | Commit | Notes |
|---|------|--------|--------|-------|
| 1 | 01-scaffold-storage | done | (see git log) | reviewed by 7 reviewers, 18 fixes applied |
| 2 | 02-ap-protocol | pending | | |
| 3 | 03-identity-repos | pending | | |
| 4 | 04-sync-firehose | pending | | |
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
