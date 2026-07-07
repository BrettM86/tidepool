# Task 01 — Scaffold, config, storage foundation (~900 LOC)

## Goal
A buildable Go project with the persistence spine every later section writes
through: the `ap_objects` mapping table, bridged-actor registry, community
registry, and event dedupe — plus config, errors, logging, migrations.

## Deliverables
- `go.mod` (module `tidepool`, Go 1.25): `bluesky-social/indigo`,
  `go-chi/chi/v5`, `pressly/goose/v3`, `lib/pq`, `stretchr/testify`.
- `cmd/tidepool/main.go` — wires config, DB, migrations-on-start (dev only),
  chi router with `/healthz`, graceful shutdown. Subsystems register as they
  land in later tasks; keep main thin.
- `internal/config/config.go` — env vars with logged dev defaults (Coves
  style, no config lib): `DATABASE_URL`, `LISTEN_ADDR`, `BRIDGE_HOSTNAME`
  (public domain, e.g. `tidepool.example`), `PLC_DIRECTORY_URL`,
  `BRIDGE_SERVICE_DID` (optional pre-provisioned), `USER_AGENT`.
- `internal/errors/errors.go` — sentinel + typed errors mirroring
  `coves/internal/core/errors`: `ErrNotFound`, `ErrAlreadyExists`,
  `ValidationError`, wrap with `%w`, `IsNotFound()` helpers.
- `internal/db/db.go` — open/ping/pool settings; `internal/db/migrations/`
  goose SQL files (`NNN_description.sql`, `-- +goose Up/Down`):
  - `ap_objects`: id, ap_id (unique), ap_type, origin_instance, did,
    collection, rkey, at_uri (unique), cid, created_at, indexed_at,
    deleted_at (soft). Index on (did, collection), origin_instance.
  - `bridged_actors`: ap_actor_id (unique), actor_type (person|group), did
    (unique), handle, signing_key_multibase (secp256k1 private key —
    NOTE: encrypt-at-rest is task 03's concern; column is bytea),
    consent_state (ok|nobridge|deleted), profile_synced_at, created_at.
  - `communities`: ap_group_id (unique), did, preferred_username, instance,
    follow_state (none|pending|accepted), followed_at, last_backfill_at.
  - `inbox_events`: activity_id (unique) for dedupe, received_at, type,
    processed_at, error text.
- `internal/store/` — one repo per table behind interfaces
  (`interfaces.go` per Coves convention), raw parameterized SQL, idempotent
  upserts (`ON CONFLICT`), soft deletes. This is the package tasks 03/05/06
  consume; get the interfaces right: `PutMapping`, `GetByAPID`,
  `GetByATURI`, `ResolveStrongRef(apID) (uri, cid, error)`.
- `Makefile`: `build`, `test` (spins up postgres-test via compose, runs
  goose, `go test ./... -short`), `db-migrate`, `lint`, `fmt`.
- `docker-compose.dev.yml`: postgres + postgres-test (profiles like Coves).
- `.golangci.yml`, `.gitignore`, `README.md` (one screen: what Tidepool is,
  pointer to PLAN.md).

## Definition of done
- `go build ./... && go vet ./... && go test ./...` green.
- Store tests run against real postgres (compose), covering upsert
  idempotency, strongRef resolution hit/miss, consent-state transitions.
- Migrations apply cleanly up and down.

## References
- Conventions: `~/Code/coves/CLAUDE.md`, `~/Code/coves/internal/core/errors/`,
  `~/Code/coves/internal/db/migrations/`, `~/Code/coves/Makefile`.
- Schema inspiration: `~/Code/bridgy-fed/models.py` (Object/User keyed by
  ap_id with `copies` cross-protocol id list — our `ap_objects` is the
  relational version of `copies`).
