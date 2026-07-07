# Task 03 — Identity minting + virtual repo layer (~1200 LOC)

## Goal
The atproto half of the bridge's core: mint did:plc identities for bridged
actors (users and communities), custody their signing keys, and maintain a
real signed Merkle Search Tree repo per DID using indigo primitives. This is
the Go port of arroba's job.

## Deliverables
- `internal/identity/minter.go` — create did:plc via PLC directory
  (`PLC_DIRECTORY_URL`): generate secp256k1 keypair per actor, build +
  sign the genesis PLC operation (rotation key = bridge escrow key;
  verification key = per-actor key), POST to directory. Handle scheme:
  communities `technology.lemmy-world.<BRIDGE_HOSTNAME>`, users
  `alice.lemmy-world.<BRIDGE_HOSTNAME>` (dots for @/!, dashes for host
  dots; collision-suffix if needed). PDS endpoint in the DID doc =
  `https://BRIDGE_HOSTNAME`. Handle resolution: since bridged handles are
  subdomains of the bridge, serve
  `/.well-known/atproto-did`-style resolution — implement the
  `com.atproto.identity.resolveHandle` XRPC + wildcard-DNS assumption;
  document the DNS requirement in README.
- `internal/identity/keys.go` — key custody: per-actor secp256k1 private
  keys encrypted at rest (AES-GCM with `BRIDGE_KEK` env key), escrow
  rotation key handling. Claiming/migration is out of scope; design the
  storage so it's possible later.
- `internal/repo/repo.go` — per-DID repo built on indigo's `mst`, `repo`,
  and `carstore`/blockstore packages: `PutRecord(did, collection, rkey,
  record) (cid, rev, error)`, `DeleteRecord`, `GetRecord`, each producing a
  properly signed commit (v3, `rev` TID monotonic per repo) persisted in
  postgres (blocks table: did, cid, bytes; repo_state: did, head_cid, rev).
  Serialize writes per DID (per-DID mutex or single writer goroutine).
- `internal/repo/events.go` — every commit also appends to a durable
  `firehose_events` table (seq bigserial, did, commit CID, CAR slice of
  the commit blocks, ops, rev, time) — task 04 serves this. Emitting here
  keeps commit+event atomic in one tx.
- `internal/repo/tid.go` — TID generation: normal clock TIDs for repo
  `rev`; deterministic content TIDs for rkeys (timestamp bits from AP
  `published`, clock-ID bits from hash of ap_id) — exposed for task 05.
- Migrations: `blocks`, `repo_state`, `firehose_events`, plus
  `bridged_actors` alterations if needed.
- Tests: mint against a real PLC container (compose has one in Coves dev —
  add `plc` service to our compose); repo round-trip (put N records,
  export CAR, re-load with indigo, verify MST root + signatures); rkey
  determinism (same AP id + published → same TID; different id → different).

## Definition of done
- Can mint a DID on a local PLC directory, create its repo, write a
  profile record, read it back, and verify the commit signature with the
  minted key.
- Deterministic rkeys are stable across process restarts.
- `go test ./...` green (PLC-dependent tests behind `-short` skip).

## References
- `~/Code/arroba/arroba/` — `repo.py`, `mst.py`, `did.py`, `storage.py`
  (the exact semantics being ported).
- indigo: `repo`, `mst`, `atproto/crypto`, `api/atproto` packages.
- Coves compose PLC service: `~/Code/coves/docker-compose.dev.yml`.
