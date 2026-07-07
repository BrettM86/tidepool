# Tidepool

Tidepool is a read-only ActivityPub → atproto bridge for the threadiverse.
It follows Lemmy/PieFed/Mbin communities (FEP-1b12 group federation),
materializes their posts, comments, and profiles as `social.coves.*` records
in a virtual PDS it operates itself, and serves them over
`com.atproto.sync.*` so the [Coves](https://github.com/coves-social) AppView
indexes fediverse communities exactly as it indexes native ones. Votes stay
bridge-side as aggregates behind one sanctioned XRPC.

See **[PLAN.md](PLAN.md)** for the architecture, locked design decisions,
and the task-by-task build plan (`tasks/`).

## Quick start

```sh
make dev-up      # start the dev postgres (localhost:5442)
make run         # run the bridge (migrations apply on start in dev)
make test        # start the test postgres (localhost:5443) and run the suite
```

Requires Go 1.25+, Docker, and (for `make db-migrate` / `make lint`) the
`goose` and `golangci-lint` CLIs. Store tests need a real postgres: they
skip with a clear message when `TIDEPOOL_TEST_DATABASE_URL` is unset.

The identity-minting tests additionally need a **local** PLC directory
(`make plc-up`, port 3002; the first start clones and builds
[did-method-plc](https://github.com/did-method-plc/did-method-plc), which
takes several minutes). They skip when it is unreachable and under
`go test -short`, and hard-refuse to run against any non-loopback
directory — the test suite can never create DIDs on the public
`plc.directory`. Point `TIDEPOOL_TEST_PLC_URL` elsewhere-on-localhost if
your directory is on a different port.

## Configuration

Environment variables with logged dev defaults (see
`internal/config/config.go`); everything below is **required in
production**:

| Variable | Dev default | Meaning |
|---|---|---|
| `DATABASE_URL` | local dev postgres | bridge state |
| `LISTEN_ADDR` | `:8091` | HTTP bind address |
| `BRIDGE_HOSTNAME` | `localhost` | public domain of the bridge; anchors handles and the PDS endpoint in minted DID docs |
| `PLC_DIRECTORY_URL` | `http://localhost:3002` (local, `make plc-up`) | did:plc directory; production uses `https://plc.directory` |
| `BRIDGE_KEK` | fixed public dev key | 32-byte key-encryption key (64 hex chars or base64) sealing per-actor signing keys and the escrow rotation key at rest (AES-256-GCM) |
| `BRIDGE_SERVICE_DID` | *(optional)* | pre-provisioned service DID for the bridge's own actor |
| `USER_AGENT` | derived | outbound HTTP user agent |
| `ALLOW_PRIVATE_FETCH` | off | dev-only: disables the SSRF egress guard (AP fetches **and** PLC directory requests) so localhost targets work |

## Handle resolution & DNS (wildcard requirement)

Every bridged actor's atproto handle is a subdomain of `BRIDGE_HOSTNAME`:
communities get `technology.lemmy-world.<BRIDGE_HOSTNAME>`, users get
`alice.lemmy-world.<BRIDGE_HOSTNAME>` (dots replace the fediverse `!`/`@`
separators; dots inside the instance hostname become dashes; colliding
handles get a `-2`, `-3`, … suffix).

For those handles to resolve, the operator **must configure wildcard DNS**:
`*.<BRIDGE_HOSTNAME>` → the bridge (a DNS wildcard matches multiple label
levels, so one record covers `*.lemmy-world.<BRIDGE_HOSTNAME>` and every
other bridged instance). The bridge then answers both resolution paths:

- `GET /xrpc/com.atproto.identity.resolveHandle?handle=…` — the XRPC query;
- `GET https://<handle>/.well-known/atproto-did` — the HTTPS well-known
  method; the wildcard DNS routes every bridged subdomain to the bridge,
  which answers from the `Host` header.

Note TLS: a single wildcard certificate only covers one label level, while
bridged handles sit two levels below `BRIDGE_HOSTNAME` — terminate TLS with
on-demand certificate issuance (e.g. Caddy) or per-instance wildcard certs.
