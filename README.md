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

Configuration is environment variables with logged dev defaults — see
`internal/config/config.go`.
