# Deploy to Production

Pull latest `main` on the production server and rebuild/restart only the affected Docker services. Adapted from the Coves `/deploy` command — Tidepool runs on the same box, beside the Coves stack.

## Production Target

- Host: `ssh <PROD_HOST>` (resolve the placeholder from memory before executing — same server as Coves)
- Repo: `/opt/tidepool`
- Compose file: `docker-compose.prod.yml`
- Public domain: `tdpl.io` (TLS terminates in the **Coves** Caddy, `coves-prod-caddy`)

## Service → Source Map

Use this to decide what to rebuild based on the incoming commits. **Never rebuild a service whose code did not change.**

| Service (container)                        | Built from                       | Triggered by changes in                                                            |
| ------------------------------------------ | -------------------------------- | ---------------------------------------------------------------------------------- |
| `tidepool` (`tidepool-prod`)               | root `Dockerfile` → `tidepool:prod` | Go sources (`cmd/**`, `internal/**`), `go.mod`/`go.sum`, `lexicons/**`, `Dockerfile` |
| `tidepool-migrate` (`tidepool-prod-migrate`) | same image as `tidepool`         | Never targeted directly — `up -d tidepool` runs it first via `depends_on`           |
| `postgres` (`tidepool-prod-postgres`)      | External image                   | Never rebuilt from this repo                                                        |
| — (`coves-prod-caddy`)                     | **Coves repo**                   | The `tdpl.io` site blocks live in the Coves `Caddyfile` — deploy those with the Coves `/deploy` (Step 5b there: `--force-recreate caddy` + inode check) |

Special case — **`communities.yaml` only**: no rebuild at all. The follow list is read through a directory bind mount and the reconciler re-reads it every `FOLLOW_LIST_INTERVAL` (15m). `git pull` is the whole deploy; to apply immediately:

```sh
ssh <PROD_HOST> "curl -s -X POST localhost:8091/admin/communities/reconcile -H \"Authorization: Bearer \$(grep ^ADMIN_TOKEN /opt/tidepool/.env | cut -d= -f2)\""
```

## Workflow

### Step 1: Inspect remote state

```sh
ssh <PROD_HOST> "cd /opt/tidepool && git status && git fetch && git log HEAD..origin/main --oneline"
```

- Note any uncommitted local changes and untracked files — leave untracked files alone unless the user asks.
- List the incoming commits; these drive what needs a rebuild.

### Step 2: Handle uncommitted local changes carefully

If `git status` shows modified tracked files, **do not blindly discard them.** Verify with `git diff <file>` on the server against the incoming commits (`git show <sha>`). Identical-in-effect hotfix → safe to `git checkout --`. Anything else → **stop and ask the user.**

### Step 3: Pull

```sh
ssh <PROD_HOST> "cd /opt/tidepool && git pull && git log --oneline -3"
```

Fast-forward only. If the pull fails for any reason other than the pre-handled local mods, stop and ask.

### Step 4: Identify affected services

Cross-reference the pulled commits against the Service → Source Map. If a commit touches `docker-compose.prod.yml` or `.env`-sensitive areas, stop and confirm — a recreate re-reads env and can break things silently.

### Step 5: Rebuild + restart (Go changes)

```sh
ssh <PROD_HOST> "cd /opt/tidepool && docker compose -f docker-compose.prod.yml build tidepool"
ssh <PROD_HOST> "cd /opt/tidepool && docker compose -f docker-compose.prod.yml up -d tidepool"
```

`up -d tidepool` re-runs `tidepool-migrate` first (shared image, `service_completed_successfully` gate) — the server only restarts on a successful migration. Never run bare `docker compose up -d` on prod.

If migrations are in the incoming commits, watch the migrate job:

```sh
ssh <PROD_HOST> "docker logs tidepool-prod-migrate --tail 50"
```

### Step 6: Verify

```sh
ssh <PROD_HOST> "docker ps --format 'table {{.Names}}\t{{.Status}}'"
ssh <PROD_HOST> "curl -sS -o /dev/null -w 'HTTP %{http_code}\n' https://tdpl.io/xrpc/_health"
ssh <PROD_HOST> "docker logs tidepool-prod --tail 30"
```

- Wait for `tidepool-prod` to reach `healthy`.
- After a restart the bridge re-announces itself to `RELAY_HOSTS` (`requestCrawl`) — check the logs for the announcement succeeding.
- Spot-check the shipped feature (admin API via `localhost:8091`, or the public surface via `https://tdpl.io`).

## Guardrails

- **Never** run `docker compose down` on prod. Targeted `up -d <service>` only.
- **Never** `git reset --hard` / `git clean -fd` / delete untracked files on the server without explicit approval.
- `/opt/tidepool/.env` holds `BRIDGE_KEK` — the key sealing every bridged repo's signing keys. Never delete, rotate, or overwrite it casually; losing it bricks every bridged identity.
- The bridge's DIDs live on the public `plc.directory` — treat identity-affecting operations (KEK, `BRIDGE_HOSTNAME`) as irreversible.
- Changing `BRIDGE_HOSTNAME` breaks every minted handle and DID document — don't, without a migration plan.
- Caddy belongs to the Coves stack: `tdpl.io` routing/TLS changes are Coves-repo `Caddyfile` changes, deployed with the Coves `/deploy` (force-recreate + inode verification — single-file bind mount trap).
- Report back: commits pulled, services rebuilt, services intentionally NOT rebuilt, verification result.
