# Deploying Tidepool v2

The production runbook for outbound federation: native Coves users posting,
commenting and voting into bridged Lemmy communities. It runs on top of the
inbound bridge. The outbound path is fully built (Jetstream consumer, delivery
workers, acceptance engine, moderation, erasure) and is gated by
`CONSUMER_ENABLED` and `OUTBOUND_WORKERS`.

Communities created natively on Coves do not federate yet. That is planned
work ("Scope B"); nothing in this runbook turns it on.

This document describes only what is in the code. Where a lever does not
exist, [Not implemented](#6-not-implemented) says so. Claims cite the file and
symbol they come from; if you change the code, change the citation.

Related: [`SELF_HOSTED_RELAY.md`](SELF_HOSTED_RELAY.md) (relay and Jetstream
ingest), [`docs/CONFIGURATION.md`](docs/CONFIGURATION.md) (every config
variable), [`docs/ADMIN_API.md`](docs/ADMIN_API.md) (every admin route).

---

## 1. Read this before deploying HEAD

A HEAD deploy onto a pre-v2 `/opt/tidepool/.env` does not start. It exits in
`config.Load` with:

```
config: AP_USER_ORIGIN is required in production
```

`AP_USER_ORIGIN` is read through `stringVar`, which uses a dev default in
development and returns an error in production (`internal/config/config.go`).
The pre-v2 env file has no such variable.

Do not fix this by adding a default. The origin is baked into every
`actor_id` this deployment mints, and serving derives every URL from the
stored `actor_id`, never from config (`internal/personas/serving.go`). A wrong
value cannot be repaired by restarting: it leaves federated identities
pointing at the wrong place permanently. Boot also refuses an origin that:

- is not `https` in production;
- has a host equal to `BRIDGE_HOSTNAME` or a subdomain of it, which would
  shadow the bridged-handle namespace;
- has a path, query, fragment or userinfo (`personas.CanonicalizeOrigin`,
  `internal/personas/origin.go`). `https://coves.social/` has the path `/`
  and fails boot. Write it with no trailing slash.

`docker-compose.prod.yml` interpolates `${AP_USER_ORIGIN:?…}`, so an unset or
empty value fails at `docker compose` time, before any container starts. The
compose file has no default on purpose: a fork of it with a default would mint
permanent identities under someone else's domain.

### Minimum `.env` delta

Every other v2 variable has a safe default. See `.env.prod.example` for the
full annotated set.

```sh
# required; the rest of the v2 knobs default safely
AP_USER_ORIGIN=https://coves.social
```

### Deploy command

Always name the service. Never run a bare `up -d`:

```sh
cd /opt/tidepool && git pull
docker compose -f docker-compose.prod.yml up -d --build tidepool
```

The migrate one-shot and the server share one locally built image. The server
starts only after the migration reports `service_completed_successfully`, so a
failed migration leaves the old container running instead of starting a new one
against an unmigrated schema. The production server never migrates on start;
outside compose, run `tidepool migrate` before starting it.

Confirm the boot got past config:

```sh
docker logs --tail 50 tidepool-prod | grep -E 'listening|config:'
curl -sf localhost:8091/xrpc/_health
```

---

## 2. The v2 flag topology

All of v2 is off by default and turns on in this order. Config is read once,
in `run()` (`cmd/tidepool/main.go`), and there is no reload signal. Changing
any value means editing `.env` and recreating the service.

```
CONSUMER_ENABLED=false                    ← default: nothing v2 runs.
        │
        │  true: the Jetstream consumer dials JETSTREAM_URL (required when on).
        │      The enqueuer persists outbound intent; the acceptance engine
        │      admits postv2 and writes community-signed acceptances; opt-out
        │      deleteRemote and confirmed account deletions purge at peers.
        │      /admin/admissions* and /admin/actors/purge start working.
        ▼
OUTBOUND_WORKERS=0                        ← intent accumulates, nothing is POSTed.
        │
        │  >0: delivery workers drain the queue onto the wire.
        ▼
OUTBOUND_DISABLED / _ACTORS / OUTBOUND_DRY_RUN
                                          ← per-scope parking, checked per delivery.
```

Operators most often get these points wrong:

**There is no `OUTBOUND_ENABLED`.** Delivery runs only when
`OUTBOUND_WORKERS > 0` and `CONSUMER_ENABLED=true`. `outbound.NewWorker` is
called only inside `if cfg.OutboundWorkers > 0` in `startConsumer`, and
`startConsumer` runs only under `if cfg.ConsumerEnabled` (`cmd/tidepool/main.go`).
`OUTBOUND_WORKERS` defaults to 0 and is read with `intVarNonNegative`, so 0 is
a legal value.

**Consumer on with workers at 0 is a staging step.** With the consumer
running, the enqueuer writes `outbound_activities` and `outbound_deliveries`
inside the consumer's gate transaction, so no intent past the gate is dropped.
Raising `OUTBOUND_WORKERS` later drains everything that accumulated, at once.
Plan for that burst.

**A kill switch parks deliveries and costs no retries.** A blocked delivery
stays `pending` and resumes when the switch clears. `park` never poisons and
never cancels (`internal/outbound/worker.go`, `internal/outbound/switches.go`).
`OUTBOUND_DRY_RUN` parks the same way, before any signing or POST.

Why it costs no retries: `ClaimNext` increments `attempts` on every claim
(`internal/store/outbound_deliveries.go`). `park` and `parkCausal` settle
through `ReleaseParked`, which differs from `Release` only by setting
`attempts = GREATEST(attempts - 1, 0)` under the same claim fence. A
claim-and-park cycle leaves `attempts` unchanged. An hour-long global kill
switch spends nothing, and the first failure after it clears is a retry, not a
poison. `DefaultMaxDeliveryAttempts = 8` counts only attempts a delivery
actually made.

⚠️ **A held switch is a continuous write load.** The worker re-claims and
re-parks each parked ordering-key head every `parkDelay` (5 seconds). That is
one claim plus one `UPDATE` per parked head every 5 seconds, for as long as the
switch is held. This is why [Rollback](#rollback) prefers `OUTBOUND_WORKERS=0`
for stopping everything. The reason is cost, not safety.

`POST /admin/outbound/redrive` repairs poisoned deliveries, not parked ones.
`RedrivePoisoned` matches only `state = 'poisoned'` and resets those rows to
`pending` with `attempts = 0`. A parked delivery is already `pending`, so a
redrive does not reach it and it does not need one.

The scoped switches match as below. `OUTBOUND_DISABLED_ACTORS` is read from
the environment (`internal/config/config.go`, `parseSet`). The host and
community sets are read from `communities.yaml` (`ingest.ParseFollowList`) at
boot and at each sweep. A file that fails to parse after a good boot keeps the
last good sets.

| Switch | Matching |
|---|---|
| `outbound_disabled_hosts` in `communities.yaml` | inbox host, lowercased on load and compared case-insensitively, so case mistakes still block |
| `outbound_disabled_communities` in `communities.yaml` | community ActivityPub id, such as `https://lemmy.world/c/comicstrips`. Exact and case-sensitive, compared against the delivery's ordering key |
| `OUTBOUND_DISABLED_ACTORS` | actor DID, exact and case-sensitive |

None of these has an allowlist form. See [Staged rollout](#5-staged-rollout)
for how that shapes a canary.

**Every switch, including `OUTBOUND_DISABLED` and `OUTBOUND_DRY_RUN`, does
nothing while `OUTBOUND_WORKERS=0`.** `run()` builds `ConfigSwitches` through
`newOutboundSwitches` whenever the bridge starts, before the consumer, but only
the delivery worker reads a switch, so with no worker no switch does anything.
Setting `OUTBOUND_DISABLED=true` with workers at 0 adds no protection. It is
also why `OUTBOUND_WORKERS=0` costs nothing: no worker exists to claim, park or
write. With `FOLLOW_LIST_PATH` set, `newOutboundSwitches` parses
`communities.yaml` at boot, and a file that fails to parse stops boot, even
with workers at 0. The host and community sets come from that file and reload
at each sweep. A file that breaks after a good boot keeps the last good sets.

### Confirming a switch engaged

`internal/outbound/metrics.go` publishes four counters on `/admin/metrics`:

| Metric | Increments when |
|---|---|
| `tidepool_outbound_delivered` | a delivery reached the peer, including Lemmy's duplicate acknowledgement |
| `tidepool_outbound_poisoned` | a delivery hit a terminal failure |
| `tidepool_outbound_cancelled` | the worker cancelled a claimed delivery because of consent state (its only increment site) |
| `tidepool_outbound_parked` | a kill switch, dry run, or causal wait deferred a delivery |

`tidepool_outbound_parked` is the only positive sign that a kill switch
engaged. `by_state` reads `pending` for a parked delivery and for one waiting
its turn, so the queue view cannot tell them apart. Read the counter as a rate.
It climbs steadily while a switch is held because each head is re-parked every
5 seconds. That climb is write load, not retries.

Two caveats:

- `parkCausal` also increments `parked`. A nonzero `parked` with no switch set
  means causal waits.
- `cancelled` rises on its own during rollout. The worker cancels a claimed
  delivery when the actor is disabled, delivery-paused or opted out
  (`consentBlocked` in `internal/outbound/worker.go`). `POST
  /admin/outbound/cancel` does not touch this counter; its effect shows only in
  `by_state`.

---

## 3. The admin surface at incident time

The full route table with request bodies is in
[docs/ADMIN_API.md](docs/ADMIN_API.md). Every route requires
`Authorization: Bearer $ADMIN_TOKEN` (`Admin.Routes` in
`internal/ingest/follow.go`, `Admin.Routes` in `internal/accept/admin.go`).
Production publishes `127.0.0.1:8091` on the host, so run admin calls there.
The Coves Caddy answers 404 for `/admin*` on every public Tidepool hostname
(`tidepool_no_public_admin` in the Coves `Caddyfile`).

Responses that look like faults but are not:

**`POST /admin/communities/reconcile` returns 501 when `FOLLOW_LIST_PATH` is
unset.** The follow reconciler exists only when the path is configured.
Production sets `FOLLOW_LIST_PATH=/repo/communities.yaml`, so a 501 there means
the environment changed.

**`/admin/admissions` and `/admin/admissions/readmit` return 404 when the
consumer is off.** They are registered inside the `if cfg.ConsumerEnabled`
block because a forced re-admit needs the acceptance engine. Every other
`/admin` route is registered regardless.

**`POST /admin/actors/purge` returns 501 when the consumer is off.** The
purger is created by `startConsumer`.

**`POST /admin/outbound/redrive` refuses an unscoped redrive.** With no
`activity`, no `community` and no explicit `{"all":true}`, it returns 400, so a
malformed body cannot replay every poisoned delivery at once.
`POST /admin/outbound/cancel` requires exactly one of `actor` or `community`.

**`GET /admin/outbound` answers with the consumer off.** The deliveries store
is wired unconditionally, so an empty `by_state` map is the real queue state.

**`GET /admin/outbound` returns only `by_state` counts.** It returns no rows,
ids or reasons (`handleOutboundInspect`). No admin route exposes
`last_error_class` or `response_excerpt`, so diagnosing a poisoned delivery
means psql on the host. The query is in [Step 3](#step-3-widen).

```sh
T="Authorization: Bearer $ADMIN_TOKEN"
curl -s -H "$T" localhost:8091/admin/outbound            # queue depth by state
curl -s -H "$T" localhost:8091/admin/divergence          # one sweep, synchronously
curl -s -H "$T" localhost:8091/admin/metrics             # tidepool_* expvars
curl -s -H "$T" 'localhost:8091/admin/admissions?status=rejected'
```

### Purging a native actor (spammer)

`POST /admin/actors/purge` (`internal/ingest/admin_actor_purge.go`):

- sends `Delete{Person, removeData:true}` for the Coves user's persona to every
  inbox that received its content;
- undoes its standing votes;
- tombstones the actor. The actor document answers 410 from then on, and the
  actor can never be re-enabled.

⚠️ **This is irreversible.** Peers that honour the Delete drop the content and
cannot restore it. A disabled or paused actor still gets the Delete delivered.

The body is `{"did":"did:...","dryRun":true|false}`; `dryRun` is required.
The route answers 404 if no native actor has that DID, 422 for a community
(`group`) actor, and 501 when the consumer is off. Removing the content on the
Coves side is a separate step (`social.coves.moderation.removeContent`).

Use `scripts/purge-actor.sh` on the host. It is in `/opt/tidepool/scripts`
once you have pulled the deploy that added it; otherwise `scp` it to the host
and `ssh` in. It reads `ADMIN_TOKEN` from `/opt/tidepool/.env` (override with
`COMPOSE_DIR`) and never echoes it or puts it in argv. Run the dry run first,
check `inboxes` and `standingVotes` in the answer, then execute:

```sh
/opt/tidepool/scripts/purge-actor.sh did:plc:...            # dry run: changes nothing
/opt/tidepool/scripts/purge-actor.sh did:plc:... --execute  # the purge
```

A real run pauses and disables the actor before it reads the purge targets.
Content admitted while the purge prepares is therefore not delivered after the
Delete; retractions still go out. The `inboxes` and `standingVotes` counts in a
real run's answer are that pre-purge snapshot. The response deadline is
extended to 10 minutes for large purges.

The log line WARN `purge committed with no federation preference to mark` is
expected on an operator purge, because the user never wrote a deleteRemote
preference.

A failed or interrupted run is safe to repeat; the purge is idempotent. Run
one purge per actor at a time: two simultaneous runs can each send an Undo for
the same standing vote. A sequential rerun is safe.

---

## 4. coves.social Caddy (a Coves-repo change)

This repository has no Caddyfile. TLS for `tdpl.io` and `coves.social`
terminates in the Coves Caddy (`coves-prod-caddy`), which reaches this stack
over the shared external `coves-prod-network`. The routes below live in the
Coves repo's `Caddyfile` and deploy from the Coves side.

Production Caddy serves these routes today, and production runs with
`CONSUMER_ENABLED=true`. The steps in this section and in section 5 are for a
fresh deploy or a re-rollout. After a Caddy change, check that the running
Caddy has the routes with the `grep` in the inode section below.

### Why Caddy routes these paths

`AP_USER_ORIGIN=https://coves.social` puts native users' ActivityPub surface
on the Coves hostname, served by Tidepool. Tidepool's Host router sends
`Host: coves.social` to the persona surface and `tdpl.io` to the bridge, and
answers 421 for an unrecognized Host (`internal/personas/hostrouter.go`).
Caddy must send the ActivityPub paths on `coves.social` to Tidepool. Without
them, `GET /.well-known/webfinger` returns a 404 from the static
`/.well-known/*` file server, and `/nodeinfo/2.0` and `/ap/*` go to the web
stack.

The apex needs an `Accept`-based split, and only the edge can do it. Tidepool
does no content negotiation (`handleInstanceActor` in
`internal/personas/instance.go`): a peer sending `ld+json`, `activity+json` or
no `Accept` header must get the instance actor. Peers fetch `GET /` to find
the instance actor; browsers fetch `GET /` to get the web app.

### ⚠️ The Caddyfile inode trap

**Edit the Caddyfile in place. Never replace the file.** The production Caddy
bind-mounts the Caddyfile as a single file, and Docker pins a single-file mount
to the inode present when the container started. Anything that replaces the
file (`git checkout`, `mv`, `sed -i`, most editors' atomic save, `scp` of a new
file) leaves the container serving the old contents while `cat` on the host
shows the new ones. This has happened on this project before, and it is why
Tidepool's own `communities.yaml` is a directory mount
(`docker-compose.prod.yml`).

Safe edits: `nano`, `vim` with `backupcopy=yes`, `cat > file`, `tee`. After
any edit, confirm the container sees it, then validate and reload:

```sh
docker exec coves-prod-caddy cat /etc/caddy/Caddyfile | grep -c '/ap/'
docker exec coves-prod-caddy caddy validate --config /etc/caddy/Caddyfile \
  --adapter caddyfile
docker exec coves-prod-caddy caddy reload --config /etc/caddy/Caddyfile \
  --adapter caddyfile
```

If the container's copy does not show your change, the inode moved. Recreate
the Caddy container from `/opt/coves`:
`docker compose -f docker-compose.prod.yml up -d --force-recreate caddy`.

### The routes

All of these sit inside the one existing `coves.social { … }` site block. Do
not add a second block for the same hostname. `handle` blocks with a single
path are sorted by specificity, so `/.well-known/webfinger` wins over the
`/.well-known/*` static block.

```caddyfile
coves.social {
    # No `header_up Host` on these proxies. Caddy forwards the original Host by
    # default, and Tidepool's Host router needs it. Rewriting Host to the
    # upstream address makes every one of these requests return 421.
    handle /.well-known/webfinger {
        reverse_proxy tidepool:80 {
            header_up X-Real-IP {remote_host}
        }
    }
    handle /.well-known/nodeinfo {
        reverse_proxy tidepool:80 {
            header_up X-Real-IP {remote_host}
        }
    }
    handle /nodeinfo/2.0 {
        reverse_proxy tidepool:80 {
            header_up X-Real-IP {remote_host}
        }
    }
    # /ap/actor/{did}, /ap/actor/{did}/outbox, /ap/object/*, /ap/activity/*,
    # and POST /ap/inbox (the shared inbox for this origin).
    handle /ap/* {
        reverse_proxy tidepool:80 {
            header_up X-Real-IP {remote_host}
        }
    }

    # Apex. `handle /` matches only the bare "/". Requests that explicitly
    # accept text/html go to the web app; everything else, including a request
    # with no Accept header, goes to Tidepool's instance actor. A positive
    # match on the ActivityPub types cannot catch the no-Accept case.
    handle / {
        @html header Accept *text/html*
        # Same upstream options as the frontend catch-all in the block.
        reverse_proxy @html coves-prod-frontend:3000 {
            header_up X-Real-IP {remote_host}
            header_up X-Forwarded-For {remote_host}
            header_up X-Forwarded-Proto {scheme}
            header_up X-Forwarded-Host {host}
        }
        reverse_proxy tidepool:80 {
            header_up X-Real-IP {remote_host}
        }
    }

    # … /.well-known/* static, /img/* redirect, @appview allowlist,
    #   frontend catch-all, headers, CSP, encode …
}
```

One visible consequence: a bare `curl https://coves.social/` (which sends
`Accept: */*`) gets the instance actor JSON. Browsers always send `text/html`
and are unaffected.

### Verify from outside

```sh
# instance actor (Application), not the web app
curl -s -H 'Accept: application/activity+json' https://coves.social/ | jq '.type,.id'
# expect: "Application"  "https://coves.social/"

# the ActivityStreams ld+json spelling must reach the same document
curl -s -H 'Accept: application/ld+json; profile="https://www.w3.org/ns/activitystreams"' \
  https://coves.social/ | jq '.type'

# a browser must still get the web app
curl -sI -H 'Accept: text/html' https://coves.social/ | head -1

# a peer sending no Accept header must still get the instance actor
curl -s -H 'Accept:' https://coves.social/ | jq '.type'
# expect: "Application"

curl -s 'https://coves.social/.well-known/webfinger?resource=acct:alice@coves.social' | jq .
curl -s https://coves.social/.well-known/nodeinfo | jq .
curl -s https://coves.social/nodeinfo/2.0 | jq '.software.name'   # "coves"
```

A 421 from any of these means something rewrote the `Host` header. A 404 on
webfinger for a user who has never federated is correct: actors are minted on
their first federating interaction.

### Unchanged by v2

`tdpl.io`, the per-instance wildcard blocks and the on-demand catch-all. The
`on_demand_tls ask` gate still points at
`http://tidepool:80/.well-known/tidepool-tls-ask`. That URL names the
container, so the request arrives with `Host: tidepool`, which the host router
recognizes as neither surface. The gate works only because the router serves
`identity.TLSAskPath` regardless of Host (`HostAgnosticPaths` in
`internal/personas/hostrouter.go`). If that exemption is removed, the gate
returns 421 and Caddy denies every new handle certificate and renewal.

---

## 5. Staged rollout

Each step is a separate `.env` edit plus
`docker compose -f docker-compose.prod.yml up -d tidepool`, because config is
read once at boot.

### Step 0: Caddy first

Do [section 4](#4-covessocial-caddy-a-coves-repo-change) before enabling the
consumer. When a native actor federates, Lemmy fetches its webfinger, its actor
document and the origin apex, all on `coves.social`. If Caddy does not yet
route those to Tidepool, Lemmy gets the web app or a 404 and caches the
failure.

### Step 1: consumer on, delivery off

```
CONSUMER_ENABLED=true
OUTBOUND_WORKERS=0
```

Nothing reaches any peer. Watch for a day:

```sh
curl -s -H "$T" localhost:8091/admin/metrics | jq '{
  cursor_age: .tidepool_consumer_cursor_age_seconds,
  last_event_age: .tidepool_consumer_last_event_age_seconds,
  connected: .tidepool_consumer_connected,
  dead_letters: .tidepool_consumer_dead_letters,
  reconnects: .tidepool_consumer_reconnects
}'
curl -s -H "$T" localhost:8091/admin/outbound          # intent accumulating
curl -s -H "$T" 'localhost:8091/admin/admissions?status=rejected' | jq
```

`tidepool_consumer_cursor_age_seconds` and
`tidepool_consumer_last_event_age_seconds` show a stalled consumer. When the
consumer stalls, the process stays up and the healthcheck stays green while
events stop arriving. A climbing `tidepool_consumer_dead_letters` means events
are failing and waiting for the dead-letter redriver; they are not lost.

How to read these keys:

- The `tidepool_consumer_*` gauges exist only while the consumer runs.
  `consume.PublishMetrics` is called inside `startConsumer`. Missing gauges
  mean the consumer is off. The exception is
  `tidepool_consumer_unclaimed_skips`, which is registered at startup and is
  always present.
- `tidepool_consumer_dead_letters: -1` means the dead-letter table could not
  be read (`deadLetterDepthUnavailable` in `internal/consume/metrics.go`). It is
  not a count.
- The `tidepool_divergence_*` gauges are registered at package init, so they
  are always present. They read `-1` (`divergenceUnswept` in
  `internal/ingest/divergence.go`) until the startup sweep completes, and for
  any class a failed sweep never wrote. `-1` means never measured; `0` means
  measured and nothing diverging. Do not alert on both as healthy.
- The four `tidepool_outbound_*` counters are registered at package init and
  stay at 0 while `OUTBOUND_WORKERS=0`. All four at 0 is correct at this step.

Check the rejections before letting anything out. A misconfigured
`ADMISSION_MAX_PER_AUTHOR_PER_COMMUNITY` (default 50), a stale community
mapping, or an unexpected consent state all show up here as `decisionCode`s
while nothing has reached a peer.

Translation happens at enqueue time, inside the consumer's gate transaction,
and the worker POSTs the stored payload unchanged. This step is where you check
translator output: read `outbound_activities.payload` in psql.

### Step 2: one community

There is no allowlist, so a one-community canary is a denylist of every other
community the bridge follows. Use the AP ids, exact case, not the `!name@host`
form `communities.yaml` uses. The `community` field of the list response is
the AP group id. Build the list from it rather than typing ids by hand:

```sh
curl -s -H "$T" localhost:8091/admin/communities | jq -r '.communities[].community'

# every community except the canary, as YAML list items for
# outbound_disabled_communities in communities.yaml:
CANARY=https://lemmy.world/c/comicstrips
curl -s -H "$T" localhost:8091/admin/communities \
  | jq -r --arg c "$CANARY" '[.communities[].community | select(. != $c)] | map("  - \"" + . + "\"") | .[]'
```

Paste the output under `outbound_disabled_communities:` in `communities.yaml`,
then set this in `.env`:

```
OUTBOUND_WORKERS=1
```

⚠️ **Adding a community to `communities.yaml` later does not add it to this
denylist.** The reconciler subscribes it and it starts federating on the next
sweep. Whenever the follow list grows during a canary, extend
`outbound_disabled_communities` in the same commit. An `instances` entry added
during a canary follows every community on that host, and none of them is in
this list.

**`OUTBOUND_DRY_RUN=true` does not test the translator.** The worker claims a
delivery, logs it, and parks before signing or POSTing. The payload was already
translated and stored at enqueue time, so the translator has run on everything
since `CONSUMER_ENABLED=true`, dry run or not. Check translator output in
Step 1. Dry run also carries the write load of any park (see
[section 2](#2-the-v2-flag-topology)), costs no retries, and needs no redrive
afterwards.

### Announcement throttling

`PLC_WRITES_PER_MINUTE` paces PLC writes for inbound mints and the rename job
(default 5 a minute). A PLC 429 pauses all of them for Retry-After or an hour.
It does not limit outbound delivery.

`internal/outbound` has no rate limiter, sleep or throttle. The only levers on
outbound volume are:

- `OUTBOUND_WORKERS`, which sets worker concurrency, not a rate. Idle workers
  poll every second (`outboundWorkerIdle`); with a full queue they run flat out.
- The scoped kill switches, which decide which communities, actors and hosts
  may deliver at all.

`OUTBOUND_WORKERS=1` plus a one-community scope is the throttle.

### Step 3: widen

Remove entries from `outbound_disabled_communities` one at a time, and raise
`OUTBOUND_WORKERS` as the queue requires. Before each widening:

```sh
# poison depth
curl -s -H "$T" localhost:8091/admin/outbound | jq .by_state

# echo suppression: bridge-origin content must never re-materialize
curl -s -H "$T" localhost:8091/admin/metrics | jq '{
  mapped_object: .tidepool_echo_drops_mapped_object,
  local_activity: .tidepool_echo_drops_local_activity,
  local_actor: .tidepool_echo_drops_local_actor,
  ancestor: .tidepool_echo_drops_ancestor_short_circuit
}'

# the reconciliation report
curl -s -H "$T" localhost:8091/admin/divergence | jq '{counts, truncated}'
```

What to look for:

- **`by_state.poisoned` climbing**: deliveries are exhausting their retries.
  Find the cause, fix it, then `redrive` scoped to the affected community.
  The reason is only in the database:

  ```sh
  docker exec -i tidepool-prod-postgres psql -U tidepool -d tidepool -c "
    SELECT ordering_key, last_error_class, last_status_code, attempts,
           left(response_excerpt, 200) AS excerpt, updated_at
      FROM outbound_deliveries
     WHERE state = 'poisoned'
     ORDER BY updated_at DESC LIMIT 50;"
  ```

  `attempts` counts claims that were tried, since a park returns its claim's
  increment. A poisoned row should read about 8. A higher number means claims
  that never settled (a lapsed lease or a crashed worker), not extra peer
  rejections.

  **`last_error_class = 'kek_misconfigured'` with `attempts = 1` is a
  configuration incident.** The actor's sealed signing key did not open under
  the configured `BRIDGE_KEK`, so the worker poisons on the first attempt
  (`store.PoisonClassKEKMisconfigured`). Retrying cannot help. Expect it in
  bulk, and expect actors minted after the wrong key was configured to work,
  because their keys are sealed under it. Fix `BRIDGE_KEK` (see the
  [rotation runbook](#runbook-rotating-bridge_kek)), restart, then `redrive`.
- **Echo drop counters rising steadily**: healthy. This is our own content
  coming back from Lemmy and being refused. A counter at 0 while native content
  is flowing is the alarm: classification is not firing and re-materialization
  is possible.
- **`tidepool_divergence_*` gauges**: the read-only reconciliation sweep.
  Watch `acceptance_undelivered_stale` (a delivery pending longer than
  `DIVERGENCE_ACCEPTANCE_STALE_AFTER`, default 12h),
  `acceptance_undelivered_poisoned` and `vote_recast_undelivered`. Also watch
  `tidepool_divergence_sweep_failures` and
  `tidepool_divergence_sweep_age_seconds`. A failed sweep publishes nothing and
  the gauges keep their previous values, so a growing sweep age with quiet
  gauges is a silent failure. The sweep only reports; it never repairs.

### Rollback

Roll back with kill switches, not by un-deploying. The steps are in escalation
order. Step 1 is a `communities.yaml` edit plus
`POST /admin/communities/reconcile`, with no restart. Each step from 2 on is
one `.env` edit plus `up -d tidepool`:

1. **Add `"<the bad one>"` to `outbound_disabled_communities`** in
   `communities.yaml`, keeping any canary entries already there: the key is
   replaced whole, so dropping them starts delivery to every community they
   held back. Use the exact AP id from `GET /admin/communities`
   (`.communities[].community`); a well-formed id that differs (a trailing
   slash, other case) is accepted and never matches. This parks one community.
   The switch applies when the reconcile starts, even if the response takes
   minutes. A 500 starting `reconcile failed: follow list` means the file did
   not parse and the switch did not change: fix the file (or restore the last
   good copy, keeping the canary entries and the new block) before any
   `up -d tidepool`, because a file
   that does not parse also stops the next boot. Confirm with
   `last_error_class = 'switch_parked'` on that community's
   `outbound_deliveries` rows (`ordering_key` is the community AP id) and a
   rising `tidepool_outbound_parked` (see
   [Confirming a switch engaged](#confirming-a-switch-engaged)).
   Everything else keeps flowing. The parked deliveries keep their retry budget
   and resume when you clear the entry, with no redrive needed. Use this first
   because it is the narrowest. Its only cost is the 5-second re-claim and
   re-park cycle on that community's head delivery.
2. **`OUTBOUND_WORKERS=0`** stops the delivery workers. This is the main stop
   button. The consumer keeps running and recording intent; nothing reaches any
   peer. With no worker, nothing is claimed, no `attempts` are spent and no rows
   are written. It costs nothing and loses nothing.
3. **`OUTBOUND_DISABLED=true`** parks everything outbound. Nothing reaches any
   peer, and deliveries keep their retries, so it is safe to hold. It ranks
   below (2) on cost, not risk: the workers keep running, so every ordering
   key's head is re-claimed and re-parked every 5 seconds, against a database
   you may want quiet during an incident. Its advantage over (2) is that each
   parked row records a `last_error_class` and `response_excerpt` saying why it
   is held; a stopped worker records nothing.
4. **`CONSUMER_ENABLED=false`** stops consuming, and intent stops being
   recorded. The consumer resumes from its stored cursor when re-enabled, so
   this is recoverable. It also removes `/admin/admissions`, your triage view,
   when you most need it. Prefer steps 1 to 3.

Do not roll back by cancelling deliveries. `POST /admin/outbound/cancel` is
for consent withdrawal and community removal, and a cancelled delivery cannot
be resumed the way a parked one can.

Redeploy the previous image only if the failure is a code defect. For a
federation problem the kill switches act faster and raise no schema-version
question.

### Stopping an instance onboarding

An `instances` entry in `communities.yaml` follows every local community on its
host, at most 20 new ones a sweep. There are two ways to stop it:

- **Remove the host's `instances` entry** and run
  `POST /admin/communities/reconcile`, or wait for the next sweep. Every
  community on that host that `communities` does not name is unfollowed.
  What is already bridged is kept.
- **Lower `PLC_WRITES_PER_MINUTE`** in `.env` and run `up -d tidepool`. The host
  stays followed, and new communities arrive at a slower pace. The minimum is
  1; 0 fails boot. The budget is shared, so this slows every PLC write, not
  only this host's onboarding. A restart ends any active PLC 429 pause; during
  a 429, use the first way instead.

---

## 6. Not implemented

Everything here is an operational gap, except two entries that are now built
and kept as runbooks: `BRIDGE_KEK` rotation and backup and restore. Their
headings say BUILT.

### Key rotation: `BRIDGE_KEK` (BUILT) and per-actor RSA keys (not built)

`BRIDGE_KEK` rotation exists: a dual-read custodian plus a `rotate-kek`
re-seal walk (`identity.Reseal` in `internal/identity/reseal.go`). Per-actor
RSA key rotation and rotation of the plaintext service-actor key do not exist;
see the end of this entry.

The KEK seals three columns, and the walk covers all three:

1. **`bridged_actors.signing_key`**: the escrowed secp256k1 atproto repo
   signing key of every bridged (Lemmy-origin) identity
   (`internal/db/migrations/002_create_bridged_actors.sql`;
   `Custodian.EncryptActorKey` and `DecryptActorKey` in
   `internal/identity/keys.go`). About 950 rows.
2. **`service_keys.key_material`, row `plc-rotation`**: the PLC escrow
   rotation key (`RotationKeyName`, `LoadOrCreateRotationKey` in
   `internal/identity/keys.go`). It is the one key that can recover the DIDs,
   and the boot canary in step 2 of the runbook opens it. The sibling
   `service-actor` row is plaintext PKCS#8 PEM and is not KEK-sealed
   (`internal/db/migrations/013_rename_service_key_column.sql`).
3. **`ap_actors.rsa_key_sealed`**: every native Coves user's ActivityPub RSA
   signing key, sealed under its own AAD prefix
   (`internal/db/migrations/017_ap_actors.sql`; `actorRSAKeyAADPrefix` in
   `internal/identity/keys.go`; `EncryptActorRSAKey` in
   `internal/identity/actor_rsa.go`). Written at mint time through the same
   `identity.Custodian` that `run()` hands to `personas.New`.

Any hand-rolled rotation must also cover (3). Skipping it leaves every native
user unable to sign an outbound activity.

`LoadOrCreateRotationKey` is not KEK rotation. It loads or generates the
did:plc escrow key, which is sealed under the KEK. KEK rotation lives in
`internal/identity/reseal.go`.

#### Why there is no key-version column

The design needs no schema change and no stored key selector. AES-GCM
authentication tells you which key sealed a blob: opening under the wrong key
fails the tag check (a false match has odds of about 2⁻¹²⁸). So the code tries
the keys. A stored column could be wrong (written by a crashed half-rotation,
restored from a mismatched backup); the ciphertext cannot.
`NewCustodianWithPrevious` (`internal/identity/keys.go`) tries both keys on
read. `identity.Reseal` classifies each blob the same way:

- opens under current: already moved, left alone;
- opens under previous: re-sealed and written back;
- opens under neither: reported and never written.

`ap_actors.rsa_key_version` is not a KEK selector. It versions the actor's RSA
key, a different rotation that is not built. The one-byte prefix on every
ciphertext is the envelope format version, not a key id.

#### Runbook: rotating `BRIDGE_KEK`

Keep the old key everywhere it is stored until step 6.

1. **Put both keys in `/opt/tidepool/.env`**: the new one as current, the old
   one as previous. Generate the new one with `openssl rand -hex 32`.

   ```sh
   BRIDGE_KEK=<new key>
   BRIDGE_KEK_PREVIOUS=<the key that is in there right now>
   ```

   `docker-compose.prod.yml` forwards both to the `tidepool` service.
   `BRIDGE_KEK_PREVIOUS` is empty in steady state, and empty means unset.

2. **Restart the bridge.**

   ```sh
   docker compose -f docker-compose.prod.yml up -d tidepool
   ```

   It must come up clean. The boot canary opens the `plc-rotation` key before
   serving traffic, and at this point that key opens only under the previous
   KEK. A clean boot proves the dual read works. A failed boot means one of the
   two values is wrong; fix it now, before anything has been rewritten.

3. **Run the walk.**

   ```sh
   docker compose -f docker-compose.prod.yml run --rm tidepool rotate-kek
   ```

   Do not use the `tidepool-migrate` service. It carries only `DATABASE_URL`,
   so `rotate-kek` there fails for lack of a KEK.

4. **Read the inventory.** One line per table:

   ```
   level=INFO msg="kek re-seal table" table=bridged_actors resealed=948 already_current=0 skipped=3 failed=0
   level=INFO msg="kek re-seal table" table=ap_actors resealed=112 already_current=0 skipped=0 failed=0
   level=INFO msg="kek re-seal table" table=service_keys resealed=1 already_current=0 skipped=0 failed=0
   ```

   Each row that could not be moved gets an ERROR line naming the table, the
   row and the reason:

   ```
   level=ERROR msg="kek re-seal failure" table=ap_actors id=did:plc:7iza… reason=malformed
   ```

   `skipped` is usually a bridged actor with no escrowed key
   (`signing_key IS NULL`), which is normal. On any table, a row deleted
   between the walk's read and its write-back is also skipped. So a nonzero
   `skipped` on `ap_actors` or `service_keys` means a row disappeared
   mid-walk (a deleted actor), not a NULL key. The plaintext `service-actor`
   row is never read or counted.

5. **Re-run until a zero run**: every table reports `resealed=0 failed=0` and
   the command exits 0. Only the zero run proves nothing is still sealed under
   the old key. The first run's counts do not, because rows written during it
   are not in them.

6. **Only then remove `BRIDGE_KEK_PREVIOUS`** from `/opt/tidepool/.env` and
   restart:

   ```sh
   docker compose -f docker-compose.prod.yml up -d tidepool
   ```

   The boot canary now runs on the new key alone. If it fails, put the old key
   back immediately and go to step 3. The data is still intact, because the
   walk never overwrites what it cannot open.

**Warnings**

- **Do not start a second rotation before the first has had its zero run.** The
  tool moves blobs from one previous key to one current key. If you introduce a
  third key while blobs are still under the first, those blobs open under
  neither key. `rotate-kek` reports them `wrong-key` and exits non-zero after
  finishing its inventory. Nothing in this repository can recover them without
  the missing key.
- **`reason=wrong-key`: check key history first.** The blob is well formed but
  failed authentication under both keys you supplied. Usually that is a
  key-history problem: look at an older `.env`, a restored volume, or another
  deployment. Less often it is corruption that kept the envelope intact, or a
  blob copied into a row with a different AAD; GCM cannot tell these apart.
  Check integrity second. Do not rotate again; that only adds a key to the
  search.
- **`reason=malformed` is data corruption.** The blob is truncated or has an
  unknown version byte, so no KEK is involved. Restore that row from backup.
  Rotating again cannot help and hides the evidence.
- ⚠️ **`reason=missing` on `service_keys`/`plc-rotation` is the most dangerous
  result.** The row is gone on a database that still holds bridged identities,
  so the key every one of those DIDs names as its rotation authority is absent.
  This is almost always a restore that missed `service_keys`. **Stop. Do not
  restart the bridge.** `LoadOrCreateRotationKey` creates a key when none
  exists, and the next boot would mint a replacement, after which those DIDs
  can never be recovered. Restore `service_keys` from a backup that has the row
  (the restore drill checks for it), then continue.
- **`reason=contended`** means another writer kept changing the row. Nothing is
  wrong. Re-run `rotate-kek`.
- **A failed walk exits non-zero and still finishes the inventory**, so one
  pass shows how much is affected. Use the exit code only to refuse:
  - Non-zero: do not unset `BRIDGE_KEK_PREVIOUS`. Something is still unmoved
    or unreadable, and retiring the old key makes it unrecoverable.
  - Zero: unset it only on the zero run, where every table reported
    `resealed=0 failed=0`. A first run that exits zero with `resealed=948` is
    progress. It does not prove nothing was written under the old key while the
    walk ran; that is why step 5 asks for another run.
  - An aborted walk still logs an inventory, but it covers only the rows it
    reached. Never treat its `resealed=0` as a zero run. The ERROR line tells
    you the run failed.

  A script may use the exit code to refuse retiring the key. Permitting it
  needs the zero-run counts as well.

#### Not built: per-actor RSA rotation

Rotating one actor's RSA key is not defined. It would mean republishing
`publicKey` in the actor document and having every peer that cached it
re-fetch, and the code has no way to publish two keys during an overlap.
`ap_actors.rsa_key_version` exists for this rotation
(`internal/db/migrations/017_ap_actors.sql`); the code stores it but nothing
acts on it.

The bridge's own service-actor RSA key is stored as plaintext PKCS#8 PEM
(`service_keys`, row `service-actor`), outside the KEK. Rotating it is a
separate procedure that is not built, and the KEK walk does not touch it.

⚠️ **Back up `BRIDGE_KEK` somewhere that survives losing the server.** It can
now be rotated, but losing it with no previous key to fall back on is still
unrecoverable.

### Backup and restore (BUILT)

A nightly `pg_dump` plus a restore drill in a throwaway container. Both
scripts have been run end to end (backup, archive check, restore, inventory
gates).

**`scripts/pg-backup.sh`** runs from a host systemd timer. It:

- dumps in custom format into the `./backups` mount from the compose file;
- checks the archive with a full `pg_restore -f /dev/null` read before giving
  it its final name (`--list` reads only the table of contents and would pass a
  truncated dump);
- sets the dump to mode 600;
- deletes completed dumps older than `RETENTION_DAYS` (default 14).

A failed run keeps only its most recent partial file, as
`backups/tidepool-last-failed.dump.partial`, so retries overwrite it instead
of piling up ~2GB files. Partials are not aged out, and the script warns about
any older than a day.

The production host has no cron daemon, so the script runs from the units in
`scripts/systemd/`: daily at 02:17 UTC, as root, with output in the journal. A
failed run retries every 5 minutes, up to 4 starts in 2 hours, which covers a
catch-up run at boot that fires before the containers are up. The unit runs a
root-owned copy at `/usr/local/sbin/tidepool-pg-backup`, because a checkout
writable by a non-root user should not supply code that runs as root.

Install from `/opt/tidepool`. Re-run the `install` line after any change to
`scripts/pg-backup.sh`:

    sudo install -m 755 scripts/pg-backup.sh /usr/local/sbin/tidepool-pg-backup
    sudo cp scripts/systemd/tidepool-pg-backup.{service,timer} /etc/systemd/system/
    sudo systemctl daemon-reload
    sudo systemctl enable --now tidepool-pg-backup.timer
    sudo systemctl start tidepool-pg-backup.service    # first backup now
    journalctl -u tidepool-pg-backup.service            # its output

⚠️ **Run `chmod 700 /opt/tidepool/backups` once, by hand.** The script sets
modes only on files it creates. Every dump contains the plaintext service-actor
PEM and every sealed blob in the database.

**`scripts/pg-restore-drill.sh`** restores the newest dump (or `$1`) into a
throwaway `postgres:16` container (no published ports, `--network none`,
reached only by `docker exec`), prints the row inventory, exits non-zero on any
gap, and removes the container. It never touches the production container,
volume or network. It fails when:

- **The migration version is behind** the `internal/db/migrations/*.sql` in
  the checkout the script sits in. A dump that restores cleanly but predates
  migrations the running code needs fails as stale. Run from outside the repo,
  this check falls back to a warning plus an any-nonzero check.
- **An irreplaceable table is missing** (and `service_keys` must be
  non-empty): `bridged_actors`, `service_keys`, `ap_actors`, `blocks`,
  `repo_state`, `blobs`, and the in-flight federation tables
  `outbound_deliveries`, `outbound_activities`, `outbound_objects`,
  `outbound_votes`, `admissions`.
- **The `plc-rotation` row is not present exactly once with non-empty
  material** while `bridged_actors` is non-empty. A missing row is the
  dangerous case: the bridge boots and mints a replacement key (see
  `reason=missing` above). If `bridged_actors` is also empty, a missing row is
  allowed with a printed note; that is a fresh-install dump.
- **A sealed blob is zero-length** in `bridged_actors.signing_key` or
  `ap_actors.rsa_key_sealed`. Zero-length ciphertext is corruption. NULL is a
  legitimate actor without a key.

With the default schedule you can lose up to 24 hours of database state. The
drill checks that data is present, not that the KEK is right: sealed columns
are checked present and non-zero-length, never opened, because the drill must
not need the KEK. The full proof is drill plus boot canary: restore, point a
bridge at it with the real `BRIDGE_KEK`, and a wrong pairing fails at startup
(the same canary as step 2 of the KEK runbook). This holds even when the
restore lost the `plc-rotation` row: the canary then checks the KEK against the
actor keys that are present (`ap_actors.rsa_key_sealed`,
`bridged_actors.signing_key`) and refuses to mint a replacement rotation key
over a populated database it cannot read. Only a database with no sealed
material at all (a fresh install) may mint on an unproven key.

**Retention can outlive your last drilled dump.** Retention keeps any dump
that passed `pg-backup.sh`'s full read. A dump that reads cleanly but would
fail the drill keeps refreshing the 14-day window while the last drilled dump
ages out. Nothing runs the drill automatically. **Run it after the first
backup, after any restore, and quarterly.**

⚠️ **`BRIDGE_KEK` and `.env` are not in any database backup.** Keep an offsite
copy of `.env` (a password manager or sealed storage, not this server), and
run `touch backups/.env-backed-up` after each copy. `pg-backup.sh` warns on
every run where `.env` is newer than that marker, so an edited or rotated KEK
that was never copied offsite shows up in the backup's journal output.

Remaining limits: dumps live on the host they protect (`./backups` is not
replicated offsite; copying it with the same routine as `.env` is the next
step), and there is no WAL archiving or point-in-time recovery.

What is at risk, most irreplaceable first:

1. **`BRIDGE_KEK`**: lives in `/opt/tidepool/.env`, not in Postgres, and no
   database backup covers it. Losing it is unrecoverable.
2. **`bridged_actors`, `service_keys`, `ap_actors`**: the sealed signing keys
   for bridged identities, the PLC escrow key, and every native Coves user.
   Losing these loses the identities even if the KEK survives. `ap_actors` is
   the whole native-user population and is easy to leave out of an older
   backup scope.
3. **Repo blocks and commits**: the atproto repos. They can be rebuilt only by
   re-bridging, which mints new DIDs; the old at-uris do not come back.
4. **`outbound_*`, `admissions`**: in-flight federation state. Losing it
   double-sends or drops deliveries.

### A divergence off switch

There is none. The reconciliation sweep is wired outside every flag in
`run()`, and `DivergenceReconciler.Run` sweeps once immediately, before its
first tick (`internal/ingest/divergence.go`). `DIVERGENCE_INTERVAL=0` does not
disable it: `durationVar` rejects zero and negative values and the process
refuses to start.

The only lever is a long interval. `DIVERGENCE_INTERVAL=8760h` quiets the
background pass. The startup sweep still runs once per boot, and
`GET /admin/divergence` still works.

The sweep writes nothing, to peers or to our own tables, so running it always
is safe. If a sweep is implicated in an incident (lock pressure, or a long
scan: two of its legs scan `outbound_activities` in full, per `FOLLOWUPS.md`),
raise the interval and restart.

### Periodic vote re-seed

Nothing re-seeds vote aggregates on a schedule. `SeedPostCounts` has one
caller, on the community-backfill path (`internal/ingest/backfill.go`).

Baseline-only voters can drift when a later flip or clear has no per-voter
baseline row to retract. `FOLLOWUPS.md` (*Votes*) says a re-seed heals the
aggregate, but for a quiet community that is never backfilled again, no re-seed
ever runs. The drift is permanent and nothing reports it: the divergence sweep
compares atproto state with outbound state, not vote aggregates with the origin
instance.

The manual fix is a backfill of the affected community
(`POST /admin/communities/backfill`), which re-seeds as a side effect and also
does other work.

---

## 7. Support matrix

| Peer | Status | Notes |
|---|---|---|
| **Lemmy 0.19.20** | Targeted. The strictness ceiling and the only peer the e2e suite runs against | Pinned as `ARG LEMMY_VERSION=0.19.20` in `e2e/lemmy/Dockerfile`; `make e2e` builds and tests against it |
| **Mbin** | Best-effort, lightly tested | No harness instance. Hand-built fixtures cover link posts and backfill only (`internal/ap/vocab_mbin_test.go`, `internal/ingest/backfill_mbin_test.go`) |
| **PieFed** | Best-effort, untested | No harness instance and no captured post, comment or vote fixture. Its votes are expected to arrive from anonymous per-user actors, which works for tallies but gives no per-voter identity |
| **Pylova** | Expected to work, untested | No harness instance and no fixtures |
| **Lemmy 1.0-beta** | Tracked, not targeted | Vote `FederationMode`, inbox collapsing and `NoteWrapper` all change behaviour we depend on. No harness coverage |
| **Mastodon** | Incidental | The `security/v1` context is published so its parser accepts our `publicKey`, and its hosts appear in production handle subdomains. Not a target; not tested |

---

## 8. Known operational gaps

These limit what the runbook above can promise:

- **Per-IP rate limits act as global limits behind Caddy.** Every in-process
  limiter keys on `RemoteAddr` and ignores `X-Forwarded-For`, so behind the
  proxy every request has Caddy's container IP. Rate limiting at the edge is
  the fix; tracked in `FOLLOWUPS.md`.
- **The e2e harness never runs with `ENVIRONMENT=production`.** It runs in
  development mode (migrations on start, HTTP, private fetch, strict lexicon
  validation; `docker-compose.e2e.yml`). The production-only refusals
  (`BRIDGE_SCHEME=http`, `ALLOW_PRIVATE_FETCH`, `AP_HOST_FALLTHROUGH_DEV`, and
  the `AP_USER_ORIGIN` https rule) have unit tests only.
- **Production lexicon validation logs and writes instead of failing.** Strict
  validation is on only in development (`StrictValidation:
  cfg.IsDevelopment()` in `run()`). Wait until
  `tidepool_lexicon_validation_failures` stays at 0 in production before
  making it strict.
