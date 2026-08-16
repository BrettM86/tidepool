# Deploying Tidepool v2

The production runbook for the v2 surface: native Coves users federating
outward (tasks 13–17), on top of the v1 inbound bridge that is already running.

**This document describes what is in the code, not what was planned.** Where a
lever does not exist, it says so under [Not implemented](#not-implemented)
rather than describing a plausible one. A runbook that names a knob nobody
built is worse than one admitting the knob is missing, because it sends an
operator hunting for it during an incident. Every claim below cites the file it
came from; if you change the code, change the citation.

Related: [`SELF_HOSTED_RELAY.md`](SELF_HOSTED_RELAY.md) (the relay + Jetstream
ingest path), [`README.md`](README.md#configuration) (the full config table and
admin route table).

---

## 1. Read this before deploying HEAD

**A HEAD deploy onto a pre-v2 `/opt/tidepool/.env` does not start.** It exits
during `config.Load` with:

```
config: AP_USER_ORIGIN is required in production
```

`AP_USER_ORIGIN` is read through `stringVar`, which falls back to a dev default
in development and returns an error in production
(`internal/config/config.go:797-806`, read at `:414`). The pre-v2 env file has
no such variable.

**This is correct behaviour and must not be "fixed" by defaulting it away.**
The origin is baked into every `actor_id` this deployment mints; serving then
derives every URL from the *stored* `actor_id` and never from config
(`internal/personas/serving.go:20-23`). A wrong value is therefore not a
restart away from repaired — it is a set of federated identities pointing at
the wrong place, permanently. Two further boot-time guards exist for the same
reason: it must be `https` in production (`config.go:782-784`), and its host
must not be `BRIDGE_HOSTNAME` or a subdomain of it, which would shadow the
bridged-handle namespace (`config.go:786-790`).

**It must also be a bare origin — no trailing slash.**
`personas.CanonicalizeOrigin` refuses any path, query, fragment, or userinfo
(`internal/personas/origin.go:42-46`), and `https://coves.social/` has a path
of `/`. That is a boot failure, not a normalization.

`docker-compose.prod.yml` now supplies `AP_USER_ORIGIN` with a
`${AP_USER_ORIGIN:-https://coves.social}` default, so a `git pull` plus the
normal deploy command is sufficient. Set it explicitly in `.env` anyway if this
deployment is not tdpl.io/coves.social.

### Minimum `.env` delta

Nothing else is *required* — every other v2 variable has a real default in
every environment. See `.env.prod.example` for the full annotated set.

```sh
# required; the rest of the v2 knobs default safely
AP_USER_ORIGIN=https://coves.social
```

### Deploy command

Unchanged, and still targeted — never a bare `up -d`:

```sh
cd /opt/tidepool && git pull
docker compose -f docker-compose.prod.yml up -d --build tidepool
```

The migrate one-shot and the server share one locally-built image and the
server only starts after `service_completed_successfully` on the migration, so
a failed migration holds the old container up rather than starting a new one
against an unmigrated schema.

Confirm the boot actually got past config:

```sh
docker logs --tail 50 tidepool-prod | grep -E 'listening|config:'
curl -sf localhost:8091/xrpc/_health
```

---

## 2. The v2 flag topology

Everything v2 is off by default and turns on in one order. Nothing here is a
runtime toggle: **config is read exactly once**, at
`cmd/tidepool/main.go:109`, and there is no reload signal — changing any value
means editing `.env` and recreating the service.

```
CONSUMER_ENABLED=false                    ← nothing v2 runs. Today's state.
        │
        │  ON: the Jetstream consumer dials JETSTREAM_URL. The REAL enqueuer
        │      persists outbound intent; the acceptance engine admits postv2
        │      and writes community-signed acceptances; opt-out deleteRemote
        │      and confirmed account deletions purge at peers.
        │      /admin/admissions* comes into existence.
        ▼
OUTBOUND_WORKERS=0                        ← intent accumulates, nothing is POSTed.
        │
        │  >0: delivery workers drain the queue onto the wire.
        ▼
OUTBOUND_DISABLED / _HOSTS / _COMMUNITIES / _ACTORS / OUTBOUND_DRY_RUN
                                          ← per-scope parking, applied per delivery.
```

Three properties of that diagram are load-bearing and are the ones operators
get wrong:

**There is no `OUTBOUND_ENABLED`.** Delivery starts only when
`OUTBOUND_WORKERS > 0` **AND** `CONSUMER_ENABLED`
(`cmd/tidepool/main.go:716` and `:843`, inside `startConsumer`, which is only
called under the consumer flag at `:537`). `OUTBOUND_WORKERS` also defaults to
**0**, via `intVarNonNegative` rather than `intVar`, precisely so that 0 is a
legal value and not a config error (`config.go:501`, `:688-702`).

**Consumer-on / workers-0 is a designed staging step, not a broken state.**
With the consumer running, the real persisting enqueuer is always wired — it
writes `outbound_activities`/`outbound_deliveries` inside the consumer's gate
transaction, so intent past the gate is never dropped
(`cmd/tidepool/main.go:695-713`). Raising `OUTBOUND_WORKERS` later drains
whatever accumulated, immediately. Budget for that.

**`OUTBOUND_DISABLED` parks, it does not fail — and a park costs writes, not
retries.** A blocked delivery stays `pending` and resumes when the switch
clears; `park`
itself never poisons and never cancels (`internal/outbound/worker.go:242-244`,
`internal/outbound/switches.go:5-12`). `OUTBOUND_DRY_RUN` parks the same way,
before any signing or POST (`worker.go:245-249`).

**A park costs the delivery no retries.** `ClaimNext` does
`attempts = attempts + 1` on every claim
(`internal/store/outbound_deliveries.go:143`), but `park` and `parkCausal`
settle through `ReleaseParked`, not `Release` (`worker.go:572`, `:592`), whose
one difference is `attempts = GREATEST(attempts - 1, 0)`
(`outbound_deliveries.go:243-244`) applied under the same claim fence. A full
claim/park cycle therefore leaves the ledger where it found it: an hour-long
global kill switch spends nothing, and once it clears the **first** retryable
failure is a retry, not a poison. `DefaultMaxDeliveryAttempts = 8`
(`worker.go:78`) counts attempts a delivery actually made.

⚠️ **It is a continuous write load, though.** A parked head is re-claimed and
re-parked every `parkDelay = 5 * time.Second` (`worker.go:560`), so a held
switch costs one claim plus one `UPDATE` per parked ordering-key head per ~5s,
for as long as it is engaged — indefinitely, since nothing now ends the cycle on
its own. That churn is the remaining argument for **preferring
`OUTBOUND_WORKERS=0` to park everything** — see [Rollback](#rollback) — and it
is a cost argument, not a safety one.

`POST /admin/outbound/redrive` is the repair for **poisoned** deliveries, not
for parked ones: `RedrivePoisoned` sets `attempts = 0` with `state = 'pending'`
(`internal/store/outbound_deliveries.go:665-666`) and matches
`state = 'poisoned'` only (`:667`). A parked delivery is still `pending`, so it
is neither reached by a redrive nor in need of one — it resumes on its own when
the switch clears.

Scope matching, from `config.go:519-525`:

| Switch | Matching |
|---|---|
| `OUTBOUND_DISABLED_HOSTS` | inbox host, **lowercased** on load and compared case-insensitively — a kill switch must fail closed on case |
| `OUTBOUND_DISABLED_COMMUNITIES` | community **AP id**, e.g. `https://lemmy.world/c/comicstrips`, **exact and case-sensitive**, compared against the delivery's ordering key (`worker.go:237-240`) |
| `OUTBOUND_DISABLED_ACTORS` | actor **DID**, exact and case-sensitive |

There is no allowlist form of any of these. See
[Staged rollout](#5-staged-rollout) for what that means for a canary.

**Every switch in that table — and `OUTBOUND_DISABLED` and `OUTBOUND_DRY_RUN`
with them — is INERT while `OUTBOUND_WORKERS=0`.** `ConfigSwitches` is only
constructed inside `if cfg.OutboundWorkers > 0`
(`cmd/tidepool/main.go:716-736`), so with no worker there is nothing holding a
switch and nothing consulting one. Setting `OUTBOUND_DISABLED=true` while
workers are 0 changes nothing and confirms nothing; it is not a second belt.
Conversely, that is also why `OUTBOUND_WORKERS=0` costs nothing: no worker
exists to claim, park, or write anything.

### Confirming a switch actually engaged

`internal/outbound/metrics.go:10-13` publishes four counters, all under
`/admin/metrics`:

| Metric | Bumped when |
|---|---|
| `tidepool_outbound_delivered` | a delivery reached the peer (incl. Lemmy's duplicate-ack) |
| `tidepool_outbound_poisoned` | a delivery hit a terminal failure |
| `tidepool_outbound_cancelled` | the **worker** cancelled a claimed delivery on consent state — its only call site (`worker.go:292`) |
| `tidepool_outbound_parked` | a kill-switch, dry-run, **or causal-wait** deferral |

`tidepool_outbound_parked` is the only **positive** confirmation that a kill
switch engaged: `by_state` still reads `pending` for a parked delivery, exactly
as it does for one merely waiting its turn, so the queue view cannot tell you.
Read it as a rate, not a level — a parked head is re-parked every ~5s, so the
counter climbs continuously while a switch is held. That climb is the write load
of the hold, not retries being spent.

Two caveats on it. It is shared with `parkCausal`, so a nonzero `parked` with no
switch engaged means causal waits, not an operator action. And
`tidepool_outbound_cancelled` rises **on its own** during rollout: the worker
cancels a claimed delivery whenever the actor is disabled, delivery-paused, or
opted out (`internal/outbound/worker.go:281-296`). That is the counter's only
increment site, so a climbing `cancelled` is consent doing its job and is never
evidence that somebody ran `POST /admin/outbound/cancel` — the admin cancel does
not touch this metric, and shows up only in `by_state`.

---

## 3. The admin surface at incident time

Full route table with request bodies: [README](README.md#the-admin-api). All
routes are bearer-authenticated (`Authorization: Bearer $ADMIN_TOKEN`) behind
one middleware group (`internal/ingest/follow.go:148-162`,
`internal/accept/admin.go:65-71`). Production publishes `127.0.0.1:8091` on the
box so admin calls skip Caddy entirely.

Four behaviours worth knowing *before* you need them, because each one reads
like a fault and is not:

**`POST /admin/communities/reconcile` → 501 means `FOLLOW_LIST_PATH` is
unset.** The follow reconciler is only constructed when the path is configured
(`cmd/tidepool/main.go:468-483`), and the handler nil-checks it
(`internal/ingest/follow.go:575-579`). Production *does* set
`FOLLOW_LIST_PATH=/repo/communities.yaml`, so a 501 there means the env changed,
not that reconciliation broke.

**`/admin/admissions` and `/admin/admissions/readmit` → 404 means the consumer
is off.** Those routes are registered inside the `if cfg.ConsumerEnabled` block
(`cmd/tidepool/main.go:537-556`) because a force re-admit needs the acceptance
engine, which only exists there. A 404 is "the consumer is not running", never
"the endpoint is broken". Every other `/admin` route answers regardless.

**`POST /admin/outbound/redrive` refuses an unscoped redrive.** With no
`activity`, no `community`, and no explicit `{"all":true}` it returns 400
(`internal/ingest/follow.go:210-213`) — an unscoped redrive would re-attempt
every poisoned delivery at once, and a malformed body must not become a silent
fleet-wide replay. `POST /admin/outbound/cancel` likewise demands exactly one
of `actor` or `community` (`follow.go:236-239`).

**`GET /admin/outbound` answers even with the consumer off.** The deliveries
store is wired unconditionally (`cmd/tidepool/main.go:454`), so an empty
`by_state` map is the truth about the queue, not a symptom of misconfiguration.

**It is also the whole route.** `by_state` counts are *all* it returns
(`internal/ingest/follow.go:172-183`) — no rows, no ids, no reasons. There is
no admin endpoint that exposes `last_error_class` or `response_excerpt`, so
"inspect, fix, then redrive" means psql on the box; the query is in
[Step 3](#step-3--widen). Plan for that before the incident, not during it.

```sh
T="Authorization: Bearer $ADMIN_TOKEN"
curl -s -H "$T" localhost:8091/admin/outbound            # queue depth by state
curl -s -H "$T" localhost:8091/admin/divergence          # one sweep, synchronously
curl -s -H "$T" localhost:8091/admin/metrics             # tidepool* expvars
curl -s -H "$T" 'localhost:8091/admin/admissions?status=rejected'
```

---

## 4. coves.social Caddy — a CROSS-REPO change

**There is no Caddyfile in this repository.** TLS for both `tdpl.io` and
`coves.social` terminates in the **Coves** Caddy (`coves-prod-caddy`), which
reaches this stack over the shared external `coves-prod-network`. The work
below is an edit to `~/Code/coves/Caddyfile` — a Coves-repo change, deployed on
the Coves side.

### Why Caddy has to do this at all

`AP_USER_ORIGIN=https://coves.social` puts the native users' ActivityPub surface
on the **Coves** hostname, served by the Tidepool process. Tidepool's Host
router sends requests whose `Host` is `coves.social` to the persona surface, and
everything under `tdpl.io` to the bridge (`internal/personas/hostrouter.go:89-115`).
**None of those AP paths reach Tidepool today** — but not because Caddy sends
the whole hostname to the AppView. The existing `coves.social` site block
(`~/Code/coves/Caddyfile`, block opens at `:70`; routing runs `:72-117`)
already splits the hostname four ways:

| Path | Today's handler |
|---|---|
| `/.well-known/*` | static `file_server` over `/srv` (`Caddyfile:72`) |
| `/client-metadata.json` | static `file_server` over `/srv` (`Caddyfile:79`) |
| `/img/*` | 301 to `img.coves.social` (`Caddyfile:97`) |
| everything else | catch-all `reverse_proxy appview:8080` (`Caddyfile:102`) |

Only the catch-all reaches the AppView. That matters for the before/after diff:
`GET /.well-known/webfinger` today returns a **static-file-server 404** from
`/srv`, not an AppView response, and `GET /nodeinfo/2.0` and `/ap/*` fall to the
AppView. So the signature to look for before the change is a bare 404 on
webfinger — and after it, a Tidepool JRD.

The apex needs a content-negotiated split, and Tidepool deliberately cannot do
it itself: `internal/personas/instance.go:32-34` states in its own comment that
content negotiation is absent *because Caddy owns it in production*, and that a
peer sending `ld+json`, `activity+json`, or no `Accept` at all must still get
the actor. Peers fetch `GET /` to find the instance actor; browsers fetch
`GET /` to find the web app. Only the edge can tell them apart.

### ⚠️ The Caddyfile inode trap — read this first

**Edit the Caddyfile in place. Never replace the file.** The production Caddy
mounts the Caddyfile as a single-file bind mount, and Docker pins a single-file
mount to the **inode present when the container started**. Anything that
replaces the file rather than writing through it — `git checkout`, `mv`,
`sed -i`, most editors' atomic-save, `scp` of a new file — leaves the container
serving the **old** contents forever, while `cat` on the host shows the new
ones. This has bitten this project before. It is the same trap that made
Tidepool's own `communities.yaml` a *directory* mount
(`docker-compose.prod.yml:18-24`).

Safe edits: `nano`/`vim` with backupcopy=yes, `cat > file`, `tee`. After any
edit, confirm the container sees it:

```sh
docker exec coves-prod-caddy cat /etc/caddy/Caddyfile | grep -c '/ap/'
docker exec coves-prod-caddy caddy validate --config /etc/caddy/Caddyfile \
  --adapter caddyfile
docker exec coves-prod-caddy caddy reload --config /etc/caddy/Caddyfile \
  --adapter caddyfile
```

If the container's copy does not show your change, the inode moved: recreate
the Caddy container (`docker compose -f docker-compose.prod.yml up -d
--force-recreate caddy` in `/opt/coves`).

### The change

Inside the **existing `coves.social { … }` site block** — do not add a second
block for the same hostname — add the AP routes. `handle` blocks in one site
are mutually exclusive and sorted by path specificity, so
`/.well-known/webfinger` wins over the existing `/.well-known/*` static block,
and `/ap/*` is disjoint from everything already there.

```caddyfile
coves.social {
    # ── Tidepool's native-user AP surface (AP_USER_ORIGIN) ──────────────
    # These paths belong to the bridge, not the AppView. More specific than
    # the /.well-known/* static block below, so they win the handle sort.
    #
    # NOTE: no `header_up Host` on these proxies. Caddy v2 forwards the
    # original Host by default, and Tidepool's Host router keys on it to
    # choose the persona surface over the bridge surface
    # (internal/personas/hostrouter.go). Rewriting Host to the upstream
    # address would 421 every one of these requests.
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
    # and POST /ap/inbox — the shared inbox for this origin.
    handle /ap/* {
        reverse_proxy tidepool:80 {
            header_up X-Real-IP {remote_host}
        }
    }

    # ── Apex: split by Accept ───────────────────────────────────────────
    # `handle /` matches the apex EXACTLY (a Caddy path matcher is exact
    # unless it ends in *), so this replaces only the bare "/" case that
    # the catch-all used to serve. Same-name directives run in Caddyfile
    # order, so the matched reverse_proxy is tried before the fallback.
    #
    # Two Accept lines, not one substring: Lemmy and Mastodon send
    # application/activity+json, but the AS2 spec form is
    # application/ld+json;profile="…activitystreams", which shares no
    # useful substring with the first. Values for the SAME header field
    # are OR'ed.
    handle / {
        @ap {
            header Accept *application/activity+json*
            header Accept *application/ld+json*
        }
        reverse_proxy @ap tidepool:80 {
            header_up X-Real-IP {remote_host}
        }
        # Fallback: the web app, with the SAME upstream options as the
        # catch-all below — copy them verbatim, header_up Host included
        # (DPoP htu matching depends on it).
        reverse_proxy appview:8080 {
            health_uri /xrpc/_health
            health_interval 30s
            health_timeout 5s
            header_up Host {host}
            header_up X-Real-IP {remote_host}
            header_up X-Forwarded-For {remote_host}
            header_up X-Forwarded-Proto {scheme}
            header_up X-Forwarded-Host {host}
        }
    }

    # … existing handle /.well-known/*, /client-metadata.json, /img/*,
    #   catch-all handle, headers, CSP, encode — all unchanged …
}
```

### Verify from outside

```sh
# instance actor (Application), not the web app
curl -s -H 'Accept: application/activity+json' https://coves.social/ | jq '.type,.id'
# expect: "Application"  "https://coves.social/"

# the AS2 spelling must reach the same document
curl -s -H 'Accept: application/ld+json; profile="https://www.w3.org/ns/activitystreams"' \
  https://coves.social/ | jq '.type'

# a browser must still get the web app
curl -sI -H 'Accept: text/html' https://coves.social/ | head -1

curl -s 'https://coves.social/.well-known/webfinger?resource=acct:alice@coves.social' | jq .
curl -s https://coves.social/.well-known/nodeinfo | jq .
curl -s https://coves.social/nodeinfo/2.0 | jq '.software.name'   # "tidepool"
```

A 421 from any of these means the `Host` header was rewritten on the way
through. A 404 on webfinger for a user who has never federated is correct —
actors are minted lazily on first federating interaction.

### Not changing

`tdpl.io`, the per-instance wildcard blocks, and the on-demand catch-all are
untouched by v2. The `on_demand_tls ask` gate still points at
`http://tidepool:80/.well-known/tidepool-tls-ask`, which is served on the
bridge Host (`cmd/tidepool/main.go:165`) and is unaffected by anything above.

---

## 5. Staged rollout

Decision 19's canary. Each step is a separate `.env` edit plus
`docker compose -f docker-compose.prod.yml up -d tidepool`, because config is
read once at boot.

### Step 0 — Caddy first

Do section 4 **before** enabling the consumer. A native actor that federates
outward triggers Lemmy to fetch its webfinger, its actor document, and the
origin apex, all at `coves.social`. If Caddy is not routing those to Tidepool
yet, Lemmy gets the web app or a 404 and caches the failure.

### Step 1 — consumer on, delivery off

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
`tidepool_consumer_last_event_age_seconds` are what make a *stalled* consumer
visible: the process stays up and the healthcheck stays green while events
quietly stop arriving (`cmd/tidepool/main.go:827-830`). A climbing
`tidepool_consumer_dead_letters` means events are failing and being parked for
the redriver, not lost.

Two reading rules for those keys:

- **The whole `tidepool_consumer_*` family only exists while the consumer
  runs** — `PublishMetrics` is called inside `startConsumer`
  (`cmd/tidepool/main.go:830`). Absent keys mean the consumer is off, not that
  it is broken and silent. The `tidepool_divergence_*` gauges are the opposite:
  registered at package init, so they are always present.
- **`tidepool_consumer_dead_letters: -1` is not a count.** It is the sentinel
  for "storage could not be read" (`internal/consume/metrics.go:26-30`), chosen
  because a `0` would claim the backlog is empty at exactly the moment nobody
  can tell.
- **The `tidepool_divergence_*` gauges use the same `-1` convention**, for the
  same reason (`internal/ingest/divergence.go:166-175`, `divergenceUnswept`).
  Because they are published at package init, they are present from process
  start — reading `-1` until the startup sweep completes, and again for any
  class a failed sweep never wrote. `-1` is "never measured", `0` is "measured,
  nothing diverging"; do not alert on them as if both meant healthy.
- **The four `tidepool_outbound_*` counters are absent until the first delivery
  worker touches them** — they are `expvar.NewInt` at package init in
  `internal/outbound/metrics.go:10-13`, so the keys exist once the package is
  linked, but they sit at 0 while `OUTBOUND_WORKERS=0`. A flat 0 across all four
  at this step is exactly right.

Check the rejections before letting anything out. A misconfigured
`ADMISSION_MAX_PER_AUTHOR_PER_COMMUNITY`, a stale community mapping, or a
consent state you did not expect all surface here as `decisionCode`s while the
blast radius is still zero.

### Step 2 — one community

There is **no allowlist**, so a one-community canary is spelled as a denylist
over the rest of `communities.yaml`. With today's four entries, canarying
`!comicstrips@lemmy.world` means:

```
OUTBOUND_WORKERS=1
OUTBOUND_DISABLED_COMMUNITIES=https://lemmy.world/c/selfhosted,https://lemmy.world/c/fediverse,https://lemmy.ml/c/linux
```

AP ids, exact case — not the `!name@host` spelling `communities.yaml` uses.
Confirm the ids you are about to paste rather than constructing them; the
`community` field of the list response **is** the AP group id
(`internal/ingest/follow.go:283-300`):

```sh
curl -s -H "$T" localhost:8091/admin/communities | jq -r '.communities[].community'
```

**Adding a community to `communities.yaml` later does not add it to this
denylist.** The reconciler will subscribe it and it will start federating on
the next sweep. Whenever the follow list grows during a canary, extend
`OUTBOUND_DISABLED_COMMUNITIES` in the same change.

**`OUTBOUND_DRY_RUN=true` is a weaker instrument than it sounds, and it is not
free.** What it does: the worker claims a delivery, logs it, and parks before
signing or POSTing (`internal/outbound/worker.go:245-249`). What it does **not**
do is validate the translator. **Translation happens at ENQUEUE time**, inside
the consumer's gate transaction (`cmd/tidepool/main.go:703-710`), and the worker
POSTs the stored payload verbatim (`worker.go:309`, `:319`). By the time a
delivery is claimable its payload has already been translated and persisted — so
the moment `CONSUMER_ENABLED=true`, the translator has already run on everything,
dry run or not. Step 1 is where you check its output (read
`outbound_activities.payload` in psql), not here.

And because dry-run parks, it carries the same cost as any other park — which
is write load, not retries: each head delivery is re-claimed and re-parked every
~5s for as long as it is engaged, and the attempt it spends doing so is handed
back (see [§2](#2-the-v2-flag-topology)). Nothing needs redriving afterwards.

### On announcement throttling — what actually exists

Decision 19 asks for "deliberate throttling of initial actor announcements".
**Read this carefully, because the obvious knob is the wrong one.**

`MINT_RATE_PER_MINUTE` / `MINT_BURST` (production overrides them to **10/20**,
`docker-compose.prod.yml`, because the public `plc.directory` 429s mint bursts
during community backfill) gate **inbound** DID minting only. They are wired
into `ingest.NewMintGate`, whose sole consumer is the materializer's minter
(`cmd/tidepool/main.go:271-276`, `:315`) — the path where an unseen *Lemmy*
author gets an atproto DID.

**There is no rate limiter on the outbound side.** `internal/outbound` contains
no `rate.Limiter`, no sleep, and no throttle of any kind; the persona actors
the consumer mints do not pass through `mintGate` (`main.go:539` hands
`personasService` directly to `startConsumer`). The only levers on outbound
volume are:

- `OUTBOUND_WORKERS` — worker **concurrency**, not a rate. Workers poll with a
  1s idle interval (`main.go:53`); with a full queue they run flat out.
- the scoped kill switches — which communities/actors/hosts may deliver at all.

So the canary *is* the throttle: `OUTBOUND_WORKERS=1` plus a one-community
scope. Do not go looking for an announcement rate knob; nobody built one, and
this is filed under [Not implemented](#not-implemented).

### Step 3 — widen

Remove entries from `OUTBOUND_DISABLED_COMMUNITIES` one at a time, raising
`OUTBOUND_WORKERS` as the queue justifies. Before each widening:

```sh
# poison depth — the queue's own verdict
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

What each one means:

- **`by_state.poisoned` climbing** — deliveries exhausting their retries.
  Inspect, fix, then `redrive` **scoped** to the affected community.

  **There is no admin inspect surface for the "why".** `GET /admin/outbound`
  returns `by_state` and nothing else (`internal/ingest/follow.go:172-183`) — a
  count, no rows, no reasons. The columns that carry the diagnosis,
  `last_error_class` and `response_excerpt`, are written by the worker but
  exposed on no route; reading them means psql on the box:

  ```sh
  docker exec -i tidepool-prod-postgres psql -U tidepool -d tidepool -c "
    SELECT ordering_key, last_error_class, last_status_code, attempts,
           left(response_excerpt, 200) AS excerpt, updated_at
      FROM outbound_deliveries
     WHERE state = 'poisoned'
     ORDER BY updated_at DESC LIMIT 50;"
  ```

  `attempts` in that output counts claims that were TRIED — a park hands its own
  claim's increment back (see [§2](#2-the-v2-flag-topology)) — so a poisoned row
  should read about 8. A materially higher number is not extra peer rejections:
  it is claims that never settled, since a lapsed lease or a crashed worker
  leaves its increment behind with nobody to return it.
- **echo drop counters rising steadily** — expected and healthy: our own
  content arriving back from Lemmy and being correctly refused. A counter at
  **zero** while native content is flowing is the alarming case; it means
  classification is not firing and re-materialization is possible.
- **`tidepool_divergence_*` gauges** — the read-only reconciliation sweep
  (`GET /admin/divergence` runs one synchronously). Watch
  `acceptance_undelivered_stale` (a delivery pending past
  `DIVERGENCE_ACCEPTANCE_STALE_AFTER`, default 12h),
  `acceptance_undelivered_poisoned`, and `vote_recast_undelivered`. Also watch
  `tidepool_divergence_sweep_failures` and
  `tidepool_divergence_sweep_age_seconds`: a failed sweep publishes **nothing**
  and the gauges keep their previous values
  (`internal/ingest/divergence.go:584-591`), so a stale age with quiet gauges
  is a *silent* failure mode. The sweep never repairs anything — it reports.

### Rollback

Rollback is a **kill switch, not an un-deploy**. In escalation order, each
step being one `.env` edit plus `up -d tidepool`:

1. **`OUTBOUND_DISABLED_COMMUNITIES=<the bad one>`** — park one community.
   Everything else keeps flowing; the parked deliveries resume with their retry
   budget intact when you clear it, and want no redrive. First because it is the
   *narrowest*. Its only cost is the same ~5s re-claim/re-park write cycle as
   (3), on that community's head delivery alone.
2. **`OUTBOUND_WORKERS=0`** — stop the workers entirely. This is the big red
   button for delivery. The consumer keeps running and keeps recording intent;
   nothing reaches any peer. `NewWorker` is only called inside
   `if cfg.OutboundWorkers > 0` (`cmd/tidepool/main.go:716`), so at 0 there is
   no worker to claim anything: nothing is re-claimed, no `attempts` are spent,
   and no rows are written. It costs nothing and it is genuinely lossless.
3. **`OUTBOUND_DISABLED=true`** — park everything outbound. Same *observable*
   effect as (2) — nothing reaches any peer — and the deliveries keep their
   retries: a park hands its claim's increment back (see
   [§2](#2-the-v2-flag-topology)), so this is safe to hold for as long as you
   need. It is **ranked below (2) on cost, not on risk**: the workers keep
   running, so every ordering key's head delivery is re-claimed and re-parked
   every ~5s — a claim and an `UPDATE` per key per 5s, indefinitely, against a
   database you may be trying to leave alone during an incident. Reach for it
   when you need the *scope* it gives you and a whole-worker stop is too blunt.
   What (3) buys over (2) is that each parked row carries a recorded
   `last_error_class` / `response_excerpt` saying why it is held; a stopped
   worker records nothing.
4. **`CONSUMER_ENABLED=false`** — stop consuming. Intent stops being recorded.
   The consumer resumes from its stored cursor when re-enabled, so this is
   recoverable, but it is the only step that stops *observing*, and
   `/admin/admissions` disappears with it — taking your triage view down at the
   moment you most want it. Prefer 1–3.

Do **not** roll back by cancelling deliveries. `POST /admin/outbound/cancel`
is for consent withdrawal and community removal; a cancelled delivery is not
resumable the way a parked one is.

Only redeploy the previous image if the failure is a code defect rather than a
federation outcome. The kill switches address the latter faster and without a
schema-version question.

---

## 6. Not implemented

Everything in this section is a real operational gap. None of it has a
mechanism in the code today. It is written down because an operator who
assumes one of these exists will look for it during the exact incident where
looking costs the most.

### Key rotation — `BRIDGE_KEK` and per-actor RSA keys

**No rotation path exists. Not partial, not manual, not scripted.**

Every mention of `BRIDGE_KEK` in this repository is a warning, never a
procedure. `internal/config/config.go:49-53` documents the value; the sealing
itself is AES-256-GCM in `internal/identity/keys.go`. There is nothing that
re-seals existing ciphertext under a new key: the binary has exactly two
subcommands, `tidepool` and `tidepool migrate`
(`cmd/tidepool/main.go:69-78`).

Blast radius of losing or changing it — **three** tables, not two:

1. **`bridged_actors.signing_key`** — the escrowed **secp256k1 atproto** repo
   signing key of every bridged (Lemmy-origin) identity
   (`internal/db/migrations/002_create_bridged_actors.sql:14`,
   `internal/identity/keys.go:86-96`). Approximately 950 of them.
2. **`service_keys.key_material`, row `plc-rotation`** — the PLC **escrow
   rotation key** (`internal/identity/keys.go:140-144`), the one thing that
   could recover the DIDs, itself sealed under the key you just replaced. Note
   the column is `key_material`, not `private_key_pem`; migration 013 renamed
   it precisely because only this row is ciphertext — the sibling
   `service-actor` row is **plaintext** PKCS#8 PEM and is *not* KEK-sealed
   (`internal/db/migrations/013_rename_service_key_column.sql`).
3. **`ap_actors.rsa_key_sealed`** — **every NATIVE Coves user's ActivityPub RSA
   signing key**, sealed under the same KEK under its own AAD prefix
   (`internal/db/migrations/017_ap_actors.sql:46`,
   `internal/identity/keys.go:39-53`). Written at mint time in
   `internal/personas/personas.go:185`, through the same `identity.Custodian`
   handed to `personas.New` at `cmd/tidepool/main.go:521-527`.

**(3) is the v2 one, and it is the one a rotation plan will forget**, because
it did not exist when this section was first written. A rotation built to
handle only `bridged_actors` and `service_keys` would leave every native user
unable to sign a single outbound activity — the exact population v2 exists to
serve. Change the KEK and all three sets of ciphertext become undecryptable.
There is no recovery.

*Naming trap:* `LoadOrCreateRotationKey` is **not** KEK rotation. It loads or
generates the did:plc escrow/recovery key — an atproto identity concept —
which is itself sealed under the KEK. Do not read that symbol as evidence that
rotation is implemented.

What a real rotation would require:

- **A key-version column or KEK-id alongside each sealed blob.** *Partly
  present, and this is worth knowing before anyone designs it from scratch.*
  `ap_actors` **already has one**: `rsa_key_version INT NOT NULL`
  (`internal/db/migrations/017_ap_actors.sql:47`), and that migration's own
  comment says it is there so "rotation [is] definable without a schema change"
  (`:23-24`). It is stamped from `currentRSAKeyVersion = 1`
  (`internal/personas/personas.go:25`, applied at `:185`) and read back, but
  **nothing uses it as a selector** — no code branches on it to choose a KEK.
  So on `ap_actors` the schema work is done and only the logic is missing.
  `bridged_actors.signing_key` and `service_keys.key_material` genuinely have
  no version column; those two need the migration as well.
- **A dual-read custodian** that tries the new KEK then the old.
- **An online re-seal pass** over all three tables above — `ap_actors`
  included, which is the one a v1-era plan omits.
- **A cutover** that retires the old KEK only after the pass completes.

The ciphertext also carries a one-byte version prefix
(`internal/identity/keys.go:127`) — but that is the *envelope format* version,
checked for equality and rejected otherwise, not a key id. Neither it nor
`rsa_key_version` selects a key today.

Per-actor **RSA** rotation is equally undefined: rotating an actor's key means
republishing `publicKey` in its actor document and having every peer that
cached it re-fetch, with no grace-overlap mechanism in the code to publish two
keys at once.

**Until this is built, treat `BRIDGE_KEK` as immutable, and back it up
somewhere that survives the loss of the server.**

### Backup and restore

**No procedure exists, and no tooling.** `docker-compose.prod.yml:53` mounts
`./backups:/backups` into the Postgres container. Nothing writes to it. There
is no cron, no `pg_dump` wrapper, no restore drill, and no documented RPO/RTO.

What is at risk, in order of irreplaceability:

1. **`BRIDGE_KEK`** — lives in `/opt/tidepool/.env`, not in Postgres, and is
   not covered by any database backup. Losing it is unrecoverable (above).
2. **`bridged_actors` / `service_keys` / `ap_actors`** — the sealed signing
   keys, for bridged identities, the PLC escrow key, **and every native Coves
   user** respectively. Losing these loses the identities even if the KEK
   survives. `ap_actors` is easy to omit from a v1-era backup scope; it is the
   whole native-user population.
3. **Repo blocks and commits** — the atproto repos themselves. Re-derivable
   from upstream only by re-bridging, which mints new DIDs; the old at-uris do
   not come back.
4. **`outbound_*`, `admissions`** — in-flight federation state. Losing it
   double-sends or drops deliveries.

Anything actually built here should be a separate task with a **restore
drill**, since an unverified backup is a claim, not a capability.

### A divergence off switch

**There is none.** The reconciliation sweep is wired unconditionally — the
constructor and `go divergence.Run(ctx)` sit outside every flag at
`cmd/tidepool/main.go:485-506` — and `Run` performs one sweep **immediately**,
before its first tick (`internal/ingest/divergence.go:561-565`). Setting
`DIVERGENCE_INTERVAL=0` does not disable it: `durationVar` rejects zero and
negative values and the process refuses to start
(`internal/config/config.go:666-668`).

The only available lever is a **large interval** — `DIVERGENCE_INTERVAL=8760h`
quiets the background pass. The startup sweep still runs, once, on every boot,
and `GET /admin/divergence` still works.

This is defensible: the sweep writes nothing, to peers or to our own tables,
which is exactly what makes an always-on schedule safe. But if a sweep is ever
implicated in an incident (lock pressure, a long multi-table scan — two of its
legs scan `outbound_activities` in full, per `FOLLOWUPS.md`), **the runbook
answer is "raise the interval and restart", not "disable it", because disabling
is not possible.**

### Periodic vote re-seed

**Nothing re-seeds vote aggregates on a schedule.** `SeedPostCounts` has one
caller, on the community-backfill path.

This makes one documented self-healing claim conditional in a way that matters:
baseline-only voters can drift when a later flip or clear has no per-voter
baseline row to retract, and the standing note is that "a re-seed heals the
aggregate" (`FOLLOWUPS.md`, *Votes*). For a **quiet community that is never
backfilled again, the next re-seed is never.** The drift is permanent, silently,
and nothing reports it — the divergence sweep compares atproto state against
outbound state, not vote aggregates against the origin instance.

The manual lever is a backfill of the affected community
(`POST /admin/communities/backfill`), which re-seeds as a side effect. That is
a workaround, not a scheduled heal, and it does other work besides.

---

## 7. Support matrix

| Peer | Status | Notes |
|---|---|---|
| **Lemmy 0.19.x** | **Targeted.** Strictness ceiling and e2e target | See the version discrepancy below |
| **PieFed** | Best-effort | No pinned instance in the harness; conformance is held by captured-wire fixtures. Its votes arrive from anonymous per-user actors, which is fine for tallies but means no per-voter identity |
| **Lemmy 1.0-beta** | Tracked, **not targeted** | Vote `FederationMode`, inbox collapsing, and `NoteWrapper` all change behaviour we depend on. No harness coverage |
| **Mastodon** | Incidental | The `security/v1` context is published so its parser accepts our `publicKey`, and its hosts appear in production handle subdomains. Not a target; not tested |

### ⚠️ Version discrepancy — unresolved, needs a decision

The tree contradicts itself about which Lemmy version is pinned:

- `e2e/lemmy/Dockerfile:35` pins **`ARG LEMMY_VERSION=0.19.19`**. This is what
  `make e2e` actually builds and tests against.
- `PLAN.md:430` (decision 19) says "**Lemmy 0.19.20** is the pinned strictness
  ceiling and e2e target". `tasks/18-e2e-deploy.md:21` repeats it, and the
  0.19.20 source is cited as the authority for specific verified behaviours
  across `tasks/13`, `14`, `15`, `17` — the `Delete`-summary convention, the
  `check_bot_account` rule, `Instance`-enum strictness.

**The matrix above says "0.19.x" deliberately, because writing either number
alone would be a claim the tree does not support.** The behaviours we verified
were read from 0.19.20 source; the behaviours we *test* are 0.19.19's.

This is not resolvable from the docs — it needs a call:

1. bump `e2e/lemmy/Dockerfile` to `0.19.20` so the tested version matches the
   decided one (preferred; the e2e stack is owned by a separate task and this
   file is out of scope for this change), **or**
2. amend decision 19 to name 0.19.19 as the pin and re-verify the source
   claims against that tag.

Until one of those happens, do not cite a specific patch version as "the
supported one" in operator-facing material.

---

## 8. Known operational gaps carried forward

Not new, but they shape what the runbook above can promise:

- **Per-IP rate limits degrade to global ones behind Caddy.** Every in-process
  limiter keys on `RemoteAddr` and ignores `X-Forwarded-For`, so from behind
  the proxy they see only Caddy's container IP. Rate limiting at the edge is
  the fix; tracked in `FOLLOWUPS.md`.
- **`ENVIRONMENT=production` has never been exercised end to end.** The e2e
  harness runs in development mode (migrations-on-start, HTTP, private fetch,
  strict lexicon validation). The production-only refusals — `BRIDGE_SCHEME=http`,
  `ALLOW_PRIVATE_FETCH`, `AP_HOST_FALLTHROUGH_DEV`, and the `AP_USER_ORIGIN`
  https rule — are unit-covered, not harness-covered. Section 1 exists because
  of this.
- **Production lexicon validation records and writes rather than failing.**
  A strict-first rollout should wait until
  `tidepool_lexicon_validation_failures` stays at zero in production.
