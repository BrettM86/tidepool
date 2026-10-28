# Bridged handle scheme and origin fields

How Tidepool names bridged actors on atproto, how it renames existing actors
to that scheme, which fediverse origin fields it writes into profiles, and
how native Coves users are addressed on the fediverse.

## Why

Bridged handles used to be `<user>.<instance host with dots as dashes>.<BRIDGE_HOSTNAME>`
(`alice.lemmy-world.tdpl.io`). That shape had three problems:

- DNS needed one NS delegation per instance label, because a DNS provider
  cannot delegate a wildcard. The record count outgrew the free DNS plan and
  needed a reconciler with flood guards. A new instance's handles did not
  resolve until its delegation was created.
- Dots became dashes, so `my-lemmy.org` and `my.lemmy.org` collided. The
  second actor got a `-2` suffix that looked like a second account, and the
  fediverse address could not be recovered from the handle.
- Third-party clients showed a handle that did not name the real instance.

The new scheme keeps the host intact under one zone, delegated once. Coves
displays bridged users as `@alice@lemmy.world`. It reads that address from
the origin fields in the profile record, because the handle loses case and
underscores.

## Handle scheme

New handles are `<user label>.<instance host>.<HANDLE_ZONE_ROOT>`, for
example `alice.lemmy.world.ap.tdpl.io`. `HANDLE_ZONE_ROOT` defaults to
`ap.<BRIDGE_HOSTNAME>` and must be a hostname strictly under
`BRIDGE_HOSTNAME`. The e2e stack uses `ap.tidepool`.

- Host: lowercased, dots kept, port dropped. Tidepool refuses a host that
  contains `:` or parses as an IP address, before any PLC call.
- User label: built from `bridged_actors.preferred_username`, the raw
  `preferredUsername`. Tidepool writes it at mint and on every profile sync.
  It is never derived from the old handle or the actor URL, except by the
  rename job's fallback (see "Username fetch" below). The name is
  NFC-normalized and reduced to `[a-z0-9-]`, so `A日本_B` becomes `a-b`.
  Only a name with no ASCII letters or digits left gets a punycode `xn--`
  label instead. A name IDNA2008 rejects, or a punycode label
  that would need a suffix or truncation, falls back to a stable
  `u<sha256 prefix>` label.
- Community prefix: a group's user label starts with `c-`, for example
  `c-technology.lemmy.world.ap.tdpl.io`. A person whose label would start
  with `c-` gets `u-` in front, for example
  `u-c-technology.lemmy.world.ap.tdpl.io`, so only communities use the `c-`
  space. The prefix counts toward the 63-character label limit. A long name
  is truncated after the prefix. A punycode label that does not fit with
  the prefix falls back to the hash label.
- Collisions: two usernames on the same host can normalize to the same
  label. A `u-` prefixed label can also match a user whose own label is
  already `u-c-...` (`u_c_tech` and `c_tech` both want `u-c-tech`, and the
  later one gets `u-c-tech-2`). The later actor gets `-2`, `-3`, and so on,
  at the end of the prefixed label (`c-technology-2`). A user labelled `c`
  whose handle collides gets `u-c-2`, because `c-2` looks like a community.
- Reservation: `handle_claims` (migration 033, `handle` primary key,
  `actor_id`) holds every handle an actor has or has reserved. The minter
  and the rename job both claim through it inside the transaction that sets
  `handle` or `pending_handle`, so neither can take the other's handle.
  `HandleTaken` checks the claims table and `bridged_actors.handle`. Handles
  that existed before migration 033 were not backfilled into the claims
  table.

Actors minted before this change had old-format handles. The rename job
has moved them to the new format in production.

## DNS

`internal/dns` serves the `HANDLE_ZONE_ROOT` zone: SOA, NS, `_atproto` TXT at
any depth, A/AAAA for handles, and an empty NOERROR for anything else. It
also serves the old per-instance subzones, which keep answering until their
delegations are deleted. Both zones use the same TXT lookup, with the same
limit, timeout and SERVFAIL behaviour. A handle with a `pending_handle`
resolves under both names, so DNS and `/.well-known/atproto-did` answer for
the new handle before PLC lists it.

UDP responses are rate limited per client prefix (IPv4 /24, IPv6 /56) to
`DNS_UDP_RESPONSE_RATE` per second (default 200). Over the limit, every
second response is an empty truncated reply, which tells the client to retry
over TCP, and the rest are dropped. TCP is not limited. There is no
aggregate cap, by decision. A large shared resolver sends many clients'
lookups from one prefix. At the old default of 20, a Coves handle sweep
through OVH's resolver lost 16 to 75 responses a second, and handles that
resolved fine afterwards came back unverified.

### Old-format handles

Old-format handles (`alice.lemmy-world.<BRIDGE_HOSTNAME>`) resolve over DNS
only through the per-instance delegations. Without those, they resolve only
if the operator configures wildcard DNS, `*.<BRIDGE_HOSTNAME>` pointing at
the bridge. A DNS wildcard matches multiple label levels, so one record
covers every bridged instance. The bridge answers both HTTP resolution
paths:

- `GET /xrpc/com.atproto.identity.resolveHandle?handle=…`
- `GET https://<handle>/.well-known/atproto-did`, answered from the `Host`
  header.

A wildcard certificate covers only one label level, and these handles sit
two levels below `BRIDGE_HOSTNAME`. Terminate TLS with on-demand certificate
issuance (for example Caddy) or per-instance wildcard certificates.

## Identity firehose events

`#identity` frames (`did`, `seq`, `time`, `handle`) are stored in
`firehose_events` (migration 035), so cursor replay includes them.
`repo.AppendIdentityEventTx` appends them inside the caller's transaction,
under the commit advisory lock, so they always become visible in `seq`
order. Tidepool emits one on mint (before the first `#commit`), on a forward
rename, and on a rollback that changed PLC.

## Rename job

`internal/handlemigration`. `POST /admin/handles/migrate` starts a run and
`GET /admin/handles/migrate` reports progress (see the
[admin API](docs/ADMIN_API.md)).

For each actor the job:

1. computes the target handle, or reuses a stored `pending_handle`;
2. reserves it in `handle_claims`;
3. submits a PLC update (`identity.Minter.UpdateHandle`, signed with the
   escrow rotation key) that copies the head operation's keys and services
   and sets `alsoKnownAs` to the new handle;
4. after PLC accepts, in one transaction, swaps `handle` and
   `previous_handle`, releases the old claim, sets `handle_migrated_at` and
   appends `#identity` last.

Details:

- Reconciliation: the job reads PLC `/log/last` again before submitting and
  after an unclear result. If the head already lists the target, it
  finalizes the actor without a second operation.
- Error codes in `handle_migration_error`:
  - `plc_rejected`: PLC answered 400. The body is kept in
    `handle_migration_message`.
  - `plc_not_controlled`: the escrow key is not in `rotationKeys`, or the
    head is a tombstone or a legacy create.
  - `handle_invalid`
  - `username_unavailable`

  429, 5xx and transport errors are retried 5 times with backoff. After
  that the actor keeps its pending handle and stays a candidate for the
  next run.
- Username fetch: when `preferred_username` is empty, the job fetches the
  actor through the guarded AP client (same-authority redirects only)
  before it reserves a target. If the origin gives no answer (timeout,
  transport error, truncated body, HTTP 429, 5xx, 401, 403 or 404), the job
  reads the username from an actor URL of the form
  `https://<host>/{u,c,m,users}/<name>`. It accepts `<name>` only when it is
  valid UTF-8 without NUL and the user-label rule maps it to the old
  handle's first label, or to that label minus one `-<digits>` collision
  suffix. The check refuses some valid names, which is safe: a name whose
  old label was truncated before a collision suffix, a name that differs
  from the minted name only by Unicode normalization, and any name with no
  ASCII letters or digits (its old label was a hash). Because a 404 counts
  as no answer, a deleted actor on software that serves its tombstone with
  404 (Mastodon) can still be renamed through this fallback. A 410 cannot.
  A 410, a refused redirect, a document that fails validation, or a name
  that fails the check records `username_unavailable`.
- Bulk runs: a body without `dids` selects every candidate. PLC POSTs are
  spaced at `HANDLE_MIGRATION_RATE` per second (default 0.1). The run is
  stored in `handle_migration_runs`, one active row at a time. A run
  interrupted by shutdown resumes in the same mode at boot.
- PLC rate limits: each 429 from plc.directory (POST or `/log/last`) logs a
  Warn with the DID and these headers: `Retry-After` (first value only),
  `RateLimit`, `RateLimit-*`, `X-RateLimit` and `X-RateLimit-*` (repeated
  values joined with `, `). Values are cut to 128 bytes, and at most 16
  headers besides `Retry-After` are logged. No other headers are logged.
  When an actor's request is still 429 after its 5 attempts, the whole run
  (forward or rollback) pauses for `Retry-After` (seconds or HTTP-date), or
  for 1 hour if there is none, then retries the same actor. It never moves
  on to the next DID while rate limited. Cancelling during the pause stops
  the run with the actor still pending.
- Rollback: `{"rollback": true}` targets `previous_handle` for renamed
  actors. For an actor caught mid-forward, the job moves its PLC head back
  to the old handle if the head already lists the new one, releases its
  pending handle and claim, and emits `#identity` only when PLC changed.
  Run rollback while both DNS zones are still served.

## Origin fields

Bridged user profiles (`social.coves.actor.profile`) carry `origin` (host)
and `originUsername` (exact `preferredUsername`, at most 256 UTF-8 bytes).
When the username is longer, both fields are omitted. User and community
profiles both carry:

- `originUrl`: the actor's `url` when it is a single https link on the actor
  id's host, otherwise the actor id. Omitted when neither is a valid http(s)
  URI.
- `originSoftware`: `software.name` from the host's NodeInfo, lowercased, at
  most 64 bytes. `internal/nodeinfo` fetches `/.well-known/nodeinfo` and the
  linked document through the guarded client. It caches the result per host
  in the `instances` table (migration 042) and refreshes it once a day when
  that host's actors are materialized. If a fetch fails, the field keeps the
  last value or is omitted. A failed fetch never fails the profile write.
  The field is for display only.

Existing profiles pick the fields up through `POST /admin/users/refresh-profile`
(body exactly `{"all":true}`) and `POST /admin/communities/refresh-profile`.

## Fediverse addresses for native Coves users

A native Coves user federates as an actor on the user origin
(`AP_USER_ORIGIN`, for example `coves.social`). `DeriveLocalPart` in
`internal/personas/localpart.go` picks the local part of their fediverse
address from their atproto handle:

- A handle with exactly one label before the user origin host keeps only
  that label. `alice.coves.social` becomes `@alice@coves.social`.
- Any other handle is kept whole. `bretton.dev` becomes
  `@bretton.dev@coves.social`, and `alice.blog.coves.social` stays
  `alice.blog.coves.social`.
- The match is on a label boundary, so `evilcoves.social` is not treated as
  native. The bare origin host is refused as a user handle.
- The result is lowercased and cut to 251 characters.
- If the local part is taken, the next actor gets `-2`, `-3`, and so on, up
  to `-99`.

The local part is fixed when the actor is first created. A later handle
change updates the profile but not the address.

## Remaining rollout

Done:

- Coves main carries the origin lexicon fields, and Tidepool's vendored
  `lexicons/` match Coves main (`scripts/check-lexicons.sh`;
  `scripts/sync-lexicons.sh` re-syncs).
- `ap.tdpl.io` is delegated to Tidepool's DNS server.
- The rename job has run in production.

Still to do:

1. Run both refresh-profile walks now that the origin fields are live.
2. Remove the per-instance delegation code (`internal/delegation`,
   `POST /admin/dns/reconcile`, the per-instance DNS answers, and old-format
   minter handling) once the per-instance NS records are gone.

## Known open items

- A PLC `/log/last` GET that answers 400, 401, 403 or 3xx is treated as
  transient, and its body is dropped.
- If finishing a run fails 5 times (a database outage longer than about
  1.5 seconds), the active run row stays until restart, which resumes it.
- `SetPendingHandle` clears `pending_revert_submitted_at` even when it
  re-reserves the same handle, so a rare rollback can miss its `#identity`.
- Error counts in the progress JSON cover the whole table, not just the
  current run.
- No e2e test asserts `originSoftware`.
- `make e2e` can fail at stack start because a Postgres container passes its
  healthcheck while initdb's temporary server is up, then restarts. A retry
  passes.
