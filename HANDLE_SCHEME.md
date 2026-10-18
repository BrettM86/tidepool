# Bridged handle scheme and origin fields

How Tidepool names bridged actors on atproto, how existing actors are renamed
to that scheme, and which fediverse origin fields it writes into profiles.

## Why

Bridged handles used to be `<user>.<instance host with dots as dashes>.<BRIDGE_HOSTNAME>`
(`alice.lemmy-world.tdpl.io`). That shape had three costs:

- DNS needed one NS delegation per instance label, because a DNS provider
  cannot delegate a wildcard. The record count outgrew the free DNS plan and
  needed a reconciler with flood guards, and a new instance waited before its
  handles resolved.
- Dots became dashes, so `my-lemmy.org` and `my.lemmy.org` collided and the
  second actor got a `-2` suffix that looked like a second account. The
  fediverse address could not be recovered from the handle.
- Third-party clients showed a handle that did not name the real instance.

The new scheme keeps the host intact under one zone that is delegated once.
Coves displays bridged users as `@alice@lemmy.world`, taken from the origin
fields in the profile record rather than parsed from the handle, because the
handle loses case and underscores.

## Handle scheme

New handles are `<user label>.<instance host>.<HANDLE_ZONE_ROOT>`, for
example `alice.lemmy.world.ap.tdpl.io`. `HANDLE_ZONE_ROOT` defaults to
`ap.<BRIDGE_HOSTNAME>` and must be a hostname strictly under
`BRIDGE_HOSTNAME`; the e2e stack uses `ap.tidepool`.

- Host: lowercased, dots kept, port dropped. Hosts containing `:` or parsing
  as an IP address are refused before any PLC call.
- User label: built from `bridged_actors.preferred_username` (the raw
  `preferredUsername`, written at mint and on every profile sync, never
  derived from the old handle or the actor URL). ASCII names are normalized
  to `[a-z0-9-]`. Other names are NFC-normalized and IDNA-mapped to a
  punycode `xn--` label. Anything IDNA2008 rejects, and any punycode label
  that would need a suffix or truncation, falls back to a stable
  `u<sha256 prefix>` label.
- Community prefix: a group's user label starts with `c-`, for example
  `c-technology.lemmy.world.ap.tdpl.io`. A person whose label would start
  with `c-` gets `u-` in front instead, for example
  `u-c-technology.lemmy.world.ap.tdpl.io`, so the `c-` space holds only
  communities. The prefix counts toward the 63-character label limit; a
  long name is truncated after the prefix, and a punycode label that does
  not fit with the prefix falls back to the hash label.
- Collisions happen between usernames on the same host that normalize
  alike, and between a `u-` prefixed label and a user whose own label
  already reads `u-c-...` (`u_c_tech` and `c_tech` both want `u-c-tech`; the
  later one gets `u-c-tech-2`). They get `-2`, `-3`, and so on, at the
  end of the prefixed label (`c-technology-2`). A user labelled `c` whose handle collides gets
  `u-c-2`, since `c-2` is community-shaped.
- Reservation: `handle_claims` (migration 033, `handle` primary key,
  `actor_id`) holds every handle an actor has or has reserved. The minter
  and the rename job both claim through it inside the transaction that sets
  `handle` or `pending_handle`, so neither can take the other's handle.
  `HandleTaken` checks the claims table and `bridged_actors.handle`. There
  was no backfill of claims for handles that existed before migration 033.

Actors minted before this change keep their old-format handle until the
rename job moves them.

## DNS

`internal/dns` serves the `HANDLE_ZONE_ROOT` zone (SOA, NS, `_atproto` TXT at
any depth, A/AAAA for handles, empty NOERROR otherwise) alongside the old
per-instance subzones, which keep answering until their delegations are
deleted. Both paths share one TXT lookup (same limit, timeout and SERVFAIL
behaviour). Handles with a `pending_handle` resolve under both names, so DNS
and `/.well-known/atproto-did` answer for the new handle before PLC lists it.

UDP responses are rate limited per client prefix (IPv4 /24, IPv6 /56),
`DNS_UDP_RESPONSE_RATE` per second (default 20). Over the limit every second
response is an empty truncated reply and the rest are dropped. TCP is not
limited. There is no aggregate cap by decision.

## Identity firehose events

`#identity` frames (`did`, `seq`, `time`, `handle`) are persisted in
`firehose_events` (migration 035) so cursor replay covers them. They are
appended by `repo.AppendIdentityEventTx` inside the caller's transaction,
under the commit advisory lock, so they never become visible out of `seq`
order. Tidepool emits one on mint (before the first `#commit`), on a forward
rename, and on a rollback that changed PLC.

## Rename job

`internal/handlemigration`, driven by `POST /admin/handles/migrate` and
reported by `GET /admin/handles/migrate` (see the README admin table).

Per actor: compute the target (or reuse a stored `pending_handle`), reserve
it in `handle_claims`, then submit a signed PLC update (`identity.Minter.UpdateHandle`,
signed with the escrow rotation key) that copies the head operation's keys and
services and sets `alsoKnownAs` to the new handle. When PLC accepts, one
transaction swaps `handle` and `previous_handle`, releases the old claim, sets
`handle_migrated_at` and appends `#identity` last.

- Reconciliation: PLC `/log/last` is read again before submitting and after
  an unclear result. If the head already lists the target, the actor is
  finalized without a second operation.
- Error codes in `handle_migration_error`: `plc_rejected` (PLC 400, body kept
  in `handle_migration_message`), `plc_not_controlled` (escrow key not in
  `rotationKeys`, or the head is a tombstone or legacy create),
  `handle_invalid`, `username_unavailable`. 429, 5xx and transport errors
  retry 5 times with backoff, then the actor keeps its pending handle and
  stays a candidate for the next run.
- Username fetch: an actor with an empty `preferred_username` is fetched
  through the guarded AP client (same-authority redirects only) before a
  target is reserved; failure records `username_unavailable`.
- Bulk runs: a body without `dids` selects every candidate. PLC POSTs are
  spaced at `HANDLE_MIGRATION_RATE` per second (default 2). The run is stored
  in `handle_migration_runs` with one active row at a time; a run interrupted
  by shutdown resumes in the same mode at boot.
- Rollback: `{"rollback": true}` targets `previous_handle` for renamed
  actors. An actor caught mid-forward has its PLC head moved back to the old
  handle if it already lists the new one, its pending handle and claim
  released, and `#identity` emitted only when PLC changed. Rollback must run
  while both DNS zones are still served.

## Origin fields

Bridged user profiles (`social.coves.actor.profile`) carry `origin` (host)
and `originUsername` (exact `preferredUsername`, at most 256 UTF-8 bytes;
both are omitted together when it is longer). User and community profiles
both carry:

- `originUrl`: the actor's `url` when it is a single https link on the actor
  id's host, otherwise the actor id; omitted when neither is a valid http(s)
  URI.
- `originSoftware`: `software.name` from the host's NodeInfo, lowercased, at
  most 64 bytes. `internal/nodeinfo` fetches `/.well-known/nodeinfo` and the
  linked document through the guarded client, caches the result per host in
  the `instances` table (migration 042) and refreshes it once a day when that
  host's actors are materialized. A failed fetch keeps the last value or
  omits the field; it never fails the profile write. Display only.

Existing profiles pick the fields up through `POST /admin/users/refresh-profile`
(body exactly `{"all":true}`) and `POST /admin/communities/refresh-profile`.

## Rollout order

1. Coves production must accept the origin lexicon fields before Tidepool
   ships the origin writes; Tidepool validates profile records against its
   vendored lexicons.
2. Tidepool's `lexicons/` were synced from a Coves branch that carries the
   origin fields before it merged. Re-sync from Coves main
   (`scripts/sync-lexicons.sh`) after the Coves merge; until then
   `scripts/check-lexicons.sh` reports drift on the origin files.
3. Delegate `HANDLE_ZONE_ROOT` (NS records) before deploying the new minter,
   rename a single user and community with `dids`, then run the full job.
4. Run both refresh-profile walks once the origin fields are live.

Not built yet: the rollout runbook in `DEPLOY.md`, and deletion of the
per-instance delegation code (`internal/delegation`, `POST /admin/dns/reconcile`,
the per-instance DNS answers, old-format minter handling). Those are removed
only after every actor is renamed and the per-instance NS records are gone.

## Known open items

- A PLC `/log/last` GET answering 400, 401, 403 or 3xx is treated as
  transient, and its body is dropped.
- If finishing a run fails 5 times (a database outage longer than about
  1.5 seconds), the active run row stays until restart, which resumes it.
- `SetPendingHandle` clears `pending_revert_submitted_at` even when it
  re-reserves the same handle, so a rare rollback can miss its `#identity`.
- Error counts in the progress JSON are table-wide, not scoped to the run.
- `originSoftware` is not asserted by any e2e test.
- `make e2e` can fail at stack start because a Postgres container passes its
  healthcheck while initdb's temporary server is up, then restarts. A retry
  passes.
