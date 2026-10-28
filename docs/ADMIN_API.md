# Admin API

Every route needs `Authorization: Bearer $ADMIN_TOKEN`. A missing or wrong
token gets 401. The routes are served on the bridge's own `Host`. Production
publishes `127.0.0.1:8091` on the host for admin calls, so they do not go
through Caddy.

Request bodies are JSON. Successful responses are JSON unless the row says
otherwise. Most errors are plain text; the admissions routes return JSON
errors.

## Routes

### Communities and profiles

| Route | What it does | Body and answers |
|---|---|---|
| `POST /admin/communities` | Subscribes to a community: WebFinger, fetch the Group, materialize it, send a signed `Follow`. | `{"community":"!name@instance"}` or a Group URL. **202** with state `pending`; **200** if already accepted; **404** community not found; **422** the community cannot be bridged (for example `#nobridge`) or has no inbox; **502** the remote failed. |
| `DELETE /admin/communities` | Unsubscribes: sends `Undo{Follow}` and clears the follow state. Records stay; new content stops. | Same body. **200**; **404** not bridged. Local state is cleared even when the remote cannot be reached. |
| `GET /admin/communities` | Lists accepted and pending subscriptions. | **200** `{"communities":[…]}`. |
| `POST /admin/communities/backfill` | Starts an outbox backfill in the background. | `{"community":…}`. **202**; **404** not bridged. |
| `POST /admin/communities/refresh-profile` | Re-materializes `community.profile` without waiting for `PROFILE_REFRESH_TTL`. Use it to add new record fields to communities that are not posting. | `{"community":…}` runs now: **200**, **404** not bridged, **422**, **502**. `{"all":true}` walks every bridged community in the background: **202** `{"communities":N}`, **409** while a community walk runs. Sending both keys is **400**. |
| `POST /admin/users/refresh-profile` | Re-materializes `actor.profile` for every bridged person. Use it to add new record fields such as `origin`, `originUsername`, `originUrl`. | Body must be exactly `{"all":true}`; anything else, including unknown keys or trailing data, is **400**. **202** `{"users":N}`; **409** while a user walk runs. |
| `POST /admin/communities/reconcile` | Runs one follow-list sweep now. | **200** with what changed; **500** `reconcile failed: …`; **501** `FOLLOW_LIST_PATH` is unset. |

Both refresh walks go one item at a time with a pause between them. The
community walk and the user walk can run at the same time. Progress and a final
`done`/`failed`/`skipped` summary go to the log. Shutdown stops a walk at its
next item.

The user walk re-checks consent for each user. A user who still has
`#nobridge` stays without a profile and counts as skipped. A user who removed
it is bridged again. A user who added it since is scrubbed. Tombstoned users
are never fetched.

### Repair and cleanup

| Route | What it does | Body and answers |
|---|---|---|
| `POST /admin/reemit` | Re-emits a repo's records on the firehose as delete-then-create pairs with identical values, so at-uris and CIDs do not change. Fixes the relay cold-start gap. | `{"did":"did:plc:…"}` for one repo; `{}` or an empty body for every active repo. Tombstoned repos are skipped. **200** `{repos, failed, result}`. |
| `POST /admin/objects/sweep-deleted` | Deletes bridged objects whose origin now serves a tombstone. | `{"ap_ids":[…]}`, at most 200 ids (**400** above that). **200** `{requested, swept, deleted, failed, truncated, result:[{ap_id, outcome}]}`. Safe to re-run. |
| `POST /admin/inbox/replay-bans` | Re-decides every recorded upstream ban that carried `removeData` and removes the bridged author's content it covers. Cleans up bans received before Tidepool acted on `removeData`. | `{"dry_run":true}` or `{"dry_run":false}`. An empty body or `{}` is **400**. **200** `{dry_run, bans:[{activity_id, subject, scope, community, removed, error?}], removed_total}`. |
| `POST /admin/actors/purge` | Purges a native Coves person actor: sends `Delete{Person, removeData:true}` to every inbox its content reached, undoes its standing votes, and tombstones the actor so it answers 410 from then on. | `{"did":"did:…","dryRun":true}` or `false`. **200** `{did, actorId, kind, enabled, deliveryPaused, tombstoned, inboxes, standingVotes, dryRun}`; **400** missing `dryRun` or a missing or invalid `did`; **404** no native actor for that DID; **422** the actor is not a person; **501** `CONSUMER_ENABLED` is off. |

Re-emit: records committed before a relay first subscribed are never emitted
again on their own, so a Jetstream-fed AppView cannot index them. If a record
is deleted and then fails to recreate, it is gone from the repo. The response
marks it `RECORD DROPPED` and the log line `reemit: recreate failed after
delete` carries the details for manual recovery.

Delete sweep:

- Each id is fetched again from its origin. A redirect to another origin
  refuses the fetch.
- An object is deleted only when the origin answers HTTP 410 or serves an
  ActivityPub `Tombstone`.
- Live objects, unknown ids, actor profiles and 404s are reported and left
  alone. A 404 also covers the 401 or 403 that a secure-mode instance returns
  for objects it will not show, so it means "not visible", not "deleted".
- `"truncated":true` (`swept` below `requested`) means the client hung up
  mid-batch. The remaining ids were never checked. Run them again.

Ban replay:

- Bans that ran out of retries (poisoned) are included. An Announce received
  only as an id is not replayed.
- A dry run lists each ban that would remove something, with the at-uris it
  would remove, and changes nothing.
- A real run cannot be undone. Running it again removes nothing new.
- A ban that fails to be decided, selected or removed is reported with an
  `error` field and logged at error level, and the run continues. A removal
  that failed partway may already have removed some records that its entry
  does not list. The next run removes the rest.
- `removed_total` counts only bans that completed.
- Each applied ban is logged as `ban replay applied a ban` with its inbox
  `activity` id and inner `block` id.

Actor purge:

- Use it for spammers. They never write the `deleteRemote` opt-out or delete
  their account, so the automatic purge never runs for them.
- `dryRun` is camelCase here; replay-bans uses `dry_run`. It is required, so a
  forgotten flag never purges.
- A dry run changes nothing and reports what a real run would reach.
- A real run cannot be undone. It first pauses and disables the actor, then
  reads its inboxes and votes, then queues the Deletes and Undos. The counts in
  the answer are taken before the purge. Running it again sends nothing new to
  inboxes already reached.
- The Deletes and Undos are outbound deliveries, sent by the outbound workers.
- Community actors cannot be purged here.
- Wrapper script: `scripts/purge-actor.sh <did> [--execute]`, dry run by
  default. Runbook: [DEPLOY.md](../DEPLOY.md) §3.

`POST /admin/dns/reconcile`, `POST /admin/inbox/replay-bans` and
`POST /admin/actors/purge` extend their own write deadline to 10 minutes, past
the server's 30-second write timeout, so a long run still returns its result.

### DNS and handles

| Route | What it does | Body and answers |
|---|---|---|
| `POST /admin/dns/reconcile` | Runs one NS delegation pass now and returns its result. Waits behind a pass that is already running. | **200** with the result; **500** `{error, result}` when the pass fails, including when it stops at `DELEGATION_MAX_NS_RECORDS` (a normal state); **501** `CLOUDFLARE_API_TOKEN` is unset. The result is also logged as `delegation pass finished` with the created and pending labels. |
| `POST /admin/handles/migrate` | Starts a paced background handle rename. Forward by default; `{"rollback":true}` rolls back. | Optional `"dids":["did:plc:…"]` limits the run; without it, every remaining candidate for that mode is included. **202** accepted; **409** while either mode is running; **400** unknown keys, `"rollback":null`, or an empty `dids` list. |
| `GET /admin/handles/migrate` | Reports migration progress. | **200** `{running, mode, started_at, did_limited, remaining, rollback_remaining, pending, migrated, errors}`. `mode` is `forward`, `rollback`, or null when idle. `errors` counts failures by code. |

Pending handle reservations survive failed PLC updates. A run interrupted by
shutdown resumes in its original mode at the next boot. The pace is
`HANDLE_MIGRATION_RATE`; see [HANDLE_SCHEME.md](../HANDLE_SCHEME.md).

### Outbound delivery and admissions

| Route | What it does | Body and answers |
|---|---|---|
| `GET /admin/outbound` | Counts deliveries by state (`pending`, `poisoned`, `cancelled`, and so on). | **200** `{"by_state":{…}}`. Answers even with the consumer off and no workers; an empty queue then is accurate. |
| `POST /admin/outbound/redrive` | Moves poisoned deliveries back to pending. | `{"activity":"…"}`, `{"community":"…"}`, or `{"all":true}`. **200** `{"redriven":N}`. **400** with no filter: an unscoped redrive is refused. |
| `POST /admin/outbound/cancel` | Cancels an actor's or a community's pending deliveries, for consent withdrawal or community removal. | Exactly one of `{"actor":"did:…"}` or `{"community":"https://…"}`, otherwise **400**. **200** `{"cancelled":N}`. |
| `GET /admin/admissions` | Lists acceptance decisions. | Optional `?status=` and `?community=`. **200** `{"admissions":[{post, community, status, decisionCode, evaluatedCid}]}`. **404** when `CONSUMER_ENABLED` is off. |
| `POST /admin/admissions/readmit` | Runs admission again for one rejected post. | `{"post":"at://…"}`. **200** `{post, status, decisionCode, enqueued}`, also when the post is still rejected; **404** no admission for that post, or `CONSUMER_ENABLED` is off; **422** no stored record snapshot to re-run from. |

Cancel is terminal: a cancelled delivery is never claimed again, and
`redrive` revives only `poisoned` rows. A delivery that a worker is already
sending can still reach the peer. To pause delivery and resume it later, use the
`OUTBOUND_DISABLED*` kill switches in [CONFIGURATION.md](CONFIGURATION.md#outbound-federation).

The admissions routes exist only when `CONSUMER_ENABLED` is on, so a 404 from
`GET /admin/admissions` means the consumer is off.

### Reports

| Route | What it does | Body and answers |
|---|---|---|
| `GET /admin/divergence` | Runs one reconciliation sweep and returns the report. Read only. | **200** report; **500** when any part of the sweep fails, with no partial report. Details go to the log. |
| `GET /admin/metrics` | Tidepool's expvar counters (keys starting `tidepool`). Go's `cmdline` and `memstats` are left out. | **200** JSON object. |

`tidepool_lexicon_validation_failures` above zero in production means
materialized records failed lexicon validation and were written anyway.
Investigate.

`POST /admin/communities/backfill`, `/admin/reemit`,
`/admin/objects/sweep-deleted`, `/admin/inbox/replay-bans`,
`/admin/divergence` and the `/admin/outbound*` routes answer 501 when their
component is not built. The `tidepool` server builds all of them, so a 501
from one of these is a bug.

## Subscribing to communities

```sh
# Follow: WebFinger, fetch Group, materialize the community, send a signed Follow.
curl -X POST localhost:8091/admin/communities \
  -H "Authorization: Bearer dev-admin-token" \
  -d '{"community":"!technology@lemmy.world"}'

# List subscriptions.
curl localhost:8091/admin/communities \
  -H "Authorization: Bearer dev-admin-token"

# Backfill on demand.
curl -X POST localhost:8091/admin/communities/backfill \
  -H "Authorization: Bearer dev-admin-token" \
  -d '{"community":"!technology@lemmy.world"}'

# Re-materialize every community profile (202; 409 if a walk is running).
curl -X POST localhost:8091/admin/communities/refresh-profile \
  -H "Authorization: Bearer dev-admin-token" \
  -d '{"all":true}'

# Re-materialize every user profile (202; 409 if a walk is running).
curl -X POST localhost:8091/admin/users/refresh-profile \
  -H "Authorization: Bearer dev-admin-token" \
  -d '{"all":true}'

# Unfollow (Undo{Follow}).
curl -X DELETE localhost:8091/admin/communities \
  -H "Authorization: Bearer dev-admin-token" \
  -d '{"community":"!technology@lemmy.world"}'
```

A new subscription is `pending` until the community's `Accept` reaches
`/inbox`. The `Accept` sets it to `accepted` and starts an outbox backfill.

Lemmy often skips the `Accept` for the first `Follow` from a new peer. The
bridge handles this: a subscription still `pending` after 2 minutes gets the
`Follow` again with a fresh activity id, up to 5 sends in total.

## Repair examples

```sh
# Re-emit one repo's records (omit "did" or send {} to re-emit every active repo).
curl -X POST localhost:8091/admin/reemit \
  -H "Authorization: Bearer dev-admin-token" \
  -d '{"did":"did:plc:aaa..."}'

# Delete objects whose origin now serves a tombstone (at most 200 ids).
curl -X POST localhost:8091/admin/objects/sweep-deleted \
  -H "Authorization: Bearer dev-admin-token" \
  -d '{"ap_ids":["https://lemmy.world/post/123"]}'

# Purge a native actor: dry run first, then dryRun:false.
curl -X POST localhost:8091/admin/actors/purge \
  -H "Authorization: Bearer dev-admin-token" \
  -d '{"did":"did:plc:aaa...","dryRun":true}'

# Redrive poisoned deliveries for one community.
curl -X POST localhost:8091/admin/outbound/redrive \
  -H "Authorization: Bearer dev-admin-token" \
  -d '{"community":"https://lemmy.world/c/comicstrips"}'

# Counters.
curl localhost:8091/admin/metrics \
  -H "Authorization: Bearer dev-admin-token"
```

## Declarative follow list (`FOLLOW_LIST_PATH`)

Set `FOLLOW_LIST_PATH` to a YAML file in the repo that names every community
the bridge should follow (see [communities.yaml](../communities.yaml)):

```yaml
communities:
  - "!comicstrips@lemmy.world"   # quote every entry; a bare ! is a YAML tag
  - "https://lemmy.ml/c/linux"   # Group URLs work too
```

A reconciler makes the subscription table match the file at startup and every
`FOLLOW_LIST_INTERVAL`. `POST /admin/communities/reconcile` runs a pass now and
reports what changed.

The file is authoritative.

- Communities in the file but not subscribed are subscribed.
- Subscriptions not in the file are unfollowed (`Undo{Follow}`, records kept).
  This includes communities added by hand with `POST /admin/communities`.
- A community's `#nobridge` still overrides the file.

Safety rails against a bad deploy:

- A missing or malformed file stops startup.
- A file that breaks after startup makes the reconciler skip sweeps. It never
  unfollows everything.
- An entry that stops resolving is compared against the table offline, so a
  resolver or remote-instance outage cannot make a listed community look
  removed.
- Only an explicit `communities: []` unfollows everything.

Git history is the moderation audit log. Additions and removals arrive as
reviewed PRs (requests come in as GitHub issues), and each entry has a comment
giving its reason.
