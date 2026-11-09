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
| `POST /admin/communities/backfill` | Queues an outbox backfill in the background. At most 4 run at once. A community already queued or walking is not queued again. | `{"community":…}`. **202**; **404** not bridged. |
| `POST /admin/communities/refresh-profile` | Re-materializes `community.profile` without waiting for `PROFILE_REFRESH_TTL`. Use it to add new record fields to communities that are not posting. | `{"community":…}` runs now: **200**, **404** not bridged, **422**, **502**. `{"all":true}` walks every bridged community in the background: **202** `{"communities":N}`, **409** while a community walk runs. Sending both keys is **400**. |
| `POST /admin/users/refresh-profile` | Re-materializes `actor.profile` for every bridged person. Use it to add new record fields such as `origin`, `originUsername`, `originUrl`. | Body must be exactly `{"all":true}`; anything else, including unknown keys or trailing data, is **400**. **202** `{"users":N}`; **409** while a user walk runs. |
| `POST /admin/communities/reconcile` | Runs one follow-list sweep now and answers when it ends. A sweep can take minutes (see [Pacing](#pacing)), so the response's write deadline is 10 minutes, and that includes any wait behind a running sweep. A client that drops does not cancel the sweep. A sweep that outlasts the deadline still finishes, and the client gets no result. | **200** `{"subscribed":[…],"unsubscribed":[…],"failed":[…]}`. `subscribed` and `failed` hold follow-list entries, and actor ids for listed communities. `unsubscribed` and a failed unfollow in `failed` hold group ids. A listed community left for a later sweep appears nowhere in the response. **500** `reconcile failed: …`. A parse failure or a failed read of the subscription table makes no subscription changes, but the outbound sets may already be applied. A sweep ended by shutdown keeps the changes it made. A failed community listing is not a 500. **501** `FOLLOW_LIST_PATH` is unset. |

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
| `POST /admin/reemit` | Re-emits a repo's records on the firehose as delete-then-create pairs with identical values, so at-uris and CIDs do not change. Fixes the relay cold-start gap. | `{"did":"did:plc:…"}` for one repo; `{}` or an empty body for every active repo. Tombstoned repos are skipped. **200** `{repos, failed, result:[{did, records, reemitted, skipped_banned, skipped_unknown_community, error?}]}`; `records` is every record in the repo, `reemitted` the ones deleted and recreated, `skipped_banned` the comments left alone because their author is banned in their community, `skipped_unknown_community` the comments left alone because their community cannot be determined. |
| `POST /admin/objects/sweep-deleted` | Deletes bridged objects whose origin now serves a tombstone. | `{"ap_ids":[…]}`, at most 200 ids (**400** above that). **200** `{requested, swept, deleted, failed, truncated, result:[{ap_id, outcome}]}`. Safe to re-run. |
| `POST /admin/inbox/replay-bans` | Re-decides every recorded upstream ban that carried `removeData` and removes the bridged author's content it covers. Cleans up bans received before Tidepool acted on `removeData`. | `{"dry_run":true}` or `{"dry_run":false}`. An empty body or `{}` is **400**. **200** `{dry_run, bans:[{activity_id, subject, scope, community, removed, error?}], removed_total}`. |
| `POST /admin/moderation/backfill-removal-withholds` | Withholds the blobs of content a community removed before removals withheld blobs. Walks every `social.coves.community.removal` record in every community repo Tidepool holds (posts and comments); each subject whose record is in a Tidepool-hosted repo gets one `community-removal` withhold, under the removal record's code, per blob CID its current record references that has none. A subject not hosted here (a native Coves post or comment) is skipped and counted. Bytes are kept. Run it once after deploying the release that added community-removal withholds; it is idempotent, so a second run adds nothing. | `{"dry_run":true}` reports what it would add and writes nothing; `{"dry_run":false}` writes. An empty body or `{}` is **400**. **200** `{dry_run, added:[{subject, community, cids}], already_withheld, skipped_unhosted, failed}`: `added` lists each subject that got (or would get) a new row with the CIDs added, `already_withheld` counts hosted subjects that needed none, `skipped_unhosted` counts removal records whose subject Tidepool does not host, `failed` counts removal records (and community repos) that could not be processed; each is logged and the walk goes on. A deleted (tombstoned) community's removals are still back-filled. |
| `POST /admin/moderation/backfill-ban-records` | Publishes a `social.coves.moderation.ban` record into the community repo, at the digest rkey of the subject DID, for every community ban in force now that was accepted before community bans became ban records. Native personas come from the `community_bans` rows in force (permanent, or `expires_at` after now); the record's `createdAt` is the row's `banned_at`. Bridged persons hold no row, so their bans are rebuilt by replaying the recorded announced `Block` and `Undo{Block}` inbox events in inbox id order, each judged at its own `received_at`: a `Block` in force at receipt replaces the standing ban, a `Block` already over at receipt replaces nothing, and an `Undo{Block}` lifts the standing ban unless the stale-unban rule keeps it (the standing ban is in force and outlives the expiry the Undo names). The final standing ban is published only if it is in force now; its `createdAt` is the deciding event's `received_at`. A later poisoned or unparseable ban event for the same community and subject, or one still pending in the inbox queue, makes that ban undecidable: it is reported, not published. Live moderation keeps running during the back-fill, so a record is written only where none stands (a standing record is the live path's, and is never overwritten) and only create-only, so a record written live between the read and the write is kept. Right before each write the source is read again: a native ban no longer in force in `community_bans` is not published, and a bridged ban with a ban event received after the run read the history is reported undecidable. A live unban landing in the moment between that re-read and the write can still leave a record; running the back-fill while no moderation arrives closes that gap. `replay-bans` is unchanged by it. Run it once, after Coves deploys ban-record indexing; it is idempotent, so a second run publishes nothing. | `{"dry_run":true}` reports what it would publish and writes nothing; `{"dry_run":false}` writes. An empty body or `{}` is **400**. **200** `{dry_run, published:[{community, subject}], already_published, undecidable:[{community, subject, activity_id}], skipped_unminted, failed}`: `published` lists each (community DID, subject DID) whose record was written (or would be), `already_published` counts bans in force whose record already stands, whatever it says, `undecidable` names each bridged ban left unpublished with the inbox event that makes it undecidable, `skipped_unminted` counts (community, actor) pairs whose recorded ban names a Lemmy user Tidepool never minted, and `failed` counts bans whose record could not be read or written; each is logged and the run goes on. Bans not in force now appear nowhere. |
| `POST /admin/actors/purge` | Purges a native Coves person actor: sends `Delete{Person, removeData:true}` to every inbox its content reached, undoes its standing votes, and tombstones the actor so it answers 410 from then on. | `{"did":"did:…","dryRun":true}` or `false`. **200** `{did, actorId, kind, enabled, deliveryPaused, tombstoned, inboxes, standingVotes, dryRun}`; **400** missing `dryRun` or a missing or invalid `did`; **404** no native actor for that DID; **422** the actor is not a person; **501** `CONSUMER_ENABLED` is off. |

Re-emit: records committed before a relay first subscribed are never emitted
again on their own, so a Jetstream-fed AppView cannot index them. If a record
is deleted and then fails to recreate, it is gone from the repo. The response
marks it `RECORD DROPPED` and the log line `reemit: recreate failed after
delete` carries the details for manual recovery.

Re-emit skips a comment whose author (the repo's DID) has a
`social.coves.moderation.ban` record in force (permanent, or `expiresAt` after
now) in the comment's community, and counts it in `skipped_banned`. Coves
records a permanent `community_ban` hide on every comment it indexes while a
ban stands, including a comment re-created after a delete, and that hide
survives the unban and any restore. Re-emitting would hide the author's
earlier comments in that community for good. The comment's community comes
from its stored mapping (`ap_objects.community_did`), so a comment under a
native Coves postv2 in a repo Tidepool does not host, or under a deleted root,
still resolves. With no stored community it comes from the thread root: a
legacy post's repo, or a postv2's `community` field. A comment whose community
cannot be determined is not re-emitted, because no ban can be ruled out for
it, and is counted in `skipped_unknown_community`. If the mapping or the ban
record cannot be read, the repo stops there with `error` set, and its
remaining records are not re-emitted.

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

`POST /admin/dns/reconcile`, `POST /admin/inbox/replay-bans`,
`POST /admin/moderation/backfill-removal-withholds`,
`POST /admin/moderation/backfill-ban-records` and
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
`PLC_WRITES_PER_MINUTE`, shared with DID mints; see
[HANDLE_SCHEME.md](../HANDLE_SCHEME.md).

### Outbound delivery and admissions

| Route | What it does | Body and answers |
|---|---|---|
| `GET /admin/outbound` | Counts deliveries by state (`pending`, `poisoned`, `cancelled`, and so on). | **200** `{"by_state":{…}}`. Answers even with the consumer off and no workers; an empty queue then is accurate. |
| `POST /admin/outbound/redrive` | Moves poisoned deliveries back to pending. | `{"activity":"…"}`, `{"community":"…"}`, or `{"all":true}`. **200** `{"redriven":N,"superseded":M}`. **400** with no filter: an unscoped redrive is refused. |
| `POST /admin/outbound/cancel` | Cancels an actor's or a community's pending deliveries, for consent withdrawal or community removal. | Exactly one of `{"actor":"did:…"}` or `{"community":"https://…"}`, otherwise **400**. **200** `{"cancelled":N}`. |
| `GET /admin/admissions` | Lists acceptance decisions. | Optional `?status=` and `?community=`. **200** `{"admissions":[{post, community, status, decisionCode, evaluatedCid}]}`. **404** when `CONSUMER_ENABLED` is off. |
| `POST /admin/admissions/readmit` | Runs admission again for one rejected post. | `{"post":"at://…"}`. **200** `{post, status, decisionCode, enqueued}`, also when the post is still rejected; **404** no admission for that post, or `CONSUMER_ENABLED` is off; **422** no stored record snapshot to re-run from. |

Cancel is terminal: a cancelled delivery is never claimed again, and
`redrive` revives only `poisoned` rows. It also skips a poisoned vote Undo
once the user has a newer vote on the same post (a vote flip) that the peer
holds or can still receive (delivered, or a delivery of it still pending), and
counts it in `superseded`. Lemmy matches an Undo of a vote by actor and post,
so sending the old Undo would remove the newer vote on the peer. The skipped
Undo stays `poisoned`; it is not owed, because the newer vote replaces the old
one on the peer. If the newer vote's deliveries were all cancelled or poisoned,
it will never reach the peer, so the old Undo is redriven. A delivery that a worker is already
sending can still reach the peer. To pause delivery and resume it later, use the
kill switches in [CONFIGURATION.md](CONFIGURATION.md#outbound-federation).

The admissions routes exist only when `CONSUMER_ENABLED` is on, so a 404 from
`GET /admin/admissions` means the consumer is off.

### Blob withholds

A withheld CID is neither stored nor served for any owner DID:
`com.atproto.sync.getBlob` answers 404 `BlobNotFound`, exactly as for a CID
never stored, and the materializer keeps the blob ref in rebuilt records
without storing bytes. `cid` takes any multibase spelling of the CID; every
answer uses canonical CIDv1 base32. `actor` is the operator's label for the
audit trail; each action records its own and never overwrites another's.

| Route | What it does | Body and answers |
|---|---|---|
| `POST /admin/blobs/withhold` | Places an operator withhold on a CID. Idempotent: while one is active, the call changes nothing and returns the existing state. | `{"cid","reason_code","actor"}` plus optional `"moderation_action_id"` and `"csam_case_id"`. `reason_code` is `illegal-content`, `csam` or `operator-discretion`. **200** state; **400** unparseable cid, other reason code, or empty actor. |
| `POST /admin/blobs/lift` | Lifts the active operator withhold. The row is kept with `lifted_at` and `lifted_by`. | `{"cid","actor"}`. **200** state; **404** no active operator withhold; **400** unparseable cid. |
| `POST /admin/blobs/delete-bytes` | Deletes the CID's stored bytes for every owner DID. The withhold stays active. A repeat call keeps the first `deleted_at` and `bytes_deleted_by`. | `{"cid","actor","confirm":true}`. **200** state; **404** no active operator withhold; **400** unparseable cid or `confirm` absent or false. |
| `GET /admin/blobs?cid=` | Inspects one CID. | **200** state; **400** unparseable cid. |
| `GET /admin/blobs/withholds` | Lists the active operator withholds. | **200** `{"withholds":[…]}`. |

The state is `{cid, withholds, bytes_deleted, stored_owners}`. `withholds`
holds every row for the CID, active and lifted, each with `source`, `scope`,
`reason_code`, the actor labels and `created_at`, `lifted_at`, `deleted_at`.
A row without `lifted_at` is active. `stored_owners` counts the DIDs still
holding the bytes.

Automatic deletes (site bans, `Delete{Actor}`, nobridge scrubs) keep the
bytes of a CID under an active operator withhold, so `delete-bytes` is the
only way they are removed.

Operator procedure for illegal media:

1. Withhold the CID first. Serving stops at once for every owner.
2. For apparent CSAM, preserve the material outside Tidepool as your legal
   process requires before going further. The withhold makes `getBlob` refuse
   the bytes and no admin route returns them, so export them from the
   database on the host, using the canonical CID from the withhold answer.
   First count the stored rows:

   ```sh
   docker exec -i tidepool-prod-postgres psql -U tidepool -d tidepool -XAt \
     -c "SELECT count(*) FROM blobs WHERE cid='<canonical cid>'"
   ```

   This command must exit 0. A count of 0 means no bytes are stored for that
   CID, so there is nothing to preserve. Otherwise export the bytes in bash,
   with `pipefail` so a failed `docker exec` or `psql` fails the pipeline:

   ```bash
   set -o pipefail
   docker exec -i tidepool-prod-postgres psql -U tidepool -d tidepool -XAt \
     -c "SELECT encode(bytes,'base64') FROM blobs WHERE cid='<canonical cid>' LIMIT 1" \
     | base64 -d > evidence.bin \
     && test -s evidence.bin && echo "evidence saved"
   ```

   Go on to step 3 only when this prints `evidence saved`. If the pipeline
   exits non-zero or `evidence.bin` is empty, stop and do not run
   `delete-bytes`. `blobs` holds one row per owner DID with identical bytes,
   so `LIMIT 1` is enough. Do not use `COPY ... (FORMAT binary)`: its output
   carries a header and field framing, not the raw bytes.
3. Delete the bytes with `"confirm":true`. This removes the rows, but the
   bytes can remain in dead tuples until `VACUUM` reclaims them, in the WAL,
   and in database backups and dumps taken before the delete.

Lifting restores serving only while the bytes are still stored. After
`delete-bytes`, a lift lets the CID be stored again the next time its media
is fetched. Never lift a `csam` or `illegal-content` withhold after
`delete-bytes`: the next fetch stores the material again.

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
`/admin/moderation/backfill-removal-withholds`,
`/admin/moderation/backfill-ban-records`, `/admin/divergence` and the `/admin/outbound*` routes answer 501 when their
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
`/inbox`. The `Accept` sets it to `accepted` and queues an outbox backfill.

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
outbound_disabled_hosts:         # optional
  - "example.com"
outbound_disabled_communities:   # optional
  - "https://lemmy.world/c/comicstrips"
```

An optional `instances` key names a whole Lemmy instance. See
[Instance entries](#instance-entries).

A reconciler makes the subscription table match the file at startup and every
`FOLLOW_LIST_INTERVAL`. `POST /admin/communities/reconcile` runs a pass now and
reports what changed.

The file is authoritative.

- Communities in the file but not subscribed are subscribed.
- Subscriptions neither named in `communities` nor covered by an `instances`
  entry are unfollowed (`Undo{Follow}`, records kept). This includes communities
  added by hand with `POST /admin/communities`.
- A community's `#nobridge` still overrides the file.

The two `outbound_disabled_*` keys are outbound kill switches. A delivery to a
listed inbox host, or for a listed community, is parked: it stays `pending` and
resumes when the entry is removed.

- `outbound_disabled_hosts` entries are bare hostnames, with no scheme, port or
  path. They are lowercased on load and match the inbox host case-insensitively.
- `outbound_disabled_communities` entries are http(s) community ActivityPub ids,
  for example `https://lemmy.world/c/comicstrips`. Matching is exact and
  case-sensitive.
- Every sweep applies the file's two sets, and a key left out empties its set.
  `POST /admin/communities/reconcile` applies them at once, even while another
  sweep is running. No restart is needed.

Each sweep also re-triggers the backfill of every accepted community the file
wants whose `last_backfill_at` is unset, once its backoff has passed.

- The backoff is 1 hour from the end of the walk. It doubles with each further
  unfinished walk and caps at 24 hours.
- A failed walk, a truncated walk and a group with no outbox all leave
  `last_backfill_at` unset and share the backoff. A group with no outbox is
  walked again every 24 hours for as long as the file wants it.
- A clean completion sets `last_backfill_at` and clears the backoff.
- A queued walk from an `Accept` or a sweep re-reads its community when a slot
  comes free, and is dropped if the community is no longer accepted or was
  backfilled within the last hour.
- The backoff is in memory, so a restart re-walks each unfinished community
  once.
- An `Accept` and `POST /admin/communities/backfill` ignore the backoff. Their
  walks still update it.
- A sweep that stops on a parse or list failure retries nothing. With
  `FOLLOW_LIST_PATH` unset there are no sweeps, so nothing retries.

Safety rails against a bad deploy:

- A missing or malformed file stops startup. So does an unknown key or an invalid
  switch or `instances` entry.
- A file that breaks after startup makes the reconciler skip sweeps and keeps the
  last good switch sets, so an entry the parser rejects cannot lift a kill
  switch. It never unfollows everything. A well-formed misspelling is accepted
  and replaces the old entry.
- An entry that stops resolving is compared against the table offline, so a
  resolver or remote-instance outage cannot make a wanted community look
  removed.
- Only an explicit `communities: []` with no `instances` entries unfollows
  everything.

Git history is the moderation audit log. Additions and removals arrive as
reviewed PRs (requests come in as GitHub issues), and each entry has a comment
giving its reason.

### Instance entries

An `instances` entry follows every local community of one Lemmy instance,
including NSFW communities and communities created later:

```yaml
communities:
  - "!comicstrips@lemmy.world"
instances:
  - host: "lemmy.world"
    exclude:                     # optional
      - "!news@lemmy.world"
      - "https://lemmy.world/c/pics"
  - host: "lemmy.ml"
```

- `communities` is still required. A file with only instance entries writes
  `communities: []`.
- `host` is a bare lowercase hostname, with no scheme, port or path. A dotless
  host such as `lemmy` is valid. A host may appear once in the file.
- `exclude` entries take the same two forms as `communities`, and each must be
  on that `host`. A handle matches the instance and the last path segment of the
  community id, case-insensitively. A URL matches the community id,
  case-insensitively.
- An entry in both `communities` and `exclude` stays followed.
- `instances:` with nothing under it is an error. Remove the key or write
  `instances: []`.
- `exclude:` with nothing under it is an error. Remove the key or write
  `exclude: []`.

Each sweep reads each host at `/api/v3/community/list` with `type_=Local`. The
scheme is `BRIDGE_SCHEME`, and the URL has no port, so the host must serve Lemmy's
API on the scheme's default port. A result is dropped and logged if its `actor_id`
is on another host, uses another scheme, or has user info, a query or a fragment.

Each sweep also reads each host's recent local posts at `/api/v3/post/list` with
`type_=Local`, `sort=New` and `show_nsfw=true`, and takes each post's community
as a candidate. This finds communities the listing does not reach. The same
`actor_id` checks apply.

### Removing an instance entry

Removing an `instances` entry unfollows that host's communities at the next
sweep (`Undo{Follow}`, records kept), unless `communities` names them. Adding a
community to `exclude` unfollows it the same way. Unfollows have no limit.

### Listing failures

A sweep picks what to unfollow from the file and the subscription table before
it reads any listing, and listing can only add to the desired set. A failed or
short listing unfollows nothing. A community that is no longer listed stays
followed.

- A failed read logs `community listing failed`. The sweep carries on and adds
  what the read returned before the failure. The failure is not in the response.
- A 200 with no `communities` array, a result with no `published` time, or a page
  of more than 50 results counts as a failed read.
- The next read starts at the failed page. If the read's first request failed,
  it starts at oldest-first page 1, so a page that always fails cannot hold a
  host in place.
- A failed post read logs `post walk failed` with the `host`. The sweep carries
  on and takes the communities of the pages read before the failed one. A 200
  with no `posts` array, a post with no `id` or `published` time, a page of more
  posts than were asked for, or a full page with no `next_page` counts as a
  failed read.
- A failed post walk keeps its mark, so the next walk reads the same posts
  again.

### Pacing

A sweep works in this order:

1. Unfollows.
2. Entries in `communities` that are not tracked, with no add limit.
3. Listed and post-discovered communities that are not tracked, not excluded
   and not skipped, at most 20 a sweep. Post-discovered communities come first,
   then the rest, each lowest actor id first across all hosts.
   Candidates are this sweep's listing results plus every community a post walk
   has found. A post-discovered community stays a candidate across sweeps until
   it is added, tracked by other means, named in `communities`, excluded, or its
   host is removed from `instances`. A community tried and refused or failed
   uses a slot, as an added one does.

Adds wait for the rate, `PLC_WRITES_PER_MINUTE`. At the default of 5 a minute, 20
mints take about 4 minutes.

No listed or post-discovered community is added in these two cases:

- The PLC budget is paused. A pause that starts mid-batch stops the adds, even
  for an add waiting on its token, and that add makes no PLC POST. Entries in
  `communities` stop too: the rest go to `failed` untried and the next sweep
  retries them. A sweep never waits out a pause.
- More than 20 accepted communities have `last_backfill_at` unset and are outside
  their backoff. A community in backoff does not count, and one whose backoff has
  lapsed does. This is checked before each listed add, and entries in
  `communities` ignore it. If the count cannot be read, the error is logged and
  listed adds stop for that sweep.

A listed or post-discovered community can be skipped for a while. A skipped
community uses no slot and no fetch.

- A refused one, for example `#nobridge`, is skipped for 24 hours.
- One whose add failed for any other reason is skipped for 1 hour.
- One stopped by the budget is not skipped.
- Entries in `communities` are never skipped and retry every sweep.

Each host has a resume position, so later sweeps reach later pages. One read
takes at most 100 pages. A host moves to its next position only when no candidate
from the read is left over. Otherwise the same pages are read again next sweep.
A left-over community that only a post walk found does not hold the position.
Every host is read and walked on every sweep, even when no add is possible.

Each host also has a post mark: the newest post of its last successful walk. A
walk reads newest first back to the mark, at most 20 pages of 50 posts. Pinned
(`featured_local`) posts come first and do not end the walk. A host removed from
`instances` loses its mark, so adding it again starts with a first walk.

The resume positions, post marks, post-discovered candidates and the skip map are
in memory and are lost on restart. The next sweep starts each host at
oldest-first page 1 with a first walk, and each skipped community costs one more
fetch.

### Listing limit

An instance entry reaches about 10,000 communities on one host. Lemmy 0.19.20
answers 400 past page 100 or limit 50, and the endpoint has no cursor, so one sort
order reaches 5,000 communities. Tidepool reads oldest first (`sort=Old`). A host
with a full page 100 is then read newest first (`sort=New`) until the two ends
meet.

If a full `sort=New` page 100 has not met the `sort=Old` end, this warning is
logged with the `host` and `limit=10000`, once per full pass:

```
community listing reached its limit: communities between the two ends may be unreachable
```

It is not a listing failure, and the host goes back to oldest-first page 1.
Communities between the oldest 5,000 and the newest 5,000 may never be listed.

The post walk reaches a community outside the two windows once a local post in
it is read:

- The first walk after a start reads at most 1,000 posts.
- Later walks read back to the previous successful walk's newest post, also at
  most 1,000 posts. A community whose only posts fall past that cap is not found
  until it posts again. The cap is logged at info as
  `post walk stopped at the page cap` with the `host`.
- A community outside the windows with no post since the walk began needs a
  `communities` entry.
- `hidden` communities are returned by neither the community list nor the post
  list. Tidepool reads anonymously, and Lemmy does not return them to an
  anonymous request.
- An entry in `communities` does not use the listing, so it still reaches a
  community past the limit.
- Check a host's community count before adding a large instance.
