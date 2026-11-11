# Protocol surfaces

What Tidepool serves and accepts on each side of the bridge.

## ActivityPub surface (bridge origin)

The bridge origin (`BRIDGE_HOSTNAME`) serves:

| Route | Purpose |
|---|---|
| `POST /inbox`, `POST /actor/inbox` | Inbox for Lemmy deliveries |
| `GET /actor` | The service actor document |
| `GET /`, `HEAD /` | The instance actor (type `Application`) |
| `GET /.well-known/webfinger` | WebFinger for the service actor |
| `GET /.well-known/nodeinfo`, `GET /nodeinfo/2.0` | nodeinfo 2.0 with `software.name: "tidepool"`, which is how remote servers and directories identify the bridge |

Lemmy resolves every peer's instance ("Site") actor at the origin apex. It
delivers send-to-all-instances activities, such as account deletions, only to
the inbox that document advertises. Without the instance actor, Lemmy skips
those activities without an error.

### What the inbox accepts

- Deliveries need a valid draft-cavage HTTP signature: rsa-sha256, a `Digest`
  header, and a `Date` within one hour.
- Posts, comments, edits and restores are materialized only from the
  subscribed community's own `Announce`. When the signer has no authority over
  an embedded object's id, the bridge re-fetches the object from its origin.
- Bare `Create` and `Update` of content are dropped and counted on
  `tidepool_content_bare_dropped`. This includes replies sent straight to a
  persona inbox and replies with no audience. Bare `Undo{Delete}` restores are
  dropped the same way.
- A bare `Update{Person|Group}` only refreshes actors already bridged.
- Activities the bridge sent itself come back inside community `Announce`s.
  The bridge recognizes them as its own and drops them, so they are not
  duplicated, counted twice, or read as moderation.

### Moderation from Lemmy

Posts live in the author's repo as `community.postv2`. The community's repo
holds a `community.acceptance` while a post is visible, and a
`community.removal` once a moderator takes it down.

- A moderator `Delete` with a reason (`summary`) replaces the acceptance with
  a removal record in one commit. The author's post is not touched.
- `Undo{Delete}` from a moderator writes a fresh acceptance pinned to the
  post's current version.
- An author's own `Delete` (no `summary`) deletes the post and its
  acceptance, and writes no removal.
- A Lemmy post purge arrives as an announced moderator `Delete` and is handled
  as a removal.
- An announced `Lock` records that the thread is locked. Native replies to a
  locked post are not federated, because Lemmy rejects them.
- An announced `Block` from a community bans a native author from posting
  into it. With `removeData`, a ban of a bridged Lemmy user removes their
  earlier content from that community.
- A home instance's site ban of its own user with `removeData` arrives as a
  direct `Block` and removes that user's bridged content. Other direct
  `Block`s are ignored.
- `Add`, `Remove` and `Flag` are not translated.

## Consent policy (#nobridge / #nobot)

Tidepool follows the [Bridgy Fed](https://fed.brid.gy/docs#opt-out) opt-out
norms. The materializer enforces them and fails closed.

- An actor whose profile summary or hashtags contain `#nobridge` or `#nobot`
  is never bridged. No DID is minted, and each post or comment they write is
  dropped with the reason logged.
- If a bridged actor adds the marker, the bridge sees it on a profile `Update`
  or the next profile re-fetch. It deletes every record they authored, the
  blobs those records referenced, and their `vote_events` rows, and stops
  materializing new content. Removing the marker upstream restores bridging
  (records, media and future votes) on the next profile refresh. No
  `#account` frame is sent and the repo stays active.
- `Delete(Actor)` (account deletion upstream) deletes all their records, the
  blobs those records referenced, and
  their `vote_events` rows. The bridged repo is then tombstoned permanently:
  the sync surface reports `active: false` with status `deleted`, and an
  `#account{active:false, status:"deleted"}` frame goes out on the firehose
  so relays purge the repo too.
- An object `Delete` tombstones the mapped record. A `Delete` that arrives
  before its object is remembered in `ap_tombstones`, so a late or re-delivered
  `Create` cannot bring the content back. `Undo{Delete}` restores by
  re-fetching the object from its origin.
- Bridged profiles are labeled. Bios and descriptions end with a "bridged
  from … by Tidepool" line, and community profiles set `hostedBy` to the
  bridge's service DID.

## Sync surface (what relays and Jetstream consume)

Tidepool serves the `com.atproto.sync.*` XRPC endpoints a relay needs to use
it as a `subscribeRepos` upstream.

`GET /xrpc/com.atproto.sync.subscribeRepos` is a WebSocket:

- `?cursor=N` replays from the durable event log, then tails live.
- A cursor older than retention gets an `#info OutdatedCursor` frame first.
- Slow consumers are disconnected and resume by reconnecting with their last
  cursor.
- The stream carries `#commit` frames.
- `#identity` frames are sent when a bridged actor is minted (before its first
  `#commit`), when a handle is renamed, and when a rename is rolled back,
  including a PLC update reverted to the old handle partway through.
- `#account{active:false, status:"deleted"}` frames are sent on
  `Delete(Actor)`, after the delete commits, in the same durable log. Relays
  purge the repo on this frame. BigSky tombstones the account and drops it
  from its `listRepos`. `#nobridge` is reversible, so it sends no `#account`
  frame.

Other endpoints:

- `getRepo` (full CAR), `getLatestCommit`, `getRecord` (proof CAR), `getBlob`,
  `listRepos` (paginated), `getRepoStatus`.
- `com.atproto.repo.getRecord`, the JSON read that AppView reconcilers such as
  the Coves profile backfill use. `repo` accepts a DID or a bridged handle.
- `com.atproto.server.describeServer` and `/xrpc/_health`.
- `com.atproto.identity.resolveHandle` and `/.well-known/atproto-did`.
- `/.well-known/did.json`, the DID document for the bridge's own
  `did:web:<BRIDGE_HOSTNAME>` identity. Every bridged community profile names
  it in `hostedBy`, and consumers verify that claim here. It returns 404 when
  `BRIDGE_SERVICE_DID` is set to something other than a did:web.

Bridged handles also resolve over DNS when `DNS_LISTEN` is set: the bridge
answers `_atproto.<handle>` TXT queries itself. See
[HANDLE_SCHEME.md](../HANDLE_SCHEME.md) and
[CONFIGURATION.md](CONFIGURATION.md#dns-and-handles).

Tombstoned repos report `RepoDeactivated` / `active: false` with status
`deleted`, and their content endpoints stop serving.

### Rate limits

- The sync surface has a per-client-IP token bucket
  (`SYNC_RATE_PER_SECOND`, `SYNC_RATE_BURST`). Over the limit returns 429
  `RateLimitExceeded`. `/xrpc/_health` is exempt so container healthchecks do
  not flap.
- `subscribeRepos` has a concurrent-subscriber cap (`SYNC_MAX_SUBSCRIBERS`).
  Over the cap returns 429 `SubscriberLimitExceeded`.
- `POST /inbox` is limited per client IP and per verified signer. The branch
  that confirms a self-delete of a tombstoned actor has a much tighter per-IP
  cap. Inbox refusals return 503, because federation queues drop 4xx
  responses permanently and retry on server errors.

Every per-IP limiter keys on the connection's `RemoteAddr` and ignores
`X-Forwarded-For`, which can be spoofed without a trusted-proxy allowlist.
[FOLLOWUPS.md](../FOLLOWUPS.md) describes a possible `TRUSTED_PROXY` setting.
If TLS termination or a load balancer sits in front of the bridge, you must
rate-limit `POST /inbox` and `com.atproto.sync.*` at the edge. Otherwise all
clients share the proxy's single IP bucket, and the self-delete confirmation
cap becomes one global cap.

## Vote aggregates

Votes never become records, because nothing may strongRef a vote. Lemmy
`Like`, `Dislike` and `Undo` activities update aggregate counts kept by the
bridge. The counts reach the AppView two ways.

The main path is the content record. Post and comment records carry an
optional `bridgedStats {upvotes, downvotes, asOf}` field. A debounced sweeper
(`STATS_REFRESH_INTERVAL`, `STATS_REFRESH_BATCH`) writes it as the
aggregates change, so the counts reach the AppView on the firehose it already
reads. Each subject gets at most one record update per sweep, so a burst of
votes becomes one event.

The second path is an XRPC the Coves AppView can poll, kept for legacy use and
debugging:

```
GET /xrpc/social.coves.bridge.getVoteAggregates?uris=at://…&uris=at://…
```

- `uris`: at-uris of bridged posts or comments, as repeated `uris` params
  (comma-separated values are also accepted). At most 100 per call; more
  returns `InvalidRequest`.
- Response: `{"aggregates":[{"uri","upvotes","downvotes","updatedAt"}]}` in
  request order. Unknown or never-voted uris are left out, which is not an
  error.
- Public, `Cache-Control: public, max-age=30`, rate limited per client IP.
  Behind a proxy, rate-limit the real client at the edge, because the bridge
  ignores `X-Forwarded-For`.

The contract is the lexicon at
[`lexicons/social/coves/bridge/getVoteAggregates.json`](../lexicons/social/coves/bridge/getVoteAggregates.json).
It is versioned by NSID: breaking changes ship under a new name.

How votes are counted:

- Counts reflect each voter's latest state. Flips (`Like` to `Dislike`) and
  `Undo`s are folded in, and re-delivered activities are deduplicated by
  activity id.
- Votes on content the bridge never materialized are dropped and logged at
  debug.
- Only votes inside the community's own `Announce` count. Bare `Like`,
  `Dislike` and `Undo` are dropped, including Mastodon- or Misskey-style Likes
  sent to a persona inbox on Coves-authored posts.
- Votes from the bridge's own personas (native Coves users' votes coming back
  from Lemmy) are dropped. Coves counts those in its native column, so the
  served aggregate is the fediverse-only tally.

Backfilled posts and seeding:

- ActivityPub delivers votes only going forward, and Lemmy outboxes announce
  historical Likes sparsely, so backfilled posts would start near zero.
- `SEED_COUNTS_FROM_API` (default on) seeds a baseline from the community
  host's public API during backfill. A post from another instance is looked
  up on the community's host, never its own. If that host does not know the
  post, it is not re-seeded and any earlier baseline is kept.
- The baseline is stored net of votes that also arrived live and of votes the
  bridge delivered for native users, clamped at zero.
- Live votes add to the baseline. An undo of a vote that exists only in the
  baseline does nothing; the drift is corrected on the next re-seed.
- Comment scores are not seeded. Comments count live votes only, and a
  comment with live votes still gets a `bridgedStats` field.

Both surfaces read the same `vote_aggregates` totals, so `bridgedStats` and
`getVoteAggregates` differ by at most the sweep delay. `asOf` is the
aggregate's `updated_at`, which the AppView can use to discard a stale update.
Every `bridgedStats` write goes through the same lexicon validation and
mapping bookkeeping as any other record commit. A Lemmy edit that rebuilds a
record keeps its existing `bridgedStats`.

## Outbound: native Coves content to Lemmy

When `CONSUMER_ENABLED` is on, the bridge reads native Coves repos from a
self-hosted Jetstream (`JETSTREAM_URL`). It turns native users' activity in
bridged communities into ActivityPub and delivers it to the community's inbox
(the Group's `sharedInbox` when it has one).

| Native record change | Delivered as |
|---|---|
| Post create, edit, delete | `Create`, `Update`, `Delete` of a `Page` |
| Comment create, edit, delete | `Create`, `Update`, `Delete` of a `Note` |
| Vote create, delete | `Like` or `Dislike`, and `Undo` |
| Profile change | Nothing. Lemmy has no `Update{Person}` handler and rejects it with 400, so peers pick up profile changes through their own actor re-fetch |

- Posts into a bridged community go through the acceptance engine. On admit,
  the bridge writes the community-signed acceptance record and queues the
  `Page` delivery in the same commit. Posts that are not accepted are not
  visible on either side.
- The engine refuses authors the community has banned and caps accepted
  posts per author per community (`ADMISSION_MAX_PER_AUTHOR_PER_COMMUNITY`,
  default 50). Every rejection is recorded with a reason code.
- A native author's AP actor is minted on their first federating post,
  comment or vote, after
  their handle is verified (DNS TXT, then the HTTPS well-known).
- Each delivery is signed as the author's persona, delivered at least once,
  ordered per community, and held until anything it depends on (such as the
  parent post) has been delivered. Consent is checked again just before
  sending.
- Nothing is sent unless `OUTBOUND_WORKERS` is above 0. With the consumer on
  and no workers, deliveries are queued and sent once workers start. Kill
  switches and dry run are in
  [CONFIGURATION.md](CONFIGURATION.md#outbound-federation).

### Coves instance takedowns

A Coves instance admin's `removeContent` or account and community takedown
reaches Tidepool as a signed `!takedown` label (see the label consumer in
[CONFIGURATION.md](CONFIGURATION.md)). For native content the bridge
delivered, Tidepool acts as the author:

- Each native object has a set of takedown sources: its record label, its
  author's account label or account status, and its community's label. The
  object is removed while the set is non-empty.
- When the set becomes non-empty and the object reached its community, the
  bridge sends a `Delete` of the object from the author's persona, without
  `summary`, so Lemmy treats it as the author's delete and not as a failed
  moderator action. An object never delivered gets no `Delete`; one still
  in flight gets it once its delivery settles.
- When the set becomes empty again, the bridge sends `Undo{Delete}` whose
  `object` is that episode's `Delete`, unless the author deleted the object
  or the author's identity was withdrawn. A second removal is a new episode
  with its own `Delete` and `Undo` ids.
- Queued `Create` and `Update` deliveries of a removed object, and replies
  under a removed parent, are held, not cancelled. They are released to the
  tail of their community's queue behind the `Undo{Delete}`, in their
  original order. The restore `Undo` waits while the author's delivery is
  paused.
- `GET` of a removed object's id answers 410 with an AS2 `Tombstone`
  (`formerType` set). The restore brings the 200 back.
- Lemmy announces the author's `Delete` and `Undo{Delete}` to the
  community's followers, so other instances that copied the object follow
  it. A Lemmy moderator's removal of the same object is separate state on
  Lemmy and stays in force after the Coves restore.
- A takedown is not an erasure: it never sends `Delete{Person}` and never
  calls the purger.

Account takedowns:

- An account label names the bare DID. On a native persona it adds an
  account-label source to the persona's pause and to every delivered object;
  a negation removes both. A label's `exp` lapsing is applied like a
  negation by a periodic check (every minute). A re-ban is a newer label
  that replaces the expiry, so an older label's lapse never lifts it.
- A `#account` frame applies its status pause at once, then confirms the
  status with `com.atproto.sync.getRepoStatus` at the DID's PDS. A confirmed
  `takendown` or `suspended` adds the account-status source to every
  delivered object. A confirmed `active`, `deactivated` or `throttled`
  removes it; any other answer changes nothing. A relay `active` lifts the
  status pause only when the PDS confirms `active`. A failed read leaves the
  object sources as they were and retries the frame. `deactivated` and
  `throttled` alone pause and send no `Delete`.
- The label and the status are separate pause sources: delivery resumes, and
  an object restores, only when every source is gone. While a takedown
  source (label, `takendown` or `suspended`) pauses the persona, its queued
  deliveries are held and released on restore, and an edit to an accepted
  post is kept and delivered after the `Undo{Delete}`. A pause from
  `deactivated` or `throttled` alone cancels queued work as before.
- On a bridged person (not a community), an account label withholds every
  blob the DID owns, including blobs stored later, and holds native replies
  beneath that account's bridged content, including replies already queued.
  Negation or lapse lifts the withhold and releases the replies.
- Labels stored before the subject existed apply when it becomes actionable:
  at start for existing personas and bridged accounts, and in the mint
  transaction for a persona or bridged person created later. A
  purger-tombstoned persona gets no restore.

Community takedowns:

- A label on a bridged community's bare DID removes the community. Tidepool
  records the removal with the community's follow state and follow
  provenance and sets the follow state to `none`; once the label is committed
  it sends `Undo{Follow}`. The removal records the `Undo{Follow}` as owed; a
  failed send stays owed and the follow retrier resends it at its start and
  every sweep. Records already bridged stay.
- While the removal stands, nothing follows the community again: a follow
  sweep skips it (listed under `suppressed`), the follow retrier does not
  resend (and takes back with `Undo{Follow}` a resend the removal overtook),
  `POST /admin/communities` and `POST /admin/communities/backfill` answer
  409, queued backfill walks are dropped and a running walk stops before its
  next item, and an `Accept` that arrives is ignored. A label that arrives
  before Tidepool has the community refuses the follow the same way.
  `DELETE /admin/communities` during the removal records the community as
  not followed at the removal.
- Every blob of the community's hosted bridged records is withheld,
  including records committed later. The negation lifts only these rows,
  except those an author delete kept (`label-takedown-author-deleted`: the
  author deleted the record while the label stood, and its bytes stay
  stored); those stay until an operator deletes the bytes or the record is
  undeleted.
- Native objects in the community get the community-label source: delivered
  ones get a `Delete`, queued `Create`, `Update`, votes and replies under the
  community's bridged posts are held. An edit of an accepted post is kept
  and delivered after the `Undo{Delete}`; a new post is refused as not
  followed.
- The negation clears the removal, sends `Undo{Delete}` and releases held
  work. It follows the community again only when the follow state at removal
  was `accepted` or `pending` and the follow was manual or the follow list
  still wants the community: it records the community `pending` with the
  `Follow` owed, and the follow retrier's next sweep sends it. An `Accept`
  before that send is ignored; the `Accept` of the sent `Follow` triggers a
  backfill. An `Undo{Follow}` still owed is then dropped unsent; one already
  in flight that lands after the community is followed again leaves it
  `pending` with a fresh `Follow` owed.
- A negation that clears a removal also marks the community's reconcile
  pending, followed again or not. A reconcile pass runs once the community
  is ready: not removed, its follow not `pending`, and an `accepted` follow's
  backfill completed cleanly. The clean backfill walk starts the pass;
  sweeps at start and every 10 minutes run pending passes too. A pass
  fetches each kept object (a live bridged post or comment of the community)
  from its origin with a pinned-authority signed GET:
  - live (200 with the requested object: same id, a supported
    `Page`/`Note`/`Article` type), with a removal record standing, and the
    object on the community's own instance (same scheme and host as the
    `Group`): the removal is restored as an `Undo{Delete}` would (removal
    record deleted, its removal withholds lifted);
  - live, otherwise: nothing. An object on another instance is fetched from
    that instance, which answers for its own deletions but not for the
    community's removals, so its removal record stays;
  - 404, or Lemmy's 400 `couldnt_find_post` for a post or
    `couldnt_find_comment` for a comment (its answer for a purged object),
    which counts as 404: the record is deleted, on any instance;
  - 410 or a `Tombstone` body: a `moderator-discretion` removal record is
    written and the record kept, unless a removal record already stands, on
    any instance;
  - anything else (401/403, 429/5xx, any other 400, an off-authority
    redirect, a transport error, a dead instance): nothing; a later pass
    fetches it again. The pinned-authority GET does not report a gone host,
    so a dead instance is retried like any other failure.
  The first pass for a pending mark records when it started. The mark is
  cleared, by compare-and-set on the value the pass read at its start (a
  newer negation during the pass sets a newer value, which keeps the
  reconcile pending for its own pass), when every kept object got one of
  the final outcomes above and every write succeeded. Until 72 hours (the
  backfill's dead-host window) have passed since the first pass, any fetch
  failure keeps it; after that, a pass whose only non-final objects are
  fetch failures counts them `unreachable`, logs one warning naming their
  ap ids, changes nothing for them, and clears the mark. A failed write
  keeps the mark however old it is, and a fetch failure never deletes or
  removes anything. A new mark restarts the 72 hours. Each outcome is
  idempotent, so processing is eventual, not exactly-once: objects handled
  before a restart are fetched again. Operator and label withholds are not
  touched. A pass stops when the community is removed again, checked before each
  fetch and again before applying its answer, and never clears the marker of
  a removed community. Counters
  `tidepool_community_reconcile_{live,restored,deleted,removed,retry,
  unreachable,passes_completed}`; one `community reconcile pass` log line
  per pass.
- Labels stored before start are applied at start.

## Coves user origin (the `coves.social` AP surface)

Coves users get their own ActivityPub identities on `AP_USER_ORIGIN`. This is
a second origin on the same listener, separate from the bridge's
`BRIDGE_HOSTNAME`. A `Host` router splits them:

- The bridge hostname, its bridged-handle subdomains, `localhost`, loopback
  IPs, and requests with no `Host` (container healthchecks) reach the bridge.
- The user origin's `Host` reaches the user surface.
- Any other `Host`, including a public bare IP, gets 421 Misdirected Request.
  `AP_HOST_FALLTHROUGH_DEV` sends those to the bridge instead. It defaults on
  in development and is refused in production.
- When both names are the same authority (the dev default, `localhost:8091`),
  the split uses the path. The user surface answers first, and its 404s fall
  through to the bridge.

What the user origin serves:

| Route | Purpose |
|---|---|
| `GET /.well-known/webfinger?resource=acct:alice@…` | Discovery for a minted local part, scoped to the request's `Host` |
| `GET /ap/actor/{did}` | The user's `Person` document (`publicKey`, `inbox`, `endpoints.sharedInbox`, `outbox`, `published`) |
| `GET /ap/actor/{did}/outbox` | An empty `OrderedCollection`. Lemmy requires the field and rejects an actor without it |
| `GET /ap/object/{did}/{collection}/{rkey}` | A native post or comment the bridge delivered, so peers can re-fetch it by id |
| `GET /ap/activity/{hash}` | An activity the bridge delivered |
| `POST /ap/inbox` | Shared inbox. Requests go unchanged to the bridge's ingest pipeline, with the same verification, dedupe and refusals, so bare content sent here is dropped like anywhere else |
| `GET /` | The origin's instance (`Application`) actor, using the bridge's key. Lemmy sends `Delete{Person}` and other send-to-all-instances activities only to this inbox |
| `GET /.well-known/nodeinfo`, `GET /nodeinfo/2.0` | `software.name: coves`, `metadata.nodeName: Coves` |

A tombstoned actor's objects and activities return 410.

An actor is minted the first time a user federates something. Its local part
is set once and never changes:

- A native handle (`alice.coves.social`) gives `alice`.
- Any other handle keeps its full name (`bretton.dev` stays `bretton.dev`).
- Collisions get `-2`, `-3`, and so on.
- A later handle change updates only the cached display name, summary and
  avatar.

The RSA private key is encrypted with `BRIDGE_KEK` (AES-256-GCM, bound to the
DID) and never stored in plain text. Only the public PEM is published.

### Federation opt-out

The opt-out is a Tidepool-owned record,
[`lexicons/social/coves/bridge/federation.json`](../lexicons/social/coves/bridge/federation.json)
(`key: literal:self`, one per repo). Federation is on by default. A repo with
no record is federated, and the record exists only to turn federation down.

With `CONSUMER_ENABLED` on, the Jetstream consumer reads and enforces it:

- `enabled: false` is a soft disable. The actor stops resolving through
  WebFinger and nothing more is delivered. The actor document and
  already-federated references stay.
- Deleting the record or writing `enabled: true` turns federation back on
  under the same actor, because the local part never changes.
- `deleteRemote: true` is permanent. The bridge sends
  `Delete{Person, removeData: true}` to every inbox that received the user's
  content, sends `Undo` for votes peers still hold, tombstones the actor
  (410), and marks the identity purged. Re-enabling is refused afterwards.

With `CONSUMER_ENABLED` off (the default), nothing reads the record and
nothing is delivered outbound. Actors already disabled or tombstoned stay
that way. The lexicon is still published so the Coves
settings UI can write against a stable shape.
