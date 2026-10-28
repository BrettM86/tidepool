# Tidepool architecture and design decisions

Tidepool is a two-way bridge between the threadiverse (Lemmy, PieFed, Mbin, and
anything else that speaks FEP-1b12 group federation) and Coves.

- Inbound: fediverse communities, users, posts and comments become
  `social.coves.*` records in repos Tidepool hosts, so the Coves AppView indexes
  them the same way it indexes native content.
- Outbound: native Coves users take part in bridged communities. Their posts,
  comments, votes, edits and deletes go back to the origin instance, and they
  appear on the fediverse as `@name@coves.social`. The outbound side lives in
  `internal/consume` (Jetstream consumer), `internal/accept` (acceptance
  engine) and `internal/outbound` (translation and delivery queue).
- Not yet built: communities created natively on Coves do not federate. Making
  them followable AP Groups is Scope B. Decisions 10 to 20 are written so that
  Scope B adds code without migrating schemas or URLs.

Decisions are numbered. Code comments and other docs cite them by number, so
the numbers stay fixed even when a decision is superseded.

## The materialization principle

Materialize as PDS records the things other records may need to reference:
communities, author profiles, posts and comments. Keep things that are only
ever counted (votes) as bridge-side state. Nothing ever strongRefs a vote.

## Architecture

Inbound:

```
 Lemmy / PieFed                     Tidepool                            Coves
┌──────────────┐   Announce   ┌──────────────────┐  subscribeRepos  ┌─────────┐
│ community    │─────────────▶│ inbox + sig      │────────────────▶│ relay + │
│ Group actor  │   (push)     │ verify           │   (CBOR frames) │Jetstream│
│              │              │        │         │                 └────┬────┘
│ outbox/API   │◀─────────────│ fetcher (signed  │                      │ JSON
│              │  backfill    │ GET, webfinger)  │                 ┌────▼────┐
└──────────────┘   (pull)     │        │         │                 │ AppView │
                              │ materializer     │                 │ postgres│
                              │  AP → coves recs │                 └────▲────┘
                              │        │         │                      │
                              │ virtual PDS      │  getVoteAggregates   │
                              │  (repos, MST,    │──────────────────────┘
                              │   did:plc, keys) │   (side channel)
                              └──────────────────┘
```

The relay and Jetstream are self-hosted and carry only Tidepool's and Coves'
own PDS hosts. See [SELF_HOSTED_RELAY.md](SELF_HOSTED_RELAY.md) for why.

Outbound:

```
 Native user PDS ─────────┐
 (postv2, comments, votes │ subscribeRepos          Coves
  in the author's own     ▼                    ┌────────────┐
  repo)             ┌──────────┐   Jetstream   │  AppView   │
                    │  relay   │──────────────▶│  postgres  │
                    └──────────┘               └────────────┘
                          ▲   (acceptance and removal records from
         subscribeRepos   │    Tidepool's community repos travel
                    ┌─────┴────────────────────┐  the same path)
                    │        Tidepool          │
                    │  virtual PDS, acceptance │◀── Jetstream (postv2 into
                    │  engine, consumer,       │    bridged communities,
                    │  outbound queue,         │    comments, votes,
                    │  AP Person actors        │    profiles, opt-outs)
                    └───────────┬──────────────┘
           Create{Page|Note}    │      ▲  Announce (our own echoes are
           Like/Dislike/Undo    ▼      │  suppressed), replies, mod actions
                    ┌──────────────────┴───────┐
                    │      Lemmy / PieFed      │
                    └──────────────────────────┘
```

## Core decisions

1. Go, module `tidepool`, using `github.com/bluesky-social/indigo` for repo,
   MST, CAR, TID and DID primitives. bridgy-fed, arroba and granary (CC0) are
   the reference for Lemmy quirk handling. Port their handling; do not
   rediscover it.
2. Tidepool is a virtual PDS. It signs commits and serves `com.atproto.sync.*`
   itself (the arroba model) instead of running a stock PDS. That gives one
   deployment, no per-account setup, and full control of the order records are
   emitted in.
3. Superseded by decision 20. Posts were first written into the community's
   repo with `author` set to the bridged user's DID, because the Coves post
   consumer required the repo DID to equal `record.community`. Coves then moved
   to author-owned posts. New posts now go into the author's repo. Posts written
   the old way stay in community repos. The rest of this decision still holds:
   comments go into the bridged user's repo, and profiles
   (`actor.profile`, `community.profile`, rkey `self`) are written before any
   content that references them, because the AppView rejects content whose
   community or author is not indexed yet.
4. Record keys are deterministic. Each rkey is a TID whose timestamp half comes
   from the AP object's `published` time and whose clock-ID bits come from a
   hash of the canonical AP id. Keys sort by time, are valid TIDs, and
   re-ingesting the same object produces the same at-uri.
5. The `ap_objects` mapping table is the spine:
   `ap_id ↔ (did, collection, rkey, at_uri, cid)`. Every materialization writes
   it and every strongRef resolution reads it. A missing parent triggers a
   recursive fetch and materialization of the ancestor chain, parents first.
6. Consent applies from the first record. `#nobridge` or `#nobot` in an AP
   actor's summary blocks materialization of that actor's content.
   `Delete(Actor)` tombstones their bridged repo. Bridged profiles carry a bio
   line naming the origin ("bridged from @alice@lemmy.world by Tidepool") and
   set `hostedBy` to the bridge's service DID.
7. Votes never become records. Inbound Like and Dislike activities update
   bridge-side tallies (`internal/votes`). Tidepool exposes them two ways: it
   folds the counts onto the bridged post or comment record as the optional
   `bridgedStats` field (`{upvotes, downvotes, asOf}`), and it serves them over
   the `social.coves.bridge.getVoteAggregates` XRPC for the AppView to poll.
8. Coves conventions apply: goose migrations, raw `database/sql` with `lib/pq`,
   chi router, `log/slog`, typed sentinel errors wrapped with `%w`, env-var
   config with logged dev defaults, `context.Context` everywhere, interfaces
   per domain package, and integration tests against real infrastructure.
9. Out of scope. Outbound federation for native users was deferred here at
   first and is now built (decisions 10 to 20). Still out of scope: key claiming
   or migration for bridged users (their keys are held in escrow so claiming
   can be built later), DMs, and Scope B. Software other than Lemmy is
   targeted through the FEP and tested only as decision 19's support matrix
   describes.

## Handles

Bridged atproto handles have the form
`<user label>.<instance host>.<HANDLE_ZONE_ROOT>`, for example
`alice.lemmy.world.ap.tdpl.io`. Tidepool's own DNS server (`internal/dns`)
answers for the zone. [HANDLE_SCHEME.md](HANDLE_SCHEME.md) has the full rules.
In short:

- The instance host is lowercased, dots kept, port dropped.
- ASCII user names are normalized. A non-ASCII name with no ASCII letters or
  digits uses an IDNA punycode label when valid, otherwise a stable hash label.
- A community label gets a `c-` prefix (`c-technology.lemmy.world.ap.tdpl.io`).
  A user label that would start with `c-` gets `u-` in front
  (`u-c-technology.lemmy.world.ap.tdpl.io`). The prefix counts toward the
  63-character label limit.
- Same-host collisions get `-2`, `-3` and so on at the end of the label
  (`c-technology-2`). Collisions come from user names that normalize alike, and
  from a `u-` prefixed label meeting a user whose own label already reads
  `u-c-...` (`u_c_tech` and `c_tech` both want `u-c-tech`; the later one gets
  `u-c-tech-2`). A user labelled `c` whose handle collides gets `u-c-2`.
- A punycode label is never suffixed or truncated, because that breaks its
  decoding. When it collides or does not fit, the hash label is used.

Older bridged actors have handles in the form `alice.lemmy-world.tdpl.io`.
`internal/handlemigration` renames them into the handle zone.

## Outbound decisions

Native Coves users take part in bridged communities in both directions. Their
fediverse identity is `@name@coves.social`. coves.social reverse-proxies the AP
surface (WebFinger, actors, objects, inboxes) to Tidepool.

Coves' move to author-owned posts made Scope B much smaller. A Lemmy user's
post into a native community would be a `postv2` in the bridged author's repo,
which Tidepool already signs, admitted by that community's own acceptance
engine on its own host. Nobody writes into a foreign community's repo. Scope B
is left with the Group-actor AP surface and the admission-policy wiring.

10. coves.social is the default AP origin for native users. AP has no portable
    identity layer. Fediverse software takes "the instance" from the HTTP origin
    of the actor id, so a bridge must serve every actor from an origin it
    controls. bridgy-fed collapses to brid.gy for the same reason. Tidepool
    collapses to coves.social and keeps provenance in the local part:
    - A native handle `alice.coves.social` federates as `alice`. Any other
      handle federates as the full handle: `bretton.dev` becomes
      `@bretton.dev@coves.social`, and `alice.bsky.social` becomes
      `@alice.bsky.social@coves.social` (the bridgy-fed precedent).
      Lemmy 0.19.20 applies no charset restriction to remote WebFinger
      resolution, and its mention regex accepts dots, so dotted local parts
      work. Hyphenated local parts resolve through search and WebFinger, but
      Lemmy's in-text mention regex does not match them; this only affects
      display. Lemmy stores a remote `preferredUsername` as `varchar(255)`, so
      local parts are capped to fit. Collisions get deterministic `-2`, `-3`
      suffixes.
    - The local part is fixed when the actor is created, which is the DID's
      first federating interaction. A later handle change updates only the
      served display name, so remote mentions and search keep working. There
      is no way to change an existing address. `ap_actors` holds one row per
      DID, and a destructive withdrawal cannot be undone, so deleting the
      presence does not free the name for a new one.
    - `ap_actors` stores each actor's full actor id, origin included. Serving,
      WebFinger, keyIds and outbound signing all derive from the stored id,
      never from a global origin constant. Per-user custom-domain federation
      (`@alice@bretton.dev`, for a user who proxies `/.well-known/webfinger` and
      `/ap/*` from their domain to the bridge) can therefore be added later as
      new rows with a different origin and no migration. It is deferred because
      it needs web configuration on the user's side, and moving an existing
      actor to a new origin is an AP identity change: remote instances see a
      new account. Default-origin assignments are permanent either way.
    - Local parts are unique per `(normalized_origin, local_part)`, not
      globally, in one namespace shared with future Group actors (Scope B).
      Every lookup carries the origin.
    - Actors are `Person` documents with ids keyed by DID, so they stay stable
      across handle changes: actor `{origin}/ap/actor/{did}`, objects
      `{origin}/ap/object/{did}/{collection}/{rkey}`, one shared inbox
      `{origin}/ap/inbox` (`internal/personas`).
    - Caddy on coves.social proxies `/.well-known/webfinger`,
      `/.well-known/nodeinfo`, `/ap/*`, and Accept-negotiated `GET /` (the
      instance actor, type `Application`, which avoids Lemmy's Instance-enum
      parsing trap) to the Tidepool container. tdpl.io stays the service origin
      for the inbound direction. Tidepool serves both origins from one listener
      and routes by Host header.
11. Federation is on by default for native users, with a two-tier opt-out
    stored as a repo record. Only interactions with bridged communities ever
    federate. Posting into a community that visibly lives on Lemmy is the
    consent act, unlike whole-account mirroring (the Bridgy Fed case), so the
    UI's job is disclosure rather than collecting consent. Mechanics:
    - Any DID that Tidepool did not mint federates when it interacts with a
      bridged community, unless it has opted out.
    - The AP actor is created lazily on the DID's first federating interaction
      (first postv2, comment or vote touching a bridged community). No
      interaction means no fediverse identity.
    - The opt-out is `social.coves.bridge.federation` (rkey `self`,
      `{enabled: bool, deleteRemote?: bool}`) in the user's own repo, so it is
      user-signed, portable and visible on the firehose. No record means
      enabled. `enabled=false` is a soft disable: new federation stops, pending
      outbound work is cancelled in the same transaction, and the actor document
      stays served. `enabled=true` or deleting the record re-enables.
    - `enabled=false, deleteRemote: true`, behind an explicit UI warning, sends
      `Delete{Person}` with Lemmy's `removeData: true` so remote content is
      purged. This is irreversible in practice (Lemmy restores an actor if it
      refetches it; other software may not). A confirmed account deletion takes
      the same path (decision 19).
    - Content written while opted out is never federated after re-enabling.
    - Posts by opted-out authors are rejected at admission (decision 13). They
      have no acceptance, so they are invisible in the community view on both
      sides, and Coves' `post.getStatus` reports the rejection. Comments by
      opted-out users are indexed in Coves but not federated, so a split thread
      only happens to users who chose it.
    - Consent is checked again at delivery time, not only at enqueue. An HTTP
      request already in flight is the one race that cannot be closed; the
      decision 19 reconciliation job reports it.
12. atproto first, with echo suppression. Native content commits to the user's
    repo first, so Coves indexes it at once through the firehose. AP delivery
    runs later from a durable outbound queue.
    - `ap_objects.origin` (`fediverse` or `bridge`) breaks the loop: objects
      with origin `bridge` are never re-materialized when the community
      Announces them back (`internal/echo`).
    - Outbound activity ids are deterministic. They derive from
      `(at-uri, operation, per-object sequence)` and are stored in outbound
      state. They never derive from the record CID, because Jetstream delete
      events carry no CID. Lemmy dedupes on activity id (its
      `received_activity` table, verified on 0.19.20), so redelivery is
      idempotent and our own echoes are recognizable.
    - Deterministic ids do not replace state. Deletes and Undo are built from
      the stored outbound state row (decision 14). Lemmy matches `Undo{vote}`
      by actor and object and requires the full inner vote embedded, not the
      activity id.
    - Delivery workers treat Lemmy's duplicate-activity error as success, since
      a crash between delivery and marking it delivered causes a redelivery.
13. Tidepool is the acceptance engine for bridged communities
    (`internal/accept`, `internal/acceptrec`). A native user posts to a bridged
    community by writing `social.coves.community.postv2` into their own repo
    through their own OAuth session. Tidepool never sees the write directly. It
    arrives through the Jetstream consumer (decision 14). Tidepool holds the
    community's key, runs admission, and writes a
    `social.coves.community.acceptance` record into the bridged community's
    repo. Community views render from acceptance records only, so acceptance is
    the visibility gate on both sides.
    - Admission policy for bridged communities: the author is a native DID that
      has not opted out and is not paused (decision 11; the actor is created on
      first acceptance); the author is not banned in the community (decision
      18's Block state); the parent is not locked; a title is present and fits
      200 characters after truncation rules. Lemmy requires `name`, while the
      postv2 title is optional, so media-only posts without a title are
      rejected with their own recorded reason.
    - Acceptance mechanics match Coves' engine. The rkey is the unpadded
      lowercase base32 of SHA-256 of the canonical subject at-uri (52
      characters). The strongRef pins URI and CID. When the author edits, the
      CID changes, admission runs again, the same rkey is repinned to the new
      CID, and `Update{Page}` is enqueued. A diff that touches only
      `bridgedStats` is repinned at once without re-admission (Coves' PRD
      §5.5 exception; Tidepool's own inbound vote sweeps use it, decision 20).
    - The `Create{Page}` enqueue runs as a transactional side effect of the
      acceptance commit (`PutRecordTx`), so acceptance and enqueue are atomic
      and posts that were not accepted never reach Lemmy. Moderation uses a
      multi-op commit (`ApplyOpsTx`): deleting the acceptance and writing the
      removal happen in one commit, as Coves does with `applyWrites`, so the
      firehose never carries a half-finished moderation action.
    - When the author deletes their post (Jetstream tombstone), Tidepool
      deletes the acceptance without writing a removal, because an author's
      deletion is not moderation, and enqueues a Delete to Lemmy from outbound
      state.
    - Blobs in native posts live on the author's PDS. Outbound image
      attachments are not built: image embeds are dropped
      (`internal/apobject/object.go` `linkAttachment`). See FOLLOWUPS.md.
    - Comments and votes stay in users' own repos and also arrive through
      Jetstream.
    - Coves' acceptance consumer fetches an unindexed subject directly from the
      author's PDS with `com.atproto.repo.getRecord` and verifies the pinned
      CID. Tidepool serves that endpoint for its hosted repos
      (`internal/sync`), which lets bridged-author posts reach Coves without
      full relay coverage.
14. One Jetstream consumer with durable outbound state (`internal/consume`).
    The consumer persists its cursor per consumer version, tolerates
    Jetstream's full replay on a quiet stream, and ports the Coves connector's
    discipline: record-revision gates and a persistent dead-letter store with
    redrive. It handles:
    - the federation opt-out lifecycle, plus lazy actor creation on a DID's
      first federating interaction;
    - `actor.profile`, which only refreshes the served actor document. Lemmy
      has no `Update{Person}` handler (verified on 0.19.20 and 1.0 main) and
      refreshes remote profiles through its own refetch within 24 hours, so
      Tidepool does not deliver an activity Lemmy would reject;
    - `community.postv2` targeting a bridged community, which goes to the
      acceptance engine (decision 13);
    - `community.comment` in bridged communities by authors who have not opted
      out, which becomes `Create`, `Update` or `Delete{Note}`;
    - `feed.vote` on bridged subjects, with the same opt-out gate, which
      becomes `Like`, `Dislike` or `Undo`;
    - `#account` lifecycle events (decision 19).

    Jetstream delete events carry no record body or CID, so every create and
    update handler stores an outbound state row keyed by at-uri (AP object id,
    last CID, community target, translated snapshot, last activity id). Votes
    have their own state keyed by `(actor_did, subject_at_uri)` (direction,
    current activity id, delivered status). Deletes and Undo are built from
    that state inside a transaction. The consumer skips all Tidepool-hosted
    repos, because their outbound work is enqueued at write time (decision 13).
15. The outbound queue follows the inbound queue's discipline and is split into
    canonical activities and per-inbox deliveries (`internal/outbound`).
    - `outbound_activities` holds one canonical payload per activity id.
      `outbound_deliveries` holds one row per `(activity, target inbox)` with
      its own claim, attempt and poison state. A single globally unique
      activity row could not represent the fan-out that `Delete{Person}` and
      Scope B need.
    - Delivery is at-least-once with stable ids the receiver can see.
    - Ordering is per community, plus explicit causal parent dependencies. A
      child is not eligible until its parent's mapping and delivery state
      exist. A poisoned parent poisons its descendants with a distinct reason.
    - Claims are fenced with `claimed_until`. Retries use exponential backoff.
      Failures are classified into a poison taxonomy. A 429 is retried with
      backoff; outbound has no per-host rate limiter of its own.
    - Target inboxes come from the community Group's actor document, cached
      with a TTL and refreshed once on 404 or 410 before a delivery is
      poisoned, so an endpoint change does not become poison. A 401 is retried
      as an auth failure. Unfollowing a community does not cancel its pending
      deliveries; cancel them with `POST /admin/outbound/cancel` or park them
      with the per-community kill switch.
    - Each native actor signs with its own RSA key, sealed under `BRIDGE_KEK`
      (`internal/identity/actor_rsa.go`). The key version columns exist, but
      rotating one actor's key is not built. The inbound service actor's RSA key
      is still stored as plaintext PEM.
    - The wire format follows pinned Lemmy 0.19.20: draft-cavage RSA-SHA256
      signing `(request-target) host date digest`, Digest required, a one-hour
      expiry window. A Page is addressed `to: [group, as:Public]`; a Note is
      addressed `to: [as:Public]`, `cc: [group]`. Both carry
      `audience: group`, a single-string `attributedTo`, HTML `content` and
      markdown `source`. Person actors include `outbox` and are type `Person`,
      because Lemmy rejects votes from `Service` actors as bots.
    - Delivery is FEP-1b12: activities go to the community Group's inbox, and
      Lemmy fans out the Announce. Announces come back to Tidepool only because
      the inbound service actor follows every bridged community. That Follow is
      also the outbound side's return channel, so it must stay in place.
16. Native votes are `social.coves.feed.vote` records in users' repos, read
    from Jetstream and sent out per actor. Inbound fediverse votes still never
    become records (decision 7). The outbound vote row from decision 14, keyed
    `(actor_did, subject_at_uri)`, holds the current intended vote and its
    delivery state. The direction changes when the new vote is queued, not
    when it is delivered; FOLLOWUPS.md lists the recast and reseed gaps this
    leaves.
    - The aggregator drops echoes of our own outbound Like and Dislike ids.
    - Outbound Undo embeds the full inner vote rebuilt from state. Lemmy
      matches by actor and object and fails to parse an Undo whose object is a
      bare URL (verified).
    - The Lemmy API count seeder subtracts the delivered tallies under the
      same aggregate lock as inbound votes, so a re-seed does not count our
      own votes twice in the normal case.
    - Counting queue history ("delivered Likes minus Undos") is not the
      mechanism, because it cannot model flips, poison or multiple
      destinations.
    - Known Lemmy 1.0 risk: per-instance `FederationMode` can silently drop
      remote votes, upvotes included. Divergence is reported in metrics rather
      than worked around.
17. Scope B guardrails. Everything in the reverse direction is kind-agnostic:
    `ap_actors` carries a kind (person or group), AP URL shapes and the shared
    inbox do not encode kind, the WebFinger local-part namespace is shared, the
    acceptance engine's admission policy is configured per community kind, and
    outbound translation dispatches on collection. Adding Group actors for
    Coves-native communities must not need a schema or URL migration.
18. Inbound moderation of native content uses its own authorization rule.
    For fediverse content, an announced Delete is authorized by repo membership
    in the announcing community: posts by `mapping.DID`, comments by their
    thread root's repo, plus a curated-follow allowance for targets with no
    mapping yet (`internal/ingest/consent.go`). Host authority
    (`SameAuthority`) applies only on the bare-delete path.
    Neither rule can authorize a Lemmy Group moderating an object from
    coves.social. A native object has no Lemmy community mapping to be a member
    of, and its host is ours, not the moderator's. The unmapped allowance must
    not be extended to it; that allowance exists to close the
    delete-before-create race for content the community already owns.
    - Native content gets its own rule: the verified Announce signer must be
      the followed community Group that owns the target's community mapping.
    - The moderation activities handled are a `Delete` with a `summary` and
      its `Undo`, `Lock`/`Undo{Lock}`, and `Block`/`Undo{Block}`. `Remove` is
      not handled.
    - Lemmy's Delete-summary convention applies: a `summary` that is present,
      even empty, means a moderator removal; an absent one means a self-delete.
      Tidepool's outbound self-deletes omit `summary`, or Lemmy would check them
      as a failed moderator action (verified on 0.19.20).
    - A moderated native post is not deleted anywhere. The post lives in the
      author's repo, which Tidepool cannot touch. Moderation acts on the
      acceptance state Tidepool owns: one multi-op commit in the community repo
      deletes the acceptance and writes a `social.coves.community.removal`
      record (digest rkey, `code` from its open knownValues set such as
      `moderator-discretion` or `author-banned`). A removal is scoped to the
      URI and stands across author edits. `Undo{Delete}` restores the post with
      one commit that deletes the removal and writes a fresh acceptance.
    - A moderated native comment is recorded bridge-side under the same removal
      code (`internal/ingest/moderation.go`). Nothing is written to the
      community repo, because the removal lexicon covers posts only. Extending
      it to comments is a cross-repo item with Coves.
19. Account lifecycle, support matrix and rollout safety.
    - An `#account` event's status is not proof of deletion. Temporary states
      (deactivated, suspended, takendown, throttled) pause the actor's
      outbound work. Only a confirmed permanent deletion (status `deleted`,
      re-checked against the DID's current PDS and PLC state) triggers the
      terminal `Delete{Person}` (`internal/optout`).
    - Support matrix. Lemmy 0.19.20 is the pinned strictness ceiling and the
      e2e target. Mbin is covered by hand-built fixtures for link posts only.
      PieFed is untested: there are no captured fixtures. Its votes arrive from
      anonymous per-user actors, which is fine for tallies. Pylova is untested.
      Lemmy 1.0-beta is tracked but not targeted (vote `FederationMode`, inbox
      collapsing, NoteWrapper).
    - Rollout safety. Outbound has global, per-host, per-community and
      per-actor kill switches and a dry-run mode that claims and logs each
      delivery without sending (`internal/outbound/switches.go`). Translation
      already happened when the activity was queued. These are read at
      boot. The admin API can inspect, redrive and cancel the outbound queue
      (`/admin/outbound`). A reconciliation job compares atproto state with
      outbound state and reports divergence without changing anything
      (`/admin/divergence`).
    - Initial actor announcements need deliberate throttling: each new actor
      costs Lemmy a WebFinger and actor fetch back at coves.social, so a mass
      backfill from one host would flood it. Outbound has no announcement rate
      limiter. The levers are `OUTBOUND_WORKERS` and the scoped kill
      switches.
    - Defederation coupling. Lemmy blocks are per domain, and the bridge
      presents two domains with different jobs: users at coves.social and the
      community-following reader at tdpl.io. Blocking one leaves a silent
      half-federated split; for example, with coves.social blocked, reads keep
      flowing while users' posts disappear from their own view. Policy: when
      a block of either origin is detected, stop both directions for that
      host. The per-host kill switch only stops outbound; stopping inbound
      means unfollowing that host's communities. A bridge that
      half-honors defederation looks like block evasion, and that perception is
      the real defederation risk. Lemmy does not federate its blocklist, so
      detection is empirical: delivery rejections, rejected follows, and inbox
      403 spikes in the reconciliation metrics. Detection does not engage the
      switch automatically; an operator sets it.
20. Inbound posts are written as `postv2` plus acceptance; older posts are left
    in place. This supersedes decision 3's post placement.
    - The materializer writes each new bridged post as
      `social.coves.community.postv2` into the bridged author's repo, the same
      placement as comments, with the same deterministic TID rkeys. The record
      has no `author` field; the repo is the author. Tidepool fills
      `originalAuthor` and `federatedFrom` (both unconstrained in the lexicon,
      so Tidepool defines their shape) and `bridgedStats`.
    - Every post materialization then writes the acceptance record into the
      community repo (digest rkey, strongRef with CID). Parents-first ordering
      becomes: author profile, then postv2, then acceptance.
    - Lemmy moderator removals of fediverse posts use decision 18's
      acceptance-delete plus removal commit instead of deleting the record.
      Upstream edits repin the acceptance. Tidepool's `bridgedStats` vote
      sweeps use the bridgedStats-only repin with no re-admission.
    - Coves trusts bridged posts by the author repo's PDS, so Tidepool's hosts
      must be listed in Coves' `TRUSTED_BRIDGE_PDS_HOSTS`.
    - Existing bridged posts in the legacy `social.coves.community.post`
      collection are not migrated. Both collections coexist in Coves, every
      path that depends on the era (edit, delete, moderation, vote sweep)
      dispatches on the mapping's collection, and no legacy record is
      rewritten. As a result, Coves' legacy consumer branch stays live while
      bridged legacy posts exist, which Coves has accepted. A migration would
      need a remap decision from Coves and an old-to-new ledger; it is
      recorded in [FOLLOWUPS.md](FOLLOWUPS.md), and nothing depends on it.
