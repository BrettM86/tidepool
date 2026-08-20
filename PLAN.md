# Tidepool — ActivityPub → atproto bridge for Coves

Tidepool bridges the threadiverse (Lemmy, PieFed, Mbin — anything speaking
FEP-1b12 group federation) into atproto as `social.coves.*` records, so the
Coves AppView indexes fediverse communities exactly as it indexes native ones.

**v1 scope is READ-ONLY**: Lemmy content flows in; nothing Coves users write
flows out. No AP representation of Coves users exists. The only outbound AP
activities are `Follow` (community subscription, sent by the bridge's own
service actor) and signed `GET` fetches.

**v2 (Scope A, added 2026-07-17) makes the bridge two-way**: Coves users
(federation default-on, opt-out honored — decision 11) participate in
bridged Lemmy communities — posts, comments, votes,
edits, deletes flow back to Lemmy, and those users appear on the fediverse
as `@name@coves.social`. See §v2 below. Scope B (Coves-native communities
as followable AP Groups) stays deferred, but every v2 design choice must
remain Scope-B-compatible.

## The materialization principle

Materialize as PDS records exactly the things other records may need to
reference (communities, author profiles, posts, comments). Keep things that
are only ever aggregated (votes) as bridge-side state exposed through one
sanctioned side-channel XRPC. Nothing ever strongRefs a vote.

## Architecture

```
 Lemmy / PieFed                     Tidepool                            Coves
┌──────────────┐   Announce   ┌──────────────────┐  subscribeRepos  ┌─────────┐
│ community    │─────────────▶│ inbox + sig      │────────────────▶│Jetstream│
│ Group actor  │   (push)     │ verify           │   (CBOR frames) │         │
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

## Design decisions (locked)

1. **Go**, module `tidepool`, using `github.com/bluesky-social/indigo` for
   repo/MST/CAR/TID/DID primitives. bridgy-fed + arroba + granary (CC0,
   cloned as siblings in `~/Code/`) are the compat encyclopedia — port their
   Lemmy quirk handling, don't rediscover it.
2. **Virtual PDS, not a stock PDS**: Tidepool signs commits and serves
   `com.atproto.sync.*` itself (the arroba model). One deployment, no
   per-account ceremony, full control of emission order.
3. **Posts are written into the community's repo** with `author` set to the
   bridged user's DID — this is what the Coves post consumer validates
   (repo DID must equal `record.community`). Comments are written into the
   bridged user's repo. Profiles (`actor.profile`, `community.profile`,
   rkey `self`) are written before any content that references them, since
   the AppView rejects content whose community/author isn't indexed yet.
   **[SUPERSEDED 2026-08-11 by decision 20 / task 19: Coves flipped to
   author-owned posts (`postv2` in the author's repo + a community-signed
   `acceptance` record; PRD_AUTHOR_OWNED_POSTS.md, merged 2026-08-09).
   NEW posts go to the bridged AUTHOR's repo like comments; the community
   repo holds acceptance/removal records. Existing legacy posts stay put —
   both collections coexist in Coves indefinitely (no migration planned).]**
4. **Deterministic rkeys**: TID whose timestamp half comes from the AP
   object's `published` time and whose clock-ID bits come from a hash of the
   canonical AP id. Sortable, format-valid, and idempotent re-ingestion
   produces the same at-uri.
5. **`ap_objects` mapping table is the spine**: `ap_id ↔ (did, collection,
   rkey, at_uri, cid)`. Every materialization writes it; every strongRef
   resolution reads it. Missing parents trigger recursive fetch-and-
   materialize of the ancestor chain (parents first).
6. **Consent from day one**: `#nobridge`/`#nobot` in an AP actor's summary
   blocks materialization of that actor's content; `Delete(Actor)` tombstones
   their bridged repo. Bridged profiles are visibly labeled with a bio line
   linking to the origin ("bridged from lemmy.world by Tidepool") and
   `hostedBy` = the bridge's service DID.
7. **Votes never become records.** Like/Dislike activities update bridge-side
   aggregate counts served over a small versioned XRPC
   (`social.coves.bridge.getVoteAggregates`) that the AppView may poll.
8. **Coves conventions apply**: goose migrations, raw `database/sql` +
   `lib/pq`, chi router, `log/slog`, typed sentinel errors wrapped with `%w`,
   env-var config with logged dev defaults, `context.Context` everywhere,
   interfaces per domain package, real-infrastructure integration tests.
9. **Deferred (explicitly out of v1)**: outbound Coves→Lemmy direction, AP
   actors for Coves users, key claiming/migration for bridged users
   (escrow the keys, build claiming later), moderation federation, DMs,
   PieFed/Mbin quirk testing (target the FEP; verify against Lemmy only).
## v2 — Scope A write-back (added 2026-07-17)

Opted-in Coves users participate in bridged Lemmy communities in both
directions. Their identity on the fediverse is `@name@coves.social` —
coves.social reverse-proxies the AP surface (WebFinger, actors, objects,
inboxes) to Tidepool. Scope B (Coves-native communities as AP Groups so
Lemmy users can follow/post) is still deferred; decisions 10–20 are written
so Scope B is purely additive. NOTE (2026-08-11): the author-owned-posts
flip DISSOLVED Scope B's hardest blocker — a Lemmy user's post into a
native community is now just a postv2 in the bridged author's repo (which
Tidepool already signs) admitted by that community's OWN acceptance
engine on its own host; no one ever writes into a foreign community repo.
Scope B shrinks to the Group-actor AP surface + admission-policy wiring.

### v2 architecture

```
 Native user PDS ─────────┐
 (postv2, comments, votes │ subscribeRepos          Coves
  ALL in the author's     ▼                    ┌────────────┐
  own repo)         ┌──────────┐   Jetstream   │  AppView   │
                    │  relay   │──────────────▶│  postgres  │
                    └──────────┘               └────────────┘
                          ▲   (acceptance/removal records from
         subscribeRepos   │    tidepool's community repos ride
                    ┌─────┴────────────────────┐  the same firehose)
                    │        Tidepool          │
                    │  virtual PDS + ACCEPTANCE│◀── Jetstream (postv2 →
                    │  engine │ consumer       │    bridged communities,
                    │  outbound queue          │    comments, votes,
                    │  AP person actors        │    profiles, opt-outs)
                    └───────────┬──────────────┘
           Create{Page|Note}    │      ▲  Announce (echo — suppressed),
           Like/Dislike/Undo    ▼      │  replies, mod actions
                    ┌──────────────────┴───────┐
                    │      Lemmy / PieFed      │
                    └──────────────────────────┘
```

REVISED 2026-08-11: Coves' author-owned-posts flip (postv2 + acceptance,
merged 2026-08-09) landed after the 2026-07-30 codex revision of this
section. Decision 13's write surface is DELETED (nothing writes posts
into community repos anymore — on either side); decision 20 and task 19
add the inbound flip (new posts only — no migration; decision 20);
decisions 12/14/18 are amended in place.

### v2 design decisions (locked)

10. **coves.social is the DEFAULT user-facing AP origin.** AP has no
    portable identity layer: fediverse software derives "the instance"
    from the HTTP origin of the actor id, so a bridge must serve every
    actor from an origin it controls — domain collapse is a protocol
    constraint, not a choice (bridgy-fed collapses to brid.gy the same
    way). We collapse to coves.social but preserve provenance in the
    local part, and we keep per-user vanity origins additive:
    - Local part: a native handle `alice.coves.social` federates as
      `alice`; any other handle federates as the FULL handle —
      `bretton.dev` → `@bretton.dev@coves.social`,
      `alice.bsky.social` → `@alice.bsky.social@coves.social`
      (bridgy-fed precedent; provenance survives in the name).
      VERIFIED (2026-07-30, Lemmy 0.19.20 source): remote webfinger
      resolution applies no charset restriction and Lemmy's mention
      regex accepts dots — dotted local parts stand. Hyphenated parts
      resolve via search/webfinger but are NOT matched by Lemmy's
      in-text mention regex (display nit only); remote
      preferredUsername is varchar(255) Lemmy-side — cap local parts
      accordingly. Collisions get deterministic `-2`/`-3` suffixes.
      The local part is FROZEN at actor creation — the DID's first
      federating interaction (product decision 2026-07-30, mint
      trigger updated 2026-08-12): handle changes update the served
      display name
      only, so remote mentions and search stay stable; a user who
      wants a new fediverse name deletes and re-creates their
      presence.
    - `ap_actors` stores each actor's FULL actor id, origin included;
      serving, webfinger, keyIds, and outbound signing all derive from
      the stored id, never from a global origin constant. Per-user
      custom-domain federation (`@alice@bretton.dev`, for a user who
      proxies `/.well-known/webfinger` + `/ap/*` from their domain to
      the bridge; TLS via the existing on-demand tls-ask path) is then
      a deferred ADDITIVE feature — new rows with a different origin,
      zero migration. Deferred deliberately: it needs user-side web
      config, and changing an EXISTING actor's origin is an AP
      identity change (remote instances see a new account), so
      default-origin assignments are forever either way.
    Local parts are unique per `(normalized_origin, local_part)` —
    NOT globally — in one namespace shared with future Group actors
    (Scope B); every lookup carries the origin, and actor/object/
    activity URLs derive from the stored actor row, never from a
    global origin constant, so vanity origins stay purely additive.
    Actors are `Person` documents with ids keyed by
    DID (stable across handle changes): actor
    `{origin}/ap/actor/{did}`, objects
    `{origin}/ap/object/{did}/{collection}/{rkey}`, one shared inbox
    `{origin}/ap/inbox`.
    Caddy on coves.social proxies `/.well-known/webfinger`,
    `/.well-known/nodeinfo`, `/ap/*`, and Accept-negotiated apex `GET /`
    (the instance/Site actor — type `Application`, the Lemmy Instance-enum
    trap from task 10) to the tidepool container. tdpl.io stays the
    service origin for the inbound direction, unchanged. Tidepool serves
    both origins from one listener, routed by Host header.
11. **Federation is DEFAULT-ON for native users; opt-OUT as a repo
    record, in two tiers.** [REVERSED 2026-08-12 from opt-in —
    product decision: Coves users come expecting to browse and reply
    across networks, and ONLY bridged-community interactions ever
    federate — posting into a community that visibly lives on Lemmy
    IS the consent act, unlike whole-account mirroring (the Bridgy
    Fed case), so no separate ceremony is owed. The UI obligation
    becomes DISCLOSURE, not consent collection.] Mechanics: any
    non-Tidepool-minted DID interacting with a bridged community
    federates unless it has opted out. The AP actor is minted LAZILY
    on the DID's first federating interaction (first postv2/comment/
    vote touching a bridged community) — no interaction, no fediverse
    identity, ever. `social.coves.bridge.federation` (rkey `self`,
    `{enabled: bool, deleteRemote?: bool}`, user's own repo —
    user-signed, portable, firehose-visible) now expresses the
    EXCEPTION: absence of the record = enabled (the default);
    `enabled=false` → SOFT DISABLE (new federation stops, pending
    outbound cancelled atomically, actor doc stays served);
    `enabled=true` or deleting the record → (re-)enabled. The
    destructive tier (`enabled=false, deleteRemote: true`, behind an
    explicit UI warning) → `Delete{Person}` carrying Lemmy's
    `removeData: true` so remote content purges; documented
    irreversible (Lemmy un-deletes on actor refetch; other software
    may not). Account deletion → the same terminal path once
    confirmed (decision 19). Content authored while opted out is
    NEVER retroactively federated on re-enable. Posts by opted-out
    authors are REJECTED at admission (decision 13 — no acceptance,
    invisible in the community view on both sides, visible via
    Coves' post.getStatus); opted-out users' comments index in Coves
    but do not federate — the split-thread case is now scoped to
    users who explicitly chose it. Consent is re-checked at DELIVERY
    time, not only at enqueue; already-in-flight HTTP is the one
    unavoidable race (documented, reconciled by the decision-19
    job).
12. **atproto-first, echo-suppressed.** Native content commits to the
    repo first (Coves indexes instantly via the existing firehose); AP
    delivery is async from a durable outbound queue. `ap_objects.origin`
    (fediverse|bridge — the column reserved in task 01) is the loop
    discriminator: origin=bridge objects are never re-materialized when
    the community Announces them back. Outbound activity ids are
    DETERMINISTIC — derived from (at-uri, operation, per-object seq)
    and PERSISTED in outbound state, never derived from record CID
    (Jetstream delete events carry no CID) — so Lemmy-side dedupe
    (its `received_activity` table, keyed by activity ap_id; verified
    0.19.20) makes redelivery idempotent and our own echoes are
    recognizable. Deterministic ids are NOT a substitute for state:
    deletes and Undo are built from the persisted outbound state row
    (decision 14), and Lemmy matches Undo{vote} by actor+object with
    the full inner vote EMBEDDED — not by activity id. Delivery
    workers classify Lemmy's duplicate-activity error response as
    success (a crash between deliver and mark-delivered redelivers).
13. **Tidepool is the acceptance engine for bridged communities.**
    [REWRITTEN 2026-08-11 — the prior write-surface design (sessions,
    community credentials, createRecord forwarding) died with Coves'
    author-owned-posts flip; see git history for the old text.]
    Under PRD_AUTHOR_OWNED_POSTS: a native user posts to a bridged
    community by writing `social.coves.community.postv2` into their
    OWN repo via their own OAuth session — Tidepool never sees the
    write. It arrives via the Jetstream consumer (decision 14), and
    Tidepool — as the community's key holder — runs admission and
    writes the `social.coves.community.acceptance` record into the
    bridged community's repo through repo.Manager, natively signed.
    Community surfaces render from acceptance records only, so
    acceptance IS the visibility gate on both sides:
    - Admission policy for bridged communities: author is a
      non-opted-out, non-paused native DID (decision 11 — an
      opted-out author's post is REJECTED and stays invisible in
      Coves too, coherent by construction, surfaced via Coves'
      post.getStatus; the actor mints lazily on first acceptance);
      author not banned in the community (decision 18's Block state);
      parent not locked; title present (Lemmy requires `name`; postv2
      title is optional — media-only posts are REJECTED with a
      distinct recorded reason until a title-derivation product
      decision says otherwise) and ≤200 chars post-truncation rules.
    - Acceptance mechanics MATCH Coves' engine exactly: deterministic
      rkey = unpadded lowercase base32 of SHA-256 of the canonical
      subject at-uri (52 chars); strongRef pins URI + CID; author
      edit → CID mismatch → re-run admission → repin same rkey with
      the new CID (post editing is now REAL — the deferred-editing
      product decision of 2026-07-30 is VOID; Coves shipped the edit
      path) → enqueue `Update{Page}`; bridgedStats-only diffs repin
      synchronously without re-admission (the PRD §5.5 exception —
      applies to our own inbound vote sweeps, decision 20).
    - Outbound delivery for posts is enqueued via the PutRecordTx
      side-effect ON THE ACCEPTANCE COMMIT — acceptance and
      Create{Page} enqueue are atomic, and un-accepted posts never
      reach Lemmy. `DeleteRecordTx`/`DeleteRecordCAS` are still NEW
      in v2 (v1's DeleteRecord commits with a nil hook — verified)
      and land here, along with a MULTI-OP commit primitive
      (delete-acceptance + write-removal in ONE commit — Coves does
      this via applyWrites so the firehose never carries a
      half-completed moderation action; our repo layer must match).
    - Author deletes their post (Jetstream tombstone) → delete the
      acceptance (removal record NOT written — author deletion is
      not moderation, per the PRD) → enqueue Delete to Lemmy from
      outbound state.
    - No write surface, no sessions, no community credentials, no
      rkey rollout dependency — all obsolete. Blobs in native posts
      live on the AUTHOR's PDS; outbound Image attachments carry a
      URL resolved from the author PDS's getBlob (or the AppView
      CDN), not a Tidepool-served blob.
    Comments and votes are unchanged: users' own repos, via
    Jetstream. Convergence: Coves' acceptance consumer direct-fetches
    an unindexed subject from the author's PDS via
    `com.atproto.repo.getRecord` with pinned-CID verification —
    Tidepool already serves that endpoint for its hosted repos
    (sync/server.go), which is what makes bridged-author posts
    converge without full relay coverage.
14. **One Jetstream consumer + durable outbound state.** The consumer
    (cursor persisted per consumer-version, idempotent against the
    known quiet-stream full-replay quirk, and porting the Coves
    connector's FULL discipline — record-revision gates and a
    persistent dead-letter store with redrive, not just the cursor)
    handles: federation opt-out lifecycle (+ lazy actor minting on a
    DID's first federating interaction), `actor.profile` → served-actor-doc
    refresh ONLY (Lemmy has NO Update{Person} handler — verified
    0.19.20 and 1.0 main; remote profiles refresh via its lazy ≤24h
    actor refetch, and we do not deliver an activity that 400s),
    `community.postv2` targeting a bridged community → the acceptance
    engine (decision 13: admission → acceptance write → Create/Update
    {Page} enqueue rides the acceptance commit; author-delete
    tombstones → acceptance delete + Delete enqueue) [ADDED
    2026-08-11 — posts now arrive here like everything else],
    `community.comment` in bridged communities by non-opted-out
    authors → `Create/Update/Delete{Note}`, `feed.vote` on bridged
    subjects (same opt-out gate) → `Like/Dislike/Undo`, and #account
    lifecycle events → decision 19. Because Jetstream DELETE events
    carry no record body or CID, every create/update handler persists
    an outbound state row keyed by at-uri (AP object id, last CID,
    community target, translated snapshot, last activity id) — and
    votes get their own state keyed `(actor_did, subject_at_uri)`
    (direction, current activity id, delivered status). Deletes and
    Undo are built from that state, transactionally. The consumer
    skips all Tidepool-hosted repos — their outbound is enqueued at
    write time by decision 13.
15. **The outbound queue mirrors the inbound queue's discipline —
    split into canonical activities and per-inbox deliveries.**
    `outbound_activities` (one canonical payload per activity id) +
    `outbound_deliveries` (one row per (activity, target inbox) with
    independent claim/attempt/poison state) — the fan-out that task
    17's Delete{Person} and Scope B need; a single globally-unique
    activity row cannot represent it. Semantics are AT-LEAST-ONCE
    with stable receiver-visible ids, never exactly-once. Ordering
    key = community, plus EXPLICIT causal parent dependencies: a
    child is ineligible until its parent's mapping and delivery state
    exist; a poisoned parent poisons descendants with a distinct
    reason. claimed_until fencing, exp backoff, poison taxonomy,
    per-host rate limiting. Target inboxes are DISCOVERED from the
    community Group's actor document (cached with TTL; refreshed once
    on 401/404/410 before poisoning — endpoint rotation must not
    become poison); community deletion/unfollow cancels pending work.
    Per-actor RSA signing — keys sealed under BRIDGE_KEK via a NEW
    custodian surface with key metadata (version, created/retired)
    so rotation is definable (v1's custodian is K256-only and the v1
    service RSA key is stored in the CLEAR — verified; task 13 fixes
    this for all new keys). Wire contract per the pinned-Lemmy
    verification (0.19.20): draft-cavage RSA-SHA256 with
    `(request-target) host date digest` signed and Digest required,
    1h expiry window; every Page/Note carries `to: [group,
    as:Public]`, `audience: group`, single-string `attributedTo`,
    HTML `content` AND markdown `source`; our Person actors MUST
    include `outbox` and be type Person (Service actors' votes are
    rejected as bots). Delivery is FEP-1b12: activities go to the
    community Group's inbox; Lemmy fans out the Announce — and
    inbound Announce delivery back to us exists ONLY because the v1
    service actor follows every bridged community (that Follow is
    v2's inbound channel too; keep the invariant).
16. **Votes write back per-actor and still never become atproto records**
    (decision 7 stands). The live outbound vote state (decision 14's
    `(actor_did, subject_at_uri)` table — updated on delivery
    success, cleared/flipped only when the corresponding Undo or
    replacement vote succeeds) is the single source of truth: the
    aggregator drops echoes of our own outbound Like/Dislike activity
    ids, outbound Undo embeds the full inner vote reconstructed from
    state (Lemmy matches by actor+object and requires the embedded
    object — a bare-URL Undo fails to parse; verified), and the Lemmy
    API count seeder subtracts the live delivered tallies under the
    same aggregate-lock discipline as inbound votes, so re-seeds
    never double-count (FOLLOWUPS item closed by design here).
    Queue-history arithmetic ("delivered Likes minus Undos") is
    explicitly NOT the mechanism — it cannot model flips, poison, or
    multi-destination. Known 1.0 risk: per-instance FederationMode
    can silently drop remote votes (even upvotes); surface divergence
    in metrics, don't fight it.
17. **Scope B guardrails.** Everything reverse-direction is kind-agnostic:
    `ap_actors` carries kind (person|group), AP URL shapes and the shared
    inbox don't encode kind, the webfinger local-part namespace is shared,
    the acceptance engine's admission policy is per-community-kind
    configuration rather than person-only assumptions, and outbound
    translation dispatches on collection. Adding
    Group actors for Coves-native communities must require no schema or
    URL migration.
18. **Inbound moderation of native content is a new contract, not
    reuse.** v1 authorizes an announced Delete by REPO MEMBERSHIP in the
    announcing community — posts by `mapping.DID`, comments by their
    thread root's repo, plus a curated-follow allowance for targets it
    has no mapping for (verified `internal/ingest/consent.go`); host
    authority (`SameAuthority`) survives only on the BARE delete path.
    Neither can authorize a Lemmy Group moderating a coves.social-origin
    object: a native object has no Lemmy-community mapping to be a member
    of, and its host is ours, not the moderator's — so the membership
    rule has nothing to match and the unmapped allowance must NOT be
    extended to it (that allowance exists to close the
    delete-before-create race for content the community already owns).
    Scope B therefore rests on explicit checks, never on host equality:
    bridge-origin moderation gets its OWN rule — the verified Announce
    signer must be the followed community Group that owns the target's
    community mapping. New vocabulary is parsed explicitly —
    `Remove`/`Undo{Remove}`, `Lock`/`Undo{Lock}`, `Block`/`Undo{Block}`
    (v1 silently skips all of these) — plus Lemmy's Delete-summary
    convention: a PRESENT `summary` (even empty) means mod-removal,
    absent means self-delete; our outbound self-deletes must OMIT
    `summary` or Lemmy verifies us as a failed mod action (verified
    0.19.20). [AMENDED 2026-08-11 for author-owned posts:] Moderated
    native POSTS are no longer deleted from anywhere — the post lives
    in the AUTHOR's repo, untouchable, and moderation acts on the
    ACCEPTANCE state Tidepool owns: one MULTI-OP commit in the
    community repo deletes the acceptance and writes the REAL
    `social.coves.community.removal` record (deterministic digest
    rkey, `code` from its open knownValues set — e.g.
    `moderator-discretion`, `author-banned`), mirroring Coves'
    applyWrites atomicity so the firehose never carries a
    half-completed action. Removal is URI-scoped and TERMINAL across
    author edits; `Undo{Remove}` restore = one atomic commit deleting
    the removal and writing a fresh acceptance (no pre-removal
    snapshot machinery needed — the post record never moved).
    Moderated native COMMENTS keep the removal-record materialization
    into the community repo (the removal lexicon is post-scoped
    today; extending it to comment subjects is a named cross-repo
    item, not an invention here) — the Coves AppView hides them from
    the community view; the author's own profile keeps them.
19. **Account lifecycle, support matrix, rollout safety.** #account
    events carry a status that is NOT proof of deletion:
    transient/local states (deactivated, suspended, takendown,
    throttled) PAUSE the actor's outbound; only confirmed permanent
    deletion (status=deleted, re-verified against the DID's current
    PDS/PLC state) triggers the terminal Delete{Person}. Support
    matrix: Lemmy 0.19.20 is the pinned strictness ceiling and e2e
    target; PieFed is best-effort behind captured-wire conformance
    (its votes arrive from anonymous per-user actors — fine for
    tallies); Lemmy 1.0-beta is tracked, not targeted (vote
    FederationMode, inbox collapsing, NoteWrapper). Rollout safety:
    global / per-host / per-community / per-actor outbound kill
    switches, a dry-run translation mode, queue inspect / redrive /
    cancel admin surface, a reconciliation job comparing atproto
    state against outbound state, staged canary (one community
    first), and deliberate throttling of initial actor announcements
    — each new actor costs Lemmy a webfinger + actor fetch back at
    coves.social, so a mass backfill from one host is a self-inflicted
    thundering herd. DEFEDERATION COUPLING (added 2026-08-12): Lemmy
    blocks are per-domain and v2 presents two domains with different
    jobs — users at coves.social, the community-following reader at
    tdpl.io — so a block of one leaves a silent half-federated split
    (e.g. coves.social blocked: reads keep flowing while users' posts
    vanish from their POV). Policy: a DETECTED block of either origin
    (delivery rejections, Rejected follows, inbox 403 spikes — Lemmy
    does not federate its blocklist, so detection is empirical via
    the decision-19 reconciliation metrics) stands down BOTH
    directions for that host via the per-host kill switch, loudly in
    metrics — a bridge that half-honors defederation reads as block
    evasion, and that perception is the real defederation risk.
20. **Inbound posts flip to postv2 + acceptance for NEW posts; the
    legacy set is frozen, not migrated (added 2026-08-11; supersedes
    decision 3's post placement; task 19 executes it FIRST).** The
    materializer writes new bridged Lemmy posts as
    `social.coves.community.postv2` into the bridged AUTHOR's repo
    (same placement as comments; deterministic TID rkeys unchanged)
    — there is no in-record `author` field (authorship = repo), and
    Tidepool populates `originalAuthor`/`federatedFrom` (both
    deliberately unconstrained lexicon-side; WE define the first
    real shape — document it in the lexicon README as the bridge's
    published convention) plus `bridgedStats` as today. Every post
    materialization then writes the acceptance record into the
    community repo (digest rkey, strongRef with CID) — parents-first
    ordering gains a step: author profile → postv2 → acceptance.
    Lemmy mod removals of fediverse content switch from
    record-delete to the decision-18 acceptance-delete + removal
    multi-op commit; upstream edits repin acceptance; Tidepool's own
    bridgedStats vote sweeps use the PRD §5.5 bridgedStats-only
    synchronous repin (no re-admission). Coves-side trust: the
    BridgeTrust gate re-keys to the AUTHOR repo's PDS — Tidepool's
    hosts must be in TRUSTED_BRIDGE_PDS_HOSTS as author-repo hosts
    (config check, cross-repo). NO MIGRATION of existing bridged
    posts (product decision 2026-08-11): the deprecated and postv2
    collections coexist in Coves indefinitely; every era-sensitive
    path (edit, delete, moderation, vote sweep) dispatches on the
    mapping's collection PERMANENTLY, and no legacy record is ever
    rewritten. Recorded consequence: Coves' legacy-drain gate never
    fires while bridged legacy posts exist — its legacy consumer
    branch stays live, which Coves has accepted. A future migration
    (needs Coves' §11 remap decision + an old→new ledger) is
    deferred with rationale in FOLLOWUPS; nothing in v2 depends on
    it.
