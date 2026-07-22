# Tidepool — ActivityPub → atproto bridge for Coves

Tidepool bridges the threadiverse (Lemmy, PieFed, Mbin — anything speaking
FEP-1b12 group federation) into atproto as `social.coves.*` records, so the
Coves AppView indexes fediverse communities exactly as it indexes native ones.

**v1 scope is READ-ONLY**: Lemmy content flows in; nothing Coves users write
flows out. No AP representation of Coves users exists. The only outbound AP
activities are `Follow` (community subscription, sent by the bridge's own
service actor) and signed `GET` fetches.

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
