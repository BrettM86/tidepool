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

## Sections (the loop iterates over these, in order)

| # | Task file | What | ~LOC | Depends on |
|---|-----------|------|------|-----------|
| 1 | tasks/01-scaffold-storage.md | Repo scaffold, config, errors, migrations, mapping-table store | 900 | — |
| 2 | tasks/02-ap-protocol.md | AP vocab types, WebFinger, signed fetch, collection paging | 1000 | 1 |
| 3 | tasks/03-identity-repos.md | did:plc minting, key custody, virtual repo layer (MST/CAR commits) | 1200 | 1 |
| 4 | tasks/04-sync-firehose.md | `com.atproto.sync.*` XRPC + subscribeRepos WebSocket serving | 1000 | 3 |
| 5 | tasks/05-materializer.md | AP → `social.coves.*` translation, rkeys, strongRef resolution | 1200 | 2,3 |
| 6 | tasks/06-ingestion.md | Inbox + sig verification, Follow lifecycle, Announce handling, backfill, consent | 1200 | 5 |
| 7 | tasks/07-vote-aggregates.md | Vote ingestion, aggregate store, side-channel XRPC | 800 | 6 |
| 8 | tasks/08-e2e-harness.md | docker-compose with real Lemmy + PLC + Jetstream, E2E tests, lexicon conformance | 1000 | 4,6 |

### v1.1 sections (added 2026-07-10 — FOLLOWUPS backlog + relay pipeline)

| # | Task file | What | ~LOC | Depends on |
|---|-----------|------|------|-----------|
| 9 | tasks/09-e2e-relay.md | BigSky relay in e2e; full pipeline Lemmy → bridge → relay → Jetstream; real requestCrawl | 600 | 8 |
| 10 | tasks/10-e2e-scenarios.md | Scenario completion: image, consent, Delete(Actor), unsubscribe, community update, vote hammer, suite-end sweep | 800 | 9 |
| 11 | tasks/11-hardening.md | Pre-internet-facing hardening: rate limits, #account frame, ordering gaps, pruners, housekeeping | 1000 | 10 |
| 12 | tasks/12-perf-scale.md | MST cache, getRepo streaming/reachable-set, blocks GC, ClaimNext — prerequisite for any votes-as-records revisit | 800 | 11 |

## Loop protocol

Each iteration (one section per iteration):

1. Read `LOOP_STATE.md`; pick the first task not marked `done`. If all are
   `done`, stop the loop.
2. Mark it `in-progress`. Spawn a Fable implementation agent with the task
   file, PLAN.md, and pointers to Coves/bridgy-fed reference code. The agent
   implements the section, keeps `go build ./... && go vet ./... && go test
   ./...` green, and commits nothing itself.
3. Review the working-tree diff with the **second-opinion** skill.
4. Fix confirmed findings (subagents for independent fixes), re-verify build
   and tests.
5. Commit with a descriptive message, update `LOOP_STATE.md` (status, commit
   hash, notes for the next iteration — surprises, deferred TODOs, interface
   changes later sections must know about).
6. Schedule the next iteration.

Reference material: `~/Code/coves` (the AppView; lexicons at
`internal/atproto/lexicon/social/coves/`, consumers at
`internal/atproto/jetstream/`), `~/Code/bridgy-fed`, `~/Code/granary`
(AP↔atproto translation), `~/Code/arroba` (virtual PDS in Python).
