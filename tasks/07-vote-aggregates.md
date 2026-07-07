# Task 07 — Vote aggregation side channel (~800 LOC)

## Goal
The deliberately-unmaterialized data path: Lemmy Like/Dislike activities
become bridge-side aggregate counts served over one small versioned XRPC.
No PDS records, no firehose traffic. (Materialization principle: nothing
ever strongRefs a vote.)

## Deliverables
- Migration `vote_aggregates`: subject_ap_id, subject_at_uri, upvotes,
  downvotes, updated_at, PK subject_ap_id; plus `vote_events` dedupe table
  (activity_id unique, voter_ap_id, subject_ap_id, direction, undone bool)
  — needed because Lemmy sends `Undo{Like}` and voters flip votes; count
  distinct voters' latest state, don't blindly increment.
- `internal/votes/aggregator.go` — implements the interface stubbed in
  task 06: handle `Like`, `Dislike`, `Undo{Like|Dislike}`; recompute or
  incrementally maintain counts per subject; only for subjects present in
  `ap_objects` (votes on unbridged content: count into a pending bucket or
  drop — DROP in v1, log at debug).
- Backfill hook: Lemmy `Page`/`Note` objects don't carry counts via AP
  reliably, but the Group outbox Announces historical Likes sparsely.
  Accept that backfilled posts start near zero; document the limitation.
  (Optional if trivial: seed from Lemmy's public API `counts` field during
  task 06 backfill via a `SeedCounts(subject, up, down)` method — gate
  behind config `SEED_COUNTS_FROM_API`, default on.)
- `internal/votes/xrpc.go` — the sanctioned side channel:
  `GET /xrpc/social.coves.bridge.getVoteAggregates?uris=at://...,at://...`
  (≤100 uris) → `{aggregates: [{uri, upvotes, downvotes, updatedAt}]}`.
  Write the lexicon JSON for it under `lexicons/social/coves/bridge/`
  (new nsid — this is Tidepool's published contract; versioned via the
  nsid, breaking changes mean a new name). Public read, cache headers,
  rate limit by IP.
- Tests: vote → count; flip vote → net change correct; undo → decrement;
  duplicate activity id → no-op; 100-uri batch query; unknown uri → omitted
  from response (not an error).

## Definition of done
- Fake-Lemmy E2E: Announce{Like} ×3 + Dislike ×1 + Undo ×1 → XRPC returns
  {up:2, down:1} (or per fixture design).
- Lexicon file validates and is documented in README as the AppView
  integration point.
- `go test ./...` green.

## References
- Coves vote semantics: `~/Code/coves/internal/atproto/lexicon/social/coves/feed/vote.json`,
  `vote_consumer.go` (what natives do; we deliberately bypass it).
- PLAN.md decision 7.
