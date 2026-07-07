# Task 05 — Materializer: AP → social.coves.* (~1200 LOC)

## Goal
The translation heart: given a fetched/delivered AP object, produce the
correct `social.coves.*` record(s) in the correct repo(s), idempotently,
with valid strongRefs. Pure translation + orchestration; transport is
tasks 02/06, repos are task 03.

## Deliverables
- `internal/materialize/actors.go` —
  - Lemmy `Person` → mint DID (task 03) if unseen →
    `social.coves.actor.profile` (rkey `self`): displayName from
    name/preferredUsername, bio from summary (HTML→text) **plus appended
    provenance line** "🌉 bridged from @user@instance by Tidepool",
    avatar/banner: fetch image, store as blob (add a minimal blob store to
    the repo layer if task 03 didn't: blobs table + `sync.getBlob`), set
    createdAt from `published`. Respect consent_state — `nobridge` actors
    are never materialized (their content dropped with a logged reason).
  - Lemmy `Group` → mint community DID → `social.coves.community.profile`
    (rkey `self`): name = preferredUsername, displayName = name,
    description = summary, `createdBy` = bridge service DID, `hostedBy` =
    bridge service DID, avatar/banner blobs.
  - Profile refresh: re-materialize on AP `Update{Person|Group}` or when
    profile_synced_at older than config TTL.
- `internal/materialize/posts.go` — Lemmy `Page` →
  `social.coves.community.post` **written into the community's repo**,
  fields per `~/Code/coves/internal/atproto/lexicon/social/coves/community/post.json`:
  community = community DID, author = bridged author DID, title = name,
  content from `source.content` (markdown) if present else HTML→markdown,
  embed union: image attachment → `embed.images` (blob), external link
  `attachment`/`url` → `embed.external` (fetch og-image optional, skip in
  v1), `sensitive` → contentLabels/nsfw label, langs from `language`,
  createdAt = published. Deterministic rkey (task 03 tid.go).
- `internal/materialize/comments.go` — Lemmy `Note` →
  `social.coves.community.comment` **in the author's repo**: content
  markdown, `reply.root` + `reply.parent` strongRefs resolved via
  `ap_objects` (`inReplyTo` chain). **Missing-parent protocol**: if parent
  ap_id unmapped, recursively fetch (task 02) and materialize ancestors
  first (depth cap ~50, cycle guard); root = walk to the Page. If an
  ancestor's author is nobridge/deleted, materialize a placeholder-free
  skip: drop the comment subtree (log).
- `internal/materialize/updates.go` — `Update` → re-put record same rkey;
  `Delete`/`Tombstone` → repo DeleteRecord + soft-delete mapping;
  `Delete(Actor)` → tombstone all their records + mark consent deleted.
- `internal/materialize/html.go` — HTML→markdown for Lemmy HTML content
  (they send both `content` HTML and usually `source` markdown; prefer
  source). Strip scripts, cap length per lexicon maxGraphemes, generate
  facets for links if cheap (else plain markdown text — check what Coves
  post consumer expects; content is markdown per lexicon).
- Validate every produced record against the actual lexicon JSON schemas —
  vendor `social/coves/*` lexicon files from Coves into `lexicons/`
  (script to re-sync: `scripts/sync-lexicons.sh`) and validate with
  `xeipuuv/gojsonschema` like Coves does, at materialization time in dev,
  in tests always.
- Golden tests: task 02 fixtures → expected record JSON
  (`internal/materialize/testdata/*.golden.json`); idempotency test
  (materialize twice → identical at-uri/cid, single mapping row);
  missing-parent chain test with a 3-deep fixture thread.

## Definition of done
- All produced records validate against vendored Coves lexicons.
- Emission ordering guarantee: community profile + author profile commits
  always precede the content commit referencing them (assert via
  firehose_events seq in a test).
- `go test ./...` green.

## References
- Coves lexicons + consumers (validation rules to satisfy):
  `~/Code/coves/internal/atproto/jetstream/post_consumer.go`,
  `comment_consumer.go`.
- `~/Code/granary/granary/as2.py` + `bluesky.py` — field-mapping prior art.
- `~/Code/coves/tests/lexicon-test-data/` — valid/invalid record fixtures.
