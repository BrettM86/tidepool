# PRD: Outbound images

Status: draft, 2026-11-05. Owner: Bretton.

Builds on the Tidepool items T1 to T11 in Coves'
`docs/plans/IMAGE_UPLOADS.md` and the URL contract in Coves'
`docs/IMAGE_URL_CONTRACT.md`. This PRD covers the outbound half only.

## 1. Problem

Images in native Coves posts and comments never reach the fediverse.

- `apobject.BuildPage` maps only `social.coves.embed.external` to an
  attachment. `social.coves.embed.images` and
  `social.coves.embed.imagesWithAttachment` are dropped, so an image post
  arrives on Lemmy as its title and text. Galleries and alt text are lost.
- `apobject.BuildNote` never reads `embed`. A comment's images are dropped, and
  an image-only reply (allowed on Coves since `e5e4a574`) is delivered as
  `content: "<p></p>\n"` with an empty markdown `source`. Lemmy stores it as a
  blank comment.
- `#image` facets are not in Tidepool's vendored `richtext/facet.json`. The
  renderer sends only the covered fallback text.
- The renderer escapes every `![` (pinned by
  `TestBuildNote_LiteralExclamationBeforeLinkCannotFormImage`), so no outbound
  body can carry an image today.

A related defect found while scoping: Tidepool never sets `updated` on a Page
or Note. Lemmy 0.19.20 applies an `Update` only when its timestamp is strictly
newer than the stored one (`Post::insert_apub` and `Comment::insert_apub`,
`coalesce(updated, published).lt(timestamp)`), and falls back to `published`.
Every outbound edit is a silent no-op on Lemmy today, with or without images.
Outbound edits are unit-tested only, which is why this went unseen.

## 2. Goal

A native Coves post or comment with images shows those images on Lemmy 0.19.20,
with alt text, in the places a Lemmy reader expects:

- An image post is a Lemmy image post: the first image is the post URL with
  its alt text, and Lemmy builds the thumbnail.
- The other images in a gallery appear in the post body.
- An image placed in the text with an `#image` facet appears at that position.
- A comment's images appear in the comment, and an image-only reply shows its
  images.
- Editing a post or comment, including adding or removing images, updates it on
  Lemmy.
- Lemmy never fetches an image before the Coves image proxy serves it.

## 3. Non-goals

- Inbound changes (T6). Lemmy images coming into Coves already work.
- Titleless image posts. Decided 2026-10-05 (Coves Q5): they stay local.
- Video embeds.
- `sensitive` on comments (T5). Lemmy comments have no NSFW field.
- Propagating Coves admin removals to peers (T7). It is G6 in Coves'
  `docs/PRD_MODERATION_GAPS.md` (chunk 7), which needs that PRD's label
  publisher, now on the unmerged Coves branch `prd/prd_moderation_gaps`. See Q3.
- Blob GC for bridged blobs (T8).
- Verifying PieFed, Mbin or Mastodon. The shapes below are chosen to be
  readable by them, but only Lemmy 0.19.20 is tested.
- Backfilling images onto posts that already federated without them. See Q4.

## 4. How Lemmy 0.19.20 reads images

Checked from source in `~/Code/lemmy` (tag 0.19.20).

- **Page.** Only `attachment[0]` is read. Its URL becomes the post URL and an
  `Image` or `Document` attachment's `name` becomes `alt_text`
  (`objects/post.rs`). Lemmy then fetches the URL once in the background
  (`generate_post_link_metadata`). If the response is `image/*`, it is an image
  post with a thumbnail. A 404 here is permanent: Lemmy records
  `text/plain`, never retries, and shows a link icon. `Page.image` is not read
  on receive. Lemmy skips thumbnail generation for NSFW posts unless the
  instance allows it.
- **Note.** Every attachment is appended to the comment as markdown after a
  blank line (`append_attachments_to_comment`): an `Image` becomes `![name](url)`.
  The body comes from `source` when `mediaType` is markdown.
- **Body images.** Markdown images in a post or comment body are rendered at
  view time and proxied through pict-rs when the instance uses
  `ProxyAllImages`. A failed fetch heals on the next view.
- **Edits.** See §1: `updated` must be set and strictly newer.
- **Debug builds** (the e2e Lemmy) skip `validate_link_ip`, so an image URL on
  the e2e network is fetchable.

## 5. Requirements

### R1. Lexicon re-sync (T1)

Vendor from Coves: the `#image` feature in `richtext/facet.json`,
`embed/imagesWithAttachment.json`, the `imagesWithAttachment` ref in the
`community/postv2.json` and `community/comment.json` embed unions, and the
updated `embed/images.json` descriptions. The content caps already match.

### R2. Image URLs

- New config `COVES_MEDIA_BASE_URL`, set to the paired AppView's
  `IMAGE_PROXY_BASE_URL` (`https://img.coves.social` in production). Required
  when outbound is enabled. Outside development it must be `https://` with no
  path, query or userinfo.
- An image URL is `<base>/img/content_full/plain/<did>/<cid>`, where `did` is
  the record's author (the at-uri's repo) and `cid` is the blob ref's `$link`.
  No network call, so it is safe inside the enqueue transaction.
- An image entry with no valid blob ref, a CID that does not parse, or a
  `mimeType` outside the lexicon's accept list is skipped, not sent.
- The Translator and `RenderObject` (the `/ap/object/...` serving surface) take
  the same base, so a re-fetched object matches the delivered one.

### R3. Renderer image path (T4)

- Images Tidepool builds from the embed render as `![alt](url)` in the markdown
  `source` and `<img src="url" alt="alt">` in the HTML `content`.
- Author text keeps the current escaping. A literal `![` written by the author
  still cannot form an image. Update the pinned test so it asserts this
  distinction rather than deleting it.
- Alt text is escaped for both forms: brackets and backslashes in markdown,
  HTML escaping in `content`, line breaks replaced by spaces. Empty alt is
  allowed.
- An `#image` facet renders its image at the facet's position in place of the
  covered fallback text. A placement Coves would filter (index out of range,
  inside code, overlapping another `#image`, out of bounds) falls back to its
  covered text, matching `richtext.FilterImageFacets` in Coves.

### R4. Posts (T2)

Let *unplaced* be the gallery images with no valid `#image` placement, in
gallery order.

- `embed.images`: `attachment[0]` is
  `{type: Image, mediaType, url, name: alt}` for the first unplaced image. The
  remaining unplaced images are appended to the body after a blank line. With
  every image placed, no attachment is sent and the post is a text post with
  inline images.
- `embed.imagesWithAttachment` with an external link: the link is
  `attachment[0]` as today (scheme-checked), and every unplaced image is
  appended to the body.
- `embed.imagesWithAttachment` with a quoted post: images are handled as for
  `embed.images`. The quote is dropped, as it is today.
- `sensitive` is unchanged.

### R5. Comments (T3)

- Placed images render inline (R3).
- Unplaced images go in `attachment[]` as `{type: Image, mediaType, url,
  name: alt}`, in gallery order. They are not also written into the body:
  Lemmy appends attachments itself, so writing both would show each image
  twice.
- An image-only comment carries empty `source`, and its images arrive through
  `attachment[]`. It must not be rejected for empty content.

### R6. Edit timestamps (T10)

Every `Update{Page}` and `Update{Note}` sets `updated` to the edit's commit time,
strictly later than `published`. `RenderObject` serves the same value. A Create
sends no `updated`.

### R7. Readiness gate (T11)

- Before the first delivery of a Create or Update that carries image URLs, the
  worker sends `HEAD` to each URL. All 2xx: deliver.
- On 404 or 503, park the delivery with a short delay (about 10 s) without
  spending its delivery retry budget, reusing the park machinery the causal
  gate uses.
- The wait is bounded by a wall-clock budget from the delivery's creation
  (about 2 minutes). When it runs out, deliver anyway and count it in a metric.
  A post with a broken thumbnail beats a missing post.
- Other probe failures (timeout, connection error, other statuses) count
  toward the same budget, never poison.
- The probe goes through the existing egress guard. It hits only
  `COVES_MEDIA_BASE_URL`, and never logs URLs at a level that would leak
  removed content into logs.
- Later deliveries of the same activity (fan-out to more inboxes, retries) do
  not probe again.

## 6. Testing

Unit tests in `internal/apobject` and `internal/outbound`, with expected wire
shapes written out by hand from the fixtures, not produced by the builders
(see the parity-test feedback in memory). Cases: single image with alt, eight-image
gallery, image plus link, image plus quote, all images placed, placement
filtered back to text, malformed blob ref skipped, alt with brackets and line
breaks, author `![` still escaped, comment with images, image-only comment,
Update carries `updated`, readiness gate park, budget exhaustion and no
re-probe on fan-out.

E2E against the real Lemmy 0.19.20 in `docker-compose.e2e.yml`: upload a blob to
the reference PDS, create the record, and read the result back from Lemmy's
API. Cases: image post (URL, `alt_text`, `url_content_type` is `image/*`,
thumbnail set), gallery body, inline placement, image plus link, comment with
an image, image-only reply, an edit that adds an image, and the gate holding a
delivery until the proxy serves. This needs an image proxy that serves only
indexed blobs. See Q1.

## 7. Rollout

1. Check `curl -I https://img.coves.social/img/content_full/plain/<did>/<cid>`
   returns 200 with `Content-Type: image/*` through Cloudflare (Coves C5).
2. Deploy Tidepool with `COVES_MEDIA_BASE_URL=https://img.coves.social`.
3. Post a test image post and image reply in a bridged test community, and
   check them on the Lemmy side.

Payloads are built at enqueue and stored verbatim, so only content enqueued
after the deploy carries images.

## 8. Chunks

| # | Chunk | Size | Depends on |
|---|---|---|---|
| 1 | R1 lexicon re-sync | S | none |
| 2 | R2 config and URL builder, threaded through Translator and `RenderObject` | S | 1 |
| 3 | R3 renderer image path and `#image` placement | M | 2 |
| 4 | R4 posts | M | 3 |
| 5 | R5 comments | S | 3 |
| 6 | R6 edit timestamps | S | none |
| 7 | R7 readiness gate | M | 2 |
| 8 | E2E image proxy in the stack (Q1) and the e2e cases | L | 4, 5, 6, 7 |

## 9. Decisions and open questions

Decided 2026-11-06: Q1 (AppView in the e2e stack), Q2 (R6 stays in this PRD)
and Q4 (no backfill) as recommended. Q3: removals stay in PRD_MODERATION_GAPS.

- **Q1. E2E image proxy.** Decided: add the Coves AppView to
  `docker-compose.e2e.yml`, built from `../coves` and modelled on the
  `appview` service in Coves' `docker-compose.ci.yml`, consuming the e2e
  Jetstream with the image proxy on. That is real infrastructure and it
  exercises the indexing race the gate exists for. Cost: one more build and
  Postgres in the stack, roughly 1 to 2 days. Smaller alternative: unit tests
  plus a one-off manual check on production (§7), with no e2e. That leaves the
  gate and Lemmy's thumbnail fetch untested.
- **Q2. Edit timestamps in this PRD?** R6 fixes an existing defect that breaks
  every outbound edit, not only image edits. Decided: keep it here, because
  "an edit that adds an image" depends on it. It is small and has no other
  dependency.
- **Q3. Removals reaching peers (T7).** Once images federate, a Coves admin
  removal (including CSAM) makes the proxy answer 404, which reaches every body
  and comment image. Under Lemmy's default `StoreLinkPreviews` mode, pict-rs
  keeps the post thumbnail it already stored, and `ProxyAllImages` instances
  also keep body images they cached. A federated `Remove` or `Delete` hides
  the post in Lemmy's UI but does not delete the pict-rs file. Only a local
  admin purge (`api/site/purge/post.rs`) does. Recommended: ship this PRD
  first. Removal propagation ships as G6 of PRD_MODERATION_GAPS once its Coves
  label publisher is merged. G6 sends `Delete` as the author's persona, which
  hides the post and its thumbnail on Lemmy. The stored thumbnail file stays
  until that instance's admin purges it.
- **Shared e2e stack.** PRD_MODERATION_GAPS D8 adds the same Coves AppView to
  `docker-compose.e2e.yml`. Chunk 8 here builds it once, for both PRDs:
  AppView with its own Postgres, indexing from the stack's Jetstream, resolving
  through the stack's PLC, image proxy on. D8's extra needs (instance admin
  account, OAuth sign-in, `did:web` hosting, second Lemmy) stay with that PRD.
- **Q4. Backfill.** Decided: no backfill. Older image posts stay text-only
  on Lemmy. Re-sending them as Updates is possible once R6 lands.

## 10. Related backlog

- `2026-10-13-tidepool-outbound-page-size-vs-lemmy-limits`: appended image
  markdown adds about 120 bytes per image to the body.
- `2026-08-19-tidepool-outbound-delivery-mediums`: the gate adds HTTP calls in
  the worker, not in the enqueue transaction, so it does not worsen item 4.4.

## Implementation notes

Built 2026-11-06 to 2026-11-07 on `prd/prd_outbound_images` by `/prd-loop`, then hardened after a whole-branch review.

- 01 Lexicon sync — 5db0831
- 02 Coves AppView and image proxy in the e2e stack — 302d5e6
- 03 Image URLs, inline `#image` placement and the readiness gate — cc1c8c0
- 04 `updated` on every Update — 5fd6a1c
- 05 Image posts, galleries and comment attachments — 4db49d5
- After the loop: origin/main merged (df320d0); e2e Postgres healthchecks probe TCP (344c1f9); production compose passes `COVES_MEDIA_BASE_URL`, default `https://img.coves.social` (11536d8); migration renumbered to 045 (892108d), then to 049 after main took 046–048; the gate probes only Tidepool-built URLs (bd755fd); consent is rechecked after the gate's probes (79431bb); added renderer and e2e tests.

What the code does now, beyond §5:

- Placement follows Coves `FilterImageFacets`, not the "overlapping" wording in R3: facets are read in array order, a repeated gallery index falls back to its text, "inside code" means wholly inside a `#code` or `#codeBlock` range, and overlapping spans with different indices are both kept (decision 03-c). A link or emphasis range that crosses a kept image drops to plain text, a heading or quote that crosses one drops, and a spoiler widens to cover it.
- The gate parks with `last_error_class = media_pending` and logs `failure_class` (`status_404`, `status_503`, `status_other`, `redirect`, `timeout`, `transport`), never a URL. The budget runs from the delivery's `created_at`. Metric: `tidepool_outbound_media_budget_exhausted`. `media_ready_at` is set on the activity (migration 049).
- `updated` comes from the snapshot's `edited` marker and `timeUs` through `apobject.SetUpdated`. RenderObject serves the same value.
- Migration 049 follows main's 046–048 (votes and moderation withholds).

Known limits:

- Lemmy 0.19.20 cannot clear a post URL through an Update: its upsert skips None fields. An edit that removes a post's image leaves the old URL, and once the proxy stops serving the blob Lemmy shows it as a link.
- When an imagesWithAttachment link fails the scheme check, no attachment is sent and every image goes in the body.

Deferred to the backlog (`~/Code/claude-skills/issues/`): `2026-11-06-tidepool-outbound-floored-edits-collapse-on-lemmy`, `2026-11-06-tidepool-outbound-restore-create-after-delete-dropped`, `2026-11-06-tidepool-lemmy-comment-image-alt-bracket-breaks`, `2026-11-06-coves-imageswithattachment-nested-type-required-for-indexing`. Low-priority test gaps that were not fixed: a test that logs carry no image URL, the egress guard on the prober, the alt escape set, `rebuildCommit` keeping `TimeUS`, and a fragment-only media base.
