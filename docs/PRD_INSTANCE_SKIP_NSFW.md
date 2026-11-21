# PRD: Skip NSFW-flagged communities on instance entries

2026-11-20. Status: decisions made 2026-11-21 (D1 to D3 below), approved for build 2026-11-21.
Source doc: https://claude.ai/artifact/DiUVhAcuiZ29GLhkqJycUa

## Goal

Let lemmy.world be added as an `instances` entry without bridging its NSFW-flagged communities, including ones created or switched to NSFW after the entry is added. The three existing instance entries (programming.dev, lemmy.zip, sh.itjust.works) keep their current behaviour unless their entry opts in.

## What Tidepool does today (main 94c0cbb)

| Path | What happens | NSFW effect |
|---|---|---|
| Instance listing (`internal/ingest/instance_listing.go`) | `/api/v3/community/list?type_=Local&show_nsfw=true`, keeps only `actor_id` and `published` | NSFW communities listed on purpose; the `nsfw` field is never decoded |
| Post walk (`internal/ingest/instance_posts.go`) | `/api/v3/post/list?type_=Local&show_nsfw=true`, keeps the community `actor_id` | Same: NSFW communities discovered on purpose |
| Candidate filter (`FollowList.Desires`, `reconcile.go:103`) | Instance entry wants every community on the host not named in `exclude` | `exclude` by name is the only filter |
| Subscribe (`Admin.subscribe`, `follow.go:457`) | `EnsureCommunity` mints the DID and writes `community.profile`, then sends Follow | No refusal. Group `sensitive:true` only becomes `contentWarnings:["nsfw"]` on the profile (`actors.go:803`) |
| Communities created later | Every sweep (15 min) re-lists the host and walks recent posts | A new NSFW community is followed at the next sweep |
| Update(Group) (`consent.go:56`) and the 24 h profile TTL | `RefreshCommunity` re-renders the profile | `contentWarnings` tracks the flag both ways; follow state never changes. Only `#nobridge` stops bridging |
| Posts and comments (`posts.go:341`, `comments.go:409`) | Page/Note `sensitive:true` becomes an `nsfw` self-label | Only the post's own flag is read; the community's flag is not inherited |

The e2e suite asserts the current inclusion (`tests/e2e/follow_list_test.go:542`, `tests/e2e/post_walk_test.go:131`), and the `communities.yaml` header documents it.

## Gaps

1. **No NSFW filter for instance entries.** Adding lemmy.world today follows all ~335 NSFW-flagged communities found in the 2026-11-09 scan. A hand-kept `exclude` list only covers names known at the time.
2. **New NSFW communities are followed automatically.** A community created after the entry is added is picked up by the next sweep with no check.
3. **A community that switches to NSFW stays followed.** Update(Group) only refreshes the profile's `contentWarnings`.
4. **The DID is minted before anything could refuse.** `EnsureCommunity` mints first, so a check placed after it would still spend PLC budget (about 7,200 mints a day shared with actors) on communities we then drop.
5. **Posts in an NSFW community can reach Coves unlabeled.** Lemmy's Page `sensitive` is the post's own `nsfw` flag. Left as is for now (D2); not part of this build.

## Requirements

**R1. Per-host switch.** An `instances` entry takes an optional `skip_nsfw: true` (default false). Existing entries are unchanged. Parsing is strict like `exclude`: a non-boolean value is a parse error.

**R2. Explicit entries win.** A community named under `communities` is followed even when it is NSFW-flagged and its host has `skip_nsfw`. This is how an NSFW-flagged but non-explicit community is kept.

**R3. Store the flag.** Add `communities.sensitive boolean not null default false`, written on every materialize and refresh of a community from the Group's `sensitive`. `Desires` gains the flag so `diffFollowList` stays offline.

**R4. Skip at listing time.** Decode `community.nsfw` in the listing and in the post walk's community view. On a `skip_nsfw` host, a flagged result is not a candidate and is counted, not tried, so it uses no add slot and no fetch.

**R5. Check before mint.** For instance and post-walk provenance on a `skip_nsfw` host, fetch the Group and refuse a `sensitive:true` document before `EnsureCommunity` mints anything. The refusal is a skip (uses `refusalSkip`) with reason "community is NSFW-flagged". This covers a community that turned NSFW between listing and subscribe, and a listing that does not report the flag.

**R6. Unfollow on switch to NSFW.** When Update(Group) or a TTL refresh sets `sensitive` on a community that only an instance entry with `skip_nsfw` wants, the next sweep treats it as an extra: Undo{Follow}, the same path as removing it from the file. Already-bridged records are kept (D1). A community that switches back is no longer flagged in the listing and is re-added by a later sweep.

**R7. Visibility.** The sweep log and `SweepResult` report a count of NSFW-skipped candidates per host, and each R5 refusal and R6 unfollow is logged with the community id.

## Decisions (Bretton, 2026-11-21)

- **D1. Existing records on switch to NSFW (R6): keep.** Already-bridged records stay and new content stops, the same as removing a community from the file. No scrub.
- **D2. Posts inheriting the community flag: not now.** Posts keep being labeled only from their own flag. Gap 5 stays open.
- **D3. Unflagged explicit communities: exclude by hand.** Run the NSFW scan against lemmy.world again just before adding the entry, and add every explicit-by-name community without Lemmy's flag to `exclude`. Operational, not code.

## Out of scope

A min-activity filter, labeling posts from their community's flag (gap 5, D2), per-post filtering (skipping NSFW posts in non-NSFW communities), Coves-side handling of community `content_warnings`, and adding lemmy.world to `communities.yaml`.

## Tests

Unit tests on listing decode, `Desires`, and the strict parse. E2e against the real local Lemmy with `skip_nsfw` on the local host:

- an NSFW community is not followed and no DID is minted for it;
- an NSFW community created after the first sweep is not followed by the next one;
- a followed community switched to NSFW is unfollowed, and is re-followed after switching back;
- an NSFW community named under `communities` is followed;
- the existing NSFW-inclusion tests still pass on a host without `skip_nsfw`.

E2e runs on home-pc use the shared lock: `flock -w 5400 ~/.cache/tidepool-e2e.lock bash -c 'test -z "$(docker compose -p tidepool-e2e ps -q)" && make e2e'`.

## Notes

- Migration number: main ends at 068. The unmerged peer-interop branch takes 069 to 079, so whichever lands second renumbers.
- Docs: update the `communities.yaml` header and `docs/ADMIN_API.md` "Instance entries" for `skip_nsfw`.
- Rollout: deploy, then add `{host: lemmy.world, skip_nsfw: true}` with the D3 exclude list. The listing reaches about 10,000 of lemmy.world's ~13.7k communities; R5 still guards ones found by the post walk.

## Implementation notes

Chunks:

- 01 Skip NSFW at discovery — 8db68cd
- 02 Unfollow on switch to NSFW — fb816b1

Notes on the landed code:

- `subscribe` takes `refuseNSFW bool`. The admin HTTP handler and explicit `communities` adds pass false; only listed and post-walk adds pass the host's `skip_nsfw` switch.
- The listing and the post walk expose `NSFW map[string]bool` holding flagged ids only. The post walk reads `community.nsfw`, not `post.nsfw`.
- R5 refusal: `refuseNSFWGroup` fetches the Group before the mint. NSFW gives a 422 `SkipError` (cause `errCommunityNSFW`) counted in `nsfw_skipped`, not `failed`. 404/410 give a 422 `SkipError` (24h `refusalSkip`, counted in `failed`). Other fetch errors give 502. The fetched document goes to `EnsureCommunityWithDocument`, so a successful add on a `skip_nsfw` host costs one Group fetch, not two.
- `SweepResult.NSFWSkipped` (`nsfw_skipped`) is always a non-nil map, and the sweep-complete log carries it whenever that log is written.
- `Desires` takes `sensitive bool`. `diffFollowList` passes the stored flag; candidate selection passes false, so flagged ids are counted by `leaveOutNSFW`, not dropped silently.
- `communities.sensitive` (migration 069) is written on insert, then by `SetSensitive` only when a Group document is at hand and the flag differs. An omitted `sensitive` stores false.
- A manual POST /admin/communities of an NSFW community on a `skip_nsfw` host is unfollowed at the next sweep. Only a `communities` entry keeps it.

Deferred:

- Review Suggestions (14 on chunk 01, 13 on chunk 02) were suggestions only and are not fixed, apart from chunk 01's double Group fetch, which `EnsureCommunityWithDocument` removed. Notable ones:
  - The post walk records `community.nsfw` only on an actor id's first occurrence, so a community flagged later in the same walk takes a batch slot and a Group fetch; R5 still refuses it.
  - The R5 pre-fetch and `deliverFollow` log the same `subscribe: fetch group` message, so a pre-mint failure (no DID) reads like a post-mint one.
  - An R5 refusal's 24h `refusalSkip` outlives turning `skip_nsfw` off; the community is not retried until it expires, and nothing says why.
  - `rematerializeProfile` writes the flag before `MarkProfileSynced`, so a failed flag write on a stale profile is retried at the next touch. A forced refresh of a still-fresh profile whose flag write fails keeps the old fresh stamp, and the flag stays stale until the 24h profile TTL. Inbox redelivery of the Update{Group} corrects it; an admin refresh is not retried.
  - `SetSensitive` is last-writer-wins from whatever document the caller holds, so an older document can overwrite a newer flag and delay an R6 unfollow until the next document.
- Gap 5 (posts inheriting the community flag) stays open per D2.
- D3 (re-scan lemmy.world and add explicit-but-unflagged communities to `exclude`) is an operational step before adding the entry.

Live checks: none blocked (the repo profile has no live check).
