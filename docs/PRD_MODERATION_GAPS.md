# PRD: Moderation gaps, Tidepool side

**Status:** Draft, 2026-11-06. Nothing here is built. Requirements are decided in the Coves PRD; this document only slices, sequences and updates them for Tidepool.

**Source of truth:** Coves `docs/PRD_MODERATION_GAPS.md` (decisions D1–D8, gaps G1–G7, questions Q1–Q9, all decided 2026-10-19/20). Every requirement below cites its section there. Where this document and the Coves PRD differ, this document records why.

**Related:** backlog `2026-10-19-tidepool-removed-content-images-still-served` (its "G1 requirements from coves-d1" list is binding for chunk T1), `PLAN.md` decisions 18 and 19, `FOLLOWUPS.md` § "Community bans and moderation", `docs/PRD_OUTBOUND_IMAGES.md` (shares the e2e AppView).

## 1. Summary

Coves instance moderation is the one moderation authority for both services (D1). Tidepool has to enforce it in two directions:

- Lemmy moderators' decisions become records in the community repo that Coves indexes: comment removals (G2) and community bans (G3).
- Coves admins' decisions reach Tidepool as signed labels from the Coves instance DID (G4). Tidepool withholds the blobs it serves and, for native content it delivered to Lemmy, sends `Delete` and `Undo{Delete}` as the author (G6, G7).

Underneath both sits a blob withhold store with one store/serve check in `PutBlob` and `getBlob` (G1).

## 2. Where the dependencies stand

- **Coves.** The Coves chunks (2, 4, 6, 8, 9) are merged on home-pc's local `main` as `8c11f3e3`, with migrations renumbered 067–076. On 2026-11-06 that commit was not pushed to origin and not deployed. The Coves PRD's implementation notes still say nothing is merged.
- **Blob check.** G1 assumed an earlier Tidepool branch would add the withhold store, the `PutBlob`/`getBlob` check and the operator API (backlog Run A). That branch was never built. Chunk T1 builds it.
- **E2E AppView.** The outbound images loop (`prd/prd_outbound_images`, its task 02) adds the Coves AppView and its Postgres to `docker-compose.e2e.yml`, indexing from the stack's Jetstream and resolving through the stack's PLC. T4 builds on that service and adds only what D8 needs beyond it: an instance admin account on the stack PDS with real OAuth sign-in, the AppView's `did:web` on a stack hostname with a label signing key, and a second Lemmy instance.

## 3. Chunks

Tidepool chunk numbers (T) map to the Coves PRD's § 6 numbers (C). "Deploy gate" is what must be in production before the chunk is deployed. Building can start before its gate.

| T | C | Scope | Needs | Deploy gate |
|---|---|---|---|---|
| T1 | 1 | G1 withhold store, store/serve check, operator API | none | none |
| T2 | 3 | G2 removal records and community-removal withholds, with back-fill | T1 | Coves task 01 deployed |
| T3 | 5 | G3 ban records and back-fill | none | Coves tasks 02–03 deployed |
| T4 | 7 | G6 consumer and record labels, D8 stack | T1, images loop task 02 on main | Coves tasks 04–05 deployed |
| T5 | 10 | G6 account and community labels, G7 bridged communities | T4 | Coves tasks 06–10 deployed |

### T1. Blob withhold store and operator API (G1)

Requirements: Coves G1 bullets 1, 2, 4, 6, 7, 8, and the backlog issue's binding items 1–9, except community-removal and label withholds, which only need to fit the schema here (T2, T4, T5 write them).

- A withhold table keyed by canonical CIDv1 base32, with source (operator now; community-removal and label later), scope (every owner; owner DID plus subject URI; owner DID; community DID), reason code, optional external references, actor label, and created/lifted/deleted times. A migration after 042.
- One check, used by `repo.Manager.PutBlob` (`internal/repo/blob.go`) and `handleGetBlob` (`internal/sync/server.go`), that answers whether a (DID, CID) may be stored or served. It is refused while any active withhold covers it.
- `getBlob` on a withheld blob answers exactly as for an absent one.
- A refused store keeps the blob ref in the rebuilt record as withheld rather than dropping the embed, so lifting the withhold serves the image again with no upstream edit. Today a store failure drops the embed.
- Admin routes under the existing bearer-token `/admin` group: withhold, lift, delete bytes, inspect one CID, and list active operator withholds. Every call returns the CID's state. Withhold is idempotent. Lifting a missing withhold is a 4xx. Delete-bytes needs an active operator withhold and a confirm flag, deletes every owner's bytes, keeps the withhold row, and documents that the operator must preserve apparent CSAM first. Routes are logged without bytes or PII.
- The routes and the operator procedure (withhold, preserve, delete) in `docs/ADMIN_API.md`. A `DEPLOY.md` runbook entry is left to the user, since production docs are outside unattended work.

Not in T1: deleting images on author delete or purge, the site-ban profile clear, and the history sweep (backlog Run B). See Q1.

### T2. Community comment removals become records (G2)

Requirements: Coves G2 "Tidepool, native comments" and "Tidepool, bridged comments", G1 bullet 3 and bullet 5.

- A Lemmy moderator removal of a native comment (`moderateNativeComment`, `internal/ingest/moderation.go`) writes a `social.coves.community.removal` record into the community repo, using `object_moderation.removed_at` as the source. The moderator's `Undo{Delete}` (`liftNativeCommentRemoval`) deletes it. Child enqueues and edit federation are gated on the removal.
- A Lemmy moderator removal of a bridged comment, and a community ban with "remove content" (the `b1547af` path, which deletes the comments today), write the removal record instead of deleting the comment's record. A self-delete is still a record delete.
- A moderator removal of a bridged post or comment adds a community-removal withhold (owner DID plus subject URI), which follows the subject's record through edits. `Undo{Delete}` lifts only that withhold.
- A one-time back-fill adds community-removal withholds for content already removed.

### T3. Community bans become records (G3)

Requirements: Coves G3 "Tidepool writer" and "Tidepool back-fill".

- Every accepted community `Block` of a Person writes a `social.coves.moderation.ban` record into the community repo, native or bridged, with or without "remove content", and the matching `Undo{Block}` deletes it, under the stale-unban rule `liftBan` applies. Bridged persons get a new branch in `handleBlock`. A Lemmy user Tidepool never minted is skipped and counted.
- `decideCommunityBan` (`internal/ingest/upstream_ban.go`) stays free of writes, because `POST /admin/inbox/replay-bans` calls it for dry runs.
- A one-time back-fill publishes active bans: native personas from `community_bans` (migration 027), bridged persons by replaying recorded ban events under the live rules. A ban whose lifting the history cannot rule out is not published.

### T4. Label consumer and record labels, and the D8 stack (G6, D8)

Requirements: Coves G4 milestone rules (consumer side), G6 bullets on subscription, takedown sources, native records, negation, held work, serving while removed, user deletes, the new outbound kind, bridged records, `nsfw`; Q4; the § 5 review cases that apply to record labels.

- Subscribe to the Coves instance DID's `subscribeLabels`, resolve its `#atproto_label` key through the configured PLC/`did:web` resolution (never a public lookup), verify every label, keep a cursor, and resubscribe from the start on a future-cursor error. Labels for subjects Tidepool has not seen are kept and applied when the subject appears.
- Per-object takedown source sets, `Delete` on empty to non-empty and `Undo{Delete}` on non-empty to empty, sent as the author with no `summary`, through echo suppression. Queued work is held off the ordering queue and restored at its tail. Removed native objects are served as a 410 Tombstone.
- A bridged record label adds a label withhold for its blobs and stops native replies beneath it.
- The e2e stack gains the D8 additions listed in § 2 and the cross-repo assertions for T2 and T3: records written by Tidepool reach Coves' served views through the relay and Jetstream.

### T5. Account and community labels (G6, G7)

Requirements: Coves G6 account bullets, G7 "Bridged communities", Q4, Q8, and the remaining § 5 review cases.

- Account DID labels: native personas pause delivery and `Delete` every delivered object; negation restores each object whose source set is empty. Bridged accounts get an account-scope withhold and no native replies beneath their content. The existing `#account` handler and the label share one pause source set so neither clears the other.
- Community DID labels on a bridged community: `Undo{Follow}` with records kept, no native delivery into it, a community-scope withhold, and `Delete` of native content already delivered. The removal is a suppression honored by every path that can follow again (§ 4). Negation follows again, backfills, and reconciles kept objects against their origin (Q8).

## 4. Changes on Tidepool main since the Coves PRD

The Coves PRD was last revised 2026-10-20 against Tidepool `b1547af`. These later commits change what the chunks must touch:

- **`740cd9c` whole-instance follows.** `communities.yaml` can name whole instances, and the follow reconciler follows every local community on them. G7's suppression must also be honored by the instance listing and its follow sweep, not only the configured community list, manual subscribe, the follow retrier and backfill (T5).
- **`74f4944` operator purge.** `POST /admin/actors/purge` sends `Delete{Person, removeData}` and tombstones the actor. That is the irreversible erasure Q4 reserves for account erasure. The label path (T4, T5) must never call the purger, and a takedown restore must skip an actor the purger has tombstoned.
- **`feac7ca` AVIF transcoding.** The stored blob, and so its CID, is the transcoded image. Withholds key on the CID Tidepool stores and records reference, which is also what Coves' image proxy sees.
- **`517cee1` link thumbnail unfurl.** Unfurled thumbnails are blobs referenced by the post record, so subject withholds cover them with no special case.

## 5. Branching and coordination

- Each chunk runs through `/prd-loop` on its own integration branch off `origin/main`: T1–T3 now, as one loop. T4 and T5 start after the images loop's AppView service is on main, so they extend it rather than adding a second copy.
- Migrations: both this loop and the images loop add migrations after 042. Whichever branch merges to main second renumbers its migrations before merging.
- T2 and T3 ship before the D8 stack exists. Their e2e tests run against the real Lemmy 0.19.20 and Tidepool alone, and T4 adds the cross-repo assertions.

## 6. Testing

Real infrastructure only, per the Coves PRD § 5 and Tidepool's local-only test rails: the e2e stack's own PLC, PDS, relay, Jetstream and Lemmy, no public services.

- T1: `getBlob` returns the same response for withheld and absent blobs; a refused re-store keeps the ref and lifting serves the image without an upstream edit; delete-bytes without an operator withhold is refused; lifting a missing withhold is a 4xx; one source's lift leaves another in force.
- T2, T3: moderator `Delete` with `summary`, `Undo{Delete}`, `Block` and `Undo{Block}` captured from the stack's Lemmy, including a bridged ban without "remove content" and its unban; the back-fills run against seeded history.
- T4, T5: the Coves PRD § 5 review cases, the Q4 checks on both Lemmy instances (Announce path), and a community removal surviving a whole-instance follow sweep.

## 7. Decisions

- **Q1 (2026-11-06). Backlog Run B stays out.** On 2026-10-19 Bretton had also approved deleting images on author delete and on a purge-detected refetch, clearing the profile on a site ban, and a dry-run history sweep, for the same branch as the withhold store. Decided 2026-11-06: leave them out for now. No moderation chunk needs them, and they stay in the backlog issue.

## Implementation notes

Recorded 2026-11-06 at the end of the `/prd-loop` build of T1–T3 on `prd/prd_moderation_gaps`. Task numbers map to PRD chunks as 01 = T1, 02 + 03 = T2, 04 = T3.

### Chunks

- 01 blob withhold store — 73a2426
- 02 comment removal records — a3fa377
- 03 community-removal withholds and back-fill — dd4336a
- 04 ban records and back-fill — 463039d (Coves lexicon sync from local main 7bf31711) + 9464c00

T4 and T5 are not built. They start after the images loop's AppView service is on main (§ 5).

### Decisions made during the build

- **2026-11-06 answers.** Bretton approved every recommendation in the loop's open questions:
  - 02-a: a Lemmy community ban with "remove content" of a native persona removes only their posts; their native comments in that community get nothing in Coves. Not built; filed to the backlog as `2026-11-05-tidepool-native-ban-remove-content-skips-comments`.
  - 03-a: each one-time back-fill is a bearer-token admin route with `dry_run`, idempotent, documented in `docs/ADMIN_API.md` and run by the operator after deploy, following the `POST /admin/inbox/replay-bans` precedent. A boot-time one-shot was the rejected alternative.
  - 04-a: the loop itself ran `scripts/sync-lexicons.sh` from Coves local `main` as its own commit on task 04's branch, before the ban work. Nothing was deployed.
  - 04-b: a community `Block` whose expiry has already passed on arrival writes no ban record and never overwrites a standing one, since Coves ignores a record past `expiresAt` and Tidepool never deletes lapsed records.
  - The scope proposal to refuse native replies beneath a moderator-removed bridged comment was not approved and is left out.
- **2026-11-07, A.** Community-removal subject withholds are add-only while the removal stands: a delete (author or sweep) and an edit or re-materialization never lift a row, only a restore does; the cost is that a deleted subject's CID stays refused for that owner.
- **2026-11-07, author delete.** Bretton: "a delete by the author should remove the bytes, since it can't come back by a moderator restore or a user restore anyway." The author's own announced delete of a postv2 or comment (`deleteIsByAuthor`, removed or not) now deletes the bytes of every CID the subject used that no other record in the author's repo references (`HandleAuthorDeleteRecord`), and lifts the subject's community-removal row for each CID whose bytes are gone. Both happen in the record delete's own commit, with the references computed under the commit lock against the tree after the delete, so a failure leaves the record and its bytes together and a record committed before it keeps its bytes. Still open: every store of the CID that finishes before the delete's commit loses its bytes, whether it inserted them or found them already stored (PutBlob holds a per-(DID, CID) lock through its withhold check and insert, which the delete takes exclusively, so a store in flight finishes first). That is harmless for the deleted subject. Another record of the author that references the CID and commits after the delete, for example a Lemmy cross-post of the same image, references bytes that are gone: nothing logs it, and getBlob answers not-found until that record is materialized again. The delete walks the author's repo only when the subject has a candidate CID. A redelivery of the deleted post (`MaterializePost`) is refused before its media is fetched; still open is a re-materialization that read the mapping while it was live and is fetching media when the delete commits, which can store the removed image under the author's DID with no withhold and no referencing record. Shared and operator-withheld CIDs keep their bytes and rows; the delete sweep, ban scrubs and moderator removals keep the bytes.
- **2026-11-08, author delete of a removed subject.** Bretton: while a community removal stands on a subject, an author delete of it keeps its blob bytes (CSAM evidence preservation: an uploader must not be able to wipe Tidepool's copy before an operator withhold exists). This supersedes the removed-subject part of the 2026-11-07 author-delete line: `deleteAuthorRecord` reads the community's removal record in the delete's commit and, when it stands, deletes no bytes and lifts no community-removal row, so getBlob keeps refusing the CIDs for that owner (add-only, as A says); it also writes any row the record's CIDs lack under the removal's code, because a removal from before removals withheld blobs has none and the back-fill cannot add them once the record is gone. A later moderator restore finds no record to re-accept (the origin serves a tombstone), so the removal record and its rows stay; the bytes stay stored and refused until an operator acts, and the operator's `delete-bytes` is the cleanup path. A restore that read the subject before a racing delete committed ends the same way: `RestorePost` and `RestoreBridgedComment` re-read the hosted subject record in the restore's commit, under the commit lock, and when it is gone they commit nothing (no acceptance, no lift, the removal record stands) and return without error; a post record at a CID other than the one the restore pinned fails the commit, so the retry pins the new version. Author deletes of subjects with no standing removal still delete unreferenced bytes.
- **2026-11-07, B.** `/admin/reemit` skips comments whose author has a ban in force in that comment's community and counts them in `skipped_banned`, because Coves permanently hides a comment indexed while a ban stands. The ban is read again for each comment right before its delete (only a banned answer is cached for the walk); still open is a ban published between that read and the comment's re-create, which Coves then hides for good. Closing it needs reemit and ban publication to share a lock, a scope addition.
- **Migrations.** This branch adds 047 (blob withholds) and 048 (partial unique index on `(subject_uri, cid)` for active community-removal rows, and a wider `reason_code` for those rows). Renumbered from 043/044 on 2026-11-08 because main gained 046 (vote event records) first.
- **Lexicon sync overlap.** 463039d synced 31 files from Coves local main 7bf31711 (which contains 8c11f3e3): the `ban.json` redesign plus additive changes and new moderation, media and actor lexicons. The images loop synced separately (its 5db0831, from Coves `e5e4a574`). Whichever branch merges second resolves lexicon conflicts by re-running the sync rather than hand-merging.

### Notes on the landed code

- **Blob withholds (01).** `repo.Manager` builds its own `blobwithhold.Store` in `NewManager`. `blobwithhold` exports `Covers`, `HasActiveOperator(ctx, cid)` and `ValidOperatorReason`; `Insert` takes other-source rows. `PutBlob` checks `Covers` after the CID is known, so a refused store has still fetched and transcoded the media from the origin. `handleGetBlob` answers a withheld blob with the same 404 `BlobNotFound` as an absent one.
- **Automatic-delete guards (01).** `DeleteBlob` skips a CID under an active operator withhold and reports success (`HasActiveOperator`); `DeleteBlobsForDID` skips such rows with `NOT EXISTS`.
- **Operator blob routes (01).** `POST /admin/blobs/withhold`, `POST /admin/blobs/lift`, `POST /admin/blobs/delete-bytes`, `GET /admin/blobs?cid=` and `GET /admin/blobs/withholds`. They are mounted at top level beside the rename admin, outside `if cfg.ConsumerEnabled`, and use `cfg.AdminToken`. Lift and delete-bytes with no active operator withhold answer 404.
- **Comment removal records (02).** `materialize/comment_removal.go` provides `WriteCommentRemoval(ctx, communityDID, commentURI, commentCID, code, reason, createdAt) (written bool, err)`, `DeleteCommentRemoval(ctx, communityDID, commentURI) (deleted bool, err)` (no commit when nothing stands), `RemoveBridgedComment(ctx, mapping, code, reason)` and `RestoreBridgedComment(ctx, mapping) (deleted bool, err)`. Ingest reads `CommentRemovalPublisher` off the Materializer by type assertion; a missing surface is a retryable error. Expvars: `tidepool_moderation_comment_removal_records_{written,deleted}`.
- **Native comment removal reads (02).** `store.ObjectModeration.SetRemoval` returns `(removedAt time.Time, err)`, and `RemovedAmong(ctx, atURIs ...string)` reads removals. Consume's `refuseUnderRemovedComment` refuses creates under a removed comment and updates of a removed comment (`ErrPermanentEvent`); edits of replies beneath a removed parent are not gated.
- **Bridged removal predicate (02).** `deleteIsCommunityRemoval` (`internal/ingest/consent.go`) treats a `Delete` as a community removal when a `summary` is present (even empty), or when there is no summary and the actor is not the author. Task 03 reuses it. A bridged comment removal no longer soft-deletes the mapping or tombstones: the record stays live and author-editable. `handleUndoDelete` deletes the bridged removal record after the re-fetch and `HandleUpdate`.
- **Lemmy 410 finding (02).** Lemmy 0.19.20 serves a moderator-removed comment as HTTP 410 Tombstone (probed on the e2e stack: 410 while removed, 200 after restore). So `POST /admin/objects/sweep-deleted` (`SweepDeleted` → `HandleDeleteRecord` → `deleteMapping`) hard-deletes a removed bridged comment's record and soft-deletes its mapping, leaving the removal record pointing at a missing subject. A later `Undo{Delete}` then re-materializes through the old v1 path. Recorded, not fixed; see the sweep proposal below.
- **Community-removal withholds (03).** One `blob_withholds` row per (subject URI, CID): source `community-removal`, scope `subject`, `owner_did` = the subject's repo DID, `reason_code` = the removal record's code. Rows are set and lifted in the same transaction as the record commit (`removePost`, `RestorePost`, `WriteCommentRemoval`, `DeleteCommentRemoval`, `commitRecord`). `blobwithhold` adds `AddSubjectRemovalWithholds` (add-only), `LiftSubjectRemovalWithholds` and `ActiveSubjectRemovalCIDs`, all on the caller's transaction. Rows are add-only while the removal stands: an edit or a failed image fetch that drops a CID, and a plain delete of the subject (`deleteMapping`), keep its row, because the bytes stay stored and lifting would serve the removed image. Only a restore lifts. Trade-off: after a delete the CID stays refused for the owner, also for the owner's other live records. New `repo.GetRecordTx`. A removal record with no code fails the in-transaction read, which blocks commits of that subject.
- **Ban remove-content conversion (03).** A community ban with "remove content" now leaves the banned person's bridged comments live with `author-banned` removal records (`BanWithdraws` covers comments through `removeBridgedCommentFrom`) instead of deleting them and their blobs.
- **Comments carry no embed (03).** `commitCommentLeaf` writes no embed today, so comment withholds do nothing in production until bridged comments carry images.
- **Removal-withhold back-fill (03).** `POST /admin/moderation/backfill-removal-withholds` `{"dry_run":…}` on `ingest.Admin` (bearer group), reaching `RemovalWithholdBackfiller` by type assertion. It re-puts records with a CAS check and no repo commit. `repo.Manager.WithCommitLock` (per-DID mutex plus the global commit advisory lock, no commit, no signing key) serialises it with live commits. A failure on one removal or community is counted `failed` and the run continues.
- **Ban records (04).** `materialize/ban_record.go` provides `BanRKey`, `WriteBanRecord`, `WriteBanRecordIfUnchanged`, `DeleteBanRecord`, `DeleteBanRecordIfUnchanged` and `StandingBanRecord`. The rkey is lowercase unpadded base32 SHA-256 of the subject DID, the same encoding `acceptrec.SubjectRKey` uses over an AT-URI. Bridged persons hold no `community_bans` row; the standing ban record's `expiresAt` is the stale-unban comparison. `community_bans.Ban()` returns `refused` when its guard declines a lapsed write over a standing ban; a refused `Ban()` writes no ban record, so the standing row and record stay in step.
- **Lapsed rule at receipt (04).** Live, a bridged stale unban is judged against now. The ban back-fill judges each replayed event at its own `received_at`: a `Block` already over at receipt replaces nothing, and an `Undo{Block}` lifts the standing ban unless the standing ban is in force and outlives the expiry the Undo names.
- **Ban-record back-fill (04).** `POST /admin/moderation/backfill-ban-records` `{"dry_run":…}`. Replay errors stop the run with a 500; a per-ban publish failure counts `failed` and the run continues. Writes are create-only and never overwrite a standing record. `tests/e2e/helpers.go` `expectedCollections` includes `social.coves.moderation.ban`.
- **Lemmy announces bans (04).** Lemmy 0.19 announces a community ban and unban of a remote native Coves persona to followers (verified on the e2e stack).
- **Ingest retries.** The ingest queue never claims a newer event while an older one on the same ordering key is backing off. Retry tests must `UPDATE inbox_events SET next_attempt_at = now()`.
- **`/admin/reemit`.** Removal records are tier 3; ban records fall into the default tier 2, so a re-emit briefly deletes and recreates them. Re-emitting a comment removal briefly restores the comment in Coves.
- **Lexicon check on home-pc.** `shasum` is not on the default PATH; run `PATH=/usr/bin/core_perl:$PATH ./scripts/check-lexicons.sh` (or use a `sha256sum` shim).

### Deferred and open

Scope proposals, not approved:

- **Extend the blob store lock** to the paths that still take none: operator withhold insert, `DeleteBytes`, `DeleteBlob` and `DeleteBlobsForDID`, to close the READ COMMITTED window between a withhold committing and a concurrent store or automatic delete. `PutBlob` (shared) and `DeleteBlobTx` (exclusive) already take `lockBlobStore` in `internal/repo/blob.go`, keyed on (DID, CID) in namespace `0x626c`. Any extension must reuse `lockBlobStore`; a second lock would not interlock with it. Operator withholds and `DeleteBytes` act on a CID across every DID, so they would take the lock for each (DID, CID) they touch. Failure prevented: bytes surviving a confirmed delete-bytes, or an automatic delete racing a withhold. The single-statement guards were applied instead. About 40 lines plus a forced-interleaving concurrency test.
- **`sweep-deleted` skips comments with a standing removal record.** This now matters: since 02 keeps removed bridged comments live, a sweep run between a Lemmy moderator removal and its restore (Lemmy answers 410) hard-deletes the comment and strands its removal record, and the restore cannot re-materialize it. Smaller alternative: never run the sweep while removals are pending. About 20 lines plus one integration test.

Known pattern, not changed: postv2 `Undo{Delete}` compensates any `HandleUpdate` error by re-soft-deleting and tombstoning, and calls `RestorePost` only after a successful re-materialization, so a skipped postv2 restore leaves the removal standing.

Remaining review suggestions, one line each (the review reports are not kept):

- 01 (review-r1):
  - Superseded for `PutBlob` against `DeleteBlobTx`: `PutBlob` now runs `CoversTx` and the INSERT in one transaction under the shared blob store lock, which `DeleteBlobTx` takes exclusively. A withhold written by a path that takes no lock can still commit between the check and the insert; closing that is the lock-extension proposal above.
  - `Inspect`'s `bytes_deleted` is true if any row ever had `deleted_at`, lifted ones included; document it as historical or compute it from active rows.
  - `Store.Insert` accepts `source='operator'` rows with any scope and no `created_by`; reject them (test-only caller today).
  - The admin `decodeBody` drops unknown JSON keys, so a typo in `csam_case_id` loses the audit reference; use `DisallowUnknownFields`.
  - `ADMIN_API.md` omits the 400 for an empty actor or non-JSON body, that a repeat withhold with a new reason stores nothing, to run delete-bytes before lifting removed content, and says "site bans" where it means all bans.
  - The `materialize/blobs.go` comment on lifting overstates; serving resumes only while the bytes are still stored.
  - Lift and delete-bytes log no reason code; an idempotent withhold logs the requested reason, not the persisted one.
  - `Canonicalize` turns a CIDv0 into a dag-pb CIDv1 that never matches a raw blob; reject non-raw codecs.
  - Small comment fixes in the migration and `requireBearer`; wrap the `NewAdmin` error in `main.go`.
  - The e2e cleanup discards a failed lift; log it.
  - `DeleteBlobsForDID` skips withheld rows without a log line.
- 01 (review-r2): no test pins `HasActiveOperator`'s `lifted_at IS NULL`.
- 02 (review-r1):
  - `moderationStamp` treats any `GetRecord` or parse error as no record and restamps `createdAt`; fall back to now only on NotFound.
  - A redelivered Delete after an author edit rewrites the removal record's `subject.cid`.
  - Add a compile-time assertion that the Materializer implements `CommentRemovalPublisher`.
  - `RemoveBridgedComment` discards `written`, so no-op redeliveries log "removal record published".
  - The comment removal counters are asserted by no test.
  - Bridged writers re-derive the community from the mapping and never compare it with the announcer DID.
  - `SetRemoval` commits before `WriteCommentRemoval`; an empty native mapping CID poisons the event after the bridge state changed.
  - The completion path does not increment `NativeCommentRemovalLifted`.
  - `RestoreBridgedComment` returns a ValidationError for v1-era rows with no community DID, poisoning the Undo after `HandleUpdate` succeeded.
  - Stale `moderateAnnouncedDelete` doc comments; unwrapped `return err` from `deleteIsCommunityRemoval`.
  - `RemovedAmong` is always called with one URI; an `IsRemoved` would read better.
  - The vendored `removal.json` description was stale; the 04 lexicon sync has since replaced it.
- 03 (review-r1):
  - The removal-withhold back-fill dry run reaches `ApplyOpsTx` and writes nothing only because re-encoding is byte-identical; use a read-only transaction.
  - A subject skipped by the CAS check is counted nowhere in the report.
  - `blobwithhold.Execer` admits `*sql.DB`, under which lift-then-insert is not atomic; take `*sql.Tx`.
  - A comment whose thread root was deleted has an empty stored community, so an edit is not re-synced against its removal.
  - Withhold rows store no community DID, so a restore in one community would lift rows another community's removal of the same subject needs (no such path today).
  - `Store.Insert` still accepts `source='community-removal'`.
  - Test gaps: the edit path's withhold-write failure has no fault test; no assertion of one row per CID after an edit.
  - Comment and doc wording (header overstates when the rule runs, "commit lock" naming, race window documented only in the back-fill doc); Go nits.
  - Pre-existing: a summary-less postv2 moderator removal can never be restored (the Undo requires `HasSummary()`), so its withhold never lifts; candidate for the backlog.
- 03 (review-r3): the NotFound guard in `internal/materialize/updates.go` is broader than the unhosted-actor case it is for; check `bridged_actors` first.
- 03 (review-r4): a cancel in the middle of the removal-withhold back-fill counts the remaining records as `failed` instead of returning the error.
- 04 (review-r1):
  - `ADMIN_API.md` states the undecidable rule too narrowly and omits the 500 outcome.
  - An Undo whose undone Block has an unreadable expiry is undecidable in the back-fill but lifts unconditionally live.
  - A CAS precondition miss (the live path got there first) counts as `failed` and logs at Error; count it separately.
  - A standing record with an unreadable `expiresAt` poisons every bridged Undo; `StandingBanRecord` does not check the stored `subject` (neither reachable while Tidepool is the only writer).
  - `banSubjectMinted = iota` makes the zero value the publishing kind.
  - `banSubject` and `bridgedPerson` duplicate the Person-with-DID check.
  - Comment accuracy: `WriteBanRecord`'s createdAt (the native back-fill passes the first `banned_at`), the `BanRKey` argument type, and the lapsed bridged Block with removeData.
  - Test gaps: clearing `undecidableBy`, per-ban failure with the run continuing, `createdAt` values, redelivered-Block idempotency, nonzero `SkippedUnminted`.
  - `/admin/reemit` puts ban records in tier 2.
- 04 (review-r2):
  - An Undo committing between the back-fill's re-read and its create-only write leaves a lifted ban published; re-read once more after the write and delete if lifted.
  - The `ListBanEventsAfter` id watermark is not commit-visibility safe; compare the pair's full event-id set instead.
- 04 (review-r3):
  - The ban expiry gate is a Go-clock check before `RemoveAuthorPosts`; a post admitted in the gap is removed after expiry. Check expiry on the DB clock in the same statement.
  - The `position('"Block"' IN payload)` filter misses a JSON-escaped type name; classify after parsing.

Rollout order:

1. Deploy the matching Coves tasks before each Tidepool chunk that publishes records Coves must index (comment removal records, ban records).
2. After Coves deploys, the operator runs each back-fill once, dry run first: `POST /admin/moderation/backfill-removal-withholds` and `POST /admin/moderation/backfill-ban-records`. Run the ban back-fill while no moderation is arriving.
3. A `DEPLOY.md` runbook entry for the operator blob withhold procedure is left to the user.
