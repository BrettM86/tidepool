# PRD: store bridged blob bytes in object storage

Status: approved 2026-11-14 after `/second-opinion` (codex plus four Claude
reviewers). D1 to D3 as recommended; D4 is retry.

## Problem

Every bridged image (avatars, banners, post images, link thumbs) is a row in
the Postgres `blobs` table with its bytes in a `BYTEA` column
(`internal/repo/blob.go`). Measured on prod 2026-11-14:

- 30,094 rows, 8.0 GB of bytes, 8.4 GB on disk with TOAST. That is over half
  of the 15 GB Tidepool database.
- 29,743 distinct CIDs: about 1% of rows share bytes with another DID.
- Average 279 kB, largest 5.1 MB (`MAX_BLOB_BYTES` defaults to 5 MiB).
- Growth is about 40 to 50 MB/day at baseline, with spikes when a new
  instance backfills (2.7 GB on 11-12 when sh.itjust.works was followed).

Each nightly dump (about 11 GB, 14-day retention, 108 GB on disk) carries
the full blob set again. Bridging lemmy.world would multiply both.

## Outcomes

1. The byte store is chosen by `BLOB_STORE`: `postgres` (the default, bytes
   in `blobs.bytes` as today) or `s3`. Dev and tests can run without a
   bucket, and prod switches by setting `BLOB_STORE=s3`.
2. In s3 mode, new blob bytes go to an OVH S3 bucket. The `blobs` table keeps
   one row per stored blob (did, cid, mime_type, size, created_at) with
   `bytes` NULL.
3. A one-time command copies the existing 8.0 GB to the bucket and sets each
   copied row's `bytes` to NULL. Dumps lose the bytes from then on; the
   database files shrink after a `VACUUM FULL`.
4. Takedown and delete semantics are unchanged from the outside:
   - getBlob answers 404 for anything withheld or not stored, exactly as now.
   - Author delete, record scrub and Delete(Actor) remove bytes as they do
     now, and still keep bytes under an active operator withhold.
   - Operator `delete-bytes` removes every owner's copy of the CID, and the
     objects are gone from the bucket, not just unreferenced. This includes
     objects whose PutBlob failed after uploading.
5. A failed bucket delete is retried until it succeeds. A removed blob's bytes
   never stay in the bucket because one request failed.
6. Dev, unit, integration and e2e flows run against a local MinIO container,
   never a real bucket.
7. A byte-store outage never bridges a post or profile without its images:
   the work is retried until the store answers.

## Design

### Packages

A new leaf package `internal/blobstore` holds the byte store interface, its
`postgres` and `s3` implementations, the object delete queue (enqueue and
process) and the sweeper. Both `internal/repo` and `internal/blobwithhold`
import it; `blobwithhold` still must not import `repo` (withhold.go:4-5).

### Byte store switch

Picked once at startup from `BLOB_STORE`; any value other than `postgres`,
`s3` or unset fails startup.

- `postgres`: bytes live in `blobs.bytes`, written and deleted in the same
  statement as the row, as today. No queue rows are written and no sweeper
  runs. A row read with NULL `bytes` is an internal error with code
  `blob_bytes_in_object_store`, never an empty 200 (today `Scan` of a NULL
  BYTEA gives nil data and no error).
- `s3`: bytes live in the bucket as described below.

The withhold checks, the blob store locks and the row-as-truth rule are the
same in both modes.

Switching modes:

- `postgres` to `s3`: rows that still have bytes are served from Postgres
  until `tidepool migrate-blobs` moves them. This is the supported direction.
- `s3` to `postgres`: startup fails with code `blob_bytes_in_object_store`
  and the count if any row has NULL `bytes`, and with code
  `blob_object_deletes_pending` and the count if `blob_object_deletes` has
  rows (postgres mode cannot drain them). Moving bytes back from the bucket
  is out of scope, so once a blob has been written in s3 mode there is no
  rollback to postgres.

### Storage layout (s3 mode)

- One object per `blobs` row, key `blobs/<did>/<cid>`. Per-(DID, CID) keys
  mirror the primary key, so deleting one row deletes exactly one object with
  no reference counting. The 1% duplication costs about 50 MB.
- Keys are built only from the stored row's `did` and `cid` (or, in PutBlob,
  from the computed CID). The key builder rejects a CID whose
  `cid.Parse(s).String() != s` and a DID containing `/` or a `.`/`..` path
  segment.
- Objects carry the row's mime type, but getBlob takes `Content-Type` only
  from the row and copies no header or metadata from the S3 response. The
  bucket is private; only Tidepool reads it.

### Locks

Today's per-(DID, CID) blob store lock stays. A new per-CID lock is added
(its own advisory lock namespace, key hashed from the CID):

- PutBlob takes the CID lock shared, then the (did, cid) lock shared.
- `DeleteBytes` takes the CID lock exclusive before its DELETE.
- The queue processor and `migrate-blobs` take the (did, cid) lock exclusive.

Without the CID lock, a PutBlob that passed its withhold check before the
operator's withhold can commit its row after `DeleteBytes` ran, leaving the
CID stored. That race exists today, but an S3 upload widens it from a
statement gap to hundreds of ms. With the lock, `DeleteBytes` waits for
every PutBlob in flight for that CID, then sees their rows. PutBlobs that
start later see the withhold. Lock order is always CID before (did, cid), so
no cycle.

### Writes (PutBlob, s3 mode)

Validate, compute the CID, begin tx, take the locks, check withholds (a
withheld blob is still never uploaded), then:

1. Read the row for (did, cid). If it exists with NULL bytes, check the
   object (StatObject) and commit without uploading only if it is there.
   This lets re-materialization repair a row whose object is missing (after
   a restore, for example).
2. Insert a queue row for (did, cid) in its own committed statement, outside
   the tx. The processor cannot act on it until this PutBlob releases its
   shared lock, and then drops it if the row exists. This covers every way
   the PutBlob can fail after uploading, including a process crash, so no
   orphan object escapes `delete-bytes`.
3. Upload with `Content-MD5` set, so the bucket rejects a corrupted body.
4. `INSERT ... ON CONFLICT DO NOTHING`, commit. The insert keeps today's
   behaviour of waiting on a concurrent uncommitted delete.

### Byte store failures retry the work (D4)

A bucket outage must not bridge a post or profile without its images. Today
`fetchMedia` (internal/materialize/blobs.go:115-118) logs "blob store
failed; omitting image" and the record commits without the image. That
changes in both modes:

- PutBlob returns an error satisfying a new `errors.IsBlobStoreUnavailable`
  for any byte-store failure (S3 error or timeout, or the Postgres insert
  failing). It is logged at Error with code `blob_object_put_failed`. A
  withhold refusal and a validation error are not this error.
- `fetchMedia` returns it instead of omitting the image, and every caller
  passes it up and fails the materialization before committing anything:
  `fetchEmbedImage`, `fetchBlob` and `unfurlThumb` (posts.go),
  `fetchBlobClassified` and `fetchBlobWithCarryForward` (actors.go). Carry-
  forward does not apply to it: the profile refresh fails and the existing
  profile record stays as it is.
- Media problems that are about the image itself (fetch failure at origin,
  wrong type, oversize, failed transcode) still omit the image, as today.
- The inbox queue (internal/ingest/queue.go) treats it like PLC throttling:
  `isThrottled` matches it, so the event is parked without spending attempts
  and retried for as long as the outage lasts. It is never deferred or
  poisoned for it.
- Backfill counts it as an item failure, so the walk is not stamped complete
  and is retried. It is never host-gone: it does not go into a host's
  failure streak (`recordHostGone`) and is not deferred.
- Reconcile stops the sweep on it, as for throttling (reconcile.go:760,
  :911), so a community is not given a failed-add skip because of the
  bucket.
- The refresh-profile admin endpoint answers 503 with "blob store
  unavailable".
- `aspect_ratio_backfill.go` reads only, so it is unchanged beyond the
  `blob_object_read_failed` handling under Reads.

### Deletes (s3 mode)

`blob_object_deletes` has primary key (did, cid), plus queued_at, attempts
and last_error. Enqueue is `ON CONFLICT DO NOTHING`. Every path that deletes
`blobs` rows enqueues in the same transaction, using
`WITH d AS (DELETE ... RETURNING did, cid) INSERT INTO blob_object_deletes
SELECT ...`:

- `DeleteBlob`, `DeleteBlobsForDID` (today single autocommit statements; the
  CTE keeps them single statements).
- `DeleteBlobTx`, in the caller's transaction.
- `blobwithhold.Store.DeleteBytes`, after taking the CID lock.

Rows kept by an operator withhold are not deleted and not queued, as now.

Processing:

- `DeleteBlob`, `DeleteBlobsForDID` and `DeleteBytes` process their own queue
  rows right after commit.
- `DeleteBlobTx` runs inside `ApplyOpsTx`, holding the exclusive lock in a
  transaction the repo Manager cannot see commit. Processing there would wait
  on its own lock, so its rows are left to the sweeper.
- A sweeper goroutine processes all remaining rows every minute.

Processing one queue row: begin tx, take the exclusive (did, cid) lock. If a
`blobs` row exists, drop the queue row (the object is live). Otherwise delete
the object (deleting a missing key succeeds), drop the queue row, commit. On
S3 failure, record attempts and last_error, keep the row, and log at Warn
with code `blob_object_delete_failed` (DID, CID, attempts). A row older than
1 hour also logs at Error each sweep with code `blob_object_delete_stuck`.

`blobwithhold.State` gains `pending_object_deletes` (queue rows for the CID),
so every admin response carries it, and `docs/ADMIN_API.md` is updated. The
`delete-bytes` response is computed after its inline processing, so it shows
0 when the bucket deletes succeeded.

### Reads (GetBlob)

Withhold check as now, then `SELECT bytes, mime_type, size` for the row (404
if none). Non-NULL bytes are served from Postgres (rows not yet migrated).
Otherwise GET the object:

- NoSuchKey (which minio-go reports on read or stat, not on GetObject):
  re-read the row and the withhold. If the row is gone or now withheld, a
  delete raced the read: answer 404 and log nothing. If the row is still
  there, log code `blob_object_missing` and return an internal error (500).
- Any other S3 error: code `blob_object_read_failed`, internal error. Never
  NotFound, so an outage never looks like a removal, to getBlob or to the
  aspect-ratio backfill (aspect_ratio_backfill.go:222), which counts NotFound
  as an unreadable image.
- The body is read through `io.LimitReader(size+1)`. A length other than
  `size` is code `blob_object_size_mismatch`, internal error, not served.

`GetBlob` keeps its `[]byte` signature. A blob is at most
min(slot cap, `MAX_BLOB_BYTES`): 5 MiB by default, and the largest slot is
10 MB.

### S3 client

- `github.com/minio/minio-go/v7` (D3), path-style addressing (as the Coves PDS
  uses with OVH), `credentials.NewStaticV4` only. No credential chain: chain
  providers read `~/.aws` and call 169.254.169.254. No `TraceOn`.
- A deadline on every call: 30 s for PUT and GET, 10 s for DELETE and STAT. A
  timeout takes the normal error path. This bounds how long a stalled bucket
  can hold a database connection and an advisory lock. The pool is 25
  connections (internal/db/db.go); PutBlob is called by ingest
  (`INGEST_WORKERS`, default 4), backfill and profile refresh.

### Configuration

- `BLOB_STORE`: `postgres` (default) or `s3`.
- Required when `BLOB_STORE=s3`, ignored otherwise: `BLOB_S3_ENDPOINT`,
  `BLOB_S3_REGION`, `BLOB_S3_BUCKET`, `BLOB_S3_ACCESS_KEY_ID`,
  `BLOB_S3_SECRET_ACCESS_KEY`. A partial set fails startup.
- With `ENVIRONMENT=development`, the endpoint must be `localhost`, a loopback
  IP or the compose host `minio`; anything else fails startup before any
  request.
- s3 startup does one bucket HEAD and fails if the bucket is unreachable or
  the credentials are wrong. Its error names the endpoint and bucket only.
  A restart during an OVH outage therefore keeps the whole bridge down;
  `DEPLOY.md` says so.
- The secret key is never logged.
- One shared function parses and validates these variables, used by
  `config.Load` and by `migrate-blobs`.
- `docker-compose.prod.yml` forwards `BLOB_STORE` and the five `BLOB_S3_*`
  variables to the `tidepool` service (it lists its environment explicitly).
- `docs/CONFIGURATION.md` documents all six.

### Migration

One release, then an operator switch and copy.

**Release** (schema + code; prod stays on `postgres`, so nothing changes):

- Migration: `ALTER TABLE blobs ALTER COLUMN bytes DROP NOT NULL`; create
  `blob_object_deletes`. The column stays: it is the `postgres` mode's store.
- New subcommand `tidepool migrate-blobs [--concurrency N]` (default 4).
  Like `rotate-kek` it reads a minimal environment (`DATABASE_URL`,
  `BLOB_STORE`, `BLOB_S3_*`) and does not call `config.Load`; `dispatch`
  gains argument parsing and the usage string lists it. It refuses to run
  unless `BLOB_STORE=s3`, and does the same bucket HEAD as startup.
- It pages rows `WHERE bytes IS NOT NULL` by (did, cid) keyset. For each row,
  in one tx: take the exclusive (did, cid) lock, `SELECT ... FOR UPDATE`
  (skip if gone or already NULL), check that the bytes hash to the row's
  CID, upload with `Content-MD5`, `UPDATE ... SET bytes = NULL`, commit. An
  UPDATE that affects 0 rows counts as skipped.
- A delete racing the copy is safe through the row lock and the queue: an
  unlocked delete path waits on the row lock and then enqueues the object,
  and the processor waits on the exclusive lock until the copy commits.
- Rows under operator or community withholds are copied too: those bytes
  must survive until lifted or deleted.
- A CID mismatch leaves the row as is, is counted as `mismatched` (not
  failed), and is listed by (did, cid). Those bytes are already corrupt in
  Postgres, so the operator deletes each listed row by hand in psql.
- It logs copied, skipped, mismatched and failed counts with did/cid only,
  exits non-zero if any row failed, and is safe to stop and re-run. 30k rows
  at 4 parallel uploads should take well under an hour, with the bridge
  serving traffic.

**Operator steps** (in `DEPLOY.md`):

1. Create the bucket and key (D1). Confirm versioning is off and Object Lock
   is not configured (D2) with `ovhcloud cloud storage object bucket get
   <REGION>_<BUCKET> --cloud-project <PROJECT_ID>` (versioning DISABLED,
   objectLock DISABLED), and that an anonymous GET of the virtual-host object
   URL `https://<BUCKET>.s3.us-west-or.io.cloud.ovh.us/blobs/<DID>/<CID>`
   answers 403. Path-style anonymous requests answer 400 InvalidRequest on
   OVH; that is not a pass.
2. Add the `BLOB_S3_*` variables and `BLOB_STORE=s3` to `/opt/tidepool/.env`
   and recreate the `tidepool` service. From here there is no rollback to
   postgres mode.
3. `docker compose -f docker-compose.prod.yml run --rm tidepool migrate-blobs`
   until it reports 0 failed.
4. Check `SELECT count(*) FROM blobs WHERE bytes IS NOT NULL` equals the
   mismatched count, and handle those rows.
5. Spot-check getBlob on a sample of old and new CIDs through `tdpl.io`. The
   next nightly dump no longer carries the bytes.
6. `VACUUM FULL blobs` to return the TOAST space to the OS. The table is
   small by then, so the rewrite takes seconds, but it blocks getBlob and
   PutBlob while it runs.

### Backups and restore

The bucket becomes the only copy of blob bytes, as the Coves PDS bucket
already is. The restore runbook in `DEPLOY.md` gains these notes:

- Restored rows for blobs deleted after the dump point at missing objects and
  answer 500 with `blob_object_missing`. Re-materializing the record repairs
  them (PutBlob step 2).
- Operator withholds and `delete-bytes` actions taken after the dump are not
  in the restored database. Before the bridge serves traffic, re-apply every
  one recorded since the dump (admin API log lines), or their bytes become
  servable again.

## Decisions

- **D1. Bucket.** Recommended: a new bucket (working name
  `tidepool-prod-blobs`) in the same OVH US account and `us-west-or`
  endpoint. OVH's built-in S3 user roles cover the whole project, which also
  holds the Coves PDS bucket, so the Tidepool user gets a bucket policy
  limited to `s3:GetObject`, `s3:PutObject` and `s3:DeleteObject` on
  `tidepool-prod-blobs/blobs/*` and `s3:ListBucket` on the bucket, with no
  policy or ACL rights. The alternative is a prefix in the Coves PDS bucket,
  which saves a bucket but gives Tidepool a key that can reach Coves' blobs.
  It must not share the backups bucket, because that bucket expires objects.
- **D2. Versioning.** Recommended: off. With versioning on, a delete only
  adds a delete marker and the bytes stay, so `delete-bytes` would need to
  delete every version and author deletes would need a noncurrent-version
  expiry. Most lost images can be re-fetched from their origin instance.
  This matches the Coves PDS bucket.
- **D3. Client.** Recommended: `minio-go/v7`, smaller than `aws-sdk-go-v2`,
  and the same library tests use against MinIO.
- **D4. Bucket outage during ingest.** Decided 2026-11-14: retry. A
  byte-store failure fails the materialization and the work is retried, so
  no post or profile is bridged without its images. Design under "Byte store
  failures retry the work".

## Testing

- The blob behaviour tests run against both modes: `postgres` needs only the
  test database, `s3` also needs MinIO. The e2e suite runs with
  `BLOB_STORE=s3`.
- `docker-compose.dev.yml` gains a MinIO service bound to `127.0.0.1`;
  `docker-compose.e2e.yml` gains one named `minio` on the compose network.
- Tests read only `TIDEPOOL_TEST_S3_*` variables, never `BLOB_S3_*`, so a
  shell with the prod `.env` sourced cannot point them at OVH. The helper
  creates a fresh bucket per test. It allows only `localhost`, a loopback IP
  or `minio`, and calls `t.Fatal` otherwise. Unlike `testPLCURL`, it fails
  instead of skipping when MinIO is unreachable or under `-short`.
- Three tests match rows on `blobs.bytes` and switch to a helper that reads
  the stored bytes in either mode: `internal/materialize/postv2_create_test.go`,
  `internal/ingest/removal_withhold_test.go`,
  `internal/ingest/removal_withhold_race_test.go`. Tests that count or list
  rows by did/cid are unchanged.
- New tests:
  - PutBlob stores the object and row; a withheld put uploads nothing.
  - A PutBlob whose commit fails after upload leaves a queue row, and the
    processor deletes the object.
  - PutBlob of a row whose object was removed out of band re-uploads it, and
    getBlob answers 200.
  - Each delete path removes the object; a kept operator-withheld blob keeps
    its object. A record delete through `deleteAuthorRecord` is removed by the
    sweeper.
  - A failing bucket delete leaves a queue row and logs
    `blob_object_delete_failed`; the caller still succeeds; the sweeper
    removes the object once MinIO answers.
  - A PutBlob racing a queued delete of the same (did, cid) ends with row and
    object both present (extends `blob_store_lock_test.go`).
  - A PutBlob paused after its withhold check while the operator withholds
    and runs `delete-bytes`: `delete-bytes` waits, and ends with no row and no
    object.
  - With MinIO stopped, an inbox Announce of an image post is parked with
    no attempt spent and nothing committed; once MinIO is back the post
    lands with its image. A backfill item in the same state fails the walk
    without touching the host's failure streak. A profile refresh keeps the
    existing profile record. Origin-side media failures still omit the
    image.
  - `delete-bytes` across several owners removes every object and reports
    `pending_object_deletes` 0.
  - getBlob: 404 for withheld; 404 with no `blob_object_missing` log when a
    delete commits between the row read and the GET; 500 with
    `blob_object_missing` for a row whose object was removed out of band;
    500 with `blob_object_read_failed` when MinIO is stopped; postgres mode
    answers 500 with `blob_bytes_in_object_store` for a NULL-bytes row.
  - `migrate-blobs`: copies and nulls rows, skips a row deleted mid-run,
    ends with no row and no object when `delete-bytes` races it on the same
    CID, counts and lists a CID-mismatch row, and a re-run is a no-op.
  - e2e `TestImagePost_EmbedImagesAndNSFWLabel` (tests/e2e/media_test.go) and
    `TestBlobWithhold_OperatorWithholdSurvivesEditAndLifts`
    (tests/e2e/blob_withhold_test.go) pass unchanged against MinIO.
  - Startup: an unknown `BLOB_STORE` fails; `s3` with a partial `BLOB_S3_*`
    set fails; development with a non-local endpoint fails before any
    request; `postgres` fails with `blob_bytes_in_object_store` for a
    NULL-bytes row and with `blob_object_deletes_pending` for a queue row.

## Out of scope

- Moving the Coves PDS or any Coves storage.
- The backups bucket and dump upload (separate spec).
- Moving bytes from the bucket back to Postgres.
- Garbage collection of blobs no record references (no GC today either).

## Proposals (not in scope, need approval)

- **Startup versioning check.** Prevents: deletes that succeed while the bytes
  stay as old versions, if someone turns versioning on later. A
  `GetBucketVersioning` call at startup would fail with
  `blob_bucket_versioning_enabled`. Smaller alternative: the DEPLOY.md check
  in operator step 1. Added work: about an hour.
- **Startup write probe.** Prevents: a read-only key that passes the HEAD
  check while every put and delete fails. A put, get and delete of
  `blobs/_probe/<random>` at startup. Smaller alternative: the
  `blob_object_put_failed` and `blob_object_delete_failed` logs. Added work:
  about an hour.
- **Recorded bucket identity.** Prevents: a changed `BLOB_S3_BUCKET` or
  endpoint making queued deletes "succeed" against the wrong bucket. Store the
  bucket and endpoint on first s3 start and fail on a mismatch. Smaller
  alternative: a DEPLOY.md warning. Added work: about two hours.
- **Hash check on read.** Prevents: serving bytes altered in the bucket by
  anyone holding the key. Re-hash on every getBlob. Smaller alternative: the
  size check. Added work: about an hour, plus CPU per read.
- **getBlob redirect.** Prevents: Tidepool proxying every image byte.
  Answering 302 to a short-lived presigned URL would move image traffic off
  the server, but loses the safety headers unless the bucket sets them.
  Smaller alternative: keep proxying. Added work: about a day.

## Implementation notes

Built in six chunks on `prd/prd_blob_object_storage`:

- 01 blob store seam, postgres mode — eb2014c
- 02 store outage retries work — f1182bb
- 03 s3 object writes and reads — 29c55cc
- 04 s3 object delete queue — 2e9e913
- 05 s3 mode switch — 893531e
- 06 migrate-blobs command — cbe3875

### Code facts

- The seam is `blobstore.Store` in `internal/blobstore`, which imports only
  `internal/errors`. Its methods are `Put`, `Get`, `Delete`, `DeleteForDID`,
  `DeleteCID` and `ProcessObjectDeletes`. The delete methods take a
  `Querier` (`*sql.DB` or `*sql.Tx`) so s3 mode can queue object deletes in
  the caller's tx. `ProcessObjectDeletes` is a no-op in postgres mode.
- `NewPostgres(db)` and `NewS3(db, S3Config)` build the two stores.
  `repo.WithBlobStore` injects the store into the repo Manager.
  `blobwithhold.NewStore(db, byteStore)` requires the byte store, and
  `cmd/tidepool/main.go` passes it the same store the Manager uses.
- `cmd/tidepool/blob_store.go` `openBlobStore` runs `NewS3` and one
  `HeadBucket`. `run` starts `S3.Run`, a one-minute delete sweeper, only in
  s3 mode. `migrate-blobs` reuses the same config parser,
  `config.LoadBlobStore`.
- Lock order is always the CID lock, then the (did, cid) lock. The
  advisory lock namespaces are `CIDLockNamespace = 0x6263` (`LockCID`) and
  `LockNamespace = 0x626c` (`Lock`). `TestCIDLockKeyIsPinned` and
  `TestLockKeyIsPinned` pin them. `blobwithhold` DeleteBytes takes the CID
  lock exclusive right after BEGIN in both modes.
- Error codes are log attributes (`"code", "<code>"`):
  `blob_bytes_in_object_store`, `blob_object_deletes_pending`,
  `blob_object_put_failed`, `blob_object_missing`, `blob_object_read_failed`,
  `blob_object_size_mismatch`, `blob_object_delete_failed`,
  `blob_object_delete_stuck`.
- PutBlob returns `IsBlobStoreUnavailable` for every failure other than a
  withhold refusal or a validation error. It logs `blob_object_put_failed`
  only while the caller's context is still live. `isThrottled` in
  `internal/ingest/queue.go` matches the same error, so the inbox parks the
  event.
- In s3 mode PutBlob's write-ahead queue row stays after a successful
  commit until the sweeper drops it. So inspect can report a non-zero
  `pending_object_deletes` for a recent upload.
- `BLOB_S3_ENDPOINT` is scheme, host and optional port only. Errors never
  repeat the raw value. In development the host must be `localhost`, a
  loopback IP or `minio`, and s3 mode also needs `ALLOW_PRIVATE_FETCH=1`,
  because S3 requests go through the SSRF-guarded client.

### Tests

- The s3 tests need the test MinIO: `make test-s3-up` starts container
  `tidepool-test-minio` on 127.0.0.1:9010 under the fixed Compose project
  `tidepool-test-s3`. `make test` exports `TIDEPOOL_TEST_S3_ENDPOINT`,
  `_ACCESS_KEY_ID`, `_SECRET_ACCESS_KEY` and `_REGION`. A shell that runs
  `go test` directly must copy those values from the Makefile. With them
  unset or MinIO down, the s3 tests fail; they never skip.
- S3 tests read only `TIDEPOOL_TEST_S3_*`, never `BLOB_S3_*`, and fail on
  any host other than `localhost`, a loopback IP or `minio`. Build every
  test S3 store with `testutil.LocalS3Transport` (no proxy, local dial
  only). To simulate MinIO stopped, cut a per-test loopback connection.
  Never stop the shared container.
- Test MinIO, CI and the e2e `minio` service all use
  `pgsty/minio:RELEASE.2026-08-04T00-00-00Z@sha256:b6bfe7239bfc83fb90d31612d9704d86039dd714f7904b3f1ad68f211e602372`.
  It is the Pigsty community build of the MinIO server, used because the
  official image could not be pulled on 2026-11-15. CI starts it with
  `docker run`, because GitHub service containers cannot pass `server /data`.
- e2e runs tidepool with `BLOB_STORE=s3` against `http://minio:9000`.
  `TestImagePost_BlobBytesInObjectStore` checks that the post image's row
  has NULL bytes. `make e2e` takes about 16 to 20 minutes.
- Other sessions on the dev host share the `tidepool-e2e` Compose project.
  Wrap every `make e2e`, `e2e-up` and `e2e-down` in the shared lock:
  `flock -w 5400 ~/.cache/tidepool-e2e.lock bash -c 'test -z "$(docker compose -p tidepool-e2e ps -q)" && make e2e'`.

### Deferred findings (open)

- PutBlob holds its tx connection and queues the object delete on a second
  pool connection. With 25 concurrent PutBlobs, the 25-connection pool could
  starve. Real concurrency today is about 4 ingest workers plus backfill.
- No e2e test checks that the withhold admin gets the s3 store. The wiring
  in `main.go` was reviewed. The test is a scope addition.
- Admin subscribe still answers 502 on a byte store failure
  (`internal/ingest/follow.go`).
- In backfill, the `classifyErr` early return in the replies pass drops a
  joined store error. If that pass is an expired item's last RetryDeferred
  retry, the item is dropped.
- `testutil.LocalS3Transport` dials only the first resolved address. A
  `localhost` that resolves to `::1` first could fail.
- The comment at `cmd/tidepool/blob_store.go:28` says the raw endpoint is
  never logged or returned. A wrapped HeadBucket transport error can carry
  the request URL with scheme, host and bucket (no credentials).
- The `migrationStopped` comment at `internal/blobstore/migrate.go:45` says
  a stopped row keeps its bytes. After an ambiguous commit the row is
  already NULL and the verified object holds the bytes. A rerun handles
  either state.

### Follow-ups not done by the loop

Production deploy files were outside the unattended loop and were done by
hand afterwards, with Bretton attending:

- Done: `docker-compose.prod.yml` forwards `BLOB_STORE` (default `postgres`)
  and the five `BLOB_S3_*` variables to the `tidepool` service, and
  `.env.prod.example` lists them commented out.
- Done: `DEPLOY.md` §6 "Blob object storage" holds the outage note (one
  bucket HEAD at startup), the prerequisites (bucket, versioning, Object
  Lock, scoped user policy, anonymous check), the switch runbook (`.env` and
  recreate, `migrate-blobs` until 0 failed, mismatched rows, spot checks,
  `VACUUM FULL blobs`) and the restore notes.

Still open:

- The release deploy (prod stays on `postgres`), then the s3 switch and
  copy. Both need the production server.
- The tidepool repo profile in claude-skills
  (`repo-profiles/tidepool.md`) needs the test MinIO (`make test-s3-up`)
  and the `TIDEPOOL_TEST_S3_*` exports in Setup, Night gate and Isolation.

No live checks were blocked; the repo profile defines none.
