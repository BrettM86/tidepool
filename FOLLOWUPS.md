# Follow-ups

Open work, accepted limitations, and operational caveats remaining after the
v1.1 hardening and performance passes. Completed task history belongs in the
task documents and git history rather than this list.

## Relay pipeline

- A fresh self-hosted BigSky refuses non-admin `requestCrawl` calls because
  its new-PDS daily limit defaults to zero. Bootstrap it with the admin
  `setPerDayLimit` call (as `relay-bootstrap` does in the e2e compose stack).
- Verify BigSky configuration against `--help`: the pinned image does not
  recognize `BGS_CRAWL_INSECURE_WS`, `BGS_PORT`, or `LOG_LEVEL`.
  `--crawl-insecure-ws` is a command argument and the log variable is
  `BSKYLOG_LOG_LEVEL`. Also set `ATP_PLC_HOST` and `RESOLVE_ADDRESS` when the
  public PLC directory and `1.1.1.1` resolver are inappropriate.
- The pinned BigSky image has no arm64 manifest. Apple Silicon uses amd64
  emulation; revisit when bumping the image.
- **Jetstream has surprising quiet-stream cursor behavior.** A cursor between
  its newest stored event and server-now replays the full retained store.
  Consumers must be idempotent and must not treat a wall-clock "now" cursor as
  a dedupe boundary.
- Indigo's slurper stops retrying after roughly three minutes of continuous
  dial failure and marks the PDS unregistered. Tidepool re-announces on its own
  startup, but a long Tidepool outage while the relay remains up requires a
  fresh `requestCrawl`.

## Federation and interop

- Lemmy's local author auto-upvote is not federated, so a newly bridged post
  can show one fewer upvote than Lemmy until a backfill re-seed.
- Lemmy 0.19.x does not federate profile edits. A new `#nobridge` marker is
  discovered only after `PROFILE_REFRESH_TTL` expires and another activity
  triggers an actor fetch; an inactive actor's opt-out is not discovered.
  Add a periodic consent re-scan of active bridged actors.
- Existing Lemmy peers that federated before Tidepool served its instance
  actor at `GET /` may need an actor re-fetch and Lemmy federation-worker
  restart before account deletions are delivered.
- Tombstoned self-delete confirmation accepts a 410 response or ActivityPub
  Tombstone, not a plain 404. This is correct for the currently supported
  Lemmy target but must be revisited for platforms that return 404 on actor
  deletion.
- Lemmy's AP `Image` attachment omits `mediaType`; only its `type` and the URL
  extension identify it as an image. `Link` attachments omit alt text, so alt
  text is lost if metadata fetching fails.
- PieFed and Mbin are untested. `communityRef` currently uses Lemmy's `/c/`
  URL heuristic; Mbin uses `/m/`.
- Lemmy 1.0/API v4 is untested. The harness, seeder, and WebFinger assumptions
  target pinned Lemmy 0.19.x/API v3.
- The ActivityPub service actor uses type `Service` because Lemmy rejects
  `Application` for Person actors. Revisit only if another supported platform
  proves incompatible.
- Production WebFinger is HTTPS-only by design. HTTP fallback exists only with
  the dev/e2e SSRF relaxation.

## Write side and product scope

- Outbound Coves-to-Lemmy federation and ActivityPub actors for Coves users.
- Key claiming/migration for bridged users. Handle-collision recovery should
  eventually reuse an orphaned minted DID via a PLC `updateHandle` operation
  rather than minting another DID.
- If vote write-back is added, suppress echoes of Tidepool-managed voters and
  subtract Tidepool's written-back tally during subsequent Lemmy re-seeds.
- Moderation federation and DMs.

## Production rollout

- Bluesky's public relay accepts new PDS hosts, but its documented default
  allowance is only 100 accounts, 50 repo-stream events/second, 2,600/hour,
  and 21,000/day. Tidepool mints one repo DID per bridged actor/community and
  will exceed the account cap quickly. Arrange a relay limit increase before
  broad subscriptions, or operate a suitably bootstrapped relay.
- `ENVIRONMENT=production` has not been exercised end-to-end. The harness uses
  development mode for migrations-on-start, HTTP/private fetching, and strict
  lexicon validation.
- Production lexicon validation currently records a metric and writes the
  record instead of failing it. A strict-first rollout should happen only
  after `tidepool_lexicon_validation_failures` remains zero in production.

## Sync surface

- `SigningKeys` could become a `SignCommit` capability so private key material
  stays inside the identity package and a future KMS implementation is easier.
- `getRepo` ignores the optional `since` optimization and returns a full
  reachable-set CAR, which the spec permits. A real diff export would require
  retaining historical blocks and revisiting the blocks-GC invariant.

## Ingestion and rate limiting

- All in-process IP limiters use `RemoteAddr` and ignore
  `X-Forwarded-For`. A deployment behind a proxy/load balancer must rate-limit
  at the edge or all clients share the proxy's bucket. A future opt-in
  `TRUSTED_PROXY` setting should trust forwarded addresses only from an
  allowlisted hop.
- A shutdown-interrupted queue attempt still consumes its `ClaimNext` attempt
  increment (cosmetic).
- The mint gate is covered only by unit tests; the e2e harness has no
  low-`MINT_RATE_PER_MINUTE` compose variant.
- The `activityID` random-source failure path is untestable without injecting
  the reader (Go 1.24+ treats `crypto/rand` failure as fatal).

## Votes

- `ScrubVoter` takes aggregate locks in deterministic subject order, but this
  deadlock-avoidance property has no true concurrency test.
- Vote subject resolution occurs outside the mutation transaction, leaving a
  narrow race with deletion.
- Baseline-only voters can temporarily drift: a later flip or clear lacks a
  per-voter baseline row to retract. A re-seed heals the aggregate.

## Materializer and storage

- A stale actor document behind a temporarily forbidden/unavailable instance
  can drop content instead of serving the last known actor state. Media blobs
  already carry forward on transient fetch failure; actor documents do not.
- Blobs are content-addressed per DID without refcounts. Two records in one
  repo can share a blob row, so scrubbing one record can remove media still
  referenced by another. Add a refcount/junction table if this occurs in
  practice.
- A consent change racing an in-flight commit can allow that one commit to
  land because consent is read outside the commit transaction.

## E2E harness and CI

- The PLC directory image is pinned by commit in `e2e/plc/Dockerfile`; bump it
  deliberately when upstream fixes are needed.
- Jetstream exits when its upstream disconnects; compose's
  `restart: unless-stopped` supplies recovery. Remove the workaround if
  Jetstream gains reconnect support.
- The suite-end replay sweep is bounded by Jetstream's 24-hour event TTL.
  Recreate the stack/volume per run, as `make e2e` does, or old events cannot
  be revalidated.
- The e2e suite has not yet completed on GitHub Actions. It cold-builds Lemmy
  from source and may exceed practical runner time/disk limits. Prefer a
  prebuilt pinned Lemmy debug image in GHCR, or persist a buildx GHA cache.
