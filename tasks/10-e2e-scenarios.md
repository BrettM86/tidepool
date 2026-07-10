# Task 10 — E2E scenario completion: every state, full pipeline (~800 LOC)

## Goal
Close the FOLLOWUPS "scenario ideas not yet covered" list so every
user-visible state transition the bridge implements is proven end-to-end
(Lemmy → tidepool → relay → Jetstream, task 09's pipeline), with lexicon
validation on the wire. After this task, "the e2e suite passes" means
every materialization arm and lifecycle flow has crossed real
infrastructure.

## Deliverables (each a scenario in `tests/e2e`, using existing helpers)
1. **Image post**: upload via pictrs → post with image → blob fetched and
   stored → `embed.images` (+ nsfw label shape if expressible via Lemmy)
   crosses the wire and lexicon-validates. Closes the materializer test
   gap ("embed.images never appears on the wire").
2. **Consent — `#nobridge`**: Lemmy user with `#nobridge` in bio posts →
   nothing materializes (and no DID mint); remove the marker → bridging
   resumes on profile refresh. Then the reverse: bridged actor adds the
   marker → scrub delete-commits observed on the firehose, repo stays
   active (reversible posture).
3. **`Delete(Actor)`**: Lemmy account deletion → all their records
   scrubbed (delete ops on firehose), repo terminally tombstoned —
   `getRepoStatus`/`listRepos` report `active:false`, handle stops
   resolving, content endpoints refuse. Assert through the relay where
   its API exposes repo state.
4. **Unsubscribe**: `DELETE /admin/communities` → `Undo{Follow}` → new
   Lemmy posts in that community produce NO bridge output (bounded
   negative assertion), while a still-subscribed community keeps flowing
   (positive control in the same window).
5. **Community profile update**: rename/description change in Lemmy →
   `community.profile` update event with rkey `self`.
6. **Vote concurrency hammer**: many voters, one post, delivered
   concurrently (parallel inbox deliveries / multiple Lemmy users voting
   in a burst) → final `getVoteAggregates` exactly correct. First real
   exercise of the aggregate-row locking claim beyond unit level.
7. **Suite-end sweep**: after all scenarios, replay the entire firehose
   from cursor 0 with an unfiltered listener and assert no event ever
   carried a collection outside the four emitted ones and every
   create/update lexicon-validates — closes the "events emitted while no
   unfiltered listener was subscribed" gap.
- Stretch (skip if it needs its own compose profile): low
  `MINT_RATE_PER_MINUTE` variant driving the mint gate end-to-end.

## Definition of done
- `make e2e` green, all scenarios through the full relay pipeline.
- Every new record shape that crosses the wire is lexicon-validated by
  the consuming listener (vetEvent path), not by unit fixtures.
- FOLLOWUPS updated: covered items removed, new discoveries added.

## References
- `tests/e2e/bridge_test.go`, `helpers.go` (vetEvent, drain, listener
  conventions — extend, don't fork).
- FOLLOWUPS.md "E2E harness itself" + materializer/vote test-gap items.
- Lemmy API: pictrs upload, account deletion, community edit endpoints
  (0.19.x, `/api/v3`).
