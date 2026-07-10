# Task 12 — Perf & scale: big-community readiness (~800 LOC)

## Goal
Remove the known scaling cliffs before any big community hits them — and
as the explicit prerequisite for ever revisiting votes-as-records
(FOLLOWUPS "Design revisits": write amplification is only viable after
this task). Every change here is behavior-preserving; the e2e suite and
golden-value tests (deterministic TIDs, at-uris) must not move.

## Deliverables
- **Per-DID MST tree cache**: `PutRecord` is currently O(repo size) with
  one SELECT per node, full-tree, per commit. Cache the tree per DID with
  invalidation tied to the commit path (single-writer discipline + the
  global advisory lock make this tractable). Benchmark before/after with
  a realistic big-repo fixture (thousands of records) and record numbers
  in the commit message.
- **`getRepo` memory + reachable-set**: stream the CAR instead of
  buffering it whole; export the reachable set from the current commit
  rather than every historical block. Coordinate with GetRecord's
  read-consistency dependence on append-only `blocks` (LOOP_STATE task
  03/04 notes) — reachable-set-only export must not break proof reads.
- **`blocks` GC**: prune blocks unreachable from any live commit, as an
  explicit background sweep with a retention guard. This is the
  load-bearing append-only table — design the invariant first (what do
  GetRecord/getRepo/replay need?), write it down in the code, then
  implement to it. If a safe GC needs the sync `since`/diff-export work,
  say so and defer that half explicitly.
- **`ClaimNext` O(N) scan** when one community's queue backs up behind a
  failing event: bound the scan (indexed skip / per-key cursor) without
  breaking the per-community serial-ordering guarantee or the fencing
  contract.
- Cheap wins while in the area: `getRepo` `since` param (diff export) if
  the reachable-set work makes it nearly free — otherwise leave the
  documented full-CAR fallback.

## Definition of done
- Full unit suite + `make e2e` green; golden TID/at-uri tests untouched.
- Before/after benchmarks for PutRecord (big repo) and getRepo (memory)
  recorded in the commit message.
- FOLLOWUPS updated (items closed/annotated); the votes-as-records
  design-revisit note updated to reflect which prerequisites now hold.

## References
- FOLLOWUPS.md Sync surface + Storage sections; LOOP_STATE task 03/04
  notes (commit serialization, advisory locks, why blocks is
  append-only).
- `internal/repo/` (MST load path, ExportCAR), `internal/store`
  (ClaimNext).
