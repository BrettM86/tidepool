# Tidepool build-loop state

Protocol: PLAN.md §Loop protocol. One task per iteration:
implement (Fable agent) → /second-opinion review → fix → verify → commit →
update this file → schedule next. Stop the loop when every task is `done`.

| # | Task | Status | Commit | Notes |
|---|------|--------|--------|-------|
| 1 | 01-scaffold-storage | pending | | |
| 2 | 02-ap-protocol | pending | | |
| 3 | 03-identity-repos | pending | | |
| 4 | 04-sync-firehose | pending | | |
| 5 | 05-materializer | pending | | |
| 6 | 06-ingestion | pending | | |
| 7 | 07-vote-aggregates | pending | | |
| 8 | 08-e2e-harness | pending | | |

Statuses: pending → in-progress → review → done (or blocked: <reason>).

## Cross-task notes for future iterations
(implementation agents & reviewers append surprises, interface changes,
and deferred TODOs here)

- Reference clones live at ~/Code/bridgy-fed, ~/Code/granary,
  ~/Code/arroba (CC0). Coves AppView at ~/Code/coves.
- Coves post consumer requires: repo DID == record.community, community
  indexed before post, author user indexed before post.
