# 0.185 Branch-Aware Query Revisited Reminders

This started as a parking note. The active 0.185 branch-aware baseline and
remaining closeout queue are documented in `0.185-design.md` and `status.md`.

## Closed In 0.185

- Revisit branch-set route representation after the 0.184 audit cleanup.
- Confirm SQL and fluent both feed the same branch-aware planner path.
- Expand perf matrix coverage around branch-heavy fluent and SQL queries.
- Close the 0.185 adaptive route-cost question by keeping generation-bound
  prefix-cardinality metadata at execution time and naming the sparse
  child-prefix expansion budget contract.

## Remaining 0.185 Branch-Aware Queue

- Decide whether shared branch-tree machinery should replace any remaining
  special-case `IN` flows.

## Future Work Outside 0.185 Branch-Aware Closeout

- Wider downstream-specific query tuning and performance benchmarking.
- Broad cost-based route optimization over runtime prefix-cardinality metadata.
- Per-child cursor anchors for future branch ordering beyond global
  primary-key suffix continuation.
