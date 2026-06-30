# 0.185 Branch-Aware Query Revisited Reminders

This started as a parking note. The closed 0.185 branch-aware baseline and
status are documented in `0.185-design.md` and `status.md`.

## Closed In 0.185

- Revisit branch-set route representation after the 0.184 audit cleanup.
- Confirm SQL and fluent both feed the same branch-aware planner path.
- Expand perf matrix coverage around branch-heavy fluent and SQL queries.
- Close the 0.185 adaptive route-cost question by keeping generation-bound
  prefix-cardinality metadata at execution time and naming the sparse
  child-prefix expansion budget contract.
- Rule out broad branch-tree replacement for the current specialized branch
  families after proving their access-shape boundaries stay distinct.
- Close final validation/docs, map the original work plan to landed evidence,
  and mark the 0.185 branch-aware line closed.

## Closed 0.185 Branch-Aware Queue

- No branch-aware work remains queued for 0.185.

## Future Work Outside 0.185 Branch-Aware Closeout

- Wider downstream-specific query tuning and performance benchmarking.
- Broad cost-based route optimization over runtime prefix-cardinality metadata.
- Generalized branch-tree algebra for future branch semantics beyond the
  current specialized access families.
- Per-child cursor anchors for future branch ordering beyond global
  primary-key suffix continuation.
