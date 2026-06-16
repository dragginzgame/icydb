# 0.183 Follow-up Reminders

This is a parking note, not a full design doc.

## Branch Tree

- Keep branch-set, union, and intersection execution on the shared
  `OrderedKeyStreamBox::{merge_all, intersect_all}` helpers.
- Do not reintroduce executor-local merge folds or runtime branch semantics
  reconstructed from loose prefix vector lengths.
- Runtime branch execution should consume lowered branch prefix specs and the
  spec-derived branch count; planner/lowering should remain the authority for
  branch slot and concrete branch prefix construction.
- Future broadening should preserve the same pull-lazy contract: open fixed
  branch streams, merge ordered heads, suppress duplicate primary keys, and stop
  at the page/lookahead boundary.

## Deferred Optimization Slices

- Broader predicate-aware covering: keep more residual-predicate queries on
  index-covered or hybrid reads without falling back to full row hydration.
- Cheap exact counts / prefix cardinality: avoid exact-count scans when the
  predicate is fully represented by an index prefix or branch set.
- Treat both as separate slices from the branch-tree cleanup; do not fold them
  into the current route hardening unless the implementation stays narrow.
