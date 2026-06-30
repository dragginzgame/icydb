# 0.183 Deferred Work Reminders

This is a parking note, not a full design doc.

## Ownership

- 0.184 owns the mega-audit cleanup pass: correctness findings, SQL/fluent
  unification, maintainability cleanup, and test hardening.
- 0.185 owns Branch-Aware Query Revisited: route broadening, continuation
  design, adaptive routing, and branch-heavy perf polish.
- Do not reopen 0.183 for new branch-aware expansion; keep 0.183 as the first
  production-shaped foundation line.

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

- Any remaining predicate-aware covering broadening belongs to the 0.184 audit
  only when it directly removes duplicated SQL/fluent flow or fixes a measured
  correctness/perf hotspot.
- Any remaining exact-count or prefix-cardinality broadening belongs to 0.184
  only when it is an audit cleanup; branch-specific count/cardinality design
  belongs to 0.185.
- Treat adaptive large-`IN` routing, branch-set continuation design, and
  branch-tree generalization as 0.185 work.
