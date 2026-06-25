# Adaptive Branch Routing Notes

This note records the current 0.185 adaptive branch-routing boundary. It is not
a broad cost-based optimizer design.

## Current Route Families

- `IndexBranchSet`: admitted for fixed leading equality prefix plus a small
  exact `IN` branch set on the next prefix field, ordered by the primary key
  suffix.
- `IndexMultiLookup`: admitted for exact multi-prefix lookups. For sparse
  parent-prefix shapes, route planning may add a child-prefix expansion hint
  when metadata can expand the parent prefixes into exact child prefixes whose
  remaining suffix is primary-key ordered.
- Over-cap fallback: large `IN` shapes that exceed the branch-set cap must not
  pretend to be branch-set routes. They fall back through a route that preserves
  residual filtering before global primary-key windowing.

## Current Admission Boundary

- Small fixed-prefix branch sets can stream by merging branch prefix streams.
- Exact-prefix multi-lookup can stream without child-prefix expansion when the
  consumed prefix already leaves only the primary-key suffix.
- Sparse parent-prefix multi-lookup can stream with child-prefix expansion when
  the expanded prefix leaves the primary-key suffix in ascending order.
- Descending sparse child-prefix expansion is not admitted yet. It must remain
  materialized/fallback until reverse ordered expansion has an explicit design.
- Child-prefix expansion is a route hint, not a new logical access path; the
  route must still identify as `IndexMultiLookup` in EXPLAIN.

## Diagnostics

Verbose EXPLAIN reports:

- `diag.r.index_prefix_child_expansion`
- `diag.r.index_prefix_child_expansion_target`
- `diag.r.index_prefix_child_expansion_cap`

These diagnostics make it possible to distinguish a plain multi-lookup from a
sparse child-prefix-expanded multi-lookup before changing thresholds.

## Deferred Work

- Cost-based choice between branch-set, child-prefix expansion, and fallback.
- Prefix-cardinality-aware estimates for dense versus sparse `IN` lists.
- DESC/reverse child-prefix expansion.
- General branch-tree replacement for every special-case `IN` path.
