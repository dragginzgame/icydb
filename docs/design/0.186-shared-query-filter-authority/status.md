# 0.186 Status

Status: active.

## Focus

Shared query filter authority after the 0.184 query-engine audit cleanup and
the 0.185 branch-aware routing revisit.

0.186 should decide whether the current query-intent `NormalizedFilter` is the
right frontend-neutral pre-access filter authority, or whether a new contract
should own the handoff from SQL and fluent filters into access planning,
residual filtering, count/cardinality shortcuts, cache identity, and EXPLAIN.

## Current Slice

- Promote the tentative 0.186 shared-filter design into the active design
  baseline.
- Document the current SQL/fluent filter authority chain before changing code.
- Keep runtime semantics, route choice, cursor format, public SQL/fluent
  behavior, and persistence unchanged.

## Initial 0.186 Queue

- Add or tighten source-audit tests for the current filter authority seams.
- Prove representative SQL and fluent filters converge at the query-intent /
  planner boundary where semantics are equivalent.
- Pick one narrow frontend-neutral fact to move or name explicitly in the
  shared contract.
- Verify cache, EXPLAIN, route, residual, and count/cardinality behavior remain
  unchanged for that slice.

## Non-Goals

- No SQL semantics changes.
- No SQL admission widening.
- No fluent public API behavior changes.
- No branch-aware route-choice changes.
- No cursor-format or cardinality metadata redesign.
- No broad cost-based route selection.

## Future Work Outside 0.186 Unless Re-scoped

- The second query-engine audit parked for 0.187.
- Full cost/selectivity-aware planning.
- Cursor redesign.
- Chunked durable mutation commits.
- Aggregate operator DTO or full physical-plan replacement.
