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

- Continue tightening source invariants so downstream consumers cannot derive
  frontend-owned predicate facts outside the shared pre-access contract.
- Prove cache, EXPLAIN, route, residual, and count/cardinality behavior remain
  unchanged for each tightening slice.

## Completed Since 0.186.2

- No pushed slices yet.

## Completed Slices

### 0.186.2

- Replaces `NormalizedFilter`'s predicate-only placeholder expression with an
  explicit predicate-only semantic authority variant, keeping logical-planning
  expression projection absent for predicate-only filters.
- Preserves predicate-only access-planning identity, full coverage semantics,
  and direct COUNT cardinality eligibility without changing cache, route, or
  residual behavior.
- Removes the EXPLAIN-only fallback that derived residual predicate DTOs from
  residual expressions. EXPLAIN residual predicate output now comes from the
  finalized residual predicate contract, while expression-owned residuals stay
  on the residual expression surface.

### 0.186.1

- Names predicate-subset coverage as a query-intent pre-access semantic fact.
- Distinguishes full, partial, and absent predicate coverage over
  user-visible filter semantics while keeping the existing
  `predicate_subset_covers_expr` projection for logical planning.
- Confirms predicate-only fluent filters can carry full semantic coverage
  without exposing a fake visible expression.
- Keeps runtime semantics, route choice, cursor format, public SQL/fluent
  behavior, cache identity, EXPLAIN shape, count/cardinality shortcuts, and
  persistence unchanged.
- Moves ordinary SQL SELECT toward expression-owned filter authority. SELECT
  filters now carry the schema-bound visible expression into `NormalizedFilter`
  as the semantic authority, while extractable SQL filters retain
  schema-canonicalized predicate mirrors for strict indexed planning.
- Removes DELETE's broad `Predicate::True` fallback so expression-only DELETE
  filters stay on the residual expression lane without claiming predicate
  coverage.
- Adds source guards for the remaining explicit SQL predicate-admission lanes,
  keeping UPDATE/global-aggregate exceptions auditable while ordinary
  SELECT/DELETE filters flow through query intent.
- Proves direct COUNT cardinality shortcut eligibility consumes the same
  predicate-coverage fact as page planning by disabling the shortcut when a
  visible residual filter is not fully covered.
- Proves expression-plus-predicate handoffs keep shared cache identity owned by
  the visible filter expression, preserving the existing cache surface while
  strict SQL predicate mirrors remain planner inputs.
- Records the existing EXPLAIN proof that residual diagnostics report
  expression-owned residual filters without deriving predicate facts from
  rendered filter text.
- Audits the remaining strict SQL UPDATE/global-aggregate predicate-admission
  lanes and keeps expression-only WHERE shapes fail-closed because moving them
  to expression-backed intent would widen accepted SQL.

### 0.186.0

- Promotes the tentative 0.186 shared-filter design into the active design
  baseline.
- Documents the current SQL/fluent filter authority chain before changing
  runtime code.
- Adds source guards for the current pre-access predicate-subset derivation
  seams and post-access residual-contract creation seam.
- Extends SQL/fluent canonical predicate parity coverage for negated
  membership, `IS NOT NULL`, and negated boolean composition.
- Keeps runtime semantics, route choice, cursor format, public SQL/fluent
  behavior, cache identity, EXPLAIN shape, count/cardinality shortcuts, and
  persistence unchanged.

## Initial 0.186 Queue

- Continue tightening source invariants so downstream consumers cannot derive
  frontend-owned predicate facts outside the shared pre-access contract.
- Keep strict SQL UPDATE/global-aggregate predicate paths separate unless a
  future design explicitly widens their admission policy.

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
