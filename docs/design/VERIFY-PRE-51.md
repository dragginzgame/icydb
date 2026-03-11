# VERIFY-PRE-51

## Features Worth Verifying Before `0.51` Stabilization

These are semantic completeness checks, not major net-new features.

## 1. Basic Predicate Completeness

Make sure the predicate system supports this minimal set:

- Core comparisons:
  - `=`
  - `!=`
  - `<`
  - `<=`
  - `>`
  - `>=`
- Logical:
  - `AND`
  - `OR`
  - `NOT`
- Range/set:
  - `IN`
  - `BETWEEN`
- Null semantics:
  - `IS NULL`
  - `IS NOT NULL`

If any are missing, add them before freezing planner contracts; otherwise planner/executor contracts will need to change later.

## 2. Deterministic `ORDER` + `LIMIT` Semantics

Confirm behavior is clear for:

- `ORDER BY`
- `LIMIT`
- `OFFSET`

Edge cases to verify:

- `ORDER` without `LIMIT`
- `LIMIT` without `ORDER`
- `OFFSET + LIMIT`
- Continuation + `ORDER`

This ties directly to continuation-envelope stabilization in `0.51`.

## 3. Full Projection Shapes

Before freezing projection behavior, ensure support for:

- `SELECT *`
- `SELECT field1, field2`
- `SELECT computed_expression`
- `SELECT aggregate(...)`

No need for full SQL surface, but these projection forms should be complete enough that projection rules stay stable.

## 4. Aggregate Coverage

Confirm the minimal aggregate set exists:

- `COUNT`
- `SUM`
- `AVG`
- `MIN`
- `MAX`

No need to add exotic aggregates yet.

## 5. Covering Index Detection

Before projection stabilization, executor should detect:

- Index fully covers projection

This allows avoiding row materialization. If missing, land it before contract freeze.

## Features That Should Not Be Added Now

Defer these until after `1.0` contract stabilization:

- Joins
- Subqueries
- Window functions
- Complex SQL parsing
- Schema migrations
- Advanced indexing
- Query hints
- Optimizer heuristics

These significantly expand planner surface area.

## Existing Capability Is Already Sufficient

From the executor tree and prior work, the engine already appears to have:

- Scans (PK + secondary)
- Predicate evaluation
- Aggregates
- Grouped execution
- Projection system
- Pagination/continuation
- Explain
- Execution routing
- Pushdown
- Fast paths

That is sufficient core capability for a `1.0`-quality engine.

## Real `1.0` Blockers Are Contract-Level

Primary blockers are contracts, not raw feature count:

- Planner/executor contract
- Continuation stability
- Projection determinism
- Aggregate strategy determinism
- Numeric semantics
- Storage invariants
- Semantic authority ownership

This aligns with the planned `0.51` stabilization line.

## Recommended Roadmap

- `0.50`: Executor simplification
- `0.51`: Contract stabilization
- `0.52`: Reduced SQL builder/CLI
- `0.53`: Final hardening
- `1.0`: Release

This keeps engine behavior stable before adding more tooling surface.

## Conclusion

No major new features are required before starting `0.51`; verify minimal predicate, projection, aggregate, and ordering semantics are already present, and fill only any true gaps.
