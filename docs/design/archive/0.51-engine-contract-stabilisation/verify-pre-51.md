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

Contract note to lock before `0.51`:

- Scalar `LIMIT`/`OFFSET` without explicit `ORDER BY` is rejected (unordered pagination is not allowed).
- Grouped pagination without explicit `ORDER BY` uses canonical grouped-key order.
- Cursor pagination requires both `ORDER BY` and `LIMIT`.

This ties directly to continuation-envelope stabilization in `0.51`.

## 2.1 Continuation Token Versioning

Lock continuation token envelope versioning before `0.51`:

- Scalar and grouped continuation token wire payloads include an explicit `version` field.
- Decode must fail closed for unknown token versions.
- Continuation compatibility checks remain signature-bound to canonical query shape and ordering.

## 3. Full Projection Shapes

Before freezing projection behavior, ensure support for:

- `SELECT *`
- `SELECT field1, field2`
- `SELECT computed_expression`
- `SELECT aggregate(...)`

Projection ordering invariant to lock before `0.51`:

- `ProjectionSelection::Fields(Vec<FieldId>)` preserves declaration order from the query shape.
- Executor/output layers must preserve planner projection order and must not reorder projected columns.

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

Covering projection invariant to lock before `0.51`:

- For index-backed execution shapes, when projected fields are covered by index key components and ordering contract is compatible, executor may skip row materialization.
- Covering eligibility must remain planner-authoritative and deterministic for identical query shapes.

This allows avoiding row materialization. If missing, land it before contract freeze.

## 6. Explain Stability

Freeze EXPLAIN determinism before `0.51`:

- Identical executable plan -> identical explain descriptor tree.
- Executor refactors must not change explain descriptor structure for unchanged executable plans.

## 7. Canonical Query-Shape Fingerprint

Lock canonical query identity before `0.51`:

- Identical logical query -> identical canonical fingerprint.
- Canonical fingerprint is the planner-owned query identity used for plan/continuation/explain stability surfaces.
- Semantically equivalent commutative predicate ordering (for example `a AND b` vs `b AND a`) must not change fingerprint identity.
- Presentation-only differences must not change fingerprint identity (for example alias-only naming differences, and whitespace-only differences where textual query surfaces exist).

## 8. Plan Shape Determinism

Lock planner determinism before `0.51`:

- Identical query + identical schema -> identical executable plan.
- Planner candidate selection must be deterministic (no map-iteration-order dependence); candidate access paths must be ordered deterministically before selection.

## 9. Cursor Envelope Immutability

Lock cursor compatibility boundaries before `0.51`:

- Cursor tokens are opaque to clients and immutable once issued.
- Clients do not interpret cursor internals.
- Internal token representation may evolve only through explicit versioned envelopes.

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
