# Crosscutting Completeness Audit - 2026-05-01

## Report Preamble

- scope: current single-entity query and mutation system, with extra attention
  on the post-0.144 expression-engine consolidation
- compared baseline report path:
  `docs/audits/reports/2026-04/2026-04-22/completeness.md`
- code snapshot identifier: `c88e171f43` (`dirty` working tree)
- method tag/version: `Completeness Method V1`
- comparability status: `comparable`
  - this rerun keeps the same boundary, taxonomy, and classification model as
    the April 22 canonical baseline
  - expression-engine implementation evidence changed, but no product
    feature-state label changed

## Executive Summary

The completeness read remains stable: the system is still **bounded and
coherent** inside the admitted single-entity SQL boundary.

Scalar query semantics remain deep, grouped/global aggregate semantics remain
intentionally narrow but strong, and the audit still does not find an in-scope
feature family that merely parses without real runtime support.

The main new read is architectural rather than product-surface expansion:

- runtime scalar projection, grouped projection, grouped `HAVING`, and scalar
  aggregate terminal expressions now route through the unified
  `CompiledExpr::evaluate` path
- `ScalarProjectionExpr` remains visible as a planner/lowering structure, which
  is expected, but no runtime scalar expression evaluator remains around it
- the old grouped projection expression families have been replaced by the
  compiled expression reader model
- recent fluent-terminal DRY cleanup reduces repeated terminal wiring without
  changing completeness labels

The current dirty tree includes grouped-runtime and grouped-contract edits, so
this audit treats grouped topology as active structural follow-through. That
does not change any feature classification in this report.

## Classification Model

This rerun reuses the same classification model as the April baseline:

- `Complete`
- `Bounded`
- `Partial`
- `Missing`
- `Out Of Scope`

No classification rules changed in this rerun.

## System Boundary

Included:

- single-entity `SELECT`, `EXPLAIN`, introspection, and mutation SQL within the
  current public SQL subset contract
- typed/fluent single-entity query and mutation surfaces where they confirm the
  same semantic boundary
- prepared SQL within the current route-owned prepare/lower split
- scalar filtering, grouped/global aggregates, bounded searched `CASE`, bounded
  projection expressions, ordering, pagination, and narrow `RETURNING`
- runtime scalar expression execution through `CompiledExpr::evaluate`,
  including row, grouped-output, `HAVING`, and scalar aggregate terminal
  contexts

Excluded:

- multi-entity SQL
- joins
- subqueries
- window functions
- general relational SQL
- scalar SQL cursor pagination
- prepared/template widening beyond the current shipped route-owned lane
- general predicate-runtime unification; executable predicates remain their own
  query-filter boundary rather than a scalar projection expression evaluator

## Evidence Sources

Primary product-boundary evidence:

- `docs/contracts/SQL_SUBSET.md`
- `crates/icydb-core/src/db/session/tests/sql_scalar.rs`
- `crates/icydb-core/src/db/session/tests/sql_grouped.rs`
- `crates/icydb-core/src/db/session/tests/sql_aggregate.rs`
- `crates/icydb-core/src/db/session/tests/sql_projection.rs`
- `crates/icydb-core/src/db/session/tests/sql_write.rs`
- `crates/icydb-core/src/db/session/tests/sql_delete.rs`
- `crates/icydb-core/src/db/session/tests/sql_explain.rs`
- `crates/icydb-core/src/db/session/tests/sql_surface.rs`
- `crates/icydb-core/src/db/session/tests/aggregate_terminals.rs`
- `crates/icydb-core/src/db/session/tests/aggregate_identity.rs`
- `crates/icydb-core/src/db/session/tests/execution_convergence.rs`
- `crates/icydb-core/src/db/session/tests/predicate_convergence.rs`
- `crates/icydb-core/src/db/session/tests/verbose_route_choice.rs`
- `crates/icydb-core/src/db/session/tests/filtered_composite_expression.rs`
- `crates/icydb-core/src/db/session/tests/expression_index.rs`
- `crates/icydb-core/src/db/sql/lowering/tests/mod.rs`

Expression-engine evidence:

- `crates/icydb-core/src/db/query/plan/expr/compiled_expr.rs`
- `crates/icydb-core/src/db/query/plan/expr/compiled_expr_compile.rs`
- `crates/icydb-core/src/db/executor/projection/eval/scalar.rs`
- `crates/icydb-core/src/db/executor/projection/grouped.rs`
- `crates/icydb-core/src/db/executor/aggregate/scalar_terminals/expr_cache.rs`
- `crates/icydb-core/src/db/executor/aggregate/contracts/state/grouped_expr.rs`
- `crates/icydb-core/src/db/query/fluent/load/terminals.rs`

Search checks used for this rerun:

- `GroupedCompiledExpr`, `GroupedProjectionExpr`, and
  `eval_grouped_projection_expr` no longer appear under
  `crates/icydb-core/src/db`
- `ScalarProjectionExpr` appears in planner/lowering code and tests, not as a
  runtime evaluator input
- remaining runtime helpers in `executor::projection::eval::scalar` are reader
  adapters that call `CompiledExpr::evaluate`

## Feature Inventory

### Primary Feature Rows

| Feature Row | State | Readout |
| ---- | ---- | ---- |
| scalar `SELECT` | Complete | Strong admitted surface, lowering, semantic identity, planning, execution, explain, and proof within the admitted single-entity boundary |
| grouped `SELECT` | Bounded | Strong execution and explain inside the admitted grouped family, but intentionally restricted and fail-closed outside that family |
| predicates (`WHERE` / `HAVING`) | Bounded | Scalar filter semantics are deep; grouped `HAVING` now evaluates through the unified compiled expression path, while the overall predicate surface remains intentionally family-scoped |
| projection expressions | Bounded | Bounded computed projections are strong and now use one runtime expression evaluator; the expression surface is still intentionally narrower than full SQL |
| aggregates | Bounded | Global and grouped aggregates are strong within the admitted aggregate family, including compiled scalar terminal expressions, but still intentionally restricted |
| `ORDER BY` | Complete | Strong within the admitted scalar/grouped boundary, including explain and route behavior for current shapes |
| `LIMIT` / `OFFSET` | Complete | Strong for the admitted scalar SQL pagination surface; scalar cursor pagination is explicitly out of scope rather than missing |
| `DISTINCT` | Bounded | Present and tested, but only within the admitted query families rather than as a generalized SQL distinct framework |
| mutation (`INSERT` / `UPDATE` / `DELETE`) | Bounded | Strong inside the admitted mutation and narrow `RETURNING` surface; broader SQL mutation shapes remain intentionally excluded |
| `EXPLAIN` | Complete | Strong public surface with good semantic fidelity and proof coverage |

### Supporting Rows

| Supporting Row | State | Readout |
| ---- | ---- | ---- |
| prepared SQL | Bounded | Behavior is strong through the canonical route-owned prepare/lower lane, but prepared widening beyond the shipped lane is still out of scope |
| semantic identity / canonicalization | Bounded | Strong for scalar surfaces and shipped grouped searched-`CASE` families, but not generalized |
| cache / reuse | Bounded | Canonical semantic reuse is visible and coherent for the shipped families, including scalar aggregate terminal expression interning |
| expression-engine authority | Complete | Runtime scalar expression evaluation has one authority, `CompiledExpr::evaluate`; planner expression trees are lowered before runtime use |
| diagnostics / verbose explain | Complete | One immutable diagnostics artifact owns verbose explain, and public/session SQL rendering follows it |
| fail-closed boundaries | Complete | Unsupported areas generally reject cleanly rather than degrading into silent partial support |

## Delta Vs April Baseline

### 1. No product feature-state labels changed

No primary feature row changed label relative to
`docs/audits/reports/2026-04/2026-04-22/completeness.md`.

The baseline result still holds:

- the admitted SQL surface is coherent
- grouped and aggregate behavior remains bounded by design
- there are no large in-scope missing families

### 2. Expression-engine completeness improved structurally

The expression consolidation is a real architectural completion, not a new SQL
surface.

The current tree shows:

- one compiled expression IR for row scalar evaluation, grouped output
  projection, grouped `HAVING`, and scalar aggregate terminal expressions
- reader adapters at execution boundaries instead of evaluator forks
- planner expression structures confined to compilation and tests
- missing slot, missing aggregate, and invalid grouped access paths mapping to
  expression diagnostics rather than SQL `NULL` fallthrough

This strengthens the `projection expressions`, `aggregates`, `HAVING`, and
`expression-engine authority` reads without widening their admitted scope.

### 3. DRY pressure dropped in fluent terminal loading

The recent terminal-driver consolidation removes repeated strategy-driver
implementations from `query::fluent::load::terminals`.

This improves maintenance confidence for the typed/fluent terminal surface, but
does not change runtime semantics or feature completeness labels.

### 4. Grouped topology remains active structural follow-through

The current dirty working tree includes grouped runtime, grouped contracts, and
group planning edits.

Those files continue to read as architecture cleanup and ownership tightening,
not as evidence of a missing grouped product family.

## Partial / Bounded Areas

### 1. Grouped semantic alignment is strong but family-scoped

Grouped searched-`CASE`, grouped `HAVING`, aggregate projections, aggregate
inputs, and aggregate filters are real runtime behavior. They remain bounded to
the shipped aggregate/grouped families rather than becoming general relational
SQL.

### 2. Computed projection support is intentionally narrow

Projection expressions are no longer shallow: arithmetic, comparison, selected
functions, `ROUND(...)`, searched `CASE`, row slots, group keys, and aggregate
outputs all share one evaluator.

The surface is still bounded because it does not claim a full SQL expression
language.

### 3. Mutation remains strong inside a narrow contract

The admitted mutation surface remains scoped to:

- `INSERT`
- `UPDATE`
- `DELETE`
- narrow `RETURNING`

Broader mutation forms remain excluded by the SQL subset contract.

### 4. Prepared SQL remains bounded rather than partial

Prepared SQL still has an explicit current boundary:

- statement normalization and entity-match preparation are route-owned
- query-lane lowering remains generic-free and fail-closed
- aggregate/projection strategy explanation remains visible in tests and
  diagnostics
- broader prepared widening beyond the shipped lane remains intentionally out
  of scope

### 5. Predicate runtime is separate by design

Executable predicates still have their own runtime program for row filtering.
That is not counted as a duplicate scalar projection evaluator in this audit,
because it owns the query predicate/filter contract rather than grouped/scalar
projection expression materialization.

## Missing In-Scope Areas

No large feature family appears to be missing inside the current admitted
boundary.

The remaining gaps are mostly:

- bounded by design
- family-scoped
- structural follow-through rather than absent product surface

## Architectural Seams

### 1. Runtime scalar expression evaluation is now a closed authority

`CompiledExpr::evaluate` is the expression engine for runtime scalar expression
materialization.

The remaining split between `compiled_expr_compile.rs` and `compiled_expr.rs`
is a protective compile/evaluate boundary, not a second evaluator.

### 2. Reader contract is the main safety boundary

The reader abstraction is now the safety boundary between expression programs
and execution context.

The current implementation maps required slot, group-key, aggregate, and field
path misses into deterministic diagnostics. Unsupported row readers returning
`None` do not become SQL `NULL` through the evaluator leaves that require those
values.

### 3. Grouped runtime ownership is still being tightened

Grouped runtime and grouped contract files are currently dirty. The shape still
looks like follow-through from the expression/grouped consolidation, so this
audit treats it as an implementation-ownership seam rather than a product
completeness hole.

### 4. Predicate/filter authority is intentionally separate

The predicate runtime remains the query-filter authority. It should not be
collapsed into `CompiledExpr` unless the product goal changes from scalar
expression unification to full predicate/expression VM unification.

## Overall Maturity Read

The admitted single-entity SQL system remains mature inside its documented
scope.

The strongest improvement since the April baseline is that expression execution
is no longer split between row scalar and grouped scalar engines. That removes
one of the largest semantic-drift risks for `CASE`, numeric operations,
comparison behavior, grouped output projection, grouped `HAVING`, and scalar
aggregate terminal expressions.

The overall completeness posture remains:

- scalar admitted surface: high confidence
- grouped/aggregate admitted surface: high confidence but bounded
- mutation admitted surface: high confidence but bounded
- prepared SQL: bounded and coherent
- unsupported SQL families: explicitly out of scope

## Recommended Next Steps

1. Keep the current completeness labels stable unless `SQL_SUBSET.md` changes.
2. Continue grouped-runtime ownership cleanup as structural work, not as a
   product completeness blocker.
3. Preserve the compile/evaluate split for expressions; it is now part of the
   layering guarantee.
4. Add or keep narrow tests around reader misuse if future readers override
   checked access methods, especially aggregate access in row contexts and slot
   access in grouped contexts.
5. If product scope expands, update `SQL_SUBSET.md` first and rerun this audit
   against the new boundary.

## Verification

Commands run during this rerun:

- `git rev-parse --short=10 HEAD`
- `git status --short`
- `rg "GroupedCompiledExpr|GroupedProjectionExpr|eval_grouped_projection_expr|evaluate_scalar|ScalarProjectionExpr" crates/icydb-core/src/db -n`
- `rg "CompiledExpr::evaluate|CompiledExprValueReader|read_group_key|read_aggregate|read_slot" crates/icydb-core/src/db -n`
- `sed -n '1,220p' docs/contracts/SQL_SUBSET.md`
- `find crates/icydb-core/src/db/session/tests -maxdepth 1 -type f | sort`
- `rg "fn .*sql|grouped|having|projection|aggregate|returning|distinct|explain|prepared|case|cursor|limit|offset" crates/icydb-core/src/db/session/tests crates/icydb-core/src/db/sql/lowering/tests -n`
- `cargo check -p icydb-core`
- `bash scripts/ci/check-layer-authority-invariants.sh`
- `git diff --check`

Verification result:

- `cargo check -p icydb-core` passed
- layer authority invariants passed
- `git diff --check` passed
