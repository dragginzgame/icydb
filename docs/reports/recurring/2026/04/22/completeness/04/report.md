# Crosscutting Completeness Audit - 2026-04-22 Rerun 4

## Report Preamble

- scope: current single-entity query and mutation system, using the same
  runtime boundary and taxonomy as
  `docs/audits/reports/2026-04/2026-04-22/completeness.md`
- compared baseline report path:
  `docs/audits/reports/2026-04/2026-04-22/completeness.md`
- code snapshot identifier: `cfb352a3dc` (`dirty` working tree)
- method tag/version: `Completeness Method V1`
- comparability status: `comparable`
  - this same-day rerun keeps the same boundary, taxonomy, and classification
    model as today’s canonical baseline
  - no feature-state labels changed in this rerun

## Executive Summary

The completeness read remains unchanged from today’s canonical baseline and the
earlier same-day reruns.

The current system is still **bounded and coherent** inside the admitted
single-entity boundary. Scalar query semantics remain deep, grouped/global
aggregate semantics remain intentionally narrow but strong, and there are still
no obvious in-scope families that merely parse without real execution support.

The main new information in this rerun is not a runtime feature-state change.
It is a cleaner supporting read after dead prepared-session scaffolding was
deleted:

- prepared SQL still reads as `Bounded`
- the old explicit session prepared API no longer appears as a misleading
  shadow surface
- the live prepared lane is now more clearly evidenced through the canonical
  route / prepare / lower path
- the remaining visible seams continue to read as structural follow-through,
  not missing product surface

## Classification Model

This rerun reuses the same classification model as today’s baseline:

- `Complete`
- `Bounded`
- `Partial`
- `Missing`
- `Out Of Scope`

No classification rules changed in this rerun.

## System Boundary

This rerun keeps the same system boundary as
`docs/audits/reports/2026-04/2026-04-22/completeness.md`:

- included:
  - single-entity `SELECT`, `EXPLAIN`, introspection, and mutation SQL within
    the current public SQL subset contract
  - typed/fluent single-entity query and mutation surfaces where they confirm
    the same semantic boundary
  - prepared SQL within the current route-owned prepare/lower split
  - scalar filtering, grouped/global aggregates, bounded searched `CASE`,
    bounded projection expressions, ordering, pagination, and narrow
    `RETURNING`
- excluded:
  - multi-entity SQL
  - joins
  - subqueries
  - window functions
  - general relational SQL
  - scalar SQL cursor pagination
  - prepared/template widening beyond the current shipped route-owned lane

## Evidence Sources

Primary runtime evidence for this rerun:

- `docs/contracts/SQL_SUBSET.md`
- `crates/icydb-core/src/db/session/tests/sql_scalar.rs`
- `crates/icydb-core/src/db/session/tests/sql_grouped.rs`
- `crates/icydb-core/src/db/session/tests/sql_aggregate.rs`
- `crates/icydb-core/src/db/session/tests/sql_projection.rs`
- `crates/icydb-core/src/db/session/tests/sql_write.rs`
- `crates/icydb-core/src/db/session/tests/sql_delete.rs`
- `crates/icydb-core/src/db/session/tests/sql_explain.rs`
- `crates/icydb-core/src/db/session/tests/sql_surface.rs`
- `crates/icydb-core/src/db/session/tests/aggregate_identity.rs`
- `crates/icydb-core/src/db/session/tests/mod.rs`
- `crates/icydb-core/src/db/session/sql/execute/route.rs`
- `crates/icydb-core/src/db/sql/lowering/prepare.rs`
- `crates/icydb-core/src/db/sql/lowering/tests/mod.rs`
- `crates/icydb-core/src/db/predicate/model.rs`
- `crates/icydb-core/src/db/query/plan/expr/ast.rs`

Notable evidence delta vs the earlier same-day passes:

- `crates/icydb-core/src/db/session/sql/parameter.rs` is gone
- `crates/icydb-core/src/db/session/tests/sql_parameterized.rs` is gone
- the old explicit prepared-session lane no longer needs to be treated as part
  of the shipped completeness surface

Secondary planning context used only for next-step interpretation:

- [0.118-design.md](/home/adam/projects/icydb/docs/design/0.118-expression-pipeline-flow-collapse/0.118-design.md:1)
- [0.119-design.md](/home/adam/projects/icydb/docs/design/0.119-structural-simplification-and-flow-deletion/0.119-design.md:1)
- [0.120-design.md](/home/adam/projects/icydb/docs/design/0.120-pipeline-unification/0.120-design.md:1)
- [0.121-design.md](/home/adam/projects/icydb/docs/design/0.121-access-explain-projection-collapse/0.121-design.md:1)

## Feature Inventory

### Primary Feature Rows

| Feature Row | State | Readout |
| ---- | ---- | ---- |
| scalar `SELECT` | Complete | Strong admitted surface, lowering, semantic identity, planning, execution, explain, and proof within the admitted single-entity boundary |
| grouped `SELECT` | Bounded | Strong execution and explain inside the admitted grouped family, but intentionally restricted and fail-closed outside that family |
| predicates (`WHERE` / `HAVING`) | Bounded | Scalar filter semantics are deep; grouped `HAVING` is strong for the shipped families but still intentionally family-scoped |
| projection expressions | Bounded | Bounded computed projections are strong, but the expression surface is intentionally narrow rather than general |
| aggregates | Bounded | Global and grouped aggregates are strong within the admitted aggregate family, but still intentionally restricted |
| `ORDER BY` | Complete | Strong within the admitted scalar/grouped boundary, including explain and route behavior for current shapes |
| `LIMIT` / `OFFSET` | Complete | Strong for the admitted scalar SQL pagination surface; scalar cursor pagination is explicitly out of scope rather than missing |
| `DISTINCT` | Bounded | Present and tested, but only within the admitted query families rather than as a generalized SQL distinct framework |
| mutation (`INSERT` / `UPDATE` / `DELETE`) | Bounded | Strong inside the admitted mutation and narrow `RETURNING` surface; broader SQL mutation shapes remain intentionally excluded |
| `EXPLAIN` | Complete | Strong public surface with good semantic fidelity and proof coverage |

### Supporting Rows

| Supporting Row | State | Readout |
| ---- | ---- | ---- |
| prepared SQL | Bounded | Behavior is strong and now reads more cleanly through the canonical route-owned prepare/lower lane after the dead explicit prepared-session API was removed |
| semantic identity / canonicalization | Bounded | Strong for scalar surfaces and the shipped grouped searched-`CASE` families, but not generalized |
| cache / reuse | Bounded | Canonical semantic reuse is visible and coherent for the shipped families, but reuse remains a bounded artifact model |
| diagnostics / verbose explain | Complete | One immutable diagnostics artifact owns verbose explain, and public/session SQL rendering follows it |
| fail-closed boundaries | Complete | Unsupported areas are generally explicit and reject cleanly rather than degrading into silent partial support |

## Delta Vs Same-Day Baseline

### 1. No state-label changes

No primary or supporting feature row changed label relative to
`docs/audits/reports/2026-04/2026-04-22/completeness.md`.

The same-day baseline result still holds:

- prepared SQL is `Bounded`
- the primary feature inventory remains stable
- there are no large in-scope missing families

### 2. Prepared SQL reads more honestly after the dead explicit session lane was deleted

The shipped completeness surface did not widen or narrow here, but the
supporting read is better:

- the live prepared lane is still present
- it is evidenced through `prepare_sql_statement(...)`,
  `lower_sql_command_from_prepared_statement(...)`, and the session route
  compilation path
- the deleted explicit prepared-session API no longer risks being mistaken for
  an active public or semi-public product surface

This improves the audit read without changing the classification label.

### 3. Remaining seams still read as structural follow-through, not missing scope

The current tree still does not show a large in-scope family that is merely
parsed or half-executed.

The visible remaining seams are still best described as:

- expression pipeline follow-through
- pipeline/topology contraction
- access / explain projection collapse

Those are architectural cleanup lines, not evidence that the admitted SQL
surface is incomplete.

## Partial / Bounded Areas

### 1. Grouped semantic alignment is still strong but family-scoped

Grouped searched-`CASE` semantics remain real rather than superficial, but
grouped canonicalization is still not a generalized semantic layer.

### 2. Computed projection support is intentionally narrow

Projection expressions remain bounded, not shallow. The shipped surface still
covers bounded arithmetic, selected text functions, `ROUND(...)`, and searched
`CASE`, but it is not trying to be a full SQL expression engine.

### 3. Mutation remains strong inside a narrow contract

The admitted mutation surface remains intentionally scoped to:

- `INSERT`
- `UPDATE`
- `DELETE`
- narrow `RETURNING`

### 4. Prepared SQL remains bounded rather than partial

Prepared SQL still has an explicit current boundary:

- statement normalization and entity-match preparation are route-owned
- query-lane lowering remains generic-free and fail-closed
- aggregate/projection strategy explanation remains visible in tests and
  diagnostics
- broader prepared widening beyond the shipped lane remains intentionally out
  of scope

## Missing In-Scope Areas

No large feature family appears to be missing inside the current admitted
boundary.

The remaining gaps are still mostly:

- bounded by design
- family-scoped
- structural follow-through rather than absent product surface

## Architectural Seams

### 1. Expression follow-through remains a cleanup seam, not a missing feature seam

The expression lane still carries architectural follow-through work, but the
shipped runtime surface continues to read as bounded and real rather than
present-but-weak.

### 2. Pipeline unification remains topology work, not completeness repair

`0.120` still reads as a structural pipeline contraction line rather than a
response to a missing SQL family.

### 3. Access / explain projection is now a clearer bounded follow-through seam

The new `0.121` line fits the same pattern:

- access-path meaning is already present
- explain is already present
- the remaining issue is repeated structural walking between planner and
  explain/diagnostic consumers

That is a locality and ownership seam, not a product completeness hole.

## Overall Maturity Read

The current system is still **narrow and deep**, not broad and shallow.

Inside the admitted single-entity boundary, most primary product rows remain
either `Complete` or `Bounded`. The bounded rows remain bounded for deliberate
and visible reasons, not because they are present-but-weak.

This rerun confirms that today’s baseline and subsequent same-day reruns remain
the right completeness read. The most useful update in this pass is supporting
surface clarity:

- the dead explicit prepared-session lane is gone
- the live prepared SQL lane still holds
- the next visible work continues to be structural cleanup rather than missing
  shipped capability

## Recommended Next Steps

1. keep treating the current SQL surface as bounded rather than product-starved
   - do not infer missing feature rows from structural cleanup work
   - keep completeness and architecture cleanup as separate reads

2. continue the topology / ownership cleanup line after `0.119`
   - [0.120-design.md](/home/adam/projects/icydb/docs/design/0.120-pipeline-unification/0.120-design.md:1)
   - [0.121-design.md](/home/adam/projects/icydb/docs/design/0.121-access-explain-projection-collapse/0.121-design.md:1)

3. rerun the same crosscutting audit set after the next non-doc structural slice
   - completeness
   - canonical semantic authority
   - velocity preservation

## Verification

- `cargo check -p icydb-core`
