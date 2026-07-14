# Crosscutting Completeness Audit - 2026-04-22 Rerun 3

## Report Preamble

- scope: current single-entity query and mutation system, using the same
  runtime boundary and taxonomy as
  `docs/audits/reports/2026-04/2026-04-22/completeness.md`
- compared baseline report path:
  `docs/audits/reports/2026-04/2026-04-22/completeness.md`
- code snapshot identifier: `23c47ac317` (`clean` working tree)
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

The main new information in this rerun is sequencing, not shipped runtime
change:

- the prepared lane remains `Bounded`, not `Partial`
- the broader expression pipeline cluster remains the clearest structural seam
- `0.116` remains the first natural contraction target inside that seam
- `0.117` now gives a plausible follow-on after `0.116`, but does not change
  the current completeness classification by itself

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
  - prepared SQL within the current predicate/access-template vs fallback split
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
  - prepared/template widening beyond the current predicate/access-only lane

## Evidence Sources

Primary runtime evidence remains unchanged from the same-day baseline:

- `docs/contracts/SQL_SUBSET.md`
- `crates/icydb-core/src/db/session/tests/sql_scalar.rs`
- `crates/icydb-core/src/db/session/tests/sql_grouped.rs`
- `crates/icydb-core/src/db/session/tests/sql_aggregate.rs`
- `crates/icydb-core/src/db/session/tests/sql_projection.rs`
- `crates/icydb-core/src/db/session/tests/sql_write.rs`
- `crates/icydb-core/src/db/session/tests/sql_delete.rs`
- `crates/icydb-core/src/db/session/tests/sql_explain.rs`
- `crates/icydb-core/src/db/session/tests/sql_surface.rs`
- `crates/icydb-core/src/db/session/tests/sql_parameterized.rs`
- `crates/icydb-core/src/db/sql/lowering/tests/mod.rs`
- `crates/icydb-core/src/db/sql/lowering/prepare.rs`
- `crates/icydb-core/src/db/session/sql/parameter.rs`
- `crates/icydb-core/src/db/predicate/model.rs`
- `crates/icydb-core/src/db/query/plan/expr/ast.rs`

Secondary planning context used only for next-step interpretation:

- [0.116-design.md](/home/adam/projects/icydb/docs/design/0.116-truth-condition-semantics-centralization/0.116-design.md:1)
- [first-step-addendum.md](/home/adam/projects/icydb/docs/design/0.116-truth-condition-semantics-centralization/first-step-addendum.md:1)
- [0.117-design.md](/home/adam/projects/icydb/docs/design/0.117-expression-family-semantics-centralization/0.117-design.md:1)

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
| prepared SQL | Bounded | Behavior is strong and the current template-vs-fallback split is explicit and fail-closed |
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

### 2. The clearest remaining seam is still the expression pipeline cluster

The broad structural seam identified in today’s baseline remains the same:

- `db/query/plan/expr/*`
- `db/predicate/*`
- expression-related parts of `db/sql/lowering/*`

No shipped runtime evidence in this rerun narrows that seam further.

### 3. The next-step ordering is clearer, not different

With the current design line in place:

- `0.116` remains the first natural contraction target
  - truth-condition semantics is still the clearest first bounded contraction
    inside the expression seam
- `0.117` now reads as the plausible follow-on after `0.116`
  - expression family semantics
  - starting with `CASE` result typing and propagation
  - keeping function-family classification inside the same later line if
    locality remains favorable

That is a better sequencing read than trying to infer additional speculative
slices before `0.116` lands.

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

Prepared SQL still has an explicit boundary:

- predicate/access-owned template shapes may stay on template lanes
- general expression-owned shapes still fall back
- grouped symbolic template admission remains intentionally bounded

That boundary still reads as coherent and fail-closed in this rerun.

## Missing In-Scope Areas

No large feature family appears to be missing inside the current admitted
boundary.

The remaining gaps are still mostly:

- bounded by design
- family-scoped
- structural follow-through rather than absent product surface

## Architectural Seams

### 1. Truth-condition semantics remains the first contraction inside the expression seam

The most natural first contraction inside the broader expression pipeline is
still truth-condition semantics:

- `WHERE` truth meaning
- `HAVING` truth meaning
- bool-valued filter expressions
- compare/null-test/truth-wrapper interpretation
- admitted searched `CASE` truth-result families

### 2. Expression family semantics is the clearest follow-on after that first contraction

After the truth lane, the next natural expression-pipeline contraction is the
broader expression family lane:

- `CASE` result typing and propagation
- planner-owned result-family classification
- bounded function-family classification
- remaining non-truth expression rewriting outside planner

### 3. SQL compiled-command caching and semantic plan reuse still follow different identity models

This remains a coherent current boundary:

- compiled SQL caching remains syntax/text-bound
- semantic plan reuse follows canonical identity

## Overall Maturity Read

The current system is still **narrow and deep**, not broad and shallow.

Inside the admitted single-entity boundary, most primary product rows remain
either `Complete` or `Bounded`. The bounded rows remain bounded for deliberate
and visible reasons, not because they are present-but-weak.

This rerun confirms that today’s baseline and subsequent same-day reruns remain
the right completeness read. The main new information is still sequencing, not
runtime movement:

- do `0.116` first
- use `0.117` as the likely follow-on only after `0.116` lands and the audits
  are rerun

## Recommended Next Steps

1. execute [0.116-design.md](/home/adam/projects/icydb/docs/design/0.116-truth-condition-semantics-centralization/0.116-design.md:1)
   - centralize truth-condition semantics behind planner-owned expression
     typing and canonicalization
   - keep predicate and lowering as adapters/consumers rather than secondary
     truth-condition owners

2. start with [first-step-addendum.md](/home/adam/projects/icydb/docs/design/0.116-truth-condition-semantics-centralization/first-step-addendum.md:1)
   - use truth-wrapper normalization as the first bounded contraction target
   - keep the first slice narrow enough to preserve locality and proof quality

3. treat [0.117-design.md](/home/adam/projects/icydb/docs/design/0.117-expression-family-semantics-centralization/0.117-design.md:1) as the likely follow-on
   - only after `0.116` lands
   - starting with `CASE` result typing and propagation
   - keeping bounded function-family classification as the later part of that
     line if locality remains favorable

4. rerun the same crosscutting audit set after `0.116`
   - completeness
   - canonical semantic authority
   - complexity accretion
   - DRY consolidation
   - velocity preservation

## Verification Readout

- `cargo check -p icydb-core` -> PASS
- status: `PASS`
