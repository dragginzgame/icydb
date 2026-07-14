# Crosscutting Completeness Audit - 2026-04-21

## Report Preamble

- scope: current post-`0.111` single-entity query and mutation system
- compared baseline report path: none; this is the first formal run under the recurring completeness method
- code snapshot identifier: `7c1946c043` (`dirty` working tree)
- method tag/version: `Completeness Method V1`
- comparability status: `baseline`
  - this run should be treated as the initial structured reference point for later completeness reruns
  - rerun note: a second pass using the tightened reusable classification
    language did not change any feature-state labels in this report

## Executive Summary

The current system is **bounded but coherent**.

Within the admitted single-entity boundary, scalar query semantics are deep and
largely complete. Grouped and global aggregate semantics are also strong, but
they remain explicitly bounded rather than generalized. Prepared SQL is present
and heavily exercised, but it is still the clearest architectural seam because
parameter-family reasoning remains partly duplicated in the prepared fallback
lowering path.

There are no obvious large in-scope features that merely parse without real
execution support. The main remaining issues are not missing product surfaces;
they are boundary and consolidation issues:

- prepared fallback typing is still too local to `prepare.rs`
- route selection is better than before, but not fully single-authority yet
- grouped semantic alignment is now real, but still family-scoped

This audit distinguishes between **feature breadth** and **pipeline depth**.
The current system clearly prioritizes depth over breadth.

## Classification Model

- `Complete`: feature is fully implemented within the audited boundary and its
  relevant pipeline stages are coherent and proven
- `Bounded`: feature is intentionally restricted, with explicit and fail-closed
  boundaries inside the admitted family
- `Partial`: feature exists but still has architectural or pipeline gaps that
  block a `Complete` or `Bounded` read
- `Missing`: feature is expected in scope but not implemented
- `Out Of Scope`: feature is intentionally excluded from the audited boundary

## Stage Evaluation Terms

- `Strong`: stage is coherent, aligned, and supported by proof
- `Partial`: stage exists but has gaps, uneven parity, or bounded limitations
- `Weak`: stage is present but is one of the main limiting factors
- `Missing`: stage does not exist for the feature
- `N/A`: stage is structurally not applicable to the feature

## System Boundary

### Included

- single-entity `SELECT`, `EXPLAIN`, introspection, and mutation SQL within the
  current public SQL subset contract
- typed/fluent single-entity query and mutation surfaces where they define or
  confirm the same semantic boundary
- prepared SQL within the current predicate/access-template vs fallback split
- scalar filtering, grouped/global aggregates, bounded searched `CASE`, bounded
  projection expressions, ordering, pagination, and narrow `RETURNING`
- semantic identity, structural cache identity, explain fidelity, and visible
  plan reuse within the admitted surface

### Excluded

- multi-entity SQL
- joins
- subqueries
- window functions
- general relational SQL
- transport-level cursor semantics for scalar SQL
- generalized grouped boolean families beyond the shipped searched-`CASE`
  families
- prepared/template widening beyond the current predicate/access-only lane

### Authoritative Proof Surfaces

This audit treats the following proof surfaces as authoritative:

- public SQL session surfaces
- fluent/typed query surfaces where they participate in the same semantic model
- prepared SQL session surfaces
- explain/session diagnostics surfaces

The canister/integration surface is treated as **supplemental confirmation**,
not a completeness gate for every feature row in this run.

## Evidence Sources

This audit uses a stable feature taxonomy. Future runs should either reuse this
taxonomy or explicitly document deviations.

Primary evidence sources used for this audit:

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
- `testing/pocket-ic/tests/sql_canister.rs`

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
| `EXPLAIN` | Complete | Strong public surface, now backed by one immutable diagnostics artifact with good semantic fidelity and proof coverage |

### Supporting Rows

| Supporting Row | State | Readout |
| ---- | ---- | ---- |
| prepared SQL | Partial | Behaviorally complete on the admitted public surface, but still architecturally partial because fallback typing/inference remains too local to `prepare.rs` |
| semantic identity / canonicalization | Bounded | Strong for scalar surfaces and the shipped grouped searched-`CASE` families, but not generalized |
| cache / reuse | Bounded | Canonical semantic reuse is visible and coherent for the shipped families, but reuse remains a bounded artifact model |
| diagnostics / verbose explain | Complete | One immutable diagnostics artifact now owns verbose explain, and public/session SQL rendering follows it |
| fail-closed boundaries | Complete | Current unsupported areas are generally explicit and reject cleanly rather than degrading into silent partial support |

## Pipeline Completeness

### Primary Rows

| Feature | Parse / Surface | Lowering | Canon / Identity | Planning | Execution | Explain | Proof | Derived State |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| scalar `SELECT` | Strong | Strong | Strong | Strong | Strong | Strong | Strong | Complete |
| grouped `SELECT` | Strong | Strong | Partial | Strong | Strong | Strong | Strong | Bounded |
| predicates (`WHERE` / `HAVING`) | Strong | Strong | Partial | Strong | Strong | Strong | Strong | Bounded |
| projection expressions | Strong | Strong | Partial | Strong | Strong | Strong | Strong | Bounded |
| aggregates | Strong | Strong | Partial | Strong | Strong | Strong | Strong | Bounded |
| `ORDER BY` | Strong | Strong | Strong | Strong | Strong | Strong | Strong | Complete |
| `LIMIT` / `OFFSET` | Strong | Strong | N/A | Strong | Strong | Strong | Strong | Complete |
| `DISTINCT` | Strong | Strong | Partial | Strong | Strong | Strong | Strong | Bounded |
| mutation | Strong | Strong | N/A | Strong | Strong | Strong | Strong | Bounded |
| `EXPLAIN` | Strong | Strong | Strong | Strong | N/A | Strong | Strong | Complete |

### Supporting Rows

| Supporting Row | Parse / Surface | Lowering | Canon / Identity | Planning | Execution | Explain | Proof | Derived State |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| prepared SQL | Strong | Partial | Partial | Strong | Strong | Strong | Strong | Partial |
| semantic identity / canonicalization | Strong | Strong | Partial | Strong | Strong | Strong | Strong | Bounded |
| cache / reuse | N/A | Strong | Strong | Strong | Strong | Strong | Strong | Bounded |
| diagnostics / verbose explain | Strong | Strong | Strong | Strong | N/A | Strong | Strong | Complete |
| fail-closed boundaries | Strong | Strong | Strong | Strong | Strong | Strong | Strong | Complete |

Stage notes:

- grouped rows land on `Bounded` rather than `Partial` because the current
  restrictions are explicit, tested, and fail-closed
- mutation rows treat canonical identity as `N/A` because this audit does not
  define a generalized mutation canonicalization model
- `EXPLAIN` treats runtime result production as `N/A` because its product
  boundary is diagnostics, not query payload production
- prepared SQL lands on `Partial` because the behavioral surface is strong, but
  the lowering/typing seam is still a main limiting factor

## Partial / Bounded Areas

### 1. Grouped semantic alignment is strong, but still family-scoped

`0.110` and `0.111` materially improved grouped semantics:

- explicit-`ELSE` grouped searched `CASE` is canonicalized
- omitted-`ELSE` grouped searched `CASE` can now join the explicit `ELSE NULL`
  family when the proof gate succeeds
- grouped explain, hash, cache identity, and reuse follow that canonical form

That is real completeness movement. It is still bounded completeness, not a
general grouped canonicalization model.

### 2. Computed projection support is intentionally narrow

Projection expressions are not shallow; they are **bounded**.

The admitted projection family includes bounded arithmetic, selected text
functions, `ROUND(...)`, and searched `CASE`, with strong lowering and proof
inside that lane. The system is not trying to be a general SQL expression
engine.

### 3. Mutation is strong inside a narrow contract

Mutation support is not missing. It is intentionally scoped to:

- `INSERT`
- `UPDATE`
- `DELETE`
- narrow `RETURNING`

That row is bounded because the surface is intentionally narrower than general
SQL mutation semantics, not because the admitted paths are weak.

### 4. Prepared SQL is behaviorally strong but architecturally partial

Prepared SQL has substantial proof coverage and a clear public split:

- predicate/access-only shapes stay on template lanes
- expression-owned `WHERE` falls back
- grouped/global prepared `HAVING` behavior is exercised
- collision and null-binding fallbacks are exercised

Prepared SQL is behaviorally complete but architecturally partial.

The reason this row is still `Partial` is architectural: too much
parameter-family reasoning still lives in
`crates/icydb-core/src/db/sql/lowering/prepare.rs`.

## Missing In-Scope Areas

No large feature family appears to be **missing** inside the current admitted
boundary.

The current gaps are mostly:

- bounded by design
- family-scoped
- or architectural seams rather than absent surfaces

That is a healthier completeness profile than “broad but shallow”.

## Out-Of-Scope Areas

The following should be treated as out of scope for this audit, not missing:

- joins
- subqueries
- window functions
- multi-entity planning
- generalized grouped boolean semantics beyond the shipped searched-`CASE`
  families
- scalar SQL cursor pagination
- broad transport-level diagnostic surfaces
- prepared/template widening beyond the current predicate/access-only model

## Architectural Seams

### 1. Prepared fallback typing duplication

This is the clearest remaining cross-cutting seam.

`crates/icydb-core/src/db/sql/lowering/prepare.rs` still owns substantial local
parameter-family and traversal logic that is semantically close to planner-owned
reasoning. The public behavior is strong, but the architecture still carries a
parallel semantic-adjacent engine here.

### 2. Route selection is improved, but not yet fully single-authority

Recent work consolidated route-choice comparison substantially, especially for
the `AND` family and non-index chosen reasons. The remaining seam is that route
selection still is not obviously “one comparison function, no exceptions” across
every family.

### 3. Grouped semantic alignment is still incremental

Grouped semantics are no longer second-class for the shipped searched-`CASE`
families, but grouped canonicalization is still growing family by family rather
than through one generalized grouped semantic layer.

### 4. SQL compiled-command caching and semantic plan reuse still follow
different identity models

This is probably the correct current boundary:

- compiled SQL caching remains syntax/text-bound
- semantic plan reuse follows canonical identity

It is coherent, but it is still a seam that needs to remain explicit to avoid
future mistakes.

## Overall Maturity Read

The current system is **narrow and deep**, not broad and shallow.

Inside the admitted single-entity boundary, most primary product rows are either
`Complete` or `Bounded`. The bounded rows are usually bounded for deliberate
reasons and backed by strong fail-closed behavior. The system does not show the
classic warning sign of many “present but weak” features.

The main remaining work is architectural consolidation, especially in prepared
SQL, plus deliberate widening if the product boundary is meant to expand later.

This rerun did not change the headline inventory. It confirmed that the current
system has very little true missing in-scope surface and that the main
remaining debt is architectural rather than product-surface absence.

## Recommended Next Steps

1. `0.112`: prepared fallback typing consolidation
   - narrow the local semantic engine in
     `crates/icydb-core/src/db/sql/lowering/prepare.rs`
   - preserve the current predicate/access-template vs fallback boundary
   - improve contract tests around parameter-family ownership while removing
     duplicated reasoning

2. route-selection unification follow-through
   - continue reducing planner-family special paths until final route choice is
     fully comparison-owned

3. grouped semantic widening only if explicitly desired
   - keep the next grouped work family-scoped and proof-gated
   - do not widen grouped semantics casually beyond the searched-`CASE` line

4. keep identity-boundary documentation and tests explicit
   - canonical semantic identity and syntax-bound prepared identity are both
     correct current boundaries, but they must stay clearly documented and
     tested
