# Crosscutting Completeness Audit - 2026-04-22

## Report Preamble

- scope: current post-`0.114.1` single-entity query and mutation system
- compared baseline report path: `docs/audits/reports/2026-04/2026-04-21/completeness.md`
- code snapshot identifier: `7cf229c3d6` (`dirty` working tree)
- method tag/version: `Completeness Method V1`
- comparability status: `comparable`
  - this rerun keeps the same taxonomy and boundary as the 2026-04-21 baseline
  - the only material state-label change in this rerun is prepared SQL moving
    from `Partial` to `Bounded`

## Executive Summary

The current system is **bounded, coherent, and structurally cleaner than the
2026-04-21 baseline**.

The primary feature inventory did not change. Inside the admitted single-entity
boundary, scalar query semantics remain deep, grouped/global aggregate
semantics remain strong but intentionally narrow, and there are still no
obvious in-scope families that merely parse without real execution support.

The main delta in this rerun is architectural rather than product-surface
expansion:

- prepared SQL is no longer the clearest `Partial` supporting row
- the prepared lane now consumes more planner- and lowering-owned structure
  instead of re-deriving it locally
- the next visible seam is the broader expression pipeline cluster, not a
  prepared-specific fallback authority split

This system still prioritizes **depth over breadth**. The difference from the
baseline is that one of the main architectural limiters now reads as an
explicit bounded boundary rather than an active partial-completeness seam.

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

This rerun keeps the 2026-04-21 feature taxonomy and refreshes evidence against
the current `0.114.1` tree.

Primary evidence sources used for this audit:

- `docs/contracts/SQL_SUBSET.md`
- `docs/changelog/0.114.md`
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
| `EXPLAIN` | Complete | Strong public surface, backed by one immutable diagnostics artifact with good semantic fidelity and proof coverage |

### Supporting Rows

| Supporting Row | State | Readout |
| ---- | ---- | ---- |
| prepared SQL | Bounded | Behavior is strong and the current template-vs-fallback split is now explicit and fail-closed; the lane still remains intentionally narrower than general prepared expression reuse |
| semantic identity / canonicalization | Bounded | Strong for scalar surfaces and the shipped grouped searched-`CASE` families, but not generalized |
| cache / reuse | Bounded | Canonical semantic reuse is visible and coherent for the shipped families, but reuse remains a bounded artifact model |
| diagnostics / verbose explain | Complete | One immutable diagnostics artifact owns verbose explain, and public/session SQL rendering follows it |
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
| prepared SQL | Strong | Strong | Strong | Strong | Strong | Strong | Strong | Bounded |
| semantic identity / canonicalization | Strong | Strong | Partial | Strong | Strong | Strong | Strong | Bounded |
| cache / reuse | N/A | Strong | Strong | Strong | Strong | Strong | Strong | Bounded |
| diagnostics / verbose explain | Strong | Strong | Strong | Strong | N/A | Strong | Strong | Complete |
| fail-closed boundaries | Strong | Strong | Strong | Strong | Strong | Strong | Strong | Complete |

Stage notes:

- grouped rows remain `Bounded` rather than `Partial` because the current
  restrictions are explicit, tested, and fail-closed
- mutation rows treat canonical identity as `N/A` because this audit does not
  define a generalized mutation canonicalization model
- `EXPLAIN` treats runtime result production as `N/A` because its product
  boundary is diagnostics, not query payload production
- prepared SQL now lands on `Bounded` because the remaining limit is boundary
  scope, not a live ownership split between the prepared lane and planner truth

## Delta Vs 2026-04-21

### 1. Prepared SQL moves from `Partial` to `Bounded`

This is the only material state-label change in the rerun.

The baseline `Partial` label was driven by architectural duplication in the
prepared lane. In the current tree:

- lowering owns template-expression admission classifiers and parser-shape
  helpers used by prepared gating
- predicate ownership now includes prepared scalar predicate templates,
  runtime-value detection, and template rebinding
- planner expression ownership now includes grouped prepared-expression
  templates and grouped template rebinding
- session binding primarily consumes those planner- and lowering-owned
  structures instead of reconstructing them locally

That does not widen prepared SQL. It makes the existing boundary more honest.
Prepared SQL remains intentionally bounded by the current template-vs-fallback
split, but it no longer reads like a behaviorally strong surface resting on an
architecturally partial semantic side engine.

### 2. Primary feature inventory is unchanged

No primary feature row changed label in this rerun.

The system still looks like:

- deep scalar single-entity query semantics
- strong but intentionally narrow grouped/global aggregate semantics
- bounded projection and aggregate expression families
- strong explain and mutation surfaces inside a narrow contract

This rerun therefore confirms that the main movement since the baseline is
structural contraction, not feature widening.

## Partial / Bounded Areas

### 1. Grouped semantic alignment is strong, but still family-scoped

Grouped semantics remain materially better than the pre-`0.110` state, and the
current grouped searched-`CASE` family is real rather than superficial.

It is still bounded completeness, not a generalized grouped semantic layer.

### 2. Computed projection support is intentionally narrow

Projection expressions remain **bounded**, not shallow.

The admitted projection family still covers bounded arithmetic, selected text
functions, `ROUND(...)`, and searched `CASE`, with strong lowering and proof
inside that lane. The system is not trying to be a general SQL expression
engine.

### 3. Mutation is strong inside a narrow contract

Mutation support remains intentionally scoped to:

- `INSERT`
- `UPDATE`
- `DELETE`
- narrow `RETURNING`

That row remains bounded because the admitted mutation surface is deliberately
narrower than general SQL mutation semantics, not because the shipped paths are
weak.

### 4. Prepared SQL is now bounded rather than partial

Prepared SQL still has an explicit boundary:

- predicate/access-owned template shapes may stay on template lanes
- general expression-owned shapes still fall back
- grouped symbolic template admission remains intentionally bounded

What changed is not feature breadth. What changed is that the bounded split now
has better structural ownership and owner-local rebinding logic has been pushed
back onto predicate and expression authorities.

## Missing In-Scope Areas

No large feature family appears to be **missing** inside the current admitted
boundary.

The current gaps are still mostly:

- bounded by design
- family-scoped
- or structural follow-through rather than absent product surface

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

### 1. Expression-pipeline contraction is now the clearest remaining seam

Prepared SQL is no longer the main `Partial` row, but the broader expression
pipeline still spans a large adjacent cluster:

- `db/query/plan/expr/*`
- `db/predicate/*`
- expression-related parts of `db/sql/lowering/*`

That is now the clearer structural follow-up seam than prepared fallback
authority.

### 2. Grouped semantic alignment is still incremental

Grouped semantics are no longer second-class for the shipped searched-`CASE`
families, but grouped canonicalization still grows family by family rather than
through one generalized grouped semantic layer.

### 3. SQL compiled-command caching and semantic plan reuse still follow
different identity models

This remains a coherent current boundary:

- compiled SQL caching remains syntax/text-bound
- semantic plan reuse follows canonical identity

That distinction is still correct, but it remains a seam that must stay
explicit and tested.

### 4. Route selection is improved, but still not the main completeness limiter

Route comparison is substantially better than in earlier lines, and it no
longer looks like the first crosscutting completeness limiter. It remains worth
monitoring as a structural ownership question, but it is no longer the main
reason any row falls short of `Complete`.

## Overall Maturity Read

The current system is still **narrow and deep**, not broad and shallow.

Inside the admitted single-entity boundary, most primary product rows remain
either `Complete` or `Bounded`. The bounded rows are bounded for deliberate and
visible reasons, not because they are present-but-weak.

The main change from the baseline is that prepared SQL no longer keeps the
supporting matrix in a `Partial` posture. The system now reads as having very
little true missing in-scope surface and one fewer major architectural
completeness blocker.

The next completeness movement should come from structural contraction of the
expression pipeline and any explicitly chosen grouped-semantic widening, not
from emergency repair of the prepared lane.

## Recommended Next Steps

1. `0.115`: expression-pipeline contraction
   - keep planner-owned expression typing and canonicalization as the single
     semantic authority
   - remove remaining semantic-adjacent duplication across
     `db/query/plan/expr`, `db/predicate`, and expression-related lowering

2. grouped semantic widening only if explicitly desired
   - keep the next grouped work family-scoped and proof-gated
   - do not widen grouped semantics casually beyond the searched-`CASE` line

3. keep identity-boundary documentation and tests explicit
   - canonical semantic identity and syntax-bound compiled SQL identity remain
     coherent current boundaries, but they must stay clearly documented and
     tested

4. keep prepared template admission intentionally bounded
   - do not let structural cleanup turn into accidental template-lane widening
   - preserve the current predicate/access-template vs fallback contract

## Verification Readout

- `cargo check -p icydb-core` -> PASS
- `cargo test -p icydb-core build_prepared_template_rebinds_compare_slot_owned_literal_leaves -- --nocapture` -> PASS
- `cargo test -p icydb-core build_prepared_grouped_template_rebinds_slot_owned_literal_leaves -- --nocapture` -> PASS
- status: `PASS`
