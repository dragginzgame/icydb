# 0.186 Filter Authority Source Map

This file records the starting point for 0.186. It is an audit map, not an
implementation contract.

## Current Authority Chain

### SQL Lowering

- Owner: `LoweredSqlFilter`.
- Source: `crates/icydb-core/src/db/sql/lowering/select/mod.rs`.
- Current job: keep SQL's visible boolean expression beside the optional
  predicate subset until the `StructuralQuery` / query-intent handoff.
- Important behavior: scalar SELECT, grouped SELECT, global aggregate, DELETE,
  and UPDATE each use distinct construction policies because SQL truth
  semantics and write admission are not identical.

### Fluent Query Builders

- Owner: fluent query model and filter builders.
- Source: `crates/icydb-core/src/db/query/intent/model.rs`.
- Current job: lower typed fluent filters into normalized planner expressions,
  or accept an already-normalized predicate at SQL/test seams.
- Important behavior: fluent filters do not carry SQL parser details; they
  enter query intent through normalized expression or predicate APIs.

### Query Intent

- Owner: `NormalizedFilter`.
- Source: `crates/icydb-core/src/db/query/intent/state.rs`.
- Current job: store one normalized semantic expression, one optional predicate
  subset, whether that subset fully covers the expression, and whether the
  expression remains visible.
- Important behavior: multiple filters are AND-combined at this seam so later
  planning consumes one scalar filter state instead of loose frontend fields.

### Access Planning

- Owner: access planning over query-intent predicate facts.
- Source: `crates/icydb-core/src/db/query/plan/planner/`.
- Current job: consume the predicate subset to choose access paths, strip
  access-proven predicates, and leave residual work for finalized planning.
- Important behavior: route selection must not reinterpret SQL truth semantics;
  it consumes the predicate subset already admitted at earlier seams.

### Finalized Planning

- Owners: `ResidualFilterContract` and `PredicatePushdownDiagnostics`.
- Source: `crates/icydb-core/src/db/query/plan/access_plan.rs` and
  `crates/icydb-core/src/db/query/plan/semantics/logical.rs`.
- Current job: freeze the post-access visible residual expression, residual
  predicate subset, compiled runtime filter program, and predicate-pushdown
  diagnostic outcome.
- Important behavior: executor, EXPLAIN, and verbose diagnostics consume the
  finalized residual contract rather than reconstructing residual shape from
  rendered text.

### Count / Cardinality Shortcuts

- Owner: aggregate/count terminal planning and execution.
- Source: `crates/icydb-core/src/db/executor/aggregate/count_terminal.rs` and
  `crates/icydb-core/src/db/session/query/cache.rs`.
- Current job: use planned predicate/access shape plus lowered exact-prefix
  cardinality specs to admit direct COUNT/EXISTS shortcuts.
- Current proof: direct COUNT cardinality eligibility consumes
  `NormalizedFilter` predicate coverage and rejects candidates with an
  uncovered visible residual filter.

### Cache / Fingerprint / EXPLAIN

- Owners: query cache keys, shape signatures, and EXPLAIN projections.
- Source: `crates/icydb-core/src/db/query/intent/cache_key.rs`,
  `crates/icydb-core/src/db/query/fingerprint/`, and
  `crates/icydb-core/src/db/query/explain/`.
- Current job: project planner-owned filter/access facts into stable identity
  and diagnostics surfaces.
- Current proof: shared cache identity is owned by the visible filter
  expression when one exists; predicate mirrors remain cache-key fallbacks only
  for predicate-only filters. EXPLAIN residual diagnostics expose the
  finalized residual filter expression and do not invent a residual predicate
  when the shared predicate subset cannot cover the visible expression.

## Divergence Risks To Audit First

- SQL lowering may pre-derive a predicate subset while fluent intent derives it
  inside `NormalizedFilter`.
- SQL can keep a visible expression and a predicate subset together before
  intent; fluent generally enters with expression-only or predicate-only facts.
- Predicate extraction ownership is split across SQL lowering, query intent,
  and residual planning.
- Count/cardinality shortcuts must continue consuming the same filter authority
  as page execution.
- EXPLAIN and cache identity must continue projecting planner-owned facts, not
  alternate interpretations of filter semantics.

## First Implementation Rule

Do not change pre-access filter authority and post-access residual authority in
the same slice. Each 0.186 code slice should move or name one fact, add parity
coverage, and prove route, residual, count/cardinality, cache, and EXPLAIN
behavior stay unchanged.

## 0.186.0 Guard Baseline

- `filter_authority_predicate_subset_derivation_sites_are_explicit` records the
  only runtime sites that currently derive a predicate subset from a normalized
  boolean expression: SQL lowering adapters, query intent, EXPLAIN projection,
  and the canonical predicate compiler implementation.
- `filter_authority_residual_contract_creation_stays_in_logical_semantics`
  records logical planning as the only runtime creator of finalized residual
  filter contracts and pushdown diagnostics.
- The SQL/fluent parity matrix now covers negated membership, `IS NOT NULL`,
  and negated boolean composition in addition to the existing numeric,
  field-to-field, membership, null, casefold-prefix, and boolean-composition
  convergence checks.

## Coverage Fact Baseline

- `NormalizedFilter` now stores `FilterPredicateCoverage` as the pre-access
  semantic coverage fact for the user-visible filter.
- The coverage fact distinguishes `Full`, `Partial`, and `None` with explicit
  gap/failure reasons, so query intent can represent predicate-only fluent
  filters and mixed extractable/unextractable filters without overloading a
  visible-expression boolean.
- `predicate_subset_covers_expr()` remains as the existing logical-planning
  projection. It preserves route, residual, count/cardinality, cache, and
  EXPLAIN behavior while the shared pre-access contract is tightened.

## SQL SELECT Extraction Cleanup

- Ordinary scalar and grouped SQL SELECT filters now hand off only the
  schema-bound visible expression to query intent.
- `NormalizedFilter` derives the shared predicate subset for those
  expression-backed filters after schema binding, matching the fluent
  expression-backed path.
- DELETE now follows the same expression-backed handoff; unsupported DELETE
  filters remain residual expressions rather than carrying a broad
  `Predicate::True` fallback.
- Strict SQL paths that require a predicate for admission, including UPDATE
  selectors and global aggregate base filters, still carry explicit predicate
  subsets and remain intentionally fail-closed for expression-only WHERE
  shapes.
- `filter_authority_sql_explicit_predicate_lanes_are_explicit` and
  `filter_authority_sql_predicate_handoffs_are_explicit` guard those remaining
  SQL exceptions so new pre-access predicate derivation or handoff sites must
  be intentionally reviewed.
- The strict-path audit keeps UPDATE and global aggregate base filters separate
  because moving them to expression-backed intent would widen accepted SQL
  rather than merely relocate predicate extraction.

## Count Cardinality Coverage Proof

- Direct COUNT cardinality shortcut eligibility now has query-intent coverage
  proving it accepts predicate-only and fully-covered visible-expression
  filters, but rejects candidates where an uncovered visible residual filter
  remains.
- This keeps direct COUNT metadata admission tied to `NormalizedFilter`
  predicate coverage rather than a separate predicate-only shortcut.

## Cache Identity Proof

- Expression-plus-predicate handoffs now have direct cache-key coverage proving
  the visible filter expression owns shared cache identity when present.
- Predicate identity remains hash-significant only for predicate-only filters,
  keeping cache identity aligned with user-visible filter semantics rather than
  frontend-specific predicate mirrors.

## EXPLAIN Projection Proof

- Existing SQL EXPLAIN coverage proves expression-owned residual filters expose
  `filter_expr` / `residual_filter_expr` without claiming a derived
  `residual_filter_predicate`.
- Existing logical EXPLAIN coverage proves residual summaries distinguish
  scalar residual expressions from predicate-only residual work.
