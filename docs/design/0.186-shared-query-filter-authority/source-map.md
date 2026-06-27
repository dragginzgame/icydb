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
- Open 0.186 question: prove these shortcuts consume the same pre-access filter
  authority as page execution for each equivalent SQL/fluent shape.

### Cache / Fingerprint / EXPLAIN

- Owners: query cache keys, shape signatures, and EXPLAIN projections.
- Source: `crates/icydb-core/src/db/query/intent/cache_key.rs`,
  `crates/icydb-core/src/db/query/fingerprint/`, and
  `crates/icydb-core/src/db/query/explain/`.
- Current job: project planner-owned filter/access facts into stable identity
  and diagnostics surfaces.
- Open 0.186 question: identify which differences are semantic identity and
  which are diagnostics-only presentation.

## Divergence Risks To Audit First

- SQL lowering may pre-derive a predicate subset while fluent intent derives it
  inside `NormalizedFilter`.
- SQL can keep a visible expression and a predicate subset together before
  intent; fluent generally enters with expression-only or predicate-only facts.
- Predicate extraction ownership is split across SQL lowering, query intent,
  and residual planning.
- Count/cardinality shortcuts need proof that they consume the same filter
  authority as page execution.
- EXPLAIN and cache identity must stay projections of planner-owned facts, not
  alternate interpreters of filter semantics.

## First Implementation Rule

Do not change pre-access filter authority and post-access residual authority in
the same slice. Each 0.186 code slice should move or name one fact, add parity
coverage, and prove route, residual, count/cardinality, cache, and EXPLAIN
behavior stay unchanged.
