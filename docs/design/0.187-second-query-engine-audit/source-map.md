# 0.187 Source Map

This file is the active audit map for the second query-engine audit. It records
where authority appears to live after 0.184, 0.185, and 0.186. It is not a
runtime contract.

## Filter Authority

- Primary sources:
  - `crates/icydb-core/src/db/query/intent/state.rs`
  - `crates/icydb-core/src/db/sql/lowering/select/mod.rs`
  - `crates/icydb-core/src/db/query/plan/semantics/logical.rs`
  - `crates/icydb-core/src/db/query/plan/tests/structural_guards.rs`
- Current classification: mostly closed by 0.186.
- Notes: `NormalizedFilter` owns pre-access semantic authority. Strict SQL
  UPDATE selector and global aggregate base-WHERE predicate-admission lanes
  remain deliberate fail-closed specializations.
- Recommendation: keep in guard mode unless a concrete SQL/fluent divergence is
  found.

## Branch-Aware Routing

- Primary sources:
  - `crates/icydb-core/src/db/query/plan/planner/`
  - `crates/icydb-core/src/db/executor/planning/route/`
  - `crates/icydb-core/src/db/executor/stream/access/`
  - `docs/design/0.185-branch-aware-revisit/status.md`
- Current classification: deliberate specialization.
- Notes: 0.185 intentionally kept `IndexMultiLookup`, `IndexBranchSet`, and
  general union/intersection composites distinct because they carry different
  planner proofs, diagnostics, cache identity, prefix arity, and cursor
  semantics.
- Recommendation: do not collapse branch families without a new route that
  requires broader branch merging semantics.

## Prefix-Cardinality Count And Exists

- Primary sources:
  - `crates/icydb-core/src/db/executor/index_prefix_cardinality.rs`
  - `crates/icydb-core/src/db/executor/aggregate/count_terminal.rs`
  - `crates/icydb-core/src/db/session/query/cache.rs`
  - `crates/icydb-core/src/db/session/sql/execute/direct_count.rs`
  - `crates/icydb-core/src/db/query/plan/pipeline.rs`
- Current classification: guarded split.
- Evidence: direct SQL count execution can carry accepted-authority
  `LoweredIndexPrefixCardinalitySpec`s, while prepared aggregate COUNT/EXISTS
  preflight lowers prefix-cardinality specs from a finalized plan. Both reach
  the same metadata terminal helpers in `count_terminal.rs`. The structural
  guard `prefix_cardinality_count_entrypoints_share_proof_and_terminal_authority`
  now checks that the direct SQL planned fallback and prepared aggregate
  preflight both use `exact_count_cardinality_prefixes_for_plan`, and that SQL
  direct COUNT does not own store-cardinality execution.
- Risk: low while the guard holds. The accepted-authority shortcut and prepared
  preflight remain different entry contracts, but they are no longer allowed to
  drift into separate metadata-count authorities.
- Recommendation: keep the split. Consolidate only if a future cleanup can
  remove purely mechanical glue without changing SQL direct-count cache identity
  or prepared aggregate preflight behavior.

## Scalar, Retained-Slot, Covering, And Aggregate Row-Sink Execution

- Primary sources:
  - `crates/icydb-core/src/db/executor/pipeline/entrypoints/scalar/`
  - `crates/icydb-core/src/db/executor/pipeline/contracts/execution/`
  - `crates/icydb-core/src/db/executor/covering.rs`
  - `crates/icydb-core/src/db/executor/aggregate/runtime/grouped_fold/`
- Current classification: guarded shared spine plus deliberate terminal
  specializations.
- Evidence: scalar entrypoints share route preparation and `ExecutionInputs`,
  while retained-slot, materialized page, covering, and aggregate row-sink
  terminals still keep payload-specific setup. The structural guard
  `scalar_entrypoints_share_execution_inputs_spine` now checks that
  materialized pages and aggregate row sinks call the shared scalar kernel, and
  that terminal adapters do not rebuild `ExecutionInputs`.
- Risk: low while the guard holds. Covering projection and aggregate projection
  remain specialized because they own different payload shapes and fast-path
  terminal work, not separate scalar route/input preparation.
- Recommendation: keep the shared scalar spine. Do not force a common terminal
  abstraction where payload shape is the real difference.

## Mutation And SQL Write Collection

- Primary sources:
  - `crates/icydb-core/src/db/session/sql/execute/write/`
  - `crates/icydb-core/src/db/executor/delete/`
  - `crates/icydb-core/src/db/executor/mutation/`
  - `docs/design/0.184-query-engine-audit/chunked-mutation-pipeline.md`
- Current classification: guarded split with architecture deferral.
- Evidence: 0.184 shared candidate-bound accounting, mutation execution, and
  DELETE row-resolution handoffs, but true chunked durable commits remain
  outside the current architecture. The structural guard
  `sql_write_candidate_bounds_keep_mutation_batch_and_delete_boundaries_explicit`
  now checks that SQL UPDATE/INSERT staged rows pass through
  `SqlWriteMutationExecution`, while SQL DELETE maps write bounds into
  delete-specific projection/count bounds before commit.
- Risk: low while the guard holds. DELETE still materializes rows before
  checking SQL policy bounds, but safe short-circuiting must happen after
  residual filtering and delete post-access selection, not on raw access keys.
- Recommendation: keep the current split. Treat bounded streaming DELETE
  collection as a future post-access collector design, not a mechanical
  duplicate-authority deletion.

## Aggregate Execution Families

- Primary sources:
  - `crates/icydb-core/src/db/sql/lowering/aggregate/`
  - `crates/icydb-core/src/db/executor/aggregate/`
  - `docs/design/0.184-query-engine-audit/shared-aggregate-operator.md`
- Current classification: deliberate specialization with deferred architecture
  item.
- Evidence: 0.184 cross-checked dedicated global aggregates against grouped
  singleton behavior, but first-class aggregate operator DTO and full
  operator-level physical plan remain deferred. The 0.184 shared aggregate
  operator note already defines the DTO acceptance gate: add it only when it
  deletes duplicate logic, becomes a shared runtime/EXPLAIN handoff, carries
  real cache/fingerprint identity, or prevents a concrete misclassification.
- Recommendation: keep global/grouped parity tests and descriptor guards as
  authority; do not start an aggregate DTO rewrite for 0.187.0.

## EXPLAIN, Diagnostics, Attribution, And Cache Identity

- Primary sources:
  - `crates/icydb-core/src/db/executor/explain/`
  - `crates/icydb-core/src/db/query/explain/`
  - `crates/icydb-core/src/db/query/intent/cache_key.rs`
  - `crates/icydb-core/src/db/session/query/cache.rs`
- Current classification: mostly projection surfaces.
- Evidence: 0.184 moved residual and pushdown diagnostics onto planner-owned
  contracts; 0.186 removed EXPLAIN-only residual predicate derivation and
  guarded downstream consumers from deriving frontend predicate facts.
- Recommendation: keep diagnostics as projections of runtime authority. Audit
  remaining diagnostics work for repeated expensive derivation, not semantic
  ownership.

## Recoverable Runtime Invariants

- Primary sources:
  - `crates/icydb-core/src/db/query/plan/expr/`
  - `crates/icydb-core/src/db/query/plan/expr/predicate/compile.rs`
  - `crates/icydb-core/src/db/query/plan/expr/rewrite/affine_numeric.rs`
  - `crates/icydb-core/src/db/query/plan/group.rs`
  - `crates/icydb-core/src/db/query/plan/semantics/`
  - `crates/icydb-core/src/db/query/plan/planner/compare.rs`
  - `crates/icydb-core/src/db/predicate/runtime/mod.rs`
  - `crates/icydb-core/src/db/query/intent/state.rs`
  - `crates/icydb-core/src/db/query/explain/`
  - `crates/icydb-core/src/db/query/fingerprint/`
  - `crates/icydb-core/src/db/session/sql/{update_policy,delete_policy,write_policy}.rs`
  - `crates/icydb-core/src/db/session/sql/execute/write/`
  - `crates/icydb-core/src/db/session/sql/execute/{mod,explain}.rs`
  - `crates/icydb-core/src/db/executor/aggregate/contracts/state/reducer.rs`
  - `crates/icydb-core/src/db/executor/aggregate/projection/mod.rs`
  - `crates/icydb-core/src/db/executor/planning/route/planner/execution/mod.rs`
  - `crates/icydb-core/src/db/sql_shared/cursor.rs`
  - `crates/icydb-core/src/db/sql_shared/lexer/`
  - `crates/icydb-core/src/db/key_taxonomy.rs`
  - `crates/icydb-core/src/db/sql/lowering/analysis.rs`
  - `crates/icydb-core/src/db/sql/lowering/aggregate/`
- Current classification: cleanup completed for the small trap-shaped
  invariants found in the 0.187 pass, including finalized static execution
  metadata access, SQL write-policy validated-plan helpers, covering aggregate
  terminal-value selection, query fingerprint hashing drift paths, and SQL
  frontend/lowering drift paths.
- Evidence: query expression preview/evaluation, predicate bridge conversion,
  grouped EXPLAIN/fingerprint projection, grouped strategy selection, resolved
  ORDER handling, SQL write primary-key normalization, and finalized
  static-execution-planning metadata now use optional, fallible, or fail-closed
  paths instead of `.expect(...)` / `unreachable!(...)`. SQL UPDATE/DELETE
  generated-policy variants stay on typed rejection paths, and bounded write
  proof construction is fallible when a limit is absent. Covering aggregate
  terminal-value selection now returns no selected value when non-FIRST/LAST
  validation drift reaches the local helper. SQL compiled-command and EXPLAIN
  rendering routing drift now returns typed query execution errors instead of
  reaching `unreachable!()` fallback arms. Query-intent grouped-shape lifting
  now returns no grouped target when non-load mode or impossible shape drift
  reaches the helper, and route execution-stage dispatch is exhaustive over the
  closed route-shape enum instead of carrying a panicking catch-all. Ordered
  range planning now returns no indexed range candidate when non-range compare
  operator drift reaches the range helper. Runtime predicate compilation now
  uses a fallible internal compiler for production predicate-subset derivation,
  so admission/lowering drift returns no predicate subset instead of reaching
  panicking compare/function/membership invariants. Affine numeric compare
  flipping now keeps the original expression when non-compare operator drift
  reaches the flip helper, and scalar COUNT reducer output falls back to the
  reducer-local count if aggregate count finalization shape ever drifts.
  Grouped projection aggregate scanning now propagates its existing traversal
  error instead of trapping through a local invariant expectation. Query
  fingerprint profile hashing now emits deterministic missing-entity-path
  sentinel material when profile wiring drifts, and grouped HAVING hashing now
  emits deterministic missing-slot sentinel material for unmatched group-field
  or aggregate lookup facts instead of trapping. SQL cursor token movers now
  restore mismatched tokens and return parse errors instead of trapping, SQL
  lowered-expression analysis now uses an infallible planner-expression
  traversal instead of unwrapping a never-failing traversal result, unsupported
  global aggregate semantic kind drift returns a lowering error, and direct
  `COUNT(*)` lowering builds the known row-count terminal without a fallible
  helper round trip. SQL lexer comparison operator drift now returns an
  unexpected-character parse error, hex blob nibble drift returns the existing
  non-hex blob syntax error, compact composite primary-key decode drift returns
  a decode error, oversized compact index-store key segment encoding returns a
  typed encode error, and raw index-key materialization now returns typed key
  encode errors for segment/count/primary-key drift instead of trapping. Scalar
  predicate runtime now treats missing field-slot drift for scalar `IS NULL`,
  emptiness, and text-contains predicates as a fail-closed non-match, matching
  the generic runtime path instead of trapping.
- Recommendation: keep runtime invariant drift recoverable with typed errors or
  conservative no-result behavior. Do not add new reference-returning helper
  surfaces that assume finalized static execution metadata or admitted SQL write
  proof state, or dispatch routing that assumes earlier adapters always
  consumed a command, or predicate-compiler internals that assume admission
  stayed aligned with lowering, or reducer/rewriter helpers that assume a
  private helper always returns a specific shape, or fingerprint hashers that
  assume profile/grouped lookup facts are always present, or SQL cursor and
  aggregate lowering helpers that assume parser/lowering prechecks stayed
  aligned, or lexer/key codec helpers that assume upstream prechecks already
  proved token, segment, count, or primary-key shape, or scalar predicate
  evaluators that assume admission and slot resolution cannot drift, without a
  typed, optional, fail-closed, propagated-error, deterministic sentinel,
  parse/lowering/key error, or exhaustive closed-enum path.

## Generated Canister Endpoints Versus Session Surfaces

- Primary sources:
  - generated canister tests and live SQL canister matrix
  - `crates/icydb-core/src/db/session/`
  - generated endpoint wrappers outside `icydb-core`
- Current classification: validated evidence gate.
- Evidence: the generated SQL canister matrix passed on 2026-06-27: 76 tests,
  0 failed. The covered surface includes generated query, update, bounded
  update, DDL, numeric, aggregate, grouped aggregate, diagnostics, and
  returning/error paths.
- Recommendation: keep generated endpoint parity in validation mode. Re-run the
  matrix before release prep or before making generated-surface behavior
  claims after further runtime changes.

## Lint Suppressions

- Primary sources:
  - `crates/icydb-core/src/db/`
  - `crates/icydb/src/db/`
- Current classification: hygiene follow-up.
- Evidence: broad search finds production `#[expect(...)]` fences in query and
  executor code, plus many test-only `.expect(...)` calls that are outside the
  production no-panic rule. The production query/executor hits are mostly
  intentional Clippy shape/style fences such as `too_many_arguments`,
  `too_many_lines`, cast-conversion documentation, descriptor field names, and
  capability-fact bool carriers. The first Clippy-backed hygiene pass removed
  mechanical cast and private-field-name suppressions where explicit
  saturating conversions or clearer private names preserved behavior.
- Recommendation: keep removing stale/mechanical suppressions only in a
  dedicated hygiene pass backed by the normal Clippy matrix. Do not treat this
  as a 0.187.0 duplicate-authority blocker, and do not replace intentional
  shape/API/diagnostic fences with larger refactors unless they delete real
  duplicate authority.
