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

## Generated Canister Endpoints Versus Session Surfaces

- Primary sources:
  - generated canister tests and live SQL canister matrix
  - `crates/icydb-core/src/db/session/`
  - generated endpoint wrappers outside `icydb-core`
- Current classification: needs fresh audit input.
- Evidence: the 0.187 reminder requires generated canister matrix results
  before making endpoint-surface conclusions.
- Recommendation: do not classify generated/session divergence until the live
  matrix is rerun or a source-only finding is isolated.

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
  capability-fact bool carriers.
- Recommendation: remove stale suppressions only in a dedicated hygiene pass
  backed by the normal Clippy matrix. Do not treat this as a 0.187.0
  duplicate-authority blocker.
