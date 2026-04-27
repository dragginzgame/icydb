# Flow Convergence Audit

Date: 2026-04-27

Recurring definition:
[/home/adam/projects/icydb/docs/audits/recurring/crosscutting/crosscutting-flow-convergence.md](/home/adam/projects/icydb/docs/audits/recurring/crosscutting/crosscutting-flow-convergence.md)

Evidence artifacts:
[/home/adam/projects/icydb/docs/audits/reports/2026-04/2026-04-27/artifacts/flow-convergence](/home/adam/projects/icydb/docs/audits/reports/2026-04/2026-04-27/artifacts/flow-convergence)

## Summary

Overall convergence risk score: 4/10.

SQL and Fluent convergence is acceptable. The highest-risk items from the prior
run have been mechanically resolved: fluent terminal request construction moved
onto prepared strategies, SQL grouped execution shares one core, SQL global
aggregate terminal construction moved to the lowered strategy, grouped cursor
boundary construction is shared, generic grouped borrowed/owned ingest branches
are hoisted out of the row loop, and grouped-key equality now lives under
`grouped_fold::utils::equality`.

Top remaining risks:
- `OwnershipBlur`: grouped generic page finalization remains a large standalone
  module outside the `generic/` subtree.
- `OwnershipBlur`: `grouped_fold::mod.rs` still owns global-DISTINCT grouped
  execution mechanics in addition to orchestration.
- `ConversionChurn`: the borrowed compiled-SQL compatibility path still clones
  global aggregate command payloads; the normal SQL query/update path now uses
  owned compiled execution and avoids that second clone.

## Query Flow Map

SQL path:
SQL text enters the session SQL cache/compile boundary, lowers through
`db::sql::lowering`, and executes as scalar/grouped structural query, SQL global
aggregate, delete, update, explain, or metadata statement. Query/update public
surfaces now consume the owned compiled artifact returned by compile/cache
lookup.

Fluent path:
Typed query builders produce `Query<E>` values, cache through
`DbSession::cached_prepared_query_plan_for_entity`, and execute through the same
prepared scalar/grouped executor boundaries used by adjacent structural paths.
Prepared fluent terminal strategies now own executor request construction;
`session/query.rs` owns DTO shaping and error mapping.

Prepared path:
Prepared execution centers on `PreparedExecutionPlan` and
`SharedPreparedExecutionPlan`. SQL and typed surfaces share plan-cache storage.
The borrowed compiled-command API remains for test and diagnostics callers that
need to execute the same cached artifact by reference.

Executor path:
Scalar execution converges through `executor::pipeline::entrypoints::scalar`.
Grouped execution converges through `executor::pipeline::entrypoints::grouped`
and `executor::aggregate::runtime::grouped_fold`. The grouped fold split now
has explicit owners for dispatch, metrics, hashing, equality, boundary helpers,
dedicated count state/ingest/window/finalize, and generic ingest.

Projection/finalization path:
Executor projection owns runtime row shaping. Session response finalizers encode
cursor bytes and attach traces. SQL projection payload construction remains
session-owned because SQL owns labels, fixed scales, and statement envelope
formatting.

## Resolved Findings

### FC-1: Fluent terminal adapter ladders

Status: resolved.

Evidence:
- `rg "match .*strategy" crates/icydb-core/src/db/session/query.rs` has no hits.
- Prepared fluent strategy methods expose `into_executor_request(...)`.
- `session/query.rs` now calls those request constructors and keeps output DTO
  shaping local.

Residual risk:
`session/query.rs` is still large at 1,069 lines, but the specific repeated
strategy-to-executor mapping ladders are gone.

### FC-2: SQL grouped normal/diagnostics dual envelope

Status: resolved.

Evidence:
- `execute_grouped_sql_core(...)` is the shared SQL grouped execution core.
- `rg "execute_grouped_sql_statement_from_prepared_plan_with" crates/icydb-core/src`
  has no hits.

Residual risk:
The core still clones the prepared logical plan at the grouped SQL handoff.
That is unchanged plan ownership behavior, not a duplicate execution envelope.

### FC-3: SQL global aggregate strategy-to-terminal mapping

Status: resolved for normal execution.

Evidence:
- `PreparedSqlScalarAggregateStrategy::into_executor_terminal(self)` constructs
  executor terminals.
- `SqlGlobalAggregateCommandCore::into_execution_parts(self)` moves query,
  strategies, projection, and HAVING into execution.
- `rg "clone\\(|cloned\\(" crates/icydb-core/src/db/session/sql/execute/global_aggregate.rs`
  has no hits.

Residual risk:
The borrowed compiled-command compatibility path still executes global
aggregates by cloning the boxed command because it cannot consume
`&CompiledSqlCommand`. Normal SQL query/update surfaces use owned compiled
execution and avoid that second clone.

### FC-4: Grouped next-cursor boundary duplication

Status: resolved.

Evidence:
- `grouped_next_cursor_boundary(...)` lives in
  `grouped_fold::utils::boundary`.
- Count and generic grouped finalizers both use the shared helper while keeping
  their selection logic separate.

### HP-1: Generic grouped borrowed/owned branch in row loop

Status: resolved.

Evidence:
- `borrowed_group_probe_supported` appears only as runner state,
  initialization, and one pre-loop branch.
- Borrowed and owned generic grouped loops are separate functions.
- `rg "dyn |Box<" crates/icydb-core/src/db/executor/aggregate/runtime/grouped_fold`
  has no hits.

### GF-1: Grouped-key equality duplication

Status: resolved.

Evidence:
- `group_key_matches_row_view(...)` and the canonical row-view comparison helper
  are defined only in `utils/equality.rs`.
- Generic `bundle.rs` uses the shared equality and bucket-index scan helper.
- COUNT keeps its metrics wrapper while delegating pure comparison and index
  scanning through `utils/equality.rs`.

## Current Findings

### CF-1: Generic grouped page finalization remains outside `generic/`

Classification: `OwnershipBlur`

Risk: 4/10

Files:
- [/home/adam/projects/icydb/crates/icydb-core/src/db/executor/aggregate/runtime/grouped_fold/page_finalize.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/executor/aggregate/runtime/grouped_fold/page_finalize.rs)
- [/home/adam/projects/icydb/crates/icydb-core/src/db/executor/aggregate/runtime/grouped_fold/generic/runner.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/executor/aggregate/runtime/grouped_fold/generic/runner.rs)

Issue:
`page_finalize.rs` is 744 lines and owns generic grouped candidate ranking,
top-k ordering, HAVING/window checks, and row shaping. It is conceptually part
of generic grouped finalization but still sits as a sibling of `generic/`.

Recommendation:
Move this file under `generic/` as `generic/finalize.rs` or
`generic/page_finalize.rs` without changing logic. Keep count finalization
separate.

### CF-2: `grouped_fold::mod.rs` still owns global-DISTINCT execution mechanics

Classification: `OwnershipBlur`

Risk: 3/10

Files:
- [/home/adam/projects/icydb/crates/icydb-core/src/db/executor/aggregate/runtime/grouped_fold/mod.rs:139](/home/adam/projects/icydb/crates/icydb-core/src/db/executor/aggregate/runtime/grouped_fold/mod.rs:139)

Issue:
`mod.rs` is below the requested size target at 187 lines, but it still contains
the full global-DISTINCT grouped execution helper. The module root is not purely
orchestration.

Recommendation:
Extract global-DISTINCT grouped execution into a small sibling module such as
`global_distinct.rs`, leaving `mod.rs` with stream construction and dispatch.

### CF-3: COUNT root still owns row-view loop helper

Classification: `OwnershipBlur`

Risk: 3/10

Files:
- [/home/adam/projects/icydb/crates/icydb-core/src/db/executor/aggregate/runtime/grouped_fold/count/mod.rs:36](/home/adam/projects/icydb/crates/icydb-core/src/db/executor/aggregate/runtime/grouped_fold/count/mod.rs:36)
- [/home/adam/projects/icydb/crates/icydb-core/src/db/executor/aggregate/runtime/grouped_fold/count/mod.rs:127](/home/adam/projects/icydb/crates/icydb-core/src/db/executor/aggregate/runtime/grouped_fold/count/mod.rs:127)

Issue:
`count/mod.rs` still owns the COUNT execution entrypoint and one row-view loop
helper. The state, ingest, finalize, and window modules are split, but the
count root still carries meaningful execution mechanics.

Recommendation:
If continuing the structural split, move the row-view fold helper into
`count/ingest.rs` or a dedicated `count/runner.rs`. Do not alter the direct
single-field path or the borrowed/owned row-view selection.

### CF-4: Borrowed compiled SQL compatibility path still clones global aggregates

Classification: `ConversionChurn`

Risk: 2/10

Files:
- [/home/adam/projects/icydb/crates/icydb-core/src/db/session/sql/execute/mod.rs:542](/home/adam/projects/icydb/crates/icydb-core/src/db/session/sql/execute/mod.rs:542)

Issue:
`execute_compiled_sql_with_cache_attribution(&CompiledSqlCommand)` cannot
consume the cached command, so it still clones the boxed global aggregate
command before executing. The normal SQL query/update path now calls
`execute_compiled_sql_owned(...)`, so this is limited to borrowed
compatibility, tests, and diagnostics.

Recommendation:
Leave this unless borrowed compiled execution becomes a production hot path.
Removing it would require either changing the borrowed API contract or adding a
borrowed structural aggregate request path.

### CF-5: Executor explain convergence is improving but still broad

Classification: `OwnershipBlur`

Risk: 3/10

Files:
- [/home/adam/projects/icydb/crates/icydb-core/src/db/executor/explain/mod.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/executor/explain/mod.rs)
- [/home/adam/projects/icydb/crates/icydb-core/src/db/session/tests/explain_cache_convergence.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/session/tests/explain_cache_convergence.rs)

Issue:
The current working tree includes new executor explain convergence work. This
appears to centralize execution descriptor assembly and adds cache-convergence
tests, but `executor/explain/mod.rs` is now 599 lines and owns multiple
descriptor/rendering-adjacent responsibilities.

Recommendation:
After the current convergence slice settles, audit explain ownership separately:
descriptor assembly should remain executor-owned, while rendering and
session-visible cache reuse assertions should stay outside executor internals.

## Shim and Compatibility Residue

### SR-1: Scalar and grouped response finalizers are similar but legitimate

Classification: `LegitimateSeparation`

Risk: 2/10

Files:
- [/home/adam/projects/icydb/crates/icydb-core/src/db/session/response/scalar.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/session/response/scalar.rs)
- [/home/adam/projects/icydb/crates/icydb-core/src/db/session/response/grouped.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/session/response/grouped.rs)

Reason:
Both encode cursor payloads and attach traces, but they validate different
cursor families and produce different public response DTOs. A generic helper
would obscure the family-specific invariant.

### SR-2: Dedicated grouped `COUNT(*)` path vs generic grouped reducer

Classification: `LegitimateSeparation`

Risk: 2/10

Reason:
Grouped count uses a single count map and specialized windowing. The generic
path owns aggregate-state bundles. They should stay separate for performance.
Only shared primitives such as hashing, equality, and cursor-boundary shaping
should converge.

### SR-3: SQL and Fluent public APIs

Classification: `LegitimateSeparation`

Risk: 1/10

Reason:
The audit target is internal convergence. SQL text compilation and typed fluent
builders should remain separate public construction APIs while converging at
prepared plan and executor boundaries.

## Recommended Patch Plan

1. Mechanical cleanup:
   - Move generic grouped page finalization under `grouped_fold/generic/`.
   - Move global-DISTINCT grouped execution out of `grouped_fold::mod.rs`.
   - Optionally move COUNT row-view loop helper out of `count/mod.rs`.

2. Containment checks:
   - Keep COUNT and generic grouped selection separate.
   - Keep COUNT metrics wrappers local when they measure COUNT-specific
     behavior.
   - Keep borrowed compiled SQL compatibility clone unless the API is changed
     intentionally.

3. Follow-up audit:
   - Audit the new executor explain convergence slice after it stabilizes.

## Validation Plan

Targeted tests:
- `cargo test -p icydb-core grouped -- --nocapture`
- `cargo test -p icydb-core sql -- --nocapture`
- `cargo test -p icydb-core execution_convergence -- --nocapture`

Invariant and lint checks:
- `cargo fmt --all`
- `cargo check -p icydb-core --all-targets`
- `cargo clippy -p icydb-core --all-targets -- -D warnings`
- `make check-invariants`
- `git diff --check`

Grep checks:
- `rg "match .*strategy" crates/icydb-core/src/db/session/query.rs`
- `rg "execute_grouped_sql_statement_from_prepared_plan_with" crates/icydb-core/src`
- `rg "clone\\(|cloned\\(" crates/icydb-core/src/db/session/sql/execute/global_aggregate.rs`
- `rg "borrowed_group_probe_supported" crates/icydb-core/src/db/executor/aggregate/runtime/grouped_fold`
- `rg "fn group_key_matches_row_view|fn canonical_group_value_matches_row_view" crates/icydb-core/src/db/executor/aggregate/runtime/grouped_fold`
- `rg "dyn |Box<" crates/icydb-core/src/db/executor/aggregate/runtime/grouped_fold`

## Evidence Summary

Captured artifacts:
- `sql-signals.txt`: SQL signal scan
- `fluent-signals.txt`: fluent signal scan
- `prepared-signals.txt`: prepared-plan signal scan
- `shim-signals.txt`: compatibility, fallback, adapter, and wrapper signal scan
- `conversion-signals.txt`: clone/to_vec/to_string signal scan
- `executor-flow-signals.txt`: executor stage, finalize, projection, cursor, and continuation scan
- `module-sizes-all.txt`: full db Rust module size scan
- `module-sizes-top40.txt`: largest db Rust modules
- `post-fix-grep-checks.txt`: targeted convergence grep checks
- `evidence-line-counts.txt`: artifact line counts

Current targeted grep evidence:
- No fluent strategy match ladder remains in `session/query.rs`.
- No old grouped SQL `*_with` helper remains.
- No `clone()` or `cloned()` remains in SQL global aggregate session execution.
- No dynamic dispatch or `Box<...>` appears in `grouped_fold`.
- Grouped row-view equality definitions are centralized in `utils/equality.rs`.
