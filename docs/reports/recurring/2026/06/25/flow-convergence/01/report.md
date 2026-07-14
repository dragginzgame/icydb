# Flow Convergence Audit

Scope: `docs/audits/recurring/crosscutting/crosscutting-flow-convergence.md`
Compared baseline report path: `docs/audits/reports/2026-04/2026-04-27/flow-convergence.md`
Code snapshot identifier: `d389eec3b`
Method tag/version: `Flow Convergence Method V2`
Comparability status: `non-comparable` - Method V2 adds audit identity, current split session-query and grouped-fold roots, shared plan-cache checks, executor-explain checks, normalized verification statuses, and read-only run discipline.
Auditor: `Codex`
Run timestamp: `2026-06-25T10:20:50+02:00`
Branch / worktree state: dirty worktree; read-only audit run after updating only the audit definition and this report.

## Method Changes

Method V2 updates the recurring definition before this run:

* declares the report scope as `flow-convergence`
* adds current hotspot roots for `session/query`, SQL execution, executor pipeline entrypoints, grouped fold, executor explain, and query planning
* replaces broad historical signal scans with targeted current convergence evidence
* separates read-only baseline verification from post-patch validation
* adds normalized verification statuses and read-only run constraints

Because the verification method changed, deltas against the April Method V1 report are treated as `N/A (method change)`.

## Summary

Overall convergence risk score: `4 / 10`.

SQL and Fluent convergence is acceptable. The current read-only evidence shows shared prepared execution, shared cache identity, and explain/trace route convergence holding under the targeted test filters.

Top remaining risks:

* `OwnershipBlur`: generic grouped page finalization remains a 757-line sibling of `grouped_fold/generic/`.
* `OwnershipBlur`: executor explain has split descriptor modules, but `executor/explain/mod.rs` remains broad at 606 lines.
* `OwnershipBlur`: grouped COUNT still keeps the row-view fold helper in `count/mod.rs`, although the module is now smaller and count-owned.

## Findings Table

| Check | Evidence | Status | Risk |
| ----- | -------- | ------ | ---- |
| Audit definition freshness | Updated Method V2 in `docs/audits/recurring/crosscutting/crosscutting-flow-convergence.md` before this run. | PASS | Low |
| SQL/Fluent scalar and grouped convergence | `execution_convergence` filter passed 13 tests covering scalar, grouped, distinct, delete, cursor, trace, and explain-route convergence. | PASS | Low |
| Explain/cache convergence | `explain_cache_convergence` filter passed 2 tests. | PASS | Low |
| Shared SQL/Fluent plan-cache reuse | `shared_query_plan_cache_is_reused_by_fluent_and_sql_select_surfaces` passed. | PASS | Low |
| Invariant suite | `make check-invariants` passed dependency graph, index/range, layer authority, mutation atomicity, SQL branch ownership, and memory-id checks. | PASS | Low |
| Old fluent strategy ladder | `rg "match .*strategy" crates/icydb-core/src/db/session/query` had no hits. | PASS | Low |
| Old grouped SQL prepared helper | `rg "execute_grouped_sql_statement_from_prepared_plan_with" crates/icydb-core/src` had no hits. | PASS | Low |
| Old boxed global-aggregate command clone | Targeted grep for `command.clone()` / `(*command).clone()` in SQL global aggregate execution had no hits. | PASS | Low |
| Generic grouped page finalization placement | `grouped_fold/page_finalize.rs` remains 757 lines and outside `grouped_fold/generic/`. | PARTIAL | Medium |
| Executor explain ownership width | `executor/explain/mod.rs` remains 606 lines despite descriptor submodules. | PARTIAL | Medium-Low |
| Grouped COUNT root helper | `fold_row_view_count_rows` remains in `grouped_fold/count/mod.rs`; count root is 172 lines and still count-owned. | PARTIAL | Low-Medium |

## Query Flow Map

SQL path:
SQL text lowers through `db::sql::lowering`, executes through `session/sql/execute`, and shares prepared query-plan cache routes with fluent query execution for SELECT and global aggregate paths.

Fluent path:
Typed query builders resolve through `session/query/cache.rs`, then execute through `session/query/execution.rs`. `execute_query_result`, `execute_delete_count`, and `execute_prepared` form the private scalar/grouped/delete prepared-plan boundary.

Prepared path:
`PreparedExecutionPlan` and shared prepared plans remain the convergence point for SQL and Fluent surfaces. The shared plan cache is used from both entity and accepted-authority routes.

Executor path:
Scalar execution converges through executor pipeline entrypoints. Grouped execution enters `execute_group_fold_stage`, then dispatches to `distinct`, `count`, or `generic` grouped fold owners based on the route-owned grouped execution mode.

Projection/finalization path:
Grouped count and generic grouped finalizers share grouped cursor-boundary helpers. Generic grouped page finalization still sits in `page_finalize.rs` rather than under the `generic/` subtree.

## Resolved Since April Report

### FC-1: Session query root no longer owns the terminal adapter ladder

Status: resolved.

Evidence:

* `session/query/mod.rs` is 36 lines and acts as a module hub.
* `session/query/execution.rs` owns the prepared scalar/grouped/delete dispatch boundary.
* `rg "match .*strategy" crates/icydb-core/src/db/session/query` had no hits.

### FC-2: Global-DISTINCT grouped fold moved out of `grouped_fold::mod.rs`

Status: resolved.

Evidence:

* `grouped_fold/distinct.rs` owns `execute_global_distinct_grouped_fold_stage`.
* `grouped_fold/mod.rs` is 135 lines and dispatches to distinct/count/generic fold owners.

### FC-3: Old grouped SQL prepared helper remains deleted

Status: resolved.

Evidence:

* `rg "execute_grouped_sql_statement_from_prepared_plan_with" crates/icydb-core/src` had no hits.

### FC-4: Boxed global aggregate command clone did not reappear

Status: resolved for the April concern.

Evidence:

* Targeted grep found no `command.clone()`, `(*command).clone()`, or `clone(*command)` pattern in SQL global aggregate execution.

Residual note:
`global_aggregate.rs` still clones strategies, projection, HAVING, authorities, and shared prepared-plan handles where execution requests/cache entries need owned values. That is low-risk conversion pressure, not the old boxed command clone.

## Current Findings

### CF-1: Generic grouped page finalization remains outside `generic/`

Classification: `OwnershipBlur`

Risk: `4 / 10`

Files:

* `/home/adam/projects/icydb/crates/icydb-core/src/db/executor/aggregate/runtime/grouped_fold/page_finalize.rs`
* `/home/adam/projects/icydb/crates/icydb-core/src/db/executor/aggregate/runtime/grouped_fold/generic/runner.rs`

Issue:
`page_finalize.rs` is 757 lines and owns generic grouped candidate ranking, top-k ordering, HAVING/window checks, projection, and next-cursor construction. It is conceptually generic grouped finalization but still sits beside `generic/`.

Recommendation:
Move it mechanically under `grouped_fold/generic/` as `page_finalize.rs` or `finalize.rs` without changing logic. Keep dedicated COUNT finalization separate.

### CF-2: Executor explain root remains broad

Classification: `OwnershipBlur`

Risk: `3 / 10`

Files:

* `/home/adam/projects/icydb/crates/icydb-core/src/db/executor/explain/mod.rs`
* `/home/adam/projects/icydb/crates/icydb-core/src/db/executor/explain/descriptor/`

Issue:
Descriptor assembly has moved into descriptor submodules, but `executor/explain/mod.rs` remains 606 lines and still owns multiple prepared terminal explain adapters plus descriptor orchestration.

Recommendation:
Keep descriptor assembly executor-owned, but split terminal explain adapters by terminal family if this module grows further.

### CF-3: Grouped COUNT root still owns row-view fold helper

Classification: `OwnershipBlur`

Risk: `3 / 10`

Files:

* `/home/adam/projects/icydb/crates/icydb-core/src/db/executor/aggregate/runtime/grouped_fold/count/mod.rs`
* `/home/adam/projects/icydb/crates/icydb-core/src/db/executor/aggregate/runtime/grouped_fold/count/ingest.rs`

Issue:
`count/mod.rs` is down to 172 lines, but `fold_row_view_count_rows` remains in the module root. The helper is COUNT-specific, so this is not duplicate flow; it is a remaining ownership split issue.

Recommendation:
Move the row-view fold helper to `count/ingest.rs` or a small `count/runner.rs` if count splitting continues.

### CF-4: SQL global aggregate conversion pressure remains bounded

Classification: `ConversionChurn`

Risk: `2 / 10`

Files:

* `/home/adam/projects/icydb/crates/icydb-core/src/db/session/sql/execute/global_aggregate.rs`
* `/home/adam/projects/icydb/crates/icydb-core/src/db/session/sql/execute/mod.rs`

Issue:
SQL global aggregate execution still clones owned request/cache payloads such as strategies, projection, HAVING, authorities, and shared prepared-plan handles. The old boxed command clone did not reappear.

Recommendation:
Leave this alone unless profiling shows a production hot-path cost; avoiding these clones would require a wider borrowed aggregate request contract.

## Legitimate Separations

SQL and Fluent public construction APIs should remain separate; the convergence target is prepared-plan and executor internals, not public API collapse.

Dedicated grouped `COUNT(*)` and generic grouped reducers should remain separate for performance. They should share equality, hashing, and cursor-boundary primitives only where the semantics are identical.

Scalar and grouped response finalizers remain similar but legitimate because they validate different cursor families and emit different public response DTOs.

## Recommended Patch Plan

1. Move generic grouped page finalization under `grouped_fold/generic/` as a mechanical ownership cleanup.
2. Move `fold_row_view_count_rows` out of `count/mod.rs` if count ownership work continues.
3. Split `executor/explain/mod.rs` only when the next explain slice needs it; do not refactor it opportunistically.
4. Leave SQL global aggregate request clones unless a measured hot-path cost appears.

## Verification Readout

* `make check-invariants` -> PASS
* `cargo test -p icydb-core --features sql execution_convergence -- --nocapture` -> PASS
* `cargo test -p icydb-core --features sql explain_cache_convergence -- --nocapture` -> PASS
* `cargo test -p icydb-core --features sql shared_query_plan_cache_is_reused_by_fluent_and_sql_select_surfaces -- --nocapture` -> PASS
* `git diff --check` -> PASS

The `execution_convergence` build emitted an existing dead-code warning for `PreparedExecutionPlan::plan_hash_hex`; tests still passed.

## Follow-Up Actions

* Owner boundary: `executor::aggregate::runtime::grouped_fold`
  Action: move generic page finalization under `generic/` without changing behavior.
  Target report run: next `flow-convergence` or grouped-executor audit.
* Owner boundary: `executor::aggregate::runtime::grouped_fold::count`
  Action: move the row-view count fold helper out of `count/mod.rs` if continuing count split work.
  Target report run: next `flow-convergence` run.
* Owner boundary: `executor::explain`
  Action: split terminal-family explain adapters only when future explain changes require touching the broad root.
  Target report run: next explain convergence or `flow-convergence` run.
