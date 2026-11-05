> Historical assessment, not current-code qualification. Raw output and superseded runs
> were retired from the working tree under Shared 0.30.4. Git history retains them.
> Current follow-up authority is GitHub issues; selected results below retain their
> original source, method and limitations, not a fresh measurement.

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

### FC-4: Boxed global aggregate command clone did not reappear

Status: resolved for the April concern.

Evidence:

* Targeted grep found no `command.clone()`, `(*command).clone()`, or `clone(*command)` pattern in SQL global aggregate execution.

Residual note:
`global_aggregate.rs` still clones strategies, projection, HAVING, authorities, and shared prepared-plan handles where execution requests/cache entries need owned values. That is low-risk conversion pressure, not the old boxed command clone.

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

[Original report and recorded commands](https://github.com/dragginzgame/icydb/blob/5bf0845a043448b0ed07d39f294da0663a16fefe/docs/reports/recurring/2026/06/25/flow-convergence/01/report.md).
