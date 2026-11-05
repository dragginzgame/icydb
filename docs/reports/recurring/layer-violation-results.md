> Historical assessment, not current-code qualification. Raw output and superseded runs
> were retired from the working tree under Shared 0.30.4. Git history retains them.
> Current follow-up authority is GitHub issues; selected results below retain their
> original source, method and limitations, not a fresh measurement.

# Strict Layer Violation Results Audit - 2026-03-02

Scope: `crates/icydb-core/src/db/` (non-test runtime modules), strict directional layer checks.

## Layer Direction Model

`intent -> query/plan -> access -> executor -> index/storage -> codec`

Rule: lower layers must not depend upward.

## Checks

| Check | Result | Evidence | Risk |
| ---- | ---- | ---- | ---- |
| A. Upward imports (`query/* -> executor/*`) | Pass | non-comment matches: `0` (`rg -P '^(?!\s*//).*(crate::db::executor|db::executor)' crates/icydb-core/src/db/query`) | Low |
| B. Logical validation ownership leaks | Pass | planner-owned semantics remain in `query/plan/validate.rs`; executor keeps defensive checks in `executor/plan_validate.rs` | Low |
| C. Physical feasibility ownership leaks | Pass | route feasibility and execution-mode decisions remain in `executor/route/*` | Low |
| D. Executor runtime logic under `query/` | Pass | non-comment `ExecutionKernel|ExecutionPreparation|LoadExecutor` in `query/*`: `0` | Low |
| E. Access canonicalization outside `access/` | Pass | implementation remains in `access/canonical.rs`; external modules call boundary functions only | Low |

## Findings

No strict layer violations detected.

## Drift Notes

- Query/planner semantics and executor runtime responsibilities remain structurally separated.
- Index/data/commit runtime modules still show no non-comment direct dependency on query internals.
- Canonicalization ownership remains with `db::access`.

## Overall Status

**PASS** (no upward dependency or ownership-boundary violations found in audited scope).

## Rerun Addendum - 2026-03-02 (post continuation + load entrypoint unification)

Rerun checks:

| Check | Result | Evidence |
| ---- | ---- | ---- |
| Query upward dependency to executor | Pass | non-comment `query/* -> executor/*` matches: **0** |
| Index/data/commit upward dependency to query | Pass | non-comment `index|data|commit/* -> query/*` matches: **0** |
| Query runtime symbol leakage | Pass | non-test query matches for `ExecutionKernel|ExecutionPreparation|LoadExecutor`: **0** |
| Executor continuation boundary drift | Pass | runtime token constructors outside `executor/continuation/mod.rs`: **0** |
| Executor cursor-boundary derivation drift | Pass | runtime `cursor_boundary_from_entity` / `CursorBoundary::from_ordered_entity` outside cursor protocol: **0** |

Rerun status remains **PASS**.

[Original report and recorded commands](https://github.com/dragginzgame/icydb/blob/5bf0845a043448b0ed07d39f294da0663a16fefe/docs/reports/recurring/2026/03/02/layer-violation-results/01/report.md).
