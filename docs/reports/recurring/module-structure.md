> Historical assessment, not current-code qualification. Raw output and superseded runs
> were retired from the working tree under Shared 0.30.4. Git history retains them.
> Current follow-up authority is GitHub issues; selected results below retain their
> original source, method and limitations, not a fresh measurement.

# Structure / Module / Visibility Discipline Audit - 2026-06-06

## Report Preamble

- scope: `icydb-core` structural boundaries and visibility discipline, with emphasis on schema DDL/mutation, schema reconciliation, relation metadata, and SQL execution after the 0.179.3 cleanup slice.

- compared baseline report path: `docs/audits/reports/2026-06/2026-06-05/module-structure.md`

- code snapshot identifier: `c373182f3` with dirty working tree at scan time

- method tag/version: `Method V5`

- comparability status: `comparable`; metric formulas and evidence scope match the 2026-06-05 report.

- exclusions applied: test-only files/modules excluded from structural conclusions and hub-threshold metrics.

- daily baseline rule: first run of day compares to latest prior comparable report.

## Circularity Findings

| Subsystem A | Subsystem B | Real Cycle? | Evidence | Risk |
| ---- | ---- | ---- | ---- | ---- |
| query/plan | executor | no | layer invariant check reported `Upward imports (tracked edges): 0`; executor consumes plan contracts and query/plan does not import executor contracts | Low |
| schema/mutation | SQL DDL | no | SQL DDL imports schema-owned contracts; schema mutation modules do not import SQL parser/binder modules | Low |
| schema/reconcile | SQL DDL | no | SQL DDL execution calls reconcile-owned publication helpers; reconcile modules do not parse or bind SQL syntax | Low |
| relation save validation | relation reverse index | no | both consume relation-owned metadata; reverse-index target keys are child-scoped | Low |
| commit/recovery | schema reconcile | no | reconcile coordinates schema publication/startup repair; commit/recovery storage logic remains in commit/journal modules | Low |

## Visibility Hygiene Findings

| Item | Path | Current Visibility | Narrowest Plausible Visibility | Why Narrower Seems Valid | Risk |
| ---- | ---- | ---- | ---- | ---- | ---- |
| SQL DDL report DTOs | `/home/adam/projects/icydb/crates/icydb-core/src/db/sql/ddl/report.rs:1` | `pub` | keep `pub` | public status/report vocabulary | Low-Medium |
| SQL DDL bound request types | `/home/adam/projects/icydb/crates/icydb-core/src/db/sql/ddl/mod.rs:61` | `pub(in crate::db)` | keep scoped | used by SQL session and schema-owned lowering/publication handoff | Low-Medium |
| schema mutation modules | `/home/adam/projects/icydb/crates/icydb-core/src/db/schema/mutation/mod.rs:1` | private modules with scoped re-exports | keep scoped | catalog-native mutation contracts are internal to `db` | Low-Medium |
| reconcile publication helpers | `/home/adam/projects/icydb/crates/icydb-core/src/db/schema/reconcile/sql_ddl.rs:165` | `pub(super)` | keep scoped | helper is local to reconcile publication owner | Low |
| reverse-index target keys | `/home/adam/projects/icydb/crates/icydb-core/src/db/relation/reverse_index/target_keys.rs:1` | private child module | keep private | reduces root reverse-index struct pressure without widening relation API | Low |
| `__macro` | `/home/adam/projects/icydb/crates/icydb-core/src/lib.rs:62` | `#[doc(hidden)] pub` | keep `pub` | derive output needs a stable generated-code path | Medium |

No test-only helper leakage was found in inspected surfaces. Test modules remain under `#[cfg(test)]` or test-only paths.

## Drift Sensitivity Summary

| Growth Vector | Affected Subsystems | Why Multiple Layers Would Change | Drift Risk |
| ---- | ---- | ---- | ---- |
| new SQL DDL verb | SQL parser, SQL DDL family module, schema mutation family module, schema reconcile/session execution | syntax, accepted-catalog derivation, admission, runner planning, and publication are intentionally split | Medium |
| new schema mutation runner | schema mutation execution/runner, schema reconcile, index/data physical stores | publication must stay gated by runner preflight and accepted identity | Medium |
| new relation cardinality or composite key shape | relation metadata, save validation, reverse-index prep, delete validation | relation authority is centralized, but runtime validation and reverse-index mutation consume different execution structs | Medium |
| new public SQL status/error type | SQL DDL report DTOs, error mapping, db facade | public facade exposure can widen quickly if raw internals leak into DTOs | Low-Medium |
| new execution terminal | query/plan, executor route planning, session SQL execution, explain diagnostics | route selection and runtime terminal contracts both need explicit ownership | Medium |

## Verification Readout

- `PASS`: `bash scripts/ci/check-module-structure-hub-thresholds.sh`

- `PASS`: `bash scripts/ci/check-layer-authority-invariants.sh`

- `PASS`: `bash scripts/ci/check-architecture-text-scan-invariants.sh`

- `PASS`: `cargo check -p icydb-core --features sql`

- `PASS`: `cargo check -p icydb-core --features 'sql diagnostics'`

- `PASS`: `bash scripts/audit/runtime_metrics.sh`

## Follow-Up Actions

- No mandatory module-structure follow-up actions for this run.

- Monitoring-only: keep `db::session::sql::execute` below the current execution-shell threshold; route future family-specific execution additions into child modules.

- Monitoring-only: keep `db::relation::reverse_index` below the current threshold; split only when a new relation feature creates a real child owner.

[Original report and recorded commands](https://github.com/dragginzgame/icydb/blob/5bf0845a043448b0ed07d39f294da0663a16fefe/docs/reports/recurring/2026/06/06/module-structure/01/report.md).
