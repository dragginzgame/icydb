# Structure / Module / Visibility Discipline Audit - 2026-06-06

## Report Preamble

- scope: `icydb-core` structural boundaries and visibility discipline, with emphasis on schema DDL/mutation, schema reconciliation, relation metadata, and SQL execution after the 0.179.3 cleanup slice.
- compared baseline report path: `docs/audits/reports/2026-06/2026-06-05/module-structure.md`
- code snapshot identifier: `c373182f3` with dirty working tree at scan time
- method tag/version: `Method V5`
- comparability status: `comparable`; metric formulas and evidence scope match the 2026-06-05 report.
- exclusions applied: test-only files/modules excluded from structural conclusions and hub-threshold metrics.
- daily baseline rule: first run of day compares to latest prior comparable report.

## Audit Definition Fitness Check

| Check | Status | Evidence | Method Impact |
| ---- | ---- | ---- | ---- |
| Stale named hub paths | PASS | Current paths exist for SQL DDL family modules, schema mutation family modules, `db/schema/reconcile.rs`, `db/schema/reconcile/sql_ddl.rs`, relation modules, and SQL execute modules. | none |
| Recent split coverage | PASS | Current owner boundaries include `db/schema/mutation/{ddl_admission,delta,execution,identity,...}`, `db/schema/reconcile/{sql_ddl,startup_expression,startup_field_path}`, `db/relation/reverse_index/target_keys.rs`, and `db/session/sql/execute/{diagnostics,metadata}.rs`. | none |
| CI threshold coverage | PASS | `scripts/ci/check-module-structure-hub-thresholds.sh` covers the current DDL, mutation, reconcile, relation, and SQL execute hub set. | none |
| Metric formula drift | PASS | `scripts/audit/runtime_metrics.sh` was reused unchanged. | comparable |

## Public Surface Map

| Item | Kind | Path | Publicly Reachable From Root? | Classification | Visibility Scope | Exposure Impact | Risk |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| `db`, `error`, `metrics`, `model`, `sanitize`, `traits`, `types`, `validate`, `value`, `visitor` | root modules | `/home/adam/projects/icydb/crates/icydb-core/src/lib.rs:16` | yes | intended core API | `pub mod` | broad but established core surface | Low |
| `prelude` | root module | `/home/adam/projects/icydb/crates/icydb-core/src/lib.rs:49` | yes | intended external API | `pub mod` | limited domain vocabulary | Low |
| `__macro` | generated-code support module | `/home/adam/projects/icydb/crates/icydb-core/src/lib.rs:62` | yes | macro-support item | `#[doc(hidden)] pub mod` | intentionally exposes generated-code wiring | Medium |
| `db` facade re-exports | facade API | `/home/adam/projects/icydb/crates/icydb-core/src/db/mod.rs:54` | yes | intended facade plus generated support | `pub use` / `#[doc(hidden)] pub use` | large but explicit facade surface | Medium |
| SQL DDL report DTOs | public diagnostics | `/home/adam/projects/icydb/crates/icydb-core/src/db/sql/ddl/report.rs:1` | yes via `db` | intended SQL status API | `pub struct` / `pub enum` with private implementation state where applicable | stable report/status vocabulary, not mutation internals | Low-Medium |
| SQL DDL bound requests | SQL DDL frontend internals | `/home/adam/projects/icydb/crates/icydb-core/src/db/sql/ddl/mod.rs:61` | no | internal plumbing | `pub(in crate::db)` | visible only inside `db` for session/reconcile handoff | Low-Medium |
| Schema mutation internals | catalog mutation owner | `/home/adam/projects/icydb/crates/icydb-core/src/db/schema/mutation/mod.rs:1` | no | internal plumbing | private modules with `pub(in crate::db)` re-exports | contained to schema/session execution boundaries | Low-Medium |
| Reconcile SQL DDL publication envelope | schema publication owner | `/home/adam/projects/icydb/crates/icydb-core/src/db/schema/reconcile/sql_ddl.rs:165` | no | internal plumbing | `pub(super)` | publication helper is scoped to reconcile owner | Low |
| Relation reverse-index target keys | relation runtime owner | `/home/adam/projects/icydb/crates/icydb-core/src/db/relation/reverse_index/target_keys.rs:1` | no | internal plumbing | child module under private relation owner | isolated from public facade | Low |

## Subsystem Dependency Graph

| Subsystem | Depends On | Depended On By | Lower-Layer Dependencies | Same-Layer Dependencies | Upward Dependency Found? | Direction Assessment | Risk |
| ---- | ---- | ---- | ----: | ----: | ---- | ---- | ---- |
| schema/mutation | data, index, predicate, schema snapshots, identity | SQL DDL, schema reconcile, runner paths | 5 | 2 | no | pressure: catalog-native mutation owner split into focused family modules | Low-Medium |
| schema/reconcile | schema mutation, schema store, index store, registry, identity | startup reconciliation and SQL DDL execution | 5 | 2 | no | pressure: publication/reconciliation coordinator with SQL DDL child module | Medium |
| SQL DDL binding | SQL parser DTOs, schema-owned admission, mutation helpers | SQL session execution | 3 | 2 | no | frontend binder; mutation authority remains schema-owned | Low-Medium |
| session SQL execution | query plans, executor, SQL lowering, cursor/session cache | public SQL query/update/DDL entrypoints | 5 | 4 | no | pressure: high-coordination runtime shell, still below hub threshold | Medium |
| relation | data, index, schema, identity, registry | save validation, reverse-index prep, commit/delete paths | 5 | 2 | no | relation metadata/reverse-index owner; target-key helper is now child-scoped | Medium |
| query/plan | access, predicate, schema, model | executor, session, explain | 4 | 3 | no | logical-plan hub remains scoped through internal re-exports | Medium |
| commit/recovery | data store, schema reconcile support, marker store | db root and runtime startup | 3 | 1 | no | contained storage boundary | Low-Medium |
| facade (`icydb-core::db`) | child subsystems | generated code, facade crate, callers | n/a | n/a | no | broad intentional facade with hidden macro support | Medium |

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

## Layering Violations

| Violation | Location | Dependency | Description | Directional Impact | Risk |
| ---- | ---- | ---- | ---- | ---- | ---- |
| none confirmed | n/a | n/a | `check-layer-authority-invariants.sh` passed with `0` tracked upward imports and `0` cross-layer policy re-derivations | no violation | Low |

## Structural Pressure Areas

| Area | Pressure Type | Why This Is Pressure (Not Yet Violation) | Drift Sensitivity | Risk |
| ---- | ---- | ---- | ---- | ---- |
| `db::session::sql::execute` | SQL execution shell | `854` LOC, fanout `4`, branch sites `25`; diagnostics attribution now lives in a child module | new SQL execution surfaces can widen this root unless family-owned | Low-Medium |
| `db::relation::reverse_index` | relation reverse-index runtime owner | `1041` LOC, fanout `5`, branch sites `30`; below the `1100` LOC guard after target-key split | new relation cardinalities or key shapes can grow this quickly | Medium |
| `db::schema::reconcile` | publication/reconciliation hub | `480` LOC, fanout `6`, branch sites `24`; SQL DDL publication is isolated in child modules | new DDL publication classes can concentrate here | Low-Medium |
| `db::schema::mutation` family | catalog mutation owner | root `534` LOC; admission/execution/identity/delta modules are owner-local | new mutation classes must stay in family modules | Low-Medium |
| `db::sql::ddl` family | SQL DDL frontend | root `434` LOC with field/index/report/admission children below thresholds | new DDL verbs can add parser/schema coupling if not kept family-owned | Low-Medium |

## Hub Import Pressure

| Hub Module | Unique Sibling Subsystems Imported | Cross-Layer Dependency Count | Delta vs Previous Report | LOC | Fanout | Max Branch Depth | Branch Sites | Pressure Band | Risk |
| ---- | ----: | ----: | ---- | ----: | ----: | ----: | ----: | ---- | ---- |
| `db::session::sql::execute` | 4 | 0 | LOC `-51` after moving diagnostics attribution to a child module | 854 | 4 | 2 | 25 | low-medium by size, below threshold | Low-Medium |
| `db::session::sql::execute::diagnostics` | 2 | 0 | new child module for diagnostics attribution scaffolding | 64 | 2 | 0 | 0 | low | Low |
| `db::session::sql::execute::write` | 4 | 0 | unchanged LOC | 284 | 4 | 1 | 10 | low | Low |
| `db::session::sql::execute::write::insert` | 7 | 0 | unchanged LOC | 393 | 7 | 3 | 24 | low by guard | Low-Medium |
| `db::session::sql::execute::write::update` | 6 | 0 | unchanged LOC | 176 | 6 | 1 | 4 | low | Low |
| `db::schema::reconcile` | 6 | 0 | LOC `+1`, effectively stable | 480 | 6 | 3 | 24 | low-medium | Low-Medium |
| `db::schema::reconcile::sql_ddl` | 3 | 0 | LOC `-457` vs 2026-06-05 pre-cleanup SQL DDL child measurement; now split from field metadata | 376 | 3 | 2 | 10 | low | Low-Medium |
| `db::schema::reconcile::sql_ddl::field_metadata` | 4 | 0 | new tracked child from cleanup split | 510 | 4 | 2 | 21 | low-medium | Low-Medium |
| `db::schema::mutation` | 3 | 0 | LOC `+18`, stable under threshold | 534 | 3 | 2 | 13 | low | Low-Medium |
| `db::schema::mutation::field` | 1 | 0 | unchanged LOC | 526 | 1 | 1 | 14 | low | Low-Medium |
| `db::schema::mutation::index` | 1 | 0 | LOC `+9`, stable under threshold | 641 | 1 | 2 | 16 | low | Low-Medium |
| `db::schema::mutation::runner` | 1 | 0 | LOC `+12`, stable under threshold | 554 | 1 | 1 | 8 | low | Low |
| `db::sql::ddl` | 1 | 0 | unchanged LOC | 434 | 1 | 2 | 6 | low | Low-Medium |
| `db::sql::ddl::field` | 1 | 0 | unchanged LOC | 583 | 1 | 2 | 14 | low | Low-Medium |
| `db::sql::ddl::index` | 1 | 0 | unchanged LOC | 505 | 1 | 2 | 20 | low | Low-Medium |
| `db::relation::reverse_index` | 5 | 0 | LOC `+4`, effectively stable after target-key split | 1041 | 5 | 2 | 30 | medium by size | Medium |

## Drift Sensitivity Summary

| Growth Vector | Affected Subsystems | Why Multiple Layers Would Change | Drift Risk |
| ---- | ---- | ---- | ---- |
| new SQL DDL verb | SQL parser, SQL DDL family module, schema mutation family module, schema reconcile/session execution | syntax, accepted-catalog derivation, admission, runner planning, and publication are intentionally split | Medium |
| new schema mutation runner | schema mutation execution/runner, schema reconcile, index/data physical stores | publication must stay gated by runner preflight and accepted identity | Medium |
| new relation cardinality or composite key shape | relation metadata, save validation, reverse-index prep, delete validation | relation authority is centralized, but runtime validation and reverse-index mutation consume different execution structs | Medium |
| new public SQL status/error type | SQL DDL report DTOs, error mapping, db facade | public facade exposure can widen quickly if raw internals leak into DTOs | Low-Medium |
| new execution terminal | query/plan, executor route planning, session SQL execution, explain diagnostics | route selection and runtime terminal contracts both need explicit ownership | Medium |

## Structural Risk Index

| Category | Risk Index | Basis |
| ---- | ----: | ---- |
| Public Surface Discipline | 4 | broad but intentional facade/root API; SQL DDL DTOs remain stable report/status vocabulary |
| Layer Directionality | 3 | invariant checks show no tracked upward imports or cross-layer policy re-derivations |
| Circularity Safety | 2 | no real subsystem-level cycles found |
| Visibility Hygiene | 3 | schema/reconcile, mutation, relation, and SQL DDL internals remain private or scoped |
| Facade Containment | 4 | hidden macro support remains intentionally broad but isolated |

### Overall Structural Risk Index

**3.6/10**

Moderate structural pressure, no confirmed high/critical violation. The cleanup moved diagnostics attribution out of the SQL execution root and keeps the 0.179.3 shape under the module hub thresholds; current risk is monitoring-only around future growth in `db::session::sql::execute` and `db::relation::reverse_index`.

## Verification Readout

- `PASS`: `bash scripts/ci/check-module-structure-hub-thresholds.sh`
- `PASS`: `bash scripts/ci/check-layer-authority-invariants.sh`
- `PASS`: `bash scripts/ci/check-architecture-text-scan-invariants.sh`
- `PASS`: `cargo check -p icydb-core --features sql`
- `PASS`: `cargo check -p icydb-core --features 'sql diagnostics'`
- `PASS`: `bash scripts/audit/runtime_metrics.sh`

## Post-Audit Cleanup Applied

| Area | Before | After | Result |
| ---- | ---- | ---- | ---- |
| `db::session::sql::execute` | `905` LOC, fanout `4`, branch sites `25` | root `854` LOC, fanout `4`, branch sites `25`; `db::session::sql::execute::diagnostics` `64` LOC | diagnostics attribution scaffolding is isolated from the root SQL execution shell |

## Follow-Up Actions

- No mandatory module-structure follow-up actions for this run.
- Monitoring-only: keep `db::session::sql::execute` below the current execution-shell threshold; route future family-specific execution additions into child modules.
- Monitoring-only: keep `db::relation::reverse_index` below the current threshold; split only when a new relation feature creates a real child owner.
