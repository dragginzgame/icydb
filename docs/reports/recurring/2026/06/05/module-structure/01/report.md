# Structure / Module / Visibility Discipline Audit - 2026-06-05

## 0. Run Metadata + Comparability Note

- target scope: `icydb-core` structural boundaries and visibility discipline,
  with emphasis on schema DDL/mutation, relation metadata, and SQL execution
  after the 0.178 and 0.179 cleanup work.
- compared baseline report path:
  `docs/audits/reports/2026-05/2026-05-30/module-structure-2.md`
- code snapshot identifier: `fc4dc729e` plus local audit-definition,
  hub-threshold, and report artifacts for this run.
- method tag/version: `Method V5`
- comparability status: comparable. Method V5 adds an audit-definition fitness
  preflight and expands the required hub evidence set; metric formulas,
  runtime-metrics script, scoring interpretation, and exclusions are unchanged.
- exclusions applied: test-only files/modules excluded from metrics and
  structural invariant scripts.
- notable methodology changes vs baseline: recurring module-structure audit now
  requires a stale-path/current-owner-boundary preflight before evidence
  collection.
- daily baseline rule: first run of day compares to latest prior comparable
  report.

## 0A. Audit Definition Fitness Check

| Check | Status | Evidence | Method Impact |
| ---- | ---- | ---- | ---- |
| Stale named hub paths | PASS | Current paths exist for SQL DDL family modules, schema mutation family modules, `db/schema/reconcile.rs`, relation modules, and SQL execute modules. | evidence-scope only |
| Recent split coverage | PASS | The audit definition now names `db/schema/mutation/{ddl_admission,delta,execution,identity,...}`, `db/relation/{mod,save_validate,reverse_index}.rs`, and `db/session/sql/execute/*`. | evidence-scope only |
| CI threshold coverage | PASS | `scripts/ci/check-module-structure-hub-thresholds.sh` now covers `db::session::sql::execute`, `db::schema::reconcile`, and relation hubs in addition to the prior SQL DDL/mutation/write family hubs. | evidence-scope only |
| Metric formula drift | PASS | `scripts/audit/runtime_metrics.sh` unchanged; Method V5 did not alter LOC, fanout, branch-depth, or branch-site computation. | comparable |

## 1. Public Surface Map

| Item | Kind | Path | Publicly Reachable From Root? | Classification | Visibility Scope | Exposure Impact | Risk |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| `db`, `error`, `metrics`, `model`, `sanitize`, `traits`, `types`, `validate`, `value`, `visitor` | root modules | `/home/adam/projects/icydb/crates/icydb-core/src/lib.rs:16` | yes | intended core API | `pub mod` | broad but established core surface | Low |
| `prelude` | root module | `/home/adam/projects/icydb/crates/icydb-core/src/lib.rs:49` | yes | intended external API | `pub mod` | limited domain vocabulary only | Low |
| `__macro` | generated-code support module | `/home/adam/projects/icydb/crates/icydb-core/src/lib.rs:62` | yes | macro-support item | `#[doc(hidden)] pub mod` | intentionally exposes generated-code wiring | Medium |
| `db` facade re-exports | facade API | `/home/adam/projects/icydb/crates/icydb-core/src/db/mod.rs:54` | yes | intended facade plus generated support | `pub use` / `#[doc(hidden)] pub use` | large root surface but still routed through explicit facade | Medium |
| SQL DDL DTOs | public diagnostics | `/home/adam/projects/icydb/crates/icydb-core/src/db/sql/ddl/report.rs:11` | yes via `db` | intended external SQL status API | `pub struct` / `pub enum` with private fields where applicable | stable developer-facing report/status, not raw mutation internals | Low-Medium |
| Relation tuple descriptor | relation-internal metadata | `/home/adam/projects/icydb/crates/icydb-core/src/db/relation/mod.rs:109` | no | internal plumbing | private | contained to relation owner; not facade reachable | Low |
| Schema mutation internals | catalog mutation owner | `/home/adam/projects/icydb/crates/icydb-core/src/db/schema/mutation/mod.rs:1` | no | internal plumbing | `pub(in crate::db)` / private child modules | contained to `db`, not root-reachable | Low |

## 2. Subsystem Dependency Graph

| Subsystem | Depends On | Depended On By | Lower-Layer Dependencies | Same-Layer Dependencies | Upward Dependency Found? | Direction Assessment | Risk |
| ---- | ---- | ---- | ----: | ----: | ---- | ---- | ---- |
| schema/mutation | data, index, predicate, schema snapshots, identity | SQL DDL, schema reconcile, runner paths | 5 | 2 | no | pressure: catalog-native mutation owner split into focused family modules | Low-Medium |
| schema/reconcile | schema mutation, schema store, index store, registry, identity | startup reconciliation and SQL DDL execution | 5 | 2 | no | pressure: publication/reconciliation coordinator; no SQL-owned schema store writes | Medium |
| SQL DDL binding | SQL parser DTOs, schema-owned admission, mutation helpers | SQL session execution | 3 | 2 | no | pressure: frontend binder, not mutation authority | Low-Medium |
| session SQL execution | query plans, executor, SQL lowering, cursor/session cache | public SQL query/update/DDL entrypoints | 5 | 4 | no | pressure: high-coordination runtime shell | Medium |
| relation | data, index, schema, identity, registry | save validation, reverse-index prep, commit/delete paths | 5 | 2 | no | pressure: relation metadata/reverse-index owner; no query/planner policy leak | Medium |
| query/plan | access, predicate, schema, model | executor, session, explain | 4 | 3 | no | pressure: logical-plan hub remains scoped via `pub(in crate::db)` re-exports | Medium |
| commit/recovery | data store, schema reconcile support, marker store | db root and runtime startup | 3 | 1 | no | contained storage boundary | Low-Medium |
| facade (`icydb-core::db`) | child subsystems | generated code, facade crate, callers | n/a | n/a | no | broad intentional facade with hidden macro support | Medium |

## 3. Circularity Findings

| Subsystem A | Subsystem B | Real Cycle? | Evidence | Risk |
| ---- | ---- | ---- | ---- | ---- |
| query/plan | executor | no | layer invariant check reported `Upward imports (tracked edges): 0`; executor consumes plan contracts while query/plan does not import executor contracts | Low |
| schema/mutation | SQL DDL | no | SQL DDL imports schema-owned contracts; schema mutation family modules do not import SQL parser/binder modules | Low |
| relation save validation | relation reverse index | no | both consume relation-owned tuple/target metadata from `db/relation/mod.rs`; neither owns the other's execution pathway | Low |
| commit/recovery | schema reconcile | no | reconcile coordinates accepted-schema publication/startup repair, while commit/recovery storage logic remains in `db/commit`, `db/journal`, and related storage modules | Low |

## 4. Visibility Hygiene Findings

| Item | Path | Current Visibility | Narrowest Plausible Visibility | Why Narrower Seems Valid | Risk |
| ---- | ---- | ---- | ---- | ---- | ---- |
| `AcceptedRelationTupleEdgeLocalComponent` | `/home/adam/projects/icydb/crates/icydb-core/src/db/relation/mod.rs:109` | private | keep private | shared only by relation child modules through parent-private access | Low |
| `AcceptedRelationTupleEdgeDescriptor` | `/home/adam/projects/icydb/crates/icydb-core/src/db/relation/mod.rs:121` | private | keep private | validates tuple relation facts without widening relation API | Low |
| SQL DDL report DTOs | `/home/adam/projects/icydb/crates/icydb-core/src/db/sql/ddl/report.rs:11` | `pub` | keep `pub` | public status/report vocabulary, not raw mutation internals | Low-Medium |
| `__macro` | `/home/adam/projects/icydb/crates/icydb-core/src/lib.rs:62` | `#[doc(hidden)] pub` | keep `pub` | derive output needs stable generated-code path | Medium |
| schema mutation modules | `/home/adam/projects/icydb/crates/icydb-core/src/db/schema/mutation/mod.rs:1` | private modules with scoped re-exports | keep scoped | SQL DDL/session need catalog-native contracts, but root public API does not | Low-Medium |

No test-only helper leakage was found in the inspected surfaces. Test modules
remain under `#[cfg(test)]` or excluded paths.

## 5. Layering Violations

| Violation | Location | Dependency | Description | Directional Impact | Risk |
| ---- | ---- | ---- | ---- | ---- | ---- |
| none confirmed | n/a | n/a | `check-layer-authority-invariants.sh` passed with `0` tracked upward imports and `0` cross-layer policy re-derivations | no violation | Low |

## 6. Structural Pressure Areas

| Area | Pressure Type | Why This Is Pressure (Not Yet Violation) | Drift Sensitivity | Risk |
| ---- | ---- | ---- | ---- | ---- |
| `db::schema::reconcile` | publication/reconciliation hub | `1290` LOC, fanout `7`, branch sites `65`; now contains shared SQL DDL publication envelope plus startup reconciliation paths | new DDL publication classes can concentrate here | Medium |
| `db::session::sql::execute` | SQL execution shell | `931` LOC, fanout `4`, branch sites `22`; coordinates compiled, context-owned, explain, DDL, and query/update paths | new SQL surfaces can widen this root unless family-owned | Medium |
| `db::relation::reverse_index` | relation reverse-index runtime owner | `1065` LOC, fanout `5`, branch sites `32`; owns scalar/composite target encoding and mutation prep | new relation cardinalities or target key shapes can grow this quickly | Medium |
| `db::schema::mutation` family | catalog mutation owner | root dropped to `516` LOC, but DDL admission/execution/identity/delta modules collectively span admission, planning, runner, and publication identity | new mutation classes must stay in family-owned modules | Low-Medium |
| `db::sql::ddl` family | SQL DDL frontend | root dropped to `434` LOC and field/index family modules are below thresholds | new DDL verbs can add parser/schema coupling if not family-owned | Low-Medium |

## 6A. Hub Import Pressure

Metric artifact:
`docs/audits/reports/2026-06/2026-06-05/artifacts/module-structure/runtime-metrics.tsv`.

| Hub Module | Top Imported Sibling Subsystems (by Symbol Count) | Unique Sibling Subsystems Imported | Cross-Layer Dependency Count | Delta vs Previous Report | HIP | LOC | Fanout | Max Branch Depth | Branch Sites | Pressure Band | Risk |
| ---- | ---- | ----: | ----: | ---- | ----: | ----: | ----: | ----: | ----: | ---- | ---- |
| `db::session::sql::execute` | `sql`, `query`, `session`, `executor`, `cursor` | 5 | 0 | LOC `+293`, fanout stable `4`; added execution shell work since baseline | 0.00 | 931 | 4 | 2 | 22 | low by HIP, medium by size | Medium |
| `db::session::sql::execute::write` | `data`, `executor`, `query`, `schema` | 4 | 0 | LOC `+26`, fanout stable `4` | 0.00 | 284 | 4 | 1 | 10 | low | Low |
| `db::session::sql::execute::write::insert` | `sql`, `schema`, `data`, `executor`, `metrics` | 7 | 0 | LOC `+4`, fanout stable `7` | 0.00 | 393 | 7 | 3 | 24 | low by HIP | Low-Medium |
| `db::session::sql::execute::write::update` | `sql`, `schema`, `executor` | 6 | 0 | LOC `+1`, fanout stable `6` | 0.00 | 176 | 6 | 1 | 4 | low by HIP | Low |
| `db::schema::reconcile` | `index`, `identity`, `schema`, `data`, `registry` | 7 | 0 | LOC `+200`, fanout stable `7`; SQL DDL publication envelope centralized here | 0.00 | 1290 | 7 | 3 | 65 | low by HIP, medium by size | Medium |
| `db::schema::mutation` | `index`, `schema`, `identity`, `data`, `predicate` | 5 | 0 | LOC `-708`; DDL/execution/identity/delta split reduced root hub | 0.00 | 516 | 3 | 2 | 13 | low | Low-Medium |
| `db::schema::mutation::field` | `schema` | 1 | 0 | LOC `+119`; field helpers moved under schema ownership | 0.00 | 526 | 1 | 1 | 14 | low | Low-Medium |
| `db::schema::mutation::index` | `index`, `schema`, `predicate` | 3 | 0 | LOC `+1`, fanout stable `1` | 0.00 | 632 | 1 | 2 | 16 | low | Low-Medium |
| `db::schema::mutation::runner` | `index`, `schema` | 2 | 0 | LOC `-221`, fanout `2 -> 1`; identity/execution split reduced runner pressure | 0.00 | 542 | 1 | 1 | 8 | low | Low |
| `db::sql::ddl` | `schema`, `index`, `sql` | 3 | 0 | LOC `-179`; report/admission/family split reduced root hub | 0.00 | 434 | 1 | 2 | 6 | low | Low-Medium |
| `db::sql::ddl::field` | `schema`, `sql` | 2 | 0 | LOC `-77`, fanout `2 -> 1` | 0.00 | 583 | 1 | 2 | 14 | low | Low-Medium |
| `db::sql::ddl::index` | `schema`, `index`, `sql` | 3 | 0 | LOC `-162`, fanout stable `1` | 0.00 | 505 | 1 | 2 | 20 | low | Low-Medium |
| `db::relation` | `data`, `schema`, `identity`, `index` | 5 | 0 | LOC `+64`; tuple-edge descriptor moved shared relation validation into owner | 0.00 | 446 | 5 | 2 | 12 | low by HIP | Low-Medium |
| `db::relation::save_validate` | `relation`, `schema`, `registry`, `data` | 4 | 0 | LOC `-12`; tuple validation moved to relation owner | 0.00 | 488 | 4 | 2 | 17 | low by HIP | Low-Medium |
| `db::relation::reverse_index` | `relation`, `index`, `identity`, `key_taxonomy`, `schema` | 5 | 0 | LOC `-20`; tuple validation moved to relation owner | 0.00 | 1065 | 5 | 2 | 32 | low by HIP, medium by size | Medium |

## 7. Drift Sensitivity Summary

| Growth Vector | Affected Subsystems | Why Multiple Layers Would Change | Drift Risk |
| ---- | ---- | ---- | ---- |
| new SQL DDL verb | SQL parser, SQL DDL family module, schema mutation family module, schema reconcile/session execution | syntax, accepted-catalog derivation, admission, runner planning, and publication are intentionally split | Medium |
| new schema mutation runner | schema mutation execution/runner, schema reconcile, index/data physical stores | publication must stay gated by runner preflight and accepted identity | Medium |
| new relation cardinality or composite key shape | relation metadata, save validation, reverse-index prep, delete validation | relation authority is centralized, but runtime validation and reverse-index mutation consume different execution structs | Medium |
| new public SQL status/error type | SQL DDL report DTOs, error mapping, db facade | public facade exposure can widen quickly if raw internals leak into DTOs | Low-Medium |
| new execution terminal | query/plan, executor route planning, session SQL execution, explain diagnostics | route selection and runtime terminal contracts both need explicit ownership | Medium |

## 8. Structural Risk Index

| Category | Risk Index | Basis |
| ---- | ----: | ---- |
| Public Surface Discipline | 4 | broad but intentional facade/root API; SQL DDL DTOs remain stable report/status vocabulary |
| Layer Directionality | 3 | invariant checks show no tracked upward imports or cross-layer policy re-derivations |
| Circularity Safety | 2 | no real subsystem-level cycles found |
| Visibility Hygiene | 3 | relation tuple descriptor and schema mutation helpers remain private/scoped to owner boundaries |
| Facade Containment | 4 | hidden macro support remains intentionally broad but isolated |

### Overall Structural Risk Index

**4/10**

Moderate structural pressure, no confirmed high/critical violation. The main
current pressure is concentrated in schema reconciliation, session SQL execution,
and relation reverse-index support; all are now included in the module hub
threshold guard.

## 9. Verification Readout

- `PASS`: no high/critical structural violations found.
- `PASS`: audit-definition fitness preflight completed and recurring checklist
  updated for current owner boundaries.
- `PASS`: layer authority invariants.
- `PASS`: architecture text-scan invariant.
- `PASS`: module-structure hub thresholds, including new relation/reconcile/SQL
  execute threshold rows.
- `PASS`: `icydb-core` compile check.
- `PASS`: module-structure metrics artifact generated.

## 10. Post-Audit Cleanup Applied

The immediate cleanup pass split the largest pressure areas without changing
their ownership boundaries:

| Area | Before | After | Result |
| ---- | ---- | ---- | ---- |
| `db::schema::reconcile` | `1290` LOC, fanout `7`, branch sites `65` | root `479` LOC, fanout `6`, branch sites `24`; `db::schema::reconcile::sql_ddl` `833` LOC, fanout `4`, branch sites `41` | SQL DDL publication envelope isolated from startup reconciliation |
| `db::session::sql::execute` | `931` LOC, fanout `4`, branch sites `22` | root `891` LOC, fanout `4`, branch sites `22`; `db::session::sql::execute::metadata` `53` LOC | metadata result shaping isolated from the execution shell |
| `db::relation::reverse_index` | `1065` LOC, fanout `5`, branch sites `32` | root `1037` LOC, fanout `5`, branch sites `32`; `db::relation::reverse_index::target_keys` `31` LOC | target-key container moved into a child module |

The hub-threshold guard now tracks the new SQL DDL reconciliation child module
and tighter root thresholds for the cleaned-up hubs. The runtime metrics
artifact was refreshed after the cleanup.

## Follow-Up Actions

- Keep future schema reconciliation growth under the tightened threshold guard;
  split publication/reconciliation phases again only if new DDL classes push
  either owner toward the guard.
- Keep relation tuple facts owner-local in `db::relation`; do not merge
  save-validation and reverse-index execution structs.
- Keep future SQL execution additions in child modules when they are family
  specific, especially DDL, explain, aggregate, and write paths.
