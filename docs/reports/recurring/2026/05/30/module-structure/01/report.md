# Structure / Module / Visibility Discipline Audit - 2026-05-30

## 0. Run Metadata + Comparability Note

- target scope: `icydb-core` structural boundaries and visibility discipline, with `db` subsystem emphasis
- compared baseline report path: `docs/audits/reports/2026-04/2026-04-13/module-structure.md`
- code snapshot identifier: `feab0cb31`
- method tag/version: `Method V4`
- comparability status: `semi-comparable` (same audit family and invariant checks; V4 adds generated runtime-metrics artifact evidence and updates hub paths to current post-load/route split module names)
- exclusions applied: test-only files/modules excluded from metrics and structural invariant scripts
- methodology changes vs baseline: generated `runtime-metrics.tsv` is now retained under the report artifacts; stale hub names in the recurring audit definition were updated after this run
- daily baseline rule: first run of day compares to latest prior comparable report

## 1. Public Surface Map

| Item | Kind | Path | Publicly Reachable From Root? | Classification | Visibility Scope | Exposure Impact | Risk |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| `db`, `error`, `metrics`, `model`, `sanitize`, `traits`, `types`, `validate`, `value`, `visitor` | root modules | [`lib.rs:16`](/home/adam/projects/icydb/crates/icydb-core/src/lib.rs:16) | yes | intended core API | `pub mod` | broad but intentional core surface | Low |
| `prelude` | root module | [`lib.rs:49`](/home/adam/projects/icydb/crates/icydb-core/src/lib.rs:49) | yes | intended external API | `pub mod` | limited domain vocabulary only | Low |
| `__macro` | generated-code support module | [`lib.rs:62`](/home/adam/projects/icydb/crates/icydb-core/src/lib.rs:62) | yes | macro-support item | `#[doc(hidden)] pub mod` | intentionally exposes generated-code wiring | Medium |
| `db` facade re-exports | facade API | [`db/mod.rs:57`](/home/adam/projects/icydb/crates/icydb-core/src/db/mod.rs:57) | yes | intended facade + generated support | `pub use` / `#[doc(hidden)] pub use` | large root surface but still routed through explicit facade | Medium |
| SQL DDL DTOs | public diagnostics | [`ddl.rs:97`](/home/adam/projects/icydb/crates/icydb-core/src/db/sql/ddl.rs:97) | yes via `db` | intended external SQL status API | `pub struct` / `pub enum` | stable developer-facing report/status, not raw mutation internals | Low-Medium |
| Schema mutation internals | catalog mutation owner | [`mutation/mod.rs:121`](/home/adam/projects/icydb/crates/icydb-core/src/db/schema/mutation/mod.rs:121) | no | internal plumbing | `pub(in crate::db)` | contained to `db`, not root-reachable | Low |

## 2. Subsystem Dependency Graph

| Subsystem | Depends On | Depended On By | Lower-Layer Dependencies | Same-Layer Dependencies | Upward Dependency Found? | Direction Assessment | Risk |
| ---- | ---- | ---- | ----: | ----: | ---- | ---- | ---- |
| schema/mutation | codec, data, index, predicate, schema snapshots | SQL DDL, schema reconcile | 5 | 1 | no | pressure: broad catalog-native mutation owner, no violation | Medium |
| SQL DDL binding | data encoding, predicate validation, accepted schema/mutation contracts, SQL parser | SQL execution/session | 4 | 2 | no | pressure: frontend binds into schema-owned contracts without owning mutation semantics | Medium |
| query/plan | access, predicate, schema, model | query intent, executor, session, explain | 4 | 3 | no | pressure: many `pub(in crate::db)` re-exports, but plan remains logical owner | Medium |
| executor | access contracts, query plans, cursor, index/data runtime, pipeline | session/query execution, diagnostics | 5 | 4 | no | pressure: runtime hub, no planner-owned policy redefinition found | Medium |
| commit/recovery | data store, schema reconcile support, marker store | db root, schema reconcile | 3 | 1 | no | contained storage boundary | Low-Medium |
| facade (`icydb-core::db`) | child subsystems | generated code, facade crate, callers | n/a | n/a | no | broad intentional facade with hidden macro support | Medium |

## 3. Circularity Findings

| Subsystem A | Subsystem B | Real Cycle? | Evidence | Risk |
| ---- | ---- | ---- | ---- | ---- |
| query/plan | executor | no | layer invariant check reported `Upward imports (tracked edges): 0`; executor consumes plan contracts while query/plan does not import executor contracts | Low |
| schema/mutation | SQL DDL | no | [`ddl.rs:1`](/home/adam/projects/icydb/crates/icydb-core/src/db/sql/ddl.rs:1) binds DDL to schema contracts; [`mutation/mod.rs:1`](/home/adam/projects/icydb/crates/icydb-core/src/db/schema/mutation/mod.rs:1) states SQL DDL parsing is not owned there | Low |
| commit/recovery | schema reconcile | no | reconcile calls supported mutation/recovery paths, while commit store access remains inside `db/commit/*` per invariant check | Low |

## 4. Visibility Hygiene Findings

| Item | Path | Current Visibility | Narrowest Plausible Visibility | Why Narrower Seems Valid | Risk |
| ---- | ---- | ---- | ---- | ---- | ---- |
| `SqlDdlPreparationReport` | [`ddl.rs:97`](/home/adam/projects/icydb/crates/icydb-core/src/db/sql/ddl.rs:97) | `pub` | keep `pub` | public fields are private and accessor methods form a stable DTO | Low |
| `SqlDdlMutationKind` / `SqlDdlExecutionStatus` | [`ddl.rs:175`](/home/adam/projects/icydb/crates/icydb-core/src/db/sql/ddl.rs:175), [`ddl.rs:215`](/home/adam/projects/icydb/crates/icydb-core/src/db/sql/ddl.rs:215) | `pub` | keep `pub` | developer-facing status vocabulary exported by `db` when SQL is enabled | Low-Medium |
| `PreparedSqlDdlCommand` | [`ddl.rs:58`](/home/adam/projects/icydb/crates/icydb-core/src/db/sql/ddl.rs:58) | `pub(in crate::db)` | keep `pub(in crate::db)` | execution/session boundary needs the prepared command, but it is not root-public | Low |
| `query/plan` re-export hub | [`plan/mod.rs:34`](/home/adam/projects/icydb/crates/icydb-core/src/db/query/plan/mod.rs:34) | mostly `pub(in crate::db)` / `pub(in crate::db::query)` | no narrower recommendation this run | usage is intentionally cross-owner between query, executor, session, and explain | Medium |
| `__macro` | [`lib.rs:62`](/home/adam/projects/icydb/crates/icydb-core/src/lib.rs:62) | `#[doc(hidden)] pub` | keep `pub` | derive output needs a stable generated-code path | Medium |

No test-only helper leakage was found in the inspected surfaces. Test modules remain under `#[cfg(test)]` or excluded paths.

## 5. Layering Violations

| Violation | Location | Dependency | Description | Directional Impact | Risk |
| ---- | ---- | ---- | ---- | ---- | ---- |
| none confirmed | n/a | n/a | `check-layer-authority-invariants.sh` passed with `0` tracked upward imports and `0` cross-layer policy re-derivations | no violation | Low |

## 6. Structural Pressure Areas

| Area | Pressure Type | Why This Is Pressure (Not Yet Violation) | Drift Sensitivity | Risk |
| ---- | ---- | ---- | ---- | ---- |
| schema mutation | large catalog-native owner | `schema::mutation` is `2219` LOC with `68` branch sites in the artifact; scope is broad but intentionally catalog-owned | new DDL verbs or mutation runners can expand this quickly | Medium |
| SQL DDL binding | broad frontend-to-catalog binding | `sql::ddl` is `1933` LOC with `65` branch sites; it binds many SQL forms to accepted schema contracts but does not own mutation execution | new DDL syntax can add parser/schema coupling | Medium |
| schema reconcile | startup + SQL DDL physical publication | `schema::reconcile` has fanout `7` and `60` branch sites; SQL DDL execution helpers are now housed beside startup reconciliation | new supported mutation execution paths can concentrate here | Medium |
| SQL write execution | write-boundary concentration | `session::sql::execute::write` has fanout `7` and imports parser, lowering, schema, executor, data, metrics, sanitize, and traits | INSERT/UPDATE/RETURNING growth can widen this unless split by write family | Medium |
| query/plan root | re-export concentration | `query/plan/mod.rs` uses many scoped re-exports, but most are `pub(in crate::db)` and serve as an explicit logical-plan boundary | adding new plan families can increase facade width | Medium |

## 5A. Hub Import Pressure

| Hub Module | Top Imported Sibling Subsystems | Unique Sibling Subsystems Imported | Cross-Layer Dependency Count | Delta vs Previous Report | HIP | LOC | Fanout | Max Branch Depth | Branch Sites | Pressure Band | Risk |
| ---- | ---- | ----: | ----: | ---- | ----: | ----: | ----: | ----: | ----: | ---- | ---- |
| `db/mod.rs` | `commit`, `data`, `executor`, `registry`, `query`, `schema`, `session`, `sql` | 8 | 0 | broader than previous narrow SQL-focused report, but intentional facade root | 0.00 | 748 | 0 | 1 | 7 | low | Medium |
| `executor/mod.rs` | `access`, `aggregate`, `pipeline`, `planning`, `projection`, `runtime_context`, `stream`, `terminal` | 8 | 0 | current replacement for old `executor/load` and `executor/route` hub names | 0.00 | 538 | 0 | 1 | 5 | low | Medium |
| `executor/planning/route/planner/entrypoints.rs` | `executor::planning`, `executor::route`, `query::plan`, `direction` | 4 | 1 | follow-up from prior report remains contained; file is `219` LOC | 0.25 | 219 | 3 | 2 | 16 | low | Low-Medium |
| `query/plan/mod.rs` | `access`, `schema`, `model`, `predicate`, plan children | 5 | 0 | stable root re-export hub | 0.00 | 215 | 4 | 1 | 5 | low | Medium |
| `session/sql/execute/write.rs` | `data`, `executor`, `query`, `schema`, `sql`, `metrics`, `sanitize` | 7 | 0 | newer write execution concentration not represented in old audit checklist | 0.00 | 769 | 7 | 3 | 37 | low by HIP, medium by size/fanout | Medium |
| `schema/mutation/mod.rs` | `codec`, `data`, `index`, `predicate`, `schema` | 5 | 0 | newer DDL/mutation pressure not represented in old audit checklist | 0.00 | 2219 | 3 | 2 | 68 | low by HIP, medium by size | Medium |

Metric artifact: `docs/audits/reports/2026-05/2026-05-30/artifacts/module-structure/runtime-metrics.tsv`.

## 7. Drift Sensitivity Summary

| Growth Vector | Affected Subsystems | Why Multiple Layers Would Change | Drift Risk |
| ---- | ---- | ---- | ---- |
| new SQL DDL verb | SQL parser, SQL DDL binding, schema mutation, schema reconcile/session execution | syntax, catalog derivation, admission, and physical publication are intentionally split | Medium |
| new execution terminal | query/plan, executor/planning/route, executor/pipeline, executor/terminal, explain diagnostics | route selection and runtime terminal contracts both need explicit ownership | Medium |
| new cursor continuation mode | cursor, query/plan, executor/planning/continuation, terminal/page | continuation contracts cross planning and runtime page boundaries | Medium |
| new public error/status type | error, db facade, SQL/session DTOs | public root exposure can widen quickly if not kept DTO-only | Low-Medium |

## 8. Structural Risk Index

| Category | Risk Index | Basis |
| ---- | ----: | ---- |
| Public Surface Discipline | 4 | broad but intentional facade/root API; SQL DDL DTOs have private fields |
| Layer Directionality | 3 | invariant checks show no tracked upward imports or cross-layer policy re-derivations |
| Circularity Safety | 2 | no real subsystem-level cycles found |
| Visibility Hygiene | 4 | many scoped re-exports remain, mostly `pub(in crate::db)`; no new narrowing recommendation |
| Facade Containment | 4 | hidden macro support is intentionally broad but isolated |

### Overall Structural Risk Index

**4/10**

Moderate structural pressure, no confirmed high/critical violation. The main current pressure moved from the old route/load hubs to catalog mutation/DDL and SQL write execution.

## 9. Verification Readout

- `PASS`: no high/critical structural violations found
- `PASS`: layer authority invariants
- `PASS`: architecture text-scan invariant
- `PASS`: `icydb-core` compile check
- `PASS`: module-structure metrics artifact generated

## Follow-Up Actions

- Owner boundary: `db/schema/mutation` + `db/sql/ddl`; action: keep SQL DDL as a frontend binder and keep mutation semantics catalog-native as new DDL verbs become executable.
- Owner boundary: `db/session/sql/execute/write.rs`; action: if write execution grows further, split by INSERT/UPDATE/RETURNING ownership rather than adding more cross-family helpers.
- Audit process: use Method V4 hub list and metrics artifact on the next `module-structure` run.
