# Velocity Preservation Audit - 2026-06-06 (Forward-Looking Rerun)

## 1. Run Metadata + Method

| Method Component | Current |
| ---- | ---- |
| code snapshot identifier | `cb2b898a5` with dirty working tree |
| dirty-worktree status | affects audited surfaces: cursor visibility, build/config endpoint generation, metrics sink, runtime helpers, audit docs |
| method tag/version | `VP-FEF-1.0` |
| subsystem taxonomy | planner/query, executor/runtime, cursor/continuation, access/index, storage/recovery, schema/catalog, SQL parser/lowering/session, facade/adapters, generated/test support |
| boundary crossing rule set | current-code import scans plus owner-contract triage |
| fan-in/fanout definition | runtime module references by file, tests excluded during triage |
| hub-family taxonomy | coordination hub, decision hub, mixed-owner hub, stable large module |
| decision-surface rule | change-relevant enum/case sites, not raw syntactic matches |
| facade/adapters inclusion | included because current dirty work touches generated endpoint configuration and integration fixtures |

This rerun is not comparable to the earlier `4.7/10` report because the
scoring basis changed. The score below uses only current-code future extension
friction. Recent file counts, commit ranges, and patch width are not scoring
inputs.

## 2. Scope + Ownership Map

| Subsystem | Primary Owner Modules | Public/Crate Boundary | Runtime Authority | Notes |
| ---- | ---- | ---- | ---- | ---- |
| planner/query | `db::query::plan`, `db::query::intent`, `db::query::explain` | mixed `pub(in crate::db)` contracts plus public facade DTOs | logical query semantics and plan contracts | healthy owner direction; executor imports mostly through contract shims |
| executor/runtime | `db::executor::*` | `pub(in crate::db::executor)` children plus DB-facing route contracts | physical route selection, projection, aggregation, paging, mutation execution | many query-plan DTO imports remain, but concentrated in `contracts` modules |
| cursor/continuation | `db::cursor::*` | mostly `pub(in crate::db)` after current visibility cleanup | cursor token decode, validation, continuation runtime | DB-wide contract is appropriate; public hex/string cursor facade remains intentional |
| access/index | `db::access::*`, `db::index::*` | DB-local access/index contracts | storage traversal, key encoding, index envelopes | no query/sql import leakage found |
| storage/recovery | `db::data`, `db::commit`, recovery-related store code | DB-local storage contracts | persisted bytes, commit/recovery behavior | no query semantic import leakage found in this run |
| schema/catalog | `db::schema::*`, `model::*` | public model DTOs plus DB-local accepted snapshot authority | accepted schema snapshots and catalog-native mutation semantics | accepted snapshots remain runtime authority; generated models feed proposals/tests |
| SQL parser/lowering/session | `db::sql`, `db::sql_shared`, `db::session::sql` | SQL public facade plus session internals | user SQL surface and session dispatch | execution root is still large but split into diagnostics/write/global aggregate children |
| facade/adapters | `icydb-build`, `icydb-config-build`, integration harness | public build/config APIs | generated endpoint emission only; not runtime semantics | current config/build defaults are endpoint-surface decisions, not schema authority |
| generated/test support | fixtures, UI tests, audit canisters | test/generated boundaries | support and verification only | support churn should not define runtime authority |

## 3. Future Feature Probes

| Future Feature Probe | Expected Owner | Required Modules | Layers Crossed | Contract Blockers | Risk |
| ---- | ---- | ----: | ----: | ---- | ---- |
| Add a new SQL aggregate terminal | query planner + executor aggregate contracts | 4-6 semantic modules | 3 | aggregate taxonomy is shared by parser/lowering/planner/executor, but contract modules exist | Medium |
| Add a new cursor/order continuation policy | `db::cursor` with query-plan order contract | 3-4 semantic modules | 2 | cursor delegates plan-shape validation to query-plan policy; no authority inversion found | Low-Medium |
| Add a new persisted scalar kind | schema/catalog + value/data codecs | 8+ semantic modules | 4 | `FieldKind`, `PersistedFieldKind`, and `Value` are broad decision surfaces | High |
| Add a new generated canister endpoint class | build/config facade | 3-5 semantic modules | 2 | build/config switches are clear; fixture and integration support may be broad but non-semantic | Medium |
| Add a new index scan route shape | query access planner + executor route/access/index contracts | 5-7 semantic modules | 3 | `AccessPath` is localized but route/executor/index handoff still spans owners | Medium |

The main future-friction risk is not routine cursor or generated endpoint
growth. It is scalar/value growth: a new persisted kind must pass through model
metadata, accepted snapshots, persisted row codecs, value semantics, SQL
capabilities, index key encoding, and executor validation.

## 4. Boundary Leakage

| Boundary | Mechanical Crossings | Allowed Contract Crossings | Suspect Crossings | Risk |
| ---- | ----: | ----: | ----: | ---- |
| planner/query -> executor runtime internals | 1 | 1 | 0 | Low |
| executor/runtime -> query/sql internals | 20 raw files, 18 after test-file triage | 16 | 2 | Low-Medium |
| index/access -> query/sql AST or lowering types | 0 | 0 | 0 | Low |
| cursor/continuation -> executable plan internals | 6 raw files | 6 | 0 | Low |
| storage/recovery -> query semantics | 0 | 0 | 0 | Low |
| generated/facade -> runtime semantic authority | endpoint generation/config references only | 2 | 0 | Low |

Executor-to-query references are mostly explicit contracts and descriptor
surfaces: prepared execution contracts, aggregate plan contracts, projection
contracts, pipeline contracts, continuation route contracts, and explain/metrics
descriptors. The remaining suspect pressure is not an authority inversion; it is
that future executor features still require reading several query-plan DTO
families before the correct contract module is obvious.

## 5. Owner / Contract Clarity

| Surface | Owner | Contract Type | Ambiguity | Extension Impact | Risk |
| ---- | ---- | ---- | ---- | ---- | ---- |
| cursor token/runtime boundary | `db::cursor` | `subsystem-boundary` | low after current narrowing | future cursor policy remains DB-local | Low |
| query-plan DTOs consumed by executor | planner/query plus executor contract shims | `crate-boundary` | moderate: many DTOs are re-exported through broad plan root | executor feature authors still need contract-module discipline | Medium |
| accepted schema snapshots | `db::schema` | `subsystem-boundary` | low: accepted snapshots are runtime authority | schema mutation remains catalog-native | Low |
| `FieldKind` / `PersistedFieldKind` / `Value` | model/schema/value | mixed public + DB-local authority | moderate-high: scalar semantics are necessarily runtime-wide | new scalar kinds multiply update sites | High |
| generated endpoint switches | build/config facade | `facade-public` + `generated-boundary` | low: booleans are validated outside codegen | adding endpoint classes is review-wide but owner-clear | Medium |
| metrics sink bridge | `metrics::sink` | crate boundary | low-moderate: single allowed state bridge, but large event match | adding metric events remains one large dispatch update | Medium |

## 6. Gravity Wells + Hub Containment

| Module | Class | LOC | Fan-In | Fanout | Domains | Owner Clarity | Risk |
| ---- | ---- | ----: | ----: | ----: | ----: | ---- | ---- |
| `db::executor::planning::route::planner::mod` | coordination hub | 21 | low | 1 | 1 | clear staged children | Low |
| `db::session::sql::execute::mod` | coordination hub | 960 | medium | 4 | 1 | clear SQL execution shell | Low-Medium |
| `db::cursor::mod` | coordination hub | 223 | medium | 3 | 2 | clear cursor boundary | Low |
| `db::relation::reverse_index` | stable large module | 1257 | medium | 5 | 1 | relation-owned but large | Medium |
| `db::index::key::build` | stable large module | 1292 | medium | 5 | 1 | index-owned but scalar-heavy | Medium |
| `db::executor::mutation::save_validation` | stable large module | 868 | medium | 7 | 1 | executor-owned validation | Low-Medium |
| `db::schema::info` | stable large module | 1086 | high | 5 | 2 | schema-owned projection hub | Medium |
| `metrics::sink` | decision hub | 815 | medium | 3 | 2 | clear bridge, large event dispatch | Medium |

| Hub Module | Contract Boundary | Cross-Layer Families | Allowed Max | Status | Risk |
| ---- | ---- | ----: | ----: | ---- | ---- |
| `crates/icydb-core/src/db/executor/planning/route/planner/mod.rs` | planner stage -> route shape -> executor dispatch | 1 | 1 | PASS | Low |
| `crates/icydb-core/src/db/session/sql/execute/mod.rs` | SQL session shell -> write/explain/diagnostics children | 1 primary family | 1 per child owner | PASS | Low-Medium |
| `crates/icydb-core/src/db/cursor/mod.rs` | plan/order contracts -> cursor validation/runtime | 2 delegated families | 2 | PASS | Low |
| `crates/icydb-core/src/metrics/sink.rs` | instrumentation events -> metrics state | 1 state bridge, many event cases | 1 bridge | PASS with monitor | Medium |

The large modules are mostly owner-clear. They are monitor items, not immediate
split mandates. The strongest future split trigger is a real new child owner in
`relation::reverse_index`, `index::key::build`, or `metrics::sink`.

## 7. Decision Shock Radius

| Decision Surface | Variants / Cases | Change-Relevant Sites | Modules | Subsystems | Shock Radius | Risk |
| ---- | ----: | ----: | ----: | ----: | ----: | ---- |
| `Value` | 24 variants | high | 364 runtime files mention `Value` | 6+ | high | High |
| `FieldKind` / `PersistedFieldKind` | 32 variants each | high | 86 runtime files mention either kind | 5+ | high | High |
| `AggregateKind` | 8 variants | moderate | 73 runtime files mention it | 4 | medium-high | Medium |
| `AccessPath` / `AccessPathKind` | 7 path families | moderate | 38 runtime files mention it | 2 | medium | Medium |
| `Keyword` | parser keyword set | moderate | 26 runtime files mention it | 1 | medium-low | Medium |
| `ContinuationMode` | small route continuation taxonomy | low | route/planning-local | 1 | low | Low |

`Value` and schema kind taxonomies are the main future-extension drag. They are
legitimate core surfaces, but adding a new persisted scalar kind should be
treated as a structured multi-owner design slice, not routine local work.

## 8. Subsystem Independence

| Subsystem | Internal Imports | External Imports | LOC | Independence | Private Decision Imports | Risk |
| ---- | ----: | ----: | ----: | ----: | ----: | ---- |
| planner/query | high | low | large | high | 0 executor authority imports found | Low |
| executor/runtime | high | moderate query-plan contract imports | large | moderate | few; mostly contract shims/descriptors | Low-Medium |
| cursor/continuation | high | low query-plan order-policy delegation | medium | high | 0 private plan internals found | Low |
| access/index | high | none to query/sql in this scan | large | high | 0 | Low |
| schema/catalog | high | model DTO imports expected | large | moderate-high | generated models restricted to proposal/reconciliation/test convenience | Low-Medium |
| facade/adapters | medium | build/config/schema metadata | medium | moderate | no runtime semantic authority found | Low-Medium |
| metrics/runtime | medium | metrics state bridge centralized in `metrics::sink` | medium | moderate | state bridge intentionally single | Medium |

Supporting guards passed: route-planner import boundary, layer-authority
invariants, architecture text-scan invariants, and module-structure hub
thresholds.

## 9. Extension Path Rehearsal

| Probe | Ideal Owner-Local Path | Actual Current Path | Extra Owners Required | Main Blocker | Risk |
| ---- | ---- | ---- | ----: | ---- | ---- |
| New SQL aggregate terminal | parser/lowering admits syntax, planner owns `AggregateKind`, executor aggregate contracts consume it | parser or builder -> query plan model/semantics -> executor aggregate contracts/state/reducer -> explain/tests | 2-3 | aggregate kind is planner-owned but executor must update fold semantics | Medium |
| New cursor/order policy | query-plan order policy exports one validation contract, cursor owns token/runtime | query plan validate cursor policy -> cursor spine/boundary/runtime -> executor page/order consumers | 1-2 | delegated validation is clear; support remains DB-wide | Low-Medium |
| New persisted scalar kind | schema kind owner adds persisted representation, value owner adds runtime representation, codecs/index/SQL capability adapt | model field kind -> persisted snapshot kind -> value/storage codecs -> SQL capabilities -> index key/order/predicate/executor validations | 4+ | scalar semantics are spread across schema, value, storage, SQL, index, executor | High |
| New generated endpoint class | config/build owner adds switch and codegen; integration harness opts in | config model/parse -> build options/codegen -> test/integration fixtures | 1-2 | review surface broad, semantic owner clear | Medium |
| New index scan route shape | query access planner selects path, executor route contracts dispatch to access/index | access choice -> route contracts -> stream/access scan -> index envelope/key semantics | 2-3 | route/index handoff is contract-based but still multi-owner | Medium |

## 10. Future Extension Friction Index

| Area | Score | Weight | Weighted Score | Evidence |
| ---- | ----: | ----: | ----: | ---- |
| future feature probe friction | 6 | 3 | 18 | scalar kind and aggregate/index probes require multi-owner semantic paths; cursor and generated endpoints are clearer |
| boundary leakage | 3 | 2 | 6 | no upward planner/executor or index/query authority inversion; executor/query contracts remain broad |
| owner/contract clarity | 4 | 2 | 8 | most surfaces have owners; query-plan DTO and scalar/value contracts remain wide |
| gravity-well and hub containment | 5 | 2 | 10 | route planner passes; relation/index/schema/metrics large owner-clear hubs require monitoring |
| decision shock radius | 6 | 2 | 12 | `Value` and schema kind surfaces are broad and change-relevant across runtime subsystems |
| subsystem independence | 4 | 1 | 4 | architecture guards pass; executor and metrics bridges still require contract discipline |

Future extension friction index: `4.8/10`.

Interpretation: moderate future extension friction. The current code is not
structurally blocked: layer guards pass, route planning is contained, cursor
authority is clearer after recent narrowing, and generated endpoint options do
not own runtime semantics. The main improvement target is to make scalar/value
and metrics-event growth more owner-local before the next feature has to add a
new kind or event family.

Follow-up actions:

- owner boundary: `model/schema/value/data`; action: before adding any new
  persisted scalar kind, create an owner-local checklist or adapter table that
  names all required semantic update sites; target report date/run: next
  forward-looking velocity audit.
- owner boundary: `metrics::sink`; action: split only when a new event family
  appears, preferably into event-family handlers behind the single sink bridge;
  target report date/run: next module-structure or velocity run after metrics
  growth.
- owner boundary: `db::schema::info`; action: continue narrowing
  `pub(crate)` schema projection helpers when consumers become DB-local; target
  report date/run: next modular cleanup pass touching schema/catalog.

## 11. Non-Scoring Delivery Context

| Context Signal | Observation | Why Non-Scoring |
| ---- | ---- | ---- |
| current dirty build/config endpoint work | generated endpoint defaults and audit fixtures are broad in file count | delivery breadth does not score; only current owner clarity and runtime authority were evaluated |
| current cursor visibility cleanup | cursor surfaces are actively being narrowed to DB boundaries | cleanup history does not lower score; current-code boundary state is the evidence |
| existing old velocity report | prior `4.7/10` mixed recent change surface with structural friction | this rerun uses `VP-FEF-1.0` and does not compute deltas |

## 12. Verification Readout

| Check | Status | Notes |
| ---- | ---- | ---- |
| score used only current-code evidence | PASS | no recent file-count, commit-size, or patch-width input contributed to the score |
| mandatory steps/tables present | PASS | report follows the rewritten `VP-FEF-1.0` output order |
| historical change data excluded from scoring | PASS | old report and dirty-slice breadth appear only as non-scoring context |
| dirty-worktree impact recorded | PASS | dirty build/config, metrics/runtime, cursor, and audit docs surfaces are acknowledged |
| route-planner import boundary guard | PASS | route planner root import families: `1` |
| layer-authority invariants | PASS | upward imports: `0`; cross-layer policy re-derivations: `0`; enum fan-out >2 layers: `1` |
| architecture text-scan invariants | PASS | no include-str source text architecture scans detected |
| module-structure hub thresholds | PASS | configured hub thresholds verified |
