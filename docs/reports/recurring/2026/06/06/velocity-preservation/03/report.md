# Velocity Preservation Audit - 2026-06-06 (Forward-Looking Rerun 2)

Preamble:

- scope: `docs/audits/recurring/crosscutting/crosscutting-velocity-preservation.md`
- compared baseline report path: `docs/audits/reports/2026-06/2026-06-06/velocity-preservation.md`
- code snapshot identifier: `cb2b898a5` with dirty working tree
- method tag/version: `VP-FEF-1.0`
- comparability status: non-comparable to the same-day canonical baseline
  because that baseline used the older mixed delivery/structure method;
  comparable in method shape to `velocity-preservation-2.md`, but scored
  sections below do not compute deltas.

## 1. Run Metadata + Method

| Method Component | Current |
| ---- | ---- |
| code snapshot identifier | `cb2b898a5` with dirty working tree |
| dirty-worktree status | affects audited surfaces: schema persisted-kind semantics, schema capabilities/types, relation key validation, cursor visibility, build/config endpoint generation, metrics/runtime helpers, audit docs, wasm scripts, canister fixtures; Cargo version files are dirty and left untouched |
| method tag/version | `VP-FEF-1.0` |
| subsystem taxonomy | planner/query, executor/runtime, cursor/continuation, access/index, storage/recovery, schema/catalog, SQL parser/lowering/session, facade/adapters, generated/test support |
| boundary crossing rule set | current-code import scans plus owner-contract triage |
| fan-in/fanout definition | runtime module references by file, tests excluded during triage |
| hub-family taxonomy | coordination hub, decision hub, mixed-owner hub, stable large module |
| decision-surface rule | change-relevant enum/case sites, not raw syntactic matches |
| facade/adapters inclusion | included because current dirty work touches generated endpoint configuration and integration fixtures |

Scoring uses only current code shape. The current code includes a
schema-owned `PersistedFieldKind` semantic classifier that is consumed by SQL
capabilities, persisted schema predicate helpers, and relation primary-key
component validation. That current structure lowers scalar-kind extension
friction, but does not remove the wider `Value`/codec/index/executor update
path.

## 2. Scope + Ownership Map

| Subsystem | Primary Owner Modules | Public/Crate Boundary | Runtime Authority | Notes |
| ---- | ---- | ---- | ---- | ---- |
| planner/query | `db::query::plan`, `db::query::intent`, `db::query::explain` | mixed `pub(in crate::db)` contracts plus public facade DTOs | logical query semantics and plan contracts | executor imports mostly through contract shims; no new planner authority pressure found |
| executor/runtime | `db::executor::*` | `pub(in crate::db::executor)` children plus DB-facing route contracts | physical route selection, projection, aggregation, paging, mutation execution | accepted-value validation still has direct persisted-kind/value ladders |
| cursor/continuation | `db::cursor::*` | DB-wide cursor contracts plus public hex/string cursor facade | cursor token decode, validation, continuation runtime | current boundary remains owner-clear |
| access/index | `db::access::*`, `db::index::*` | DB-local access/index contracts | storage traversal, key encoding, index envelopes | index key encoding remains scalar-heavy but owner-clear |
| storage/recovery | `db::data`, `db::commit`, schema store/recovery code | DB-local storage contracts | persisted bytes, commit/recovery behavior | no query semantic import leakage found in this run |
| schema/catalog | `db::schema::*`, `model::*` | public model DTOs plus DB-local accepted snapshot authority | accepted schema snapshots and catalog-native mutation semantics | `db::schema::field_kind_semantics` now owns accepted persisted-kind semantic classification |
| SQL parser/lowering/session | `db::sql`, `db::sql_shared`, `db::session::sql` | SQL public facade plus session internals | user SQL surface and session dispatch | SQL capability checks consume accepted persisted-kind semantics instead of rematching raw persisted kinds |
| facade/adapters | `icydb-build`, `icydb-config-build`, integration harness | public build/config APIs | generated endpoint emission only; not runtime semantics | current generated endpoint options remain facade-owned |
| generated/test support | fixtures, UI tests, audit canisters | test/generated boundaries | support and verification only | support churn should not define runtime authority |

## 3. Future Feature Probes

| Future Feature Probe | Expected Owner | Required Modules | Layers Crossed | Contract Blockers | Risk |
| ---- | ---- | ----: | ----: | ---- | ---- |
| Add a new SQL aggregate terminal | query planner + executor aggregate contracts | 4-6 semantic modules | 3 | aggregate taxonomy is shared by parser/lowering/planner/executor, but contract modules exist | Medium |
| Add a new cursor/order continuation policy | `db::cursor` with query-plan order contract | 3-4 semantic modules | 2 | cursor delegates plan-shape validation to query-plan policy; no authority inversion found | Low-Medium |
| Add a new persisted scalar kind | schema/catalog + value/data codecs | 6-8 semantic modules | 3-4 | accepted persisted-kind classifier localizes SQL capability, predicate type, strict literal, and relation-key classification; `Value`, accepted-value validation, codecs, and index key semantics still need direct updates | Medium-High |
| Add a new generated canister endpoint class | build/config facade | 3-5 semantic modules | 2 | build/config switches are clear; fixture and integration support may be broad but non-semantic | Medium |
| Add a new index scan route shape | query access planner + executor route/access/index contracts | 5-7 semantic modules | 3 | `AccessPath` is localized but route/executor/index handoff still spans owners | Medium |

The current code no longer makes SQL capabilities and relation key eligibility
separate raw persisted-kind tables. A new persisted scalar kind still crosses
schema, value, storage, index, and executor validation, so this is an
improvement in extension path clarity rather than a full scalar-kind
containment.

## 4. Boundary Leakage

| Boundary | Mechanical Crossings | Allowed Contract Crossings | Suspect Crossings | Risk |
| ---- | ----: | ----: | ----: | ---- |
| planner/query -> executor runtime internals | 1 | 1 | 0 | Low |
| executor/runtime -> query/sql internals | broad query-plan contract references | mostly contract/descriptors | low suspect pressure | Low-Medium |
| index/access -> query/sql AST or lowering types | 0 | 0 | 0 | Low |
| cursor/continuation -> executable plan internals | DB-local plan/order contracts | allowed cursor validation contracts | 0 | Low |
| storage/recovery -> query semantics | 0 | 0 | 0 | Low |
| generated/facade -> runtime semantic authority | endpoint generation/config references only | facade/config contracts | 0 | Low |

The new persisted-kind classifier is exported only inside `crate::db` from
`db::schema` and is consumed by schema and relation policy. It does not move
runtime authority into generated models or facade code.

## 5. Owner / Contract Clarity

| Surface | Owner | Contract Type | Ambiguity | Extension Impact | Risk |
| ---- | ---- | ---- | ---- | ---- | ---- |
| accepted persisted field-kind semantics | `db::schema::field_kind_semantics` | `subsystem-boundary` | low | SQL capabilities, persisted schema predicate helpers, and relation key validation have one accepted-schema semantic spine | Low-Medium |
| accepted schema snapshots | `db::schema` | `subsystem-boundary` | low | schema mutation remains catalog-native; generated models still feed proposals/tests only | Low |
| `Value` and accepted value validation | `value`, executor mutation validation, data/index codecs | mixed DB-local runtime authority | moderate-high | new runtime values still require direct semantic and codec updates across owners | High |
| query-plan DTOs consumed by executor | planner/query plus executor contract shims | `crate-boundary` | moderate | executor feature authors still need contract-module discipline | Medium |
| cursor token/runtime boundary | `db::cursor` | `subsystem-boundary` | low | future cursor policy remains DB-local | Low |
| generated endpoint switches | build/config facade | `facade-public` + `generated-boundary` | low | adding endpoint classes is review-wide but owner-clear | Medium |
| metrics sink bridge | `metrics::sink` | crate boundary | low-moderate | adding metric events remains one large dispatch update | Medium |

## 6. Gravity Wells + Hub Containment

| Module | Class | LOC | Fan-In | Fanout | Domains | Owner Clarity | Risk |
| ---- | ---- | ----: | ----: | ----: | ----: | ---- | ---- |
| `db::schema::field_kind_semantics` | decision hub | 455 | low | 1 | 1 | schema-owned accepted-kind semantic classifier | Low-Medium |
| `db::executor::planning::route::planner::mod` | coordination hub | 21 | low | 1 | 1 | clear staged children | Low |
| `db::session::sql::execute::mod` | coordination hub | 960 | medium | 4 | 1 | clear SQL execution shell | Low-Medium |
| `db::cursor::mod` | coordination hub | 223 | medium | 3 | 2 | clear cursor boundary | Low |
| `db::relation::reverse_index` | stable large module | 1257 | medium | 5 | 1 | relation-owned but large; scalar fast path still local | Medium |
| `db::index::key::build` | stable large module | 1292 | medium | 5 | 1 | index-owned but scalar-heavy | Medium |
| `db::executor::mutation::save_validation` | stable large module | 868 | medium | 7 | 1 | executor-owned validation; accepted-value ladders remain | Low-Medium |
| `db::schema::info` | stable large module | 1086 | high | 5 | 2 | schema-owned projection hub | Medium |
| `metrics::sink` | decision hub | 842 | medium | 3 | 2 | clear bridge, large event dispatch | Medium |

| Hub Module | Contract Boundary | Cross-Layer Families | Allowed Max | Status | Risk |
| ---- | ---- | ----: | ----: | ---- | ---- |
| `crates/icydb-core/src/db/schema/field_kind_semantics.rs` | accepted persisted kind -> schema/relation/SQL capability consumers | 1 DB-local family | 1 | PASS | Low-Medium |
| `crates/icydb-core/src/db/executor/planning/route/planner/mod.rs` | planner stage -> route shape -> executor dispatch | 1 | 1 | PASS | Low |
| `crates/icydb-core/src/db/session/sql/execute/mod.rs` | SQL session shell -> write/explain/diagnostics children | 1 primary family | 1 per child owner | PASS | Low-Medium |
| `crates/icydb-core/src/db/cursor/mod.rs` | plan/order contracts -> cursor validation/runtime | 2 delegated families | 2 | PASS | Low |
| `crates/icydb-core/src/metrics/sink.rs` | instrumentation events -> metrics state | 1 state bridge, many event cases | 1 bridge | PASS with monitor | Medium |

The new classifier is a decision hub, but it has one owner and a narrow
vocabulary: it describes persisted kind semantics and leaves SQL, executor, and
relation admission policy at their owning layers.

## 7. Decision Shock Radius

| Decision Surface | Variants / Cases | Change-Relevant Sites | Modules | Subsystems | Shock Radius | Risk |
| ---- | ----: | ----: | ----: | ----: | ----: | ---- |
| `Value` | 24 variants | high | 360+ runtime files mention `Value` | 6+ | high | High |
| `FieldKind` / `PersistedFieldKind` | 32 variants each | high | many runtime files mention either kind; semantic classifier now absorbs selected accepted-schema decisions | 5+ | medium-high | Medium-High |
| `PersistedFieldKindSemantics` | 21 scalar classes plus collection/relation/structured categories | moderate | 3 production consumer families plus tests | 2 | medium-low | Low-Medium |
| `AggregateKind` | 8 variants | moderate | 70+ runtime files mention it | 4 | medium-high | Medium |
| `AccessPath` / `AccessPathKind` | 7 path families | moderate | 30+ runtime files mention it | 2 | medium | Medium |
| `ContinuationMode` | small route continuation taxonomy | low | route/planning-local | 1 | low | Low |

The current code has reduced one specific shock radius: SQL capability,
predicate type/literal, and relation key-eligibility decisions now ask the
accepted-schema classifier. The larger runtime `Value` and storage/index codec
surfaces still make scalar-kind work a structured multi-owner feature.

## 8. Subsystem Independence

| Subsystem | Internal Imports | External Imports | LOC | Independence | Private Decision Imports | Risk |
| ---- | ----: | ----: | ----: | ----: | ----: | ---- |
| planner/query | high | low | large | high | 0 executor authority imports found | Low |
| executor/runtime | high | moderate query-plan contract imports | large | moderate | accepted-value validation still owns direct persisted-kind/value checks | Low-Medium |
| cursor/continuation | high | low query-plan order-policy delegation | medium | high | 0 private plan internals found | Low |
| access/index | high | none to query/sql in this scan | large | high | index scalar encoding remains owner-local | Low-Medium |
| schema/catalog | high | model DTO imports expected | large | moderate-high | accepted persisted-kind semantics are schema-owned; generated models restricted to proposal/reconciliation/test convenience | Low-Medium |
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
| New persisted scalar kind | schema kind owner classifies persisted semantics, value owner adds runtime representation, codecs/index/executor validations adapt | `PersistedFieldKind` -> `PersistedFieldKindSemantics` -> schema SQL/predicate/relation helpers, then value/storage/index/executor accepted-value updates | 3-4 | classifier localizes semantic questions but not runtime value or codec storage behavior | Medium-High |
| New generated endpoint class | config/build owner adds switch and codegen; integration harness opts in | config model/parse -> build options/codegen -> test/integration fixtures | 1-2 | review surface broad, semantic owner clear | Medium |
| New index scan route shape | query access planner selects path, executor route contracts dispatch to access/index | access choice -> route contracts -> stream/access scan -> index envelope/key semantics | 2-3 | route/index handoff is contract-based but still multi-owner | Medium |

## 10. Future Extension Friction Index

| Area | Score | Weight | Weighted Score | Evidence |
| ---- | ----: | ----: | ----: | ---- |
| future feature probe friction | 5 | 3 | 15 | persisted scalar-kind probe now has a schema-owned semantic spine, but still needs value/codec/index/executor work |
| boundary leakage | 3 | 2 | 6 | no upward planner/executor or index/query authority inversion; new schema classifier remains DB-local |
| owner/contract clarity | 3 | 2 | 6 | accepted persisted-kind semantics have a clear owner; query-plan DTO and runtime value contracts remain broad |
| gravity-well and hub containment | 5 | 2 | 10 | new classifier is owner-clear; relation/index/schema/metrics large modules remain monitor items |
| decision shock radius | 5 | 2 | 10 | selected persisted-kind decisions are centralized, but `Value` and codec/update validation surfaces remain broad |
| subsystem independence | 4 | 1 | 4 | architecture guards pass; executor accepted-value and metrics bridges still require discipline |

Future extension friction index: `4.3/10`.

Interpretation: moderate future extension friction, improved in the current
code around accepted persisted-kind semantic ownership. The main remaining
cost for future scalar work is outside SQL capability and predicate admission:
runtime `Value` semantics, accepted-value validation, storage codecs, and index
key encoding still require direct multi-owner updates.

Follow-up actions:

- owner boundary: `db::executor::mutation::save_validation` plus
  `db::schema::field_kind_semantics`; action: when the next scalar-validation
  change appears, evaluate whether queryable/value-acceptance checks can
  consume classifier-backed helpers without moving executor write policy into
  schema; target report date/run: next velocity or module-structure run
  touching accepted write validation.
- owner boundary: `db::relation::reverse_index`; action: keep the scalar-slot
  fast-path allowlist relation-owned unless a new relation key lane appears,
  then decide whether a classifier-backed "fast-path eligible" answer is real
  behavior or taxonomy-only; target report date/run: next relation/index audit
  touching relation key lanes.
- owner boundary: `value` and `db::data`/`db::index` codecs; action: before
  adding any new persisted scalar kind, list the exact value tag, comparison,
  storage codec, accepted validation, and index key update sites; target report
  date/run: next forward-looking velocity audit.

## 11. Non-Scoring Delivery Context

| Context Signal | Observation | Why Non-Scoring |
| ---- | ---- | ---- |
| same-day forward-looking rerun | `velocity-preservation-2.md` reported `4.8/10` under `VP-FEF-1.0` | prior score is not a scoring input; current code structure is the evidence |
| persisted-kind semantic cleanup | current code routes SQL capabilities, schema type/literal helpers, and relation key validation through `db::schema::field_kind_semantics` | counted only as current-code owner clarity, not as delivery speed or patch width |
| dirty build/config, cursor, metrics, wasm audit work | dirty worktree includes unrelated generated endpoint, cursor visibility, metrics/runtime, wasm fixture/report, and Cargo file changes | delivery breadth does not score; dirty surfaces are recorded for audit context |

## 12. Verification Readout

| Check | Status | Notes |
| ---- | ---- | ---- |
| score used only current-code evidence | PASS | no recent file-count, commit-size, or patch-width input contributed to the score |
| mandatory steps/tables present | PASS | report follows the `VP-FEF-1.0` output order |
| historical change data excluded from scoring | PASS | prior reports appear only as preamble/non-scoring context |
| dirty-worktree impact recorded | PASS | schema classifier, relation validation, cursor, build/config, metrics/runtime, wasm/audit docs, and Cargo dirty files are acknowledged |
| route-planner import boundary guard | PASS | route planner root import families: `1` |
| layer-authority invariants | PASS | upward imports: `0`; cross-layer policy re-derivations: `0`; enum fan-out >2 layers: `1` |
| architecture text-scan invariants | PASS | no include-str source text architecture scans detected |
| module-structure hub thresholds | PASS | configured hub thresholds verified |
| focused core compile check | PASS | `icydb-core` checked successfully |
| focused semantic/schema/relation tests | PASS | field-kind semantics, schema capabilities, schema types, and relation filters passed |
