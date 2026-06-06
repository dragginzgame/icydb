# Velocity Preservation Audit - 2026-06-06 (Forward-Looking Rerun 3)

Preamble:

- scope: `docs/audits/recurring/crosscutting/crosscutting-velocity-preservation.md`
- compared baseline report path: `docs/audits/reports/2026-06/2026-06-06/velocity-preservation-3.md`
- code snapshot identifier: `2b97a0d33` with dirty working tree
- method tag/version: `VP-FEF-1.0`
- comparability status: comparable in method shape to
  `velocity-preservation-2.md` and `velocity-preservation-3.md`, but scored
  sections below do not compute deltas.

## 1. Run Metadata + Method

| Method Component | Current |
| ---- | ---- |
| code snapshot identifier | `2b97a0d33` with dirty working tree |
| dirty-worktree status | affects audited surfaces: accepted persisted-kind queryability validation, executor projection/materialization/facade/pipeline/terminal/delete paths, predicate rewrite/render helpers, facade/session SQL response code, diagnostics helpers, audit fixture Cargo manifests, and release metadata; Cargo version files are dirty and left untouched |
| method tag/version | `VP-FEF-1.0` |
| subsystem taxonomy | planner/query, executor/runtime, cursor/continuation, access/index, storage/recovery, schema/catalog, SQL parser/lowering/session, facade/adapters, generated/test support |
| boundary crossing rule set | current-code import scans plus owner-contract triage |
| fan-in/fanout definition | runtime module references by file, tests excluded during triage |
| hub-family taxonomy | coordination hub, decision hub, mixed-owner hub, stable large module |
| decision-surface rule | change-relevant enum/case sites, not raw syntactic matches |
| facade/adapters inclusion | included because current dirty work touches facade/session SQL response and integration fixtures |

Scoring uses only current code shape. The current code now routes the executor
accepted-kind queryability gate through `db::schema::field_kind_semantics`, so
SQL capability, predicate/literal helpers, relation key validation, and
accepted query visibility no longer maintain separate broad scalar allowlists.
The current dirty snapshot also contains executor projection/facade changes
that fail `icydb-core` compilation, which is recorded as a current owner-boundary
and verification risk rather than as delivery-width evidence.

## 2. Scope + Ownership Map

| Subsystem | Primary Owner Modules | Public/Crate Boundary | Runtime Authority | Notes |
| ---- | ---- | ---- | ---- | ---- |
| planner/query | `db::query::plan`, `db::query::intent`, `db::query::explain` | mixed `pub(in crate::db)` contracts plus public facade DTOs | logical query semantics and plan contracts | route-planner import guard still reports one executor family crossing |
| executor/runtime | `db::executor::*` | `pub(in crate::db::executor)` children plus DB-facing route contracts | physical route selection, projection, aggregation, paging, mutation execution | accepted queryability is classifier-backed; accepted value-shape validation remains executor-owned and scalar-heavy; current projection/facade dirty code fails compile |
| cursor/continuation | `db::cursor::*` | DB-wide cursor contracts plus public hex/string cursor facade | cursor token decode, validation, continuation runtime | no new cursor authority leak found |
| access/index | `db::access::*`, `db::index::*` | DB-local access/index contracts | storage traversal, key encoding, index envelopes | index key encoding remains scalar-heavy but owner-clear |
| storage/recovery | `db::data`, `db::commit`, schema store/recovery code | DB-local storage contracts | persisted bytes, commit/recovery behavior | no query semantic import leakage found in this run |
| schema/catalog | `db::schema::*`, `model::*` | public model DTOs plus DB-local accepted snapshot authority | accepted schema snapshots and catalog-native mutation semantics | `db::schema::field_kind_semantics` remains accepted persisted-kind semantic owner |
| SQL parser/lowering/session | `db::sql`, `db::sql_shared`, `db::session::sql` | SQL public facade plus session internals | user SQL surface and session dispatch | SQL capability checks consume accepted persisted-kind semantics; facade/session response dirty files are current context |
| facade/adapters | `icydb-build`, `icydb-config-build`, integration harness | public build/config APIs | generated endpoint emission only; not runtime semantics | current facade SQL response edits are dirty but no runtime authority inversion was found mechanically |
| generated/test support | fixtures, UI tests, audit canisters | test/generated boundaries | support and verification only | dirty Cargo fixture manifests are non-runtime context |

## 3. Future Feature Probes

| Future Feature Probe | Expected Owner | Required Modules | Layers Crossed | Contract Blockers | Risk |
| ---- | ---- | ----: | ----: | ---- | ---- |
| Add a new SQL aggregate terminal | query planner + executor aggregate contracts | 4-6 semantic modules | 3 | aggregate taxonomy is shared by parser/lowering/planner/executor, but contract modules exist | Medium |
| Add a new cursor/order continuation policy | `db::cursor` with query-plan order contract | 3-4 semantic modules | 2 | cursor delegates plan-shape validation to query-plan policy; no authority inversion found | Low-Medium |
| Add a new persisted scalar kind | schema/catalog + value/data codecs | 5-7 semantic modules | 3-4 | accepted persisted-kind classifier now localizes SQL capability, predicate type/literal, relation-key classification, and executor queryability; `Value`, accepted-value validation, codecs, and index key semantics still need direct updates | Medium-High |
| Add a new structural projection/delete result mode | executor projection/delete/session contracts | 5-7 semantic modules | 3 | current dirty projection/facade code shows feature-gated re-export and materialization contracts are fragile under change | Medium-High |
| Add a new index scan route shape | query access planner + executor route/access/index contracts | 5-7 semantic modules | 3 | `AccessPath` is localized but route/executor/index handoff still spans owners | Medium |

The queryability cleanup removes one executor update point from the persisted
scalar-kind probe. The remaining scalar-kind cost is runtime value and storage
behavior, not SQL/query visibility classification.

## 4. Boundary Leakage

| Boundary | Mechanical Crossings | Allowed Contract Crossings | Suspect Crossings | Risk |
| ---- | ----: | ----: | ----: | ---- |
| planner/query -> executor runtime internals | 1 | 1 | 0 | Low |
| executor/runtime -> query/sql internals | broad query-plan contract references | mostly contract/descriptors | low suspect pressure | Low-Medium |
| index/access -> query/sql AST or lowering types | 0 | 0 | 0 | Low |
| cursor/continuation -> executable plan internals | DB-local plan/order contracts | allowed cursor validation contracts | 0 | Low |
| storage/recovery -> query semantics | 0 | 0 | 0 | Low |
| generated/facade -> runtime semantic authority | facade/session response references only | facade/config contracts | 0 | Low |

No new cross-layer authority inversion was found mechanically. The compile
failure is concentrated inside executor projection/materialization/facade
contracts, so it raises owner clarity and verification risk more than boundary
leakage risk.

## 5. Owner / Contract Clarity

| Surface | Owner | Contract Type | Ambiguity | Extension Impact | Risk |
| ---- | ---- | ---- | ---- | ---- | ---- |
| accepted persisted field-kind semantics | `db::schema::field_kind_semantics` | `subsystem-boundary` | low | SQL capabilities, persisted schema predicate helpers, relation key validation, and executor queryability now share one accepted-schema semantic spine | Low-Medium |
| accepted queryability policy | executor mutation validation consuming schema classifier | `subsystem-boundary` | low | map/list/structured visibility remains executor policy while scalar/relation categories come from schema semantics | Low |
| accepted schema snapshots | `db::schema` | `subsystem-boundary` | low | schema mutation remains catalog-native; generated models still feed proposals/tests only | Low |
| `Value` and accepted value validation | `value`, executor mutation validation, data/index codecs | mixed DB-local runtime authority | moderate-high | new runtime values still require direct semantic and codec updates across owners | High |
| executor projection/materialization facade | `db::executor::projection` plus terminal/pipeline contracts | `crate-boundary` | moderate-high in current dirty snapshot | current no-feature compile breaks on gated re-exports, duplicate `project_distinct`, and materialization API drift | Medium-High |
| query-plan DTOs consumed by executor | planner/query plus executor contract shims | `crate-boundary` | moderate | executor feature authors still need contract-module discipline | Medium |
| cursor token/runtime boundary | `db::cursor` | `subsystem-boundary` | low | future cursor policy remains DB-local | Low |
| generated endpoint switches | build/config facade | `facade-public` + `generated-boundary` | low | adding endpoint classes is review-wide but owner-clear | Medium |

## 6. Gravity Wells + Hub Containment

| Module | Class | LOC | Fan-In | Fanout | Domains | Owner Clarity | Risk |
| ---- | ---- | ----: | ----: | ----: | ----: | ---- | ---- |
| `db::schema::field_kind_semantics` | decision hub | 455 | low | 1 | 1 | schema-owned accepted-kind semantic classifier | Low-Medium |
| `db::executor::mutation::save_validation` | stable large module | 882 | medium | 7 | 1 | executor-owned validation; queryability now delegates kind category to schema, but accepted-value ladders remain | Low-Medium |
| `db::executor::projection::mod` | coordination hub | 57 | medium | 4 | 1 | projection shell is owner-local, but current dirty re-export/API drift breaks compile | Medium |
| `db::executor::planning::route::planner::mod` | coordination hub | 21 | low | 1 | 1 | clear staged children | Low |
| `db::session::sql::execute::mod` | coordination hub | 960 | medium | 4 | 1 | clear SQL execution shell | Low-Medium |
| `db::cursor::mod` | coordination hub | 223 | medium | 3 | 2 | clear cursor boundary | Low |
| `db::relation::reverse_index` | stable large module | 1257 | medium | 5 | 1 | relation-owned but large; scalar fast path still local | Medium |
| `db::index::key::build` | stable large module | 1292 | medium | 5 | 1 | index-owned but scalar-heavy | Medium |
| `db::schema::info` | stable large module | 1086 | high | 5 | 2 | schema-owned projection hub | Medium |
| `metrics::sink` | decision hub | 842 | medium | 3 | 2 | clear bridge, large event dispatch | Medium |

| Hub Module | Contract Boundary | Cross-Layer Families | Allowed Max | Status | Risk |
| ---- | ---- | ----: | ----: | ---- | ---- |
| `crates/icydb-core/src/db/schema/field_kind_semantics.rs` | accepted persisted kind -> schema/relation/SQL/executor queryability consumers | 1 DB-local family | 1 | PASS | Low-Medium |
| `crates/icydb-core/src/db/executor/mutation/save_validation.rs` | accepted write preflight -> schema kind categories and runtime `Value` policy | 1 DB-local family | 1 | PASS with monitor | Low-Medium |
| `crates/icydb-core/src/db/executor/projection/mod.rs` | projection facade/materialization shell -> executor internals | 1 DB-local family | 1 | PARTIAL: current dirty code fails compile | Medium |
| `crates/icydb-core/src/db/executor/planning/route/planner/mod.rs` | planner stage -> route shape -> executor dispatch | 1 | 1 | PASS | Low |
| `crates/icydb-core/src/db/session/sql/execute/mod.rs` | SQL session shell -> write/explain/diagnostics children | 1 primary family | 1 per child owner | PASS | Low-Medium |
| `crates/icydb-core/src/db/cursor/mod.rs` | plan/order contracts -> cursor validation/runtime | 2 delegated families | 2 | PASS | Low |
| `crates/icydb-core/src/metrics/sink.rs` | instrumentation events -> metrics state | 1 state bridge, many event cases | 1 bridge | PASS with monitor | Medium |

The new classifier remains a narrow decision hub. The newly visible risk in
this dirty snapshot is not that projection owns too much policy; it is that
projection feature-gating and re-export contracts are currently easy to break.

## 7. Decision Shock Radius

| Decision Surface | Variants / Cases | Change-Relevant Sites | Modules | Subsystems | Shock Radius | Risk |
| ---- | ----: | ----: | ----: | ----: | ----: | ---- |
| `Value` | 24 variants | high | 360+ runtime files mention `Value` | 6+ | high | High |
| `FieldKind` / `PersistedFieldKind` | 32 variants each | medium-high | many runtime files mention either kind; semantic classifier now absorbs selected accepted-schema decisions and executor queryability | 5+ | medium-high | Medium-High |
| `PersistedFieldKindSemantics` | 21 scalar classes plus collection/relation/structured categories | moderate | 4 production consumer families plus tests | 2 | medium-low | Low-Medium |
| projection materialization contracts | several row/layout/result shapes | moderate | projection, terminal, pipeline, delete/session facade | 2-3 | medium | Medium |
| `AggregateKind` | 8 variants | moderate | 70+ runtime files mention it | 4 | medium-high | Medium |
| `AccessPath` / `AccessPathKind` | 7 path families | moderate | 30+ runtime files mention it | 2 | medium | Medium |
| `ContinuationMode` | small route continuation taxonomy | low | route/planning-local | 1 | low | Low |

The current code reduced persisted-kind shock radius for query-visible scalar
classification. Runtime `Value` acceptance and projection materialization
contracts remain the higher-cost extension surfaces in this snapshot.

## 8. Subsystem Independence

| Subsystem | Internal Imports | External Imports | LOC | Independence | Private Decision Imports | Risk |
| ---- | ----: | ----: | ----: | ----: | ----: | ---- |
| planner/query | high | low | large | high | 0 executor authority imports found | Low |
| executor/runtime | high | moderate query-plan contract imports | large | moderate | accepted-value validation still owns direct persisted-kind/value checks; projection dirty code has gated contract drift | Medium |
| cursor/continuation | high | low query-plan order-policy delegation | medium | high | 0 private plan internals found | Low |
| access/index | high | none to query/sql in this scan | large | high | index scalar encoding remains owner-local | Low-Medium |
| schema/catalog | high | model DTO imports expected | large | moderate-high | accepted persisted-kind semantics are schema-owned; generated models restricted to proposal/reconciliation/test convenience | Low-Medium |
| facade/adapters | medium | build/config/schema metadata | medium | moderate | no runtime semantic authority found, but session/facade dirty code is part of current compile failure context | Low-Medium |
| metrics/runtime | medium | metrics state bridge centralized in `metrics::sink` | medium | moderate | state bridge intentionally single | Medium |

Supporting guards passed: route-planner import boundary, layer-authority
invariants, architecture text-scan invariants, and module-structure hub
thresholds. The Rust compile check did not pass on the dirty snapshot.

## 9. Extension Path Rehearsal

| Probe | Ideal Owner-Local Path | Actual Current Path | Extra Owners Required | Main Blocker | Risk |
| ---- | ---- | ---- | ----: | ---- | ---- |
| New SQL aggregate terminal | parser/lowering admits syntax, planner owns `AggregateKind`, executor aggregate contracts consume it | parser or builder -> query plan model/semantics -> executor aggregate contracts/state/reducer -> explain/tests | 2-3 | aggregate kind is planner-owned but executor must update fold semantics | Medium |
| New cursor/order policy | query-plan order policy exports one validation contract, cursor owns token/runtime | query plan validate cursor policy -> cursor spine/boundary/runtime -> executor page/order consumers | 1-2 | delegated validation is clear; support remains DB-wide | Low-Medium |
| New persisted scalar kind | schema kind owner classifies persisted semantics, value owner adds runtime representation, codecs/index/executor validations adapt | `PersistedFieldKind` -> `PersistedFieldKindSemantics` -> schema SQL/predicate/relation/queryability helpers, then value/storage/index/executor accepted-value updates | 3-4 | classifier localizes semantic questions but not runtime value or codec storage behavior | Medium-High |
| New structural projection/delete result mode | projection owner exposes one stable materialization contract, delete/session call it | projection materialize/facade -> terminal row/layout contracts -> pipeline structural cursor page -> delete/session facade | 2-3 | current dirty code shows API drift across materialization, terminal, and facade call sites | Medium-High |
| New index scan route shape | query access planner selects path, executor route contracts dispatch to access/index | access choice -> route contracts -> stream/access scan -> index envelope/key semantics | 2-3 | route/index handoff is contract-based but still multi-owner | Medium |

## 10. Future Extension Friction Index

| Area | Score | Weight | Weighted Score | Evidence |
| ---- | ----: | ----: | ----: | ---- |
| future feature probe friction | 5 | 3 | 15 | persisted scalar-kind probe improved, but projection/materialization dirty code exposes a current fragile extension path |
| boundary leakage | 3 | 2 | 6 | no upward planner/executor or index/query authority inversion; guards passed |
| owner/contract clarity | 4 | 2 | 8 | accepted persisted-kind semantics and queryability are clear; executor projection/facade contracts are ambiguous in the dirty snapshot |
| gravity-well and hub containment | 5 | 2 | 10 | classifier is owner-clear; relation/index/schema/metrics large modules remain monitor items; projection shell currently fails compile |
| decision shock radius | 5 | 2 | 10 | selected persisted-kind decisions are centralized, but `Value`, codec/update validation, and projection materialization surfaces remain broad |
| subsystem independence | 4 | 1 | 4 | architecture guards pass; executor accepted-value and projection/facade contracts still require discipline |

Future extension friction index: `4.4/10`.

Interpretation: moderate future extension friction with a partial verification
status. The scalar-kind extension path improved because accepted queryability
now consumes the schema classifier. The current code’s bigger immediate
velocity warning is executor projection/materialization contract fragility: the
dirty snapshot fails compilation around gated exports and row/materialization
APIs.

Follow-up actions:

- owner boundary: `db::executor::projection` plus terminal/pipeline contracts;
  action: fix the current dirty compile failures by narrowing projection
  re-exports and aligning feature gates before adding more projection modes;
  target report date/run: next velocity or module-structure run touching
  projection.
- owner boundary: `db::executor::mutation::save_validation` plus
  `db::schema::field_kind_semantics`; action: leave accepted value-shape policy
  executor-owned, but consider classifier-backed scalar helpers only when the
  next real scalar-validation change appears; target report date/run: next
  velocity run touching accepted write validation.
- owner boundary: `value` and `db::data`/`db::index` codecs; action: before
  adding any new persisted scalar kind, list the exact value tag, comparison,
  storage codec, accepted validation, queryability, and index key update sites;
  target report date/run: next forward-looking velocity audit.

## 11. Non-Scoring Delivery Context

| Context Signal | Observation | Why Non-Scoring |
| ---- | ---- | ---- |
| same-day forward-looking rerun | `velocity-preservation-3.md` reported `4.3/10` under `VP-FEF-1.0` | prior score is not a scoring input; current code structure is the evidence |
| accepted queryability cleanup | current code routes executor accepted-kind queryability through `db::schema::field_kind_semantics` while retaining map/list/structured policy locally | counted only as current-code owner clarity, not as delivery speed or patch width |
| dirty projection/session/facade code | `cargo check -p icydb-core` fails on current dirty executor projection and feature-gated API drift | compile failure is a current-code verification and owner-boundary signal; file count and patch breadth are not score inputs |

## 12. Verification Readout

| Check | Status | Notes |
| ---- | ---- | ---- |
| score used only current-code evidence | PASS | no recent file-count, commit-size, or patch-width input contributed to the score |
| mandatory steps/tables present | PASS | report follows the `VP-FEF-1.0` output order |
| historical change data excluded from scoring | PASS | prior reports appear only as preamble/non-scoring context |
| dirty-worktree impact recorded | PASS | accepted queryability, executor projection/facade/pipeline/terminal/delete, predicate rewrite/render, diagnostics, facade response, fixture Cargo, and release metadata dirty files are acknowledged |
| route-planner import boundary guard | PASS | route planner root import families: `1` |
| layer-authority invariants | PASS | upward imports: `0`; cross-layer policy re-derivations: `0`; enum fan-out >2 layers: `1`; predicate boundary drift imports: `3` |
| architecture text-scan invariants | PASS | no include-str source text architecture scans detected |
| module-structure hub thresholds | PASS | configured hub thresholds verified |
| focused core compile check | FAIL | `cargo check -p icydb-core` fails in current dirty executor projection/materialization/facade code; first errors include duplicate `project_distinct`, feature-gated projection imports, missing `RetainedSlotLayout` re-export, and materialization row/API drift |
| focused semantic/schema/relation/executor tests | BLOCKED | not rerun after compile failure; current dirty crate does not pass the focused compile gate |
