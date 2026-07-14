# Velocity Preservation Audit - 2026-06-06 (Forward-Looking Rerun 4)

Preamble:

- scope: `docs/audits/recurring/crosscutting/crosscutting-velocity-preservation.md`
- prior non-scoring context report:
  `docs/audits/reports/2026-06/2026-06-06/velocity-preservation-4.md`
- code snapshot identifier: `2b97a0d33` with dirty working tree
- method tag/version: `VP-FEF-1.0`
- comparability status: method-compatible with the earlier same-day
  forward-looking velocity reports; scored sections below use only the current
  code shape.

## 1. Run Metadata + Method

| Method Component | Current |
| ---- | ---- |
| code snapshot identifier | `2b97a0d33` with dirty working tree |
| dirty-worktree status | affects audited surfaces: accepted persisted-kind semantics, executor accepted write validation, core DB public re-export facade, executor/query/schema/facade dirty context, audit fixtures, wasm audit artifacts, and release metadata; Cargo version files are dirty and left untouched |
| method tag/version | `VP-FEF-1.0` |
| subsystem taxonomy | planner/query, executor/runtime, cursor/continuation, access/index, storage/recovery, schema/catalog, SQL parser/lowering/session, facade/adapters, generated/test support |
| boundary crossing rule set | current-code import scans plus owner-contract triage |
| fan-in/fanout definition | runtime module references by file, tests excluded during triage |
| hub-family taxonomy | coordination hub, decision hub, mixed-owner hub, stable large module |
| decision-surface rule | change-relevant enum/case sites, not raw syntactic matches |
| facade/adapters inclusion | included because current dirty work touches core facade/session SQL response and integration fixture surfaces |

Scoring uses only current code shape. Accepted persisted-kind semantics now sit
under `db::schema::field_kind_semantics` and are consumed by SQL capability
checks, persisted schema predicate/literal/type helpers, relation key
validation, executor queryability, and executor accepted value-shape dispatch.
Executor write policy still owns exact runtime `Value` matching, scalar bounds,
collection recursion, and map/list admission.

The prior partial result was caused by current-code compile failures around
feature-gated projection/facade re-exports. The current snapshot now passes the
focused no-feature and SQL-feature core checks, so this rerun is scored as a
verified current-code pass.

## 2. Scope + Ownership Map

| Subsystem | Primary Owner Modules | Public/Crate Boundary | Runtime Authority | Notes |
| ---- | ---- | ---- | ---- | ---- |
| planner/query | `db::query::plan`, `db::query::intent`, `db::query::explain` | mixed `pub(in crate::db)` contracts plus public facade DTOs | logical query semantics and plan contracts | route-planner import guard reports one allowed executor-family crossing |
| executor/runtime | `db::executor::*` | `pub(in crate::db::executor)` children plus DB-facing contracts | physical route selection, projection, aggregation, paging, mutation execution | accepted queryability and value-shape category dispatch consume schema semantics; exact `Value` policy remains local |
| cursor/continuation | `db::cursor::*` | DB-wide cursor contracts plus public hex/string cursor facade | cursor token decode, validation, continuation runtime | no cursor authority leak found |
| access/index | `db::access::*`, `db::index::*` | DB-local access/index contracts | storage traversal, key encoding, index envelopes | index key encoding remains owner-clear and scalar-heavy |
| storage/recovery | `db::data`, `db::commit`, schema store/recovery code | DB-local storage contracts | persisted bytes, commit/recovery behavior | no query semantic import leakage found in this run |
| schema/catalog | `db::schema::*`, `model::*` | public model DTOs plus DB-local accepted snapshot authority | accepted schema snapshots and catalog-native mutation semantics | accepted schema snapshots remain durable runtime authority |
| SQL parser/lowering/session | `db::sql`, `db::sql_shared`, `db::session::sql` | SQL public facade plus session internals | user SQL surface and session dispatch | SQL capability checks consume accepted persisted-kind semantics |
| facade/adapters | root `db` facade, `icydb`, build/config adapters, integration harness | public facade re-exports plus generated/config APIs | generated endpoint emission and API convenience only | no-feature public facade now re-exports DTOs directly from owner-visible modules |
| generated/test support | fixtures, UI tests, audit canisters | test/generated boundaries | support and verification only | generated model semantics stay upstream input, not runtime truth |

## 3. Future Feature Probes

| Future Feature Probe | Expected Owner | Required Modules | Layers Crossed | Contract Blockers | Risk |
| ---- | ---- | ----: | ----: | ---- | ---- |
| Add a new SQL aggregate terminal | query planner + executor aggregate contracts | 4-6 semantic modules | 3 | aggregate taxonomy spans parser/lowering/planner/executor, but contract modules exist | Medium |
| Add a new cursor/order continuation policy | `db::cursor` with query-plan order contract | 3-4 semantic modules | 2 | delegated order validation is clear; executor page consumers still participate | Low-Medium |
| Add a new persisted scalar kind | schema/catalog + value/data codecs | 5-7 semantic modules | 3-4 | classifier localizes SQL, predicate, relation-key, queryability, and value-shape category decisions; runtime value tags, codecs, and index keys still need direct updates | Medium-High |
| Add a new structural projection/delete result mode | executor projection/delete/session contracts | 4-6 semantic modules | 2-3 | projection/materialization row contracts remain multi-module and easy to drift | Medium |
| Add a new index scan route shape | query access planner + executor route/access/index contracts | 5-7 semantic modules | 3 | `AccessPath` is localized but route/executor/index handoff still spans owners | Medium |

The persisted scalar-kind probe is now narrower: a future kind should first
land in schema-owned semantics, then only policy-owning layers should decide
their local behavior from that classification. The remaining cost is runtime
representation and storage/index behavior, not repeated raw kind re-matching
for query visibility.

## 4. Boundary Leakage

| Boundary | Mechanical Crossings | Allowed Contract Crossings | Suspect Crossings | Risk |
| ---- | ----: | ----: | ----: | ---- |
| planner/query -> executor runtime internals | 1 route-planner import family | 1 | 0 | Low |
| executor/runtime -> query/sql internals | broad query-plan contract references | plan/access/projection contracts | low suspect pressure | Low-Medium |
| index/access -> query/sql AST or lowering types | 0 | 0 | 0 | Low |
| cursor/continuation -> executable plan internals | DB-local plan/order contracts | cursor validation contracts | 0 | Low |
| storage/recovery -> query semantics | 0 | 0 | 0 | Low |
| generated/facade -> runtime semantic authority | facade/session response references only | facade/config contracts | 0 | Low |

No current-code crossing turns generated or facade code into runtime semantic
authority. The public facade fix is a visibility/export cleanup: `db::mod`
exposes selected public DTOs directly from owner-visible modules instead of
depending on feature-gated intermediate re-export chains.

## 5. Owner / Contract Clarity

| Surface | Owner | Contract Type | Ambiguity | Extension Impact | Risk |
| ---- | ---- | ---- | ---- | ---- | ---- |
| accepted persisted field-kind semantics | `db::schema::field_kind_semantics` | `subsystem-boundary` | low | SQL capabilities, schema predicate/literal helpers, relation key validation, executor queryability, and value-shape dispatch consume one accepted-schema semantic spine | Low |
| accepted write value policy | executor mutation validation consuming schema categories | `subsystem-boundary` | moderate | exact `Value` matching, numeric bounds, collection recursion, and map/list admission stay executor-owned | Medium |
| accepted schema snapshots | `db::schema` | `subsystem-boundary` | low | schema mutation remains catalog-native; generated models feed proposals/tests only | Low |
| core DB public facade | `db::mod` directly re-exporting owner-visible public DTOs | `facade-public` | low-medium | no-feature and SQL-feature builds now share explicit export paths, but the root remains a broad public facade | Low-Medium |
| `Value` and data/index codecs | `value`, `db::data`, `db::index` | mixed DB-local runtime authority | moderate-high | new runtime scalar behavior still requires coordinated value, storage, comparison, and index key updates | High |
| executor projection/materialization facade | `db::executor::projection` plus terminal/pipeline contracts | `crate-boundary` | moderate | compile now passes, but projection row/materialization contracts remain a likely extension friction point | Medium |
| query-plan DTOs consumed by executor | planner/query plus executor contract shims | `crate-boundary` | moderate | executor feature authors still need contract-module discipline | Medium |
| generated endpoint switches | build/config facade | `facade-public` + `generated-boundary` | low | adding endpoint classes is review-wide but owner-clear | Medium |

## 6. Gravity Wells + Hub Containment

| Module | Class | LOC | Fan-In | Fanout | Domains | Owner Clarity | Risk |
| ---- | ---- | ----: | ----: | ----: | ----: | ---- | ---- |
| `db::schema::field_kind_semantics` | decision hub | 455 | low | 1 | 1 | schema-owned accepted-kind semantic classifier | Low |
| `db::executor::mutation::save_validation` | stable large module | 995 | medium | 7 | 1 | executor-owned write preflight; category dispatch delegates to schema semantics | Low-Medium |
| `db::mod` | facade coordination hub | 575 | high | 5 | 1 | public DB facade; direct owner re-exports reduce feature-gate fragility | Low-Medium |
| `db::query::plan::mod` | coordination hub | 614 | high | 5 | 1 | plan root and validation contracts; still broad but owner-clear | Medium |
| `db::session::sql::execute::mod` | coordination hub | 960 | medium | 4 | 1 | SQL execution shell | Low-Medium |
| `db::relation::reverse_index` | stable large module | 1257 | medium | 5 | 1 | relation-owned but large; scalar fast path remains local | Medium |
| `db::index::key::build` | stable large module | 1292 | medium | 5 | 1 | index-owned but scalar-heavy | Medium |
| `db::schema::info` | stable large module | 1103 | high | 5 | 2 | schema-owned projection hub | Medium |
| `metrics::sink` | decision hub | 843 | medium | 3 | 2 | clear bridge, large event dispatch | Medium |

| Hub Module | Contract Boundary | Cross-Layer Families | Allowed Max | Status | Risk |
| ---- | ---- | ----: | ----: | ---- | ---- |
| `crates/icydb-core/src/db/schema/field_kind_semantics.rs` | accepted persisted kind -> schema/relation/SQL/executor consumers | 1 DB-local family | 1 | PASS | Low |
| `crates/icydb-core/src/db/executor/mutation/save_validation.rs` | accepted write preflight -> schema kind categories and runtime `Value` policy | 1 DB-local family | 1 | PASS with monitor | Low-Medium |
| `crates/icydb-core/src/db/mod.rs` | public DB facade -> owner-visible public DTOs | 1 DB-local facade family | 1 | PASS with monitor | Low-Medium |
| `crates/icydb-core/src/db/session/sql/execute/mod.rs` | SQL session shell -> write/explain/diagnostics children | 1 primary family | 1 per child owner | PASS | Low-Medium |
| `crates/icydb-core/src/db/cursor/mod.rs` | plan/order contracts -> cursor validation/runtime | 2 delegated families | 2 | PASS | Low |
| `crates/icydb-core/src/metrics/sink.rs` | instrumentation events -> metrics state | 1 state bridge, many event cases | 1 bridge | PASS with monitor | Medium |

The classifier remains a narrow decision hub. The root DB facade is still broad
by design, but the current cleanup makes its public handoff explicit instead
of relying on intermediate feature-gated module roots to carry public exports.

## 7. Decision Shock Radius

| Decision Surface | Variants / Cases | Change-Relevant Sites | Modules | Subsystems | Shock Radius | Risk |
| ---- | ----: | ----: | ----: | ----: | ----: | ---- |
| `Value` | 24 variants | high | 360+ runtime files mention `Value` | 6+ | high | High |
| `FieldKind` / `PersistedFieldKind` | 32 variants each | medium-high | many runtime files mention either kind; selected accepted-schema decisions now flow through the classifier | 5+ | medium | Medium |
| `PersistedFieldKindSemantics` | 21 scalar classes plus collection/relation/structured categories | moderate | 5 production consumer families plus tests | 2 | medium-low | Low-Medium |
| projection materialization contracts | several row/layout/result shapes | moderate | projection, terminal, pipeline, delete/session facade | 2-3 | medium | Medium |
| `AggregateKind` | 8 variants | moderate | 70+ runtime files mention it | 4 | medium-high | Medium |
| `AccessPath` / `AccessPathKind` | 7 path families | moderate | 30+ runtime files mention it | 2 | medium | Medium |
| `ContinuationMode` | small route continuation taxonomy | low | route/planning-local | 1 | low | Low |

Persisted-kind shock radius is now materially lower for classification
questions: queryability, comparison eligibility, relation key eligibility, SQL
capability, and write value-shape dispatch share one schema-owned view. Runtime
`Value`, storage codec, index key, and projection/materialization decisions are
still the higher-cost extension surfaces.

## 8. Subsystem Independence

| Subsystem | Internal Imports | External Imports | LOC | Independence | Private Decision Imports | Risk |
| ---- | ----: | ----: | ----: | ----: | ----: | ---- |
| planner/query | high | low | large | high | 0 executor authority imports found | Low |
| executor/runtime | high | moderate query-plan contract imports | large | moderate | accepted-value policy remains local; kind classification now consumed from schema | Medium |
| cursor/continuation | high | low query-plan order-policy delegation | medium | high | 0 private plan internals found | Low |
| access/index | high | none to query/sql in this scan | large | high | index scalar encoding remains owner-local | Low-Medium |
| schema/catalog | high | model DTO imports expected | large | moderate-high | accepted persisted-kind semantics are schema-owned; generated models restricted to proposal/reconciliation/test convenience | Low |
| facade/adapters | medium | build/config/schema metadata | medium | moderate | no runtime semantic authority found; root facade exports are explicit | Low-Medium |
| metrics/runtime | medium | metrics state bridge centralized in `metrics::sink` | medium | moderate | state bridge intentionally single | Medium |

Supporting guards passed: route-planner import boundary, layer-authority
invariants, architecture text-scan invariants, and module-structure hub
thresholds. Both focused Rust compile configurations pass in the current dirty
snapshot.

## 9. Extension Path Rehearsal

| Probe | Ideal Owner-Local Path | Actual Current Path | Extra Owners Required | Main Blocker | Risk |
| ---- | ---- | ---- | ----: | ---- | ---- |
| New SQL aggregate terminal | parser/lowering admits syntax, planner owns `AggregateKind`, executor aggregate contracts consume it | parser or builder -> query plan model/semantics -> executor aggregate contracts/state/reducer -> explain/tests | 2-3 | aggregate kind is planner-owned but executor must update fold semantics | Medium |
| New cursor/order policy | query-plan order policy exports one validation contract, cursor owns token/runtime | query plan validate cursor policy -> cursor boundary/runtime -> executor page/order consumers | 1-2 | delegated validation is clear; support remains DB-wide | Low-Medium |
| New persisted scalar kind | schema kind owner classifies persisted semantics, value owner adds runtime representation, codecs/index/executor validations adapt | `PersistedFieldKind` -> `PersistedFieldKindSemantics` -> schema SQL/predicate/relation/queryability/value-shape consumers, then value/storage/index exact behavior | 3-4 | classifier localizes semantic questions but not runtime value or codec storage behavior | Medium-High |
| New structural projection/delete result mode | projection owner exposes one stable materialization contract, delete/session call it | projection materialize/facade -> terminal row/layout contracts -> pipeline structural cursor page -> delete/session facade | 2-3 | materialization row contracts remain multi-module | Medium |
| New index scan route shape | query access planner selects path, executor route contracts dispatch to access/index | access choice -> route contracts -> stream/access scan -> index envelope/key semantics | 2-3 | route/index handoff is contract-based but still multi-owner | Medium |

## 10. Future Extension Friction Index

| Area | Score | Weight | Weighted Score | Evidence |
| ---- | ----: | ----: | ----: | ---- |
| future feature probe friction | 5 | 3 | 15 | new scalar kinds still cross value/codec/index/executor, but repeated persisted-kind classification work is now centralized |
| boundary leakage | 3 | 2 | 6 | no upward planner/executor or index/query authority inversion; guards passed |
| owner/contract clarity | 3 | 2 | 6 | accepted persisted-kind semantics are clear and facade export paths compile in no-feature and SQL-feature builds |
| gravity-well and hub containment | 5 | 2 | 10 | classifier is owner-clear; relation/index/schema/metrics large modules remain monitor items |
| decision shock radius | 4 | 2 | 8 | selected persisted-kind decisions are centralized; `Value`, codec, index key, and projection materialization surfaces remain broad |
| subsystem independence | 4 | 1 | 4 | architecture guards pass; executor value policy and projection contracts still require discipline |

Future extension friction index: `4.1/10`.

Interpretation: moderate current-code future extension friction with a verified
pass. The strongest improvement is that persisted-kind classification is now a
schema-owned runtime contract consumed by policy-owning layers. The remaining
velocity risks are real but narrower: new runtime values, storage codecs, index
keys, and projection/materialization result modes still require coordinated
owner changes.

Follow-up actions:

- owner boundary: `value` plus `db::data`/`db::index` codecs; action: before
  adding a persisted scalar kind, list the exact value tag, accepted validation,
  comparison, storage codec, and index key update sites; target report date/run:
  next velocity run touching scalar persistence.
- owner boundary: `db::executor::projection` plus terminal/pipeline contracts;
  action: keep materialization row/result contracts narrow before adding another
  structural projection mode; target report date/run: next velocity or module
  structure run touching projection.
- owner boundary: `db::relation::reverse_index`, `db::index::key::build`,
  `db::schema::info`, and `metrics::sink`; action: monitor large owner-clear
  hubs for decision families that deserve their own child modules; target
  report date/run: next recurring structure/velocity sweep.

## 11. Non-Scoring Delivery Context

| Context Signal | Observation | Why Non-Scoring |
| ---- | ---- | ---- |
| same-day forward-looking rerun | `velocity-preservation-4.md` reported `4.4/10` with partial verification | prior score is not a scoring input; current code structure is the evidence |
| persisted-kind classifier cleanup | current code routes SQL capability, schema predicate helpers, relation validation, executor queryability, and executor value-shape dispatch through `db::schema::field_kind_semantics` | counted only as current-code owner clarity and decision shock radius, not as delivery speed or patch width |
| no-feature public facade fix | current code re-exports public diagnostics, numeric projection helpers, and `PlanError` from owner-visible modules | counted as current-code owner/contract clarity and verification status, not as a history delta |
| dirty broad worktree | many unrelated files remain dirty, including Cargo manifests and wasm audit artifacts | dirty breadth is recorded for context but does not raise or lower the score |

## 12. Verification Readout

| Check | Status | Notes |
| ---- | ---- | ---- |
| score used only current-code evidence | PASS | no recent file-count, commit-size, or patch-width input contributed to the score |
| mandatory steps/tables present | PASS | report follows the `VP-FEF-1.0` output order |
| historical change data excluded from scoring | PASS | prior reports appear only as non-scoring context |
| dirty-worktree impact recorded | PASS | accepted semantics, executor validation, facade re-exports, broad runtime dirty context, fixtures, audit artifacts, and release metadata are acknowledged |
| route-planner import boundary guard | PASS | route planner root import families: `1` |
| layer-authority invariants | PASS | upward imports: `0`; cross-layer policy re-derivations: `0`; enum fan-out >2 layers: `1`; predicate boundary drift imports: `3` |
| architecture text-scan invariants | PASS | no include-str source text architecture scans detected |
| module-structure hub thresholds | PASS | configured hub thresholds verified |
| focused core compile check | PASS | `cargo check -p icydb-core` passes |
| focused SQL-feature core compile check | PASS | `cargo check -p icydb-core --features sql` passes |
| focused semantic/schema/relation/executor tests | PASS | `persisted_field_kind_` focused tests pass under `--features sql` |
