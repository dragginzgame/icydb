# Complexity / Surface Hygiene Audit - icydb - 2026-05-21

## Run Metadata

| Field [M/C] | Value |
| ---- | ---- |
| `method_version` | `CSH-1.1` |
| `surface_taxonomy` | `ST-1` |
| `authority_taxonomy` | `AT-1` |
| `deletion_confidence_model` | `DC-1` |
| `compatibility_policy` | `pre-1.0-hard-cut` |
| `wasm_signal_rule` | `raw-wasm-primary` |
| `baseline_report` | `N/A` |
| `comparability_status` | `non-comparable`; first focused `icydb` facade run under CSH-1.1 |
| `code_snapshot` | `11b77ad92` plus dirty worktree |
| `in_scope_roots` | `crates/icydb/src`, `crates/icydb/Cargo.toml`, `crates/icydb/README.md`, sampled generated-code and CLI consumers |
| `excluded_roots` | `target`, generated build output, historical changelog entries |
| `generated_code_inclusion` | `sampled`; `icydb-build`, `icydb-derive`, and `icydb-schema-derive` references to facade paths inspected |
| `test_surface_inclusion` | `sampled`; facade crate tests and downstream macro/CLI consumers inspected where they justify public surface |

## Step Status

| Step [M] | Status [C] | Evidence Artifact [M/C] | Comparability Impact [C] |
| ---- | ---- | ---- | ---- |
| STEP 0 | PASS | metadata table above | first CSH-1.1 run |
| STEP 1 | PASS | crate-root facade inventory, generated-code path scan, CLI/test consumer scan | non-comparable with prior informal cleanup |
| STEP 2 | PASS | stale-signal scan for dead-code, compatibility, fallback, shim, deprecated, `EntityModel`, `IndexModel`, `StorageKey`, and low-level storage traits | one stale facade export removed |
| STEP 3 | PASS | accepted-schema, generated-model, SQL DDL, and generated endpoint authority review | none |
| STEP 4 | PASS | facade module complexity reviewed against live user/generated/tooling boundaries | none |
| STEP 5 | PASS | `__macro`, `__reexports`, `db`, `model`, `metrics`, `traits`, and prelude boundaries reviewed | none |
| STEP 6 | PASS | SQL/diagnostics/test surfaces reviewed under feature gates | none |
| STEP 7 | PASS | one high-confidence stale facade export removed; no other removal candidate found | none |
| STEP 8 | PASS | risk score table below | none |

## Footprint Summary

| Metric [M] | Value [D] | Notes [C] |
| ---- | ----: | ---- |
| total inspected LOC | 7,924 | source plus manifest and README |
| source LOC | 7,891 | public facade, base helpers, SQL rendering, response/session wrappers |
| largest source file | 896 | `db/session/mod.rs`; public session wrapper and SQL attribution |
| public facade module groups | 13 | build/schema/macros/types/value/model/metrics/visitor/base/db/traits/preludes/errors |
| hidden generated wiring modules | 2 | `__macro` and `__reexports` |
| generated-code consumers sampled | 3 crates | `icydb-build`, `icydb-derive`, `icydb-schema-derive` |
| host-tool consumers sampled | 1 crate | `icydb-cli` SQL, metrics, snapshot, schema surfaces |

## Reachable Surface And Retention Inventory Summary

| Item [M] | Kind [M] | Path [M] | Visibility [M] | Feature/Cfg [M] | Consumer Evidence [M/C] | Authority Reason [C] | Surface Class [C] | Owner [C] | Risk [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| `build` / `build_with_options` | crate re-export + macro re-export | `crates/icydb/src/lib.rs:63` | public | build script | canister `build.rs` files call `icydb::build::BuildOptions` and `icydb::build_with_options!` | downstream canisters depend on the facade crate instead of direct build-crate wiring | live-generated-boundary | build facade | Low |
| `schema` / `macros` | crate re-exports | `crates/icydb/src/lib.rs:65` | public | schema/design time | macro tests and generated schema code target `::icydb::schema` and `::icydb::macros` | one public dependency gives user code schema nodes and derive macros | live-generated-boundary | schema facade | Low |
| `types` | core module re-export | `crates/icydb/src/lib.rs:70` | `#[doc(hidden)]` module, public path | none | user code and macro tests use `icydb::types::{Id, Ulid, Timestamp, Decimal, ...}` | primitive/runtime type surface must be reachable through the facade and prelude | live-authority | type facade | Low |
| `value` | module re-export | `crates/icydb/src/lib.rs:72` | public | none | preludes export `InputValue` and `OutputValue`; SQL/schema helpers use value DTOs | public value input/output bridge remains useful without exposing storage-key compatibility names | live-authority | value facade | Low |
| `model` | hidden model re-export | `crates/icydb/src/lib.rs:80` | `#[doc(hidden)]` | generated/schema tests | schema derive emits model constants through `::icydb::model::*`; macro tests inspect model-only structural metadata | generated `EntityModel` / `IndexModel` remain proposal/model-only surfaces, not runtime authority | live-generated-boundary | generated model facade | Low |
| `metrics` | hidden metrics re-export | `crates/icydb/src/lib.rs:104` | `#[doc(hidden)]` | generated endpoint/tooling | generated metrics endpoints call `icydb::metrics`; CLI decodes `EventReport` | generated observability endpoints and CLI reports need stable DTO paths | live-diagnostics | metrics facade | Low |
| `visitor` | module re-export | `crates/icydb/src/lib.rs:111` | public | design/runtime validation | macro-generated validate/sanitize impls and tests use `icydb::visitor` | validation/sanitization visitor contracts are public design-time extension points | live-authority | visitor facade | Low |
| `base` | public module | `crates/icydb/src/base/mod.rs` | public | design-time helpers | macro tests and design prelude use base validators, sanitizers, and type markers | validators/sanitizers are explicit user-facing schema design helpers | live-authority | design helper facade | Low |
| `db` | public module | `crates/icydb/src/db/mod.rs` | public | runtime; `sql` feature for SQL module | canister user code calls `db().load/insert/delete`; CLI and generated endpoints use SQL/result DTOs | public database session/query/response facade hides core session internals | live-authority | DB facade | Low |
| `traits` | public module | `crates/icydb/src/traits.rs` | public | user/generic types | user code imports semantic `Entity`, `EntityFor`, `CreateInput`, and schema-facing traits; generated `EntityValue` now targets `__macro` | semantic traits remain public while low-level value-projection ownership stays out of the normal traits facade | live-authority | trait facade | Low |
| `Error` family | types | `crates/icydb/src/error.rs` | public | none | generated endpoints and user code return `icydb::Error` | facade-owned public error taxonomy isolates `icydb-core::InternalError` | live-diagnostics | error facade | Low |
| `__macro` | hidden module | `crates/icydb/src/lib.rs:137` | `#[doc(hidden)]` | generated code | derive/build crates emit many `::icydb::__macro::*` references | generated code needs a narrow, explicit non-semver-stable runtime wiring lane | live-generated-boundary | macro wiring facade | Low |
| `__reexports` | hidden module | `crates/icydb/src/lib.rs:192` | `#[doc(hidden)]` | generated code | generated actor/schema derive code targets `canic_cdk`, `serde`, `ctor`, `ic_memory`, and derive helper crates through this path | generated code should compile without users adding every transitive crate manually | live-generated-boundary | dependency wiring facade | Low |
| `prelude` | public module | `crates/icydb/src/lib.rs:208` | public | user runtime | downstream canisters use `icydb::prelude::*` | canister actor code needs concise runtime imports without exposing storage internals directly | live-authority | runtime prelude | Low |
| `design::prelude` | public module | `crates/icydb/src/lib.rs:234` | public | schema/design time | macro tests and schema code use it | schema authors need validators, macros, traits, and base helpers in one design-time import | live-authority | design prelude | Low |
| `start!` / `db!` | macros | `crates/icydb/src/lib.rs:276`, `:287` | exported macro | generated actor/user code | canister crates call `icydb::start!()`; user code can call `db!()` | actor glue inclusion and session access are facade responsibilities | live-generated-boundary | actor facade | Low |
| top-level `sanitize` / `validate` | functions | `crates/icydb/src/lib.rs:296`, `:303` | public | user/runtime validation | macro tests call `icydb::sanitize` and `icydb::validate` | public convenience wrapper converts core visitor errors into `icydb::Error` | live-authority | validation facade | Low |

## Dead / Stale Candidate Table

| Candidate [M] | File [M] | Lines [M] | Signal [M] | Current Consumers [M/C] | Authority Reason [C] | Surface Class [C] | Deletion Confidence [C] | Risk If Removed [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| `StorageKey`, `StorageKeyDecodeError`, `StorageKeyEncodeError` facade value exports | `crates/icydb/src/lib.rs` | `74-75` | post-0.159 key vocabulary audit; `rg` found no in-tree `icydb::value::StorageKey` consumer | none in live in-tree facade/generated/tooling consumers | core still owns `StorageKey` as decoded scalar compatibility value, but facade should not expose it as normal public `value` vocabulary | stale-compatibility | high | removed; compile validation covers in-tree generated/tooling consumers |
| `EntityValue` public trait path | `crates/icydb/src/traits.rs`, `crates/icydb/src/lib.rs` design prelude | former public re-export and design-prelude import | low-level trait surface signal | generated schema derive now emits `::icydb::__macro::EntityValue`; tests use the hidden generated lane for contract checks | generated code still needs the trait, but it belongs in `__macro`, not the normal semantic traits facade | overexposed-internal | high | removed from `icydb::traits` and `design::prelude`; generated code retargeted to `__macro` |
| `PersistedRow` facade path | `crates/icydb/src/db/mod.rs` | former hidden `db` re-export | low-level storage trait signal | `icydb-derive` and schema derive now emit `::icydb::__macro::PersistedRow`; structural field tests use the hidden generated lane | generated persistence impls need the trait, but it belongs in `__macro`, not the public DB facade | overexposed-internal | high | removed from `icydb::db`; generated code retargeted to `__macro` |
| `SlotReader`, `SlotWriter`, and `InternalError` generated signatures | `crates/icydb/src/db/mod.rs` | former hidden `db` re-exports | low-level generated wiring signal | generated persisted-row and structured-codec paths now emit `::icydb::__macro::{SlotReader, SlotWriter, InternalError}` | generated code needs these core plumbing types, but they belong in `__macro`, not the normal DB facade | overexposed-internal | high | removed from `icydb::db`; generated code and macro tests retargeted to `__macro` |
| `EntityModel` / `IndexModel` hidden facade path | `crates/icydb/src/lib.rs` | `80-100` | generated-model signal | schema derive emits model constants; macro tests inspect model-only structural metadata | generated models are proposal/model-only and do not rebuild runtime accepted schema authority | live-generated-boundary | low | deleting breaks derive output and tests; not runtime authority drift |

No runtime generated-model fallback reconstruction, endpoint-name override shim, stale SQL DDL authority, or duplicate public query/session surface was found in the `icydb` facade.

## Runtime Authority Drift Findings

| Area [C] | Runtime Authority [C] | Alternate Authority Found? [C] | Evidence [M/C] | Allowed Role? [C] | Finding [C] | Risk [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| accepted schema | accepted schema snapshots in `icydb-core` sessions | No | facade `model` exports are hidden/generated-boundary only; SQL/schema endpoints call session/core methods | Yes | facade does not reconstruct runtime authority from generated models | Low |
| SQL DDL | catalog-native core mutation runners | No | facade SQL module converts and renders already-executed core SQL outputs | Yes | SQL facade is endpoint/result surface, not mutation authority | Low |
| generated endpoints | fixed `__icydb_*` generated names | No | build and derive crates target `__macro`, `__reexports`, `db`, `metrics`, and fixed endpoint names | Yes | low-level generated persistence/value plumbing now uses `__macro`; no endpoint `name = ...` override lane in facade | Low |
| primary-key storage vocabulary | compact key taxonomy in core | No after patch | `StorageKey` removed from `icydb::value`; core still retains internal decoded compatibility type | Yes | facade no longer advertises legacy storage-key value surface | Low |
| metrics/snapshot/schema | generated endpoint gates and CLI DTOs | No | hidden metrics and public DB DTOs are consumed by generated endpoints/CLI | Yes | observability DTOs remain diagnostics surfaces only | Low |

## Complexity Tied To Dead Surface

| Module [M] | Complexity Signal [M] | Dead-Surface Link [C] | Public/Hidden Items [M] | Current Consumers [M/C] | Shrink Action [C] | Expected Blast Radius [C] | Risk [C] |
| ---- | ---- | ---- | ----: | ---- | ---- | ---- | ---- |
| `lib.rs` facade re-exports | broad facade hub | stale `StorageKey` and generated-only persistence/value/slot facade exposure removed or retargeted | crate re-exports, hidden generated modules, preludes, macros | generated code, canister code, CLI, macro tests | move generated-only plumbing to `__macro` and remove storage-key facade export | low; generated/test consumers retargeted | Low |
| `db/session/*` | large public query/write wrapper | none found | session/query/mutation methods | canister code and tests | none | N/A | Low |
| `db/sql/*` | render/table payload helpers | none found | public SQL result/render surface under `sql` feature | generated SQL endpoints and CLI shell rendering | none | N/A | Low |
| `base/*` | many validator/sanitizer/type marker structs | none found in this pass | public design-time helper modules | design prelude and macro tests | none | N/A | Low |
| `traits.rs` | semantic wrappers plus schema-facing trait re-exports | no stale low-level generated trait exposure after `EntityValue` removal | public trait facade | user generic bounds, schema-facing generated traits | none remaining | N/A | Low |

## Facade / Generated Boundary Findings

| Surface [M] | Boundary Type [C] | Generated Consumer Evidence [M/C] | Could Narrow? [C] | Required Replacement [C] | Deletion Confidence [C] | Risk [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| `__macro` | generated wiring lane | derive/build crates emit direct `::icydb::__macro::*` references | No | coordinated generated-code retargeting | low | High |
| `__reexports` | generated dependency lane | generated code uses facade paths for `canic_cdk`, `serde`, `ctor`, `ic_memory`, `icydb_derive` | No | users would need explicit transitive dependencies or derive code would need direct crate paths | low | High |
| `__macro::PersistedRow`, slot traits, stores | hidden generated storage lane | derive-generated persistence impls now target `__macro::{PersistedRow, SlotReader, SlotWriter, InternalError}` | No immediate need | none | low | Low |
| `__macro::EntityValue` | generated entity trait lane | schema derive now emits `::icydb::__macro::EntityValue` | No | none | low | Low |
| `model::*` | generated model lane | schema derive emits `MODEL` constants using model DTOs | No | schema derive architecture change | low | Medium |
| SQL rendering DTOs | endpoint/tooling surface | generated SQL endpoints return `SqlQueryResult`; CLI renders it | No | duplicate DTOs or CLI/core coupling | low | Medium |

## Feature / Diagnostics / Test Surface Review

| Surface [M] | Feature/Cfg [M] | Production Consumer? [M/C] | Test/Diagnostics Consumer? [M/C] | Visibility Could Narrow? [C] | Action [C] | Risk [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| SQL module | `feature = "sql"` | generated SQL endpoints | CLI shell and SQL tests | No | retain | Low |
| SQL perf attribution DTOs | `feature = "sql"` and optionally `diagnostics` | generated diagnostics endpoints when enabled | perf audit tests | No | retain | Low |
| diagnostics metrics re-exports | `feature = "diagnostics"` | opt-in diagnostics builds | CLI/test metrics consumers | No | retain | Low |
| `StorageKey` facade export | none | no in-tree consumer | no in-tree consumer | Yes | removed | Low |
| `EntityValue` / `PersistedRow` normal facade exports | none | no normal user-facing consumer after retarget | generated and structural tests use `__macro` | Yes | moved to hidden generated lane | Low |
| `SlotReader` / `SlotWriter` / `InternalError` normal DB facade exports | none | no normal user-facing consumer after retarget | generated and structural tests use `__macro` | Yes | moved to hidden generated lane | Low |
| facade tests | `#[cfg(test)]` | no | error/sql/base behavior tests | No production exposure | retain | Low |

## Removal Safety Plan

| Candidate [M] | Action [C] | Owner Boundary [C] | Required Proof [C] | Focused Validation [C] | Wasm Raw Bytes Relevant? [C] | Follow-Up Required? [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| `icydb::value::{StorageKey, StorageKeyDecodeError, StorageKeyEncodeError}` | removed | facade value surface | repo search found no in-tree facade consumer; core still owns compatibility type internally | `cargo check/test/clippy -p icydb`; downstream generated/tooling checks as needed | no | no |
| `icydb::traits::EntityValue` and `icydb::db::PersistedRow` | removed/retargeted | hidden generated-code lane | derive/build references retargeted to `::icydb::__macro`; macro tests retargeted to hidden generated lane | facade, derive, schema-derive, and macro-test checks | no | no |
| `icydb::db::{SlotReader, SlotWriter, InternalError}` | removed/retargeted | hidden generated-code lane | generated persisted-row and structured-codec references retargeted to `::icydb::__macro`; macro tests retargeted to hidden generated lane | facade, derive, schema-derive, macro-test, and workspace checks | no | no |

## Risk Score

| Bucket [C] | Count [D] | Highest Risk [C] | Notes [C] |
| ---- | ----: | ---- | ---- |
| stale compatibility | 1 | Low | stale storage-key facade export removed |
| stale generated fallback | 0 | N/A | hidden model facade is generated/proposal support only |
| orphaned helper | 0 | N/A | no unowned facade helper found |
| overexposed internal | 0 | N/A | `EntityValue`, `PersistedRow`, `SlotReader`, `SlotWriter`, and generated `InternalError` usage moved to hidden `__macro` lane |
| duplicate surface | 0 | N/A | no duplicate public query/session route found |
| unclear | 0 | N/A | no owner decision required for this slice |

Dead-surface pressure score: **1/10** after the storage-key facade cut and generated plumbing retarget. Remaining facade complexity is live user/runtime, diagnostics, or generated endpoint surface.

## Verification Readout

| Check [M] | Result [M/C] |
| ---- | ---- |
| public facade inventory via `rg` | pass |
| stale-signal scan via `rg` | pass; stale `StorageKey`, `EntityValue`, `PersistedRow`, `SlotReader`, `SlotWriter`, and generated `InternalError` facade exposure removed/retargeted |
| generated-code consumer scan | pass; derive paths now target `__macro` for low-level generated plumbing |
| CLI/tooling consumer scan | pass |
| `cargo check -p icydb` | pass |
| `cargo test -p icydb` | pass; 46 unit tests, 0 doc tests |
| `cargo clippy -p icydb --all-targets -- -D warnings` | pass |
| `cargo check -p icydb-derive` | pass |
| `cargo check -p icydb-schema-derive` | pass |
| `cargo test -p icydb-schema-tests` | pass; generated macro fixtures and compile-fail suite |
| `cargo clippy -p icydb -p icydb-derive -p icydb-schema-derive --all-targets -- -D warnings` | pass |
| `cargo check --workspace` | pass |
| `cargo fmt --all --check` | pass |
| `git diff --check` | pass |

## Follow-Up Actions

None for this facade slice. Future generated-code cleanup can still reduce direct `db` references for SQL/session DTOs if a concrete stale surface appears, but no current deletion candidate remains under CSH-1.1.
