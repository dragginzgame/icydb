# Complexity / Surface Hygiene Audit - icydb-schema + derive - 2026-05-21

## Run Metadata

| Field [M/C] | Value |
| ---- | ---- |
| `method_version` | `CSH-1.0` |
| `surface_taxonomy` | `ST-1` |
| `authority_taxonomy` | `AT-1` |
| `deletion_confidence_model` | `DC-1` |
| `compatibility_policy` | `pre-1.0-hard-cut` |
| `wasm_signal_rule` | `raw-wasm-primary` |
| `baseline_report` | `docs/audits/reports/2026-05/2026-05-20/complexity-surface-hygiene-icydb-schema.md` |
| `comparability_status` | `non-comparable`; this run expands from schema-only plus sampled derive to full schema + full derive inspection after the `ic-memory` / Canic alignment |
| `code_snapshot` | `396da4bd9` |
| `in_scope_roots` | `crates/icydb-schema/src`, `crates/icydb-schema-derive/src` |
| `excluded_roots` | `target`, generated build output, historical docs/changelogs, downstream generated canisters except reference scans |
| `generated_code_inclusion` | `sampled`; quote targets, generated schema nodes, generated `CanisterKind`, and generated `IndexModel` wiring inspected |
| `test_surface_inclusion` | `sampled`; in-crate tests inspected where they are the only visible consumer |

## Step Status

| Step [M] | Status [C] | Evidence Artifact [M/C] | Comparability Impact [C] |
| ---- | ---- | ---- | ---- |
| STEP 0 | PASS | metadata table above | non-comparable due expanded derive scope |
| STEP 1 | PASS | root exports, proc-macro entrypoints, schema node facade, derive node/imp facades | expanded scope |
| STEP 2 | PASS | stale-signal scans for compatibility/fallback/shim/deprecated/model symbols and memory policy | expanded scope |
| STEP 3 | PASS | runtime authority drift review across schema, derive, build, and core declaration paths | expanded scope |
| STEP 4 | PASS | branch hotspot and low-consumer public helper review | expanded scope |
| STEP 5 | PASS | generated-boundary review for schema nodes, build registry, generated `CanisterKind`, and generated model metadata | expanded scope |
| STEP 6 | PASS | cfg/test/diagnostics scan | expanded scope |
| STEP 7 | PASS | removal safety plan below | expanded scope |
| STEP 8 | PASS | risk bucket table below | expanded scope |

## Footprint Summary

| Metric [M] | Value [D] | Notes [C] |
| ---- | ----: | ---- |
| total schema + derive LOC | 16,048 | `wc -l crates/icydb-schema/src/**/*.rs crates/icydb-schema-derive/src/**/*.rs` |
| `icydb-schema` LOC | 3,962 | unchanged order of magnitude from 2026-05-20 |
| `icydb-schema-derive` LOC | 12,086 | now fully in scope |
| schema public/scoped-public declarations | 271 | same mechanical count as prior schema run |
| derive public/scoped-public declarations | 238 | mostly crate-internal proc-macro implementation surface |
| largest file | 2,519 LOC | `crates/icydb-schema-derive/src/node/field.rs` |
| next largest files | 990 / 875 / 731 LOC | derive `imp/runtime_value.rs`, `node/index.rs`, `node/entity.rs` |
| highest branch hotspot | 157 branch-ish sites | derive `node/field.rs`; default/database-default generation dominates |
| in-crate unit tests | 18 + 78 | `icydb-schema` + `icydb-schema-derive` |

## Reachable Surface Inventory Summary

| Item [M] | Kind [M] | Path [M] | Visibility [M] | Feature/Cfg [M] | Consumer Evidence [M/C] | Surface Class [C] | Owner [C] | Risk [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| schema crate modules | root exports | `crates/icydb-schema/src/lib.rs` | public `build/error/node/types/validate/visit` modules | none | `icydb` facade, derive generated code, and `icydb-build` consume schema/build/node surface | mixed | schema/build boundary | Medium |
| schema node facade | module re-exports | `crates/icydb-schema/src/node/mod.rs` | public `pub use *` over schema node descriptors | none | derive emits `::icydb::schema::node::*`; build/core consume node metadata | live-generated-boundary | schema AST | Low |
| build registry | global schema access | `crates/icydb-schema/src/build/mod.rs` | public `schema_write`, `get_schema`; crate-only `schema_read` | none | derive registers nodes; `icydb-build` reads validated schema | live-generated-boundary | build codegen | Low |
| derive proc macros | macro entrypoints | `crates/icydb-schema-derive/src/lib.rs` | public proc macro attributes | proc-macro crate | user schema attributes and downstream generated canisters | live-generated-boundary | macro boundary | Low |
| derive node facade | internal re-exports | `crates/icydb-schema-derive/src/node/mod.rs` | `pub use` inside proc-macro crate | none | proc-macro prelude imports broad `node::*` | overexposed-internal | derive parser/codegen | Medium |
| derive imp facade | internal re-exports | `crates/icydb-schema-derive/src/imp/mod.rs` | `pub use` inside proc-macro crate | none | trait generation prelude imports broad `imp::*` | overexposed-internal | derive implementation generators | Medium |
| memory policy helpers | constants/functions | `crates/icydb-schema/src/node/mod.rs`, `crates/icydb-schema-derive/src/validate/memory.rs` | public in both crates | none | schema and derive both validate app memory IDs and key segments | duplicate-surface | memory allocation policy | Medium |
| stable-key formatter | helper + literal format | `crates/icydb-schema/src/node/store.rs`, `crates/icydb-schema-derive/src/node/canister.rs` | public helper plus derive literal format | none | build consumes schema allocation keys; derive emits `COMMIT_STABLE_KEY` | duplicate-surface | memory allocation key generation | Medium |
| schema validation namespace | module/function | `crates/icydb-schema/src/validate/mod.rs`, `validate/naming.rs` | public module/function | none | only schema validation path uses it in-tree | overexposed-internal | schema validation | Medium |
| schema graph helpers | methods | `crates/icydb-schema/src/node/schema.rs` | public methods | none | `get_type`, `check_node_as`, `get_node_values` have no visible in-tree consumers | orphaned-helper / overexposed-internal | schema graph | Medium |
| concrete validation visitor | struct/methods | `crates/icydb-schema/src/visit.rs` | public `ValidateVisitor` | none | only `validate/mod.rs` constructs it | overexposed-internal | schema validation traversal | Medium |
| stable allocation DTO | DTO + accessors | `crates/icydb-schema/src/node/store.rs` | public type/methods | none | build consumes `memory_id`/`stable_key`; metadata accessors mostly tests/future-facing | overexposed-internal | stable memory codegen | Medium |
| derive generated model output | quote target | `crates/icydb-schema-derive/src/node/index.rs`, `imp/inherent/entity.rs` | generated output | none | generated `EntityModel`/`IndexModel` proposal metadata | live-generated-boundary | generated schema proposal | Low |

## Dead / Stale Candidate Table

| Candidate [M] | File [M] | Lines [M] | Signal [M] | Current Consumers [M/C] | Surface Class [C] | Authority Reason [C] | Deletion Confidence [C] | Risk If Removed [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| duplicated app-memory range and reserved-id policy | schema `node/mod.rs`, derive `validate/memory.rs` | schema `57-58`, derive `1-24` | two local copies of `100..=254` and `255` rejection after `ic-memory` is current | schema runtime validation and derive-time diagnostics | duplicate-surface | IcyDB still needs early diagnostics, but generic memory ID policy now also exists in `ic-memory` runtime validation | medium | drift between macro-time, schema-time, and runtime validation |
| duplicated stable-key segment grammar | schema `node/mod.rs`, derive `validate/memory.rs` | schema `176-218`, derive `33-38` | same canonical segment helper body; full key validator only schema-side | derive validates attributes; schema validates generated nodes | duplicate-surface | early validation is useful, but grammar should have one owner or parity guard | medium | compile-time accepts could diverge from schema/build/runtime rejects |
| commit stable-key literal formatter | derive `node/canister.rs` | `102-108` | derives `format!("icydb.{}.commit.control.v1", ...)` separately from schema `stable_memory_key()` | generated `CanisterKind::COMMIT_STABLE_KEY`; build gets schema `commit_stable_key()` | duplicate-surface | key identity is durable ABI, so formatting duplication is higher-cost than ordinary helper duplication | medium | generated and schema declaration keys can drift silently |
| public schema validation namespace | schema `validate/mod.rs`, `validate/naming.rs` | `3`, `7` | public helper has only internal call path | `validate_schema -> validate_global -> naming::validate_entity_naming` | overexposed-internal | validation is invoked through `build::get_schema`, not a facade API | medium | unknown out-of-tree direct users |
| `SchemaNode::get_type` | schema `node/schema.rs` | `28-43` | no in-tree references found | none found in `crates/*` | orphaned-helper | no current generated/runtime authority role found | medium | unknown out-of-tree schema introspection users |
| `Schema::check_node_as` / `get_node_values` | schema `node/schema.rs` | `156-172` | no in-tree references found | none found in `crates/*` | overexposed-internal | active graph consumers use `cast_node`, `get_nodes`, and `filter_nodes` | medium | unknown out-of-tree schema introspection users |
| public `ValidateVisitor` concrete type | schema `visit.rs` | `40-107` | concrete visitor exposed although only internal validation constructs it | `validate/mod.rs` only | overexposed-internal | traversal trait remains live; concrete validation visitor does not need public reachability | medium | possible out-of-tree diagnostics users |
| schema crate dependency on `canic-cdk` for timestamp | schema `node/schema.rs`, `Cargo.toml` | `now_secs` import | schema crate depends on Canic CDK only to initialize schema timestamp | `Schema::new()` | overexposed dependency surface | schema metadata timestamp is not Canic-runtime policy | medium | changing timestamp source could affect build/schema metadata expectations |
| derive `node::field` mega-module | derive `node/field.rs` | `2,519 LOC`, `157 branch-ish sites` | field parsing, Rust defaults, database defaults, binary default encoding, and tests in one file | heavily used by entity/record/newtype generation | live-generated-boundary | not dead, but complexity is concentrated behind one generated-boundary owner | low for deletion, medium for split | broad macro behavior risk if split carelessly |
| derive `imp::runtime_value` mega-module | derive `imp/runtime_value.rs` | `990 LOC`, `71 branch-ish sites` | enum/record/map/list/set runtime value and persisted structured codec generation in one file | generated runtime value impls | live-generated-boundary | not dead; owns codec generation surface | low for deletion, medium for split | generated codec regressions |
| derive `node::index` core-coupled parser/generator | derive `node/index.rs` | `875 LOC`, `75 branch-ish sites` | macro-time filtered-index SQL parsing and generated `IndexModel` tokenization | generated index metadata | live-generated-boundary | generated models are allowed for proposal/reconciliation; not a runtime fallback | low | macro validation/index naming regressions |

No stale compatibility branch, deprecated shim, or generated-model runtime fallback was found. The `fallback` hits in derive are local trait/default strategy wording, not compatibility shims.

## Runtime Authority Drift Findings

| Area [C] | Runtime Authority [C] | Alternate Authority Found? [C] | Evidence [M/C] | Allowed Role? [C] | Finding [C] | Risk [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| schema acceptance | accepted runtime schema snapshots | No | schema crate builds proposal-time nodes; core owns accepted schema runtime | yes | no runtime fallback authority in schema crate | Low |
| generated models | proposal/codegen metadata only | No | derive emits `EntityModel`/`IndexModel`; no schema crate runtime reconstruction path found | yes | generated model use is macro/proposal boundary, not runtime authority | Low |
| stable memory declarations | `ic-memory` runtime validation plus generated declarations | Partial | schema/derive still locally validate memory IDs and stable-key syntax; build emits `ic_memory_*` declarations | yes, but duplicated | early diagnostics remain useful, but local copies should be centralized or guarded against `ic-memory` behavior drift | Medium |
| Canic coupling | Canic/CDK runtime lifecycle outside schema | Partial | `icydb-schema` imports `canic_cdk::utils::time::now_secs` only for schema timestamp | unclear | schema crate still has a Canic dependency unrelated to memory declarations | Medium |
| SQL DDL | catalog-native mutation in core | No | schema/derive have no SQL DDL lowering code | yes | no drift found | Low |
| endpoint names | generated `__icydb_*` endpoint names | No | schema/derive do not expose endpoint override metadata | yes | no endpoint-name shim surface | Low |
| persisted decoding | core codecs remain bounded/fallible | No | derive calls core `__macro` encode/decode helpers for generated defaults/codecs | yes | no ad hoc persisted decoder found in schema crates | Low |

## Complexity Tied To Dead Surface

| Module [M] | Complexity Signal [M] | Dead-Surface Link [C] | Public/Hidden Items [M] | Current Consumers [M/C] | Shrink Action [C] | Expected Blast Radius [C] | Risk [C] |
| ---- | ---- | ---- | ----: | ---- | ---- | ---- | ---- |
| `schema::node` memory policy | public constants/helpers plus crate-private validators | duplicates derive memory validation and partly overlaps `ic-memory` generic policy | 2 public constants, 2 public key helpers, 4 crate-private validators | schema node validation/tests | centralize through one schema memory-policy module or add parity guards against derive and `ic-memory` | schema + derive validation | Medium |
| `derive::validate::memory` | public helpers inside proc-macro crate | duplicates schema policy and has slightly different message wording | 5 public helpers/constants | store/canister derive validation | import shared policy where practical or add parity tests | macro diagnostics | Medium |
| commit stable-key generation | schema helper vs derive literal format | durable ABI formatting duplicated | 1 public helper plus derive literal | build declarations and generated `CanisterKind` | centralize formatter or add explicit parity test | generated memory declarations | Medium |
| `schema::validate` | public namespace for internal validation | no in-tree external consumer | 1 public module + function | `build::get_schema` validation path | make module private after owner approval | schema facade | Medium |
| `schema::node::schema` helpers | public type-erasure conveniences | no in-tree consumer for some helpers | 3 suspect public methods | none found for listed helpers | delete or narrow unused graph helpers | possible external introspection | Medium |
| `schema::visit::ValidateVisitor` | public concrete visitor | internal-only constructor/consumer | 1 public concrete struct | schema validation only | make concrete visitor crate-private; keep `Visitor` trait if intended facade | validation traversal | Medium |
| `derive::node::field` | large branch-heavy generated boundary | no dead surface, but multiple concerns retained in one module | many crate-local methods | broad derive generation | split by concern only after behavior-free test guard | macro expansion | Medium |
| `derive::imp::runtime_value` | large branch-heavy generated boundary | no dead surface, but mixed generated codec owners | public strategy structs | generated trait impls | split enum/record/collection codec generation when next touched | generated codecs | Medium |
| `icydb-schema` Canic timestamp dependency | package dependency for one function | dependency surface wider than schema ownership needs | 1 imported function | `Schema::new()` | replace with schema/local time source if no Canic-specific reason remains | schema metadata construction | Medium |

## Facade / Generated-Boundary Findings

| Surface [M] | Boundary Type [C] | Generated Consumer Evidence [M/C] | Could Narrow? [C] | Required Replacement [C] | Deletion Confidence [C] | Risk [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| `schema::node::*` | generated code target | derive emits concrete `::icydb::schema::node::*` constructors | no broad narrowing without derive rewrite | none | low | High |
| `schema::build::schema_write` | generated registration | derive emits `schema_write().insert_node(...)` | no | none | low | High |
| `schema::build::get_schema` | build codegen input | `icydb-build` calls `get_schema()` | no | none | low | High |
| derive proc-macro functions | user macro API | public attribute macros | no | none | low | High |
| derive `node::*` / `imp::*` internal facades | proc-macro implementation convenience | crate-local prelude and generator modules consume broad re-exports | yes | narrower crate-local imports by owner | medium | Medium |
| memory policy helpers | schema/derive policy helpers | store/canister validation and tests | yes | shared policy module or parity tests | medium | Medium |
| generated `EntityModel` / `IndexModel` quote output | generated proposal metadata | generated entity/index model constants | no for current architecture | accepted schema reconciliation still needs proposal metadata | low | High |
| `schema::visit::Visitor` | possible schema traversal facade | schema nodes implement traversal through trait | unclear | owner decision on public traversal API | blocked | Medium |

## Feature / Diagnostics / Test Surface Review

| Surface [M] | Feature/Cfg [M] | Production Consumer? [M/C] | Test/Diagnostics Consumer? [M/C] | Visibility Could Narrow? [C] | Action [C] | Risk [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| schema tests | `#[cfg(test)]` | no | yes, 18 tests | no production impact | keep | Low |
| derive tests | `#[cfg(test)]` | no | yes, 78 tests | no production impact | keep | Low |
| `#[expect(clippy::too_many_arguments)]` helper | `#[cfg(test)]` | no | naming test helper only | no production impact | keep | Low |
| `#[expect(clippy::struct_excessive_bools)]` | derive item struct | yes | no | no | retained parse-shape exception | Low |
| feature-gated diagnostics | none found | N/A | N/A | N/A | none | Low |
| `#[doc(hidden)]` / `__macro` exports in schema crates | none found | N/A | N/A | N/A | none | Low |

## Removal Safety Plan

| Candidate [M] | Action [C] | Owner Boundary [C] | Required Proof [C] | Focused Validation [C] | Wasm Raw Bytes Relevant? [C] | Follow-Up Required? [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| duplicated memory ID/range policy | consolidate or guard | schema/derive memory validation and `ic-memory` declaration policy | prove 100-254 and 255 rejection cannot drift between derive, schema, and runtime declaration validation | `cargo test -p icydb-schema`; `cargo test -p icydb-schema-derive`; macro validation tests | no | yes |
| duplicated stable-key segment/full-key grammar | consolidate or guard | stable allocation identity | prove segment grammar, `canic.` rejection, dot segmentation, and `.v1` generation stay aligned | same plus generated canister declaration compile | no | yes |
| commit stable-key literal format | centralize or parity-test | generated `CanisterKind` + schema allocation declarations | generated constant and schema `Canister::commit_stable_key()` match for same namespace | schema canister test plus derive unit/trybuild test | no | yes |
| schema `validate` namespace | narrow visibility | schema validation facade | no downstream in-repo consumer; owner confirms no public facade promise | `cargo test -p icydb-schema`; `cargo check -p icydb-build` | no | yes |
| unused schema graph helpers | delete or narrow | schema graph | `rg` proves no generated/build/core consumer | `cargo test -p icydb-schema`; `cargo check -p icydb-build` | no | yes |
| public concrete `ValidateVisitor` | make crate-private | schema validation traversal | keep traversal trait if public traversal is intended; narrow concrete validator only | `cargo test -p icydb-schema` | no | yes |
| `canic-cdk::utils::time::now_secs` dependency | replace or justify | schema metadata construction | decide whether schema timestamp needs Canic CDK; if not, move to local/std time helper | `cargo check -p icydb-schema`; workspace check if dependency removed | no | yes |
| derive `node::field` split | defer until behavior work or dedicated slice | derive field/default generation | split without semantic changes and keep default encoding tests green | `cargo test -p icydb-schema-derive node::field`; macro tests | no | yes, but not release-blocking |

## Risk Score

Overall risk index: `5/10`.

The risk is moderate: no stale runtime authority or compatibility shim was found, but `ic-memory` alignment makes the remaining local memory-policy duplication more visible, and the derive crate has concentrated generated-boundary complexity that will slow future schema/default work if it keeps growing.

| Bucket [C] | Count [D] | Highest Risk [C] | Notes [C] |
| ---- | ----: | ---- | ---- |
| stale compatibility | 0 | Low | no legacy/compat/shim/deprecated branch found |
| stale generated fallback | 0 | Low | generated models remain proposal/codegen metadata, not runtime fallback |
| orphaned helper | 1 | Medium | `SchemaNode::get_type` has no in-tree consumer |
| overexposed internal | 7 | Medium | validation namespace, graph helpers, concrete visitor, derive facades, allocation metadata, Canic timestamp dependency |
| duplicate surface | 4 | Medium | memory range/reserved policy, stable-key segment grammar, commit key formatting, schema/derive message divergence |
| unclear | 2 | Medium | public traversal API intent and allocation schema metadata intent |

## Verification Readout

| Check [M] | Result [C] | Notes [C] |
| ---- | ---- | ---- |
| stale-signal scan | PASS | no dead-code allowances, deprecated shims, compatibility branches, or runtime model fallback found |
| public-surface scan | PASS | 271 schema public/scoped-public declarations; 238 derive public/scoped-public declarations |
| memory-policy scan | PASS | local schema/derive policy duplication confirmed after `ic-memory = 0.6.1` / Canic `0.40.15` alignment |
| hotspot scan | PASS | derive field/default/runtime-value/index modules dominate local branch and LOC concentration |
| `cargo check -p icydb-schema` | PASS | package compiles |
| `cargo test -p icydb-schema` | PASS | 18 unit tests; doc tests empty |
| `cargo check -p icydb-schema-derive` | PASS | package compiles |
| `cargo test -p icydb-schema-derive` | PASS | 78 unit tests; doc tests empty |
| `cargo clippy -p icydb-schema -p icydb-schema-derive --all-targets -- -D warnings` | PASS | schema crates lint clean |

## Follow-Up Actions

1. Memory policy owner: either centralize derive/schema memory ID and stable-key validation around one shared policy source, or add explicit parity guards against `ic-memory` declaration validation.
2. Stable key ABI: add a parity test proving derive `COMMIT_STABLE_KEY` generation matches schema `Canister::commit_stable_key()` for the same namespace.
3. Dependency surface: decide whether `icydb-schema` still needs `canic-cdk` for `now_secs()`; if not, replace it with a schema/local time helper and remove that dependency edge.
4. Schema facade narrowing: make `schema::validate` private and remove/narrow unused schema graph helpers if no public facade owner claims them.
5. Visitor narrowing: keep `Visitor` public only if direct schema traversal is intended; make the concrete `ValidateVisitor` crate-private.
6. Derive modularity: split `node/field.rs` by field parsing, Rust defaults, and database-default encoding in a dedicated behavior-preserving slice if 0.160/0.161 continues schema-default work.
