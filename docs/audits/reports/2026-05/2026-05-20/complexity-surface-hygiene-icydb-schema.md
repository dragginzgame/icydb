# Complexity / Surface Hygiene Audit - icydb-schema - 2026-05-20

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
| `comparability_status` | `comparable-refresh`; same focused crate and method as earlier 2026-05-20 run |
| `code_snapshot` | `ce24f4323` plus dirty worktree |
| `in_scope_roots` | `crates/icydb-schema/src` |
| `excluded_roots` | `target`, generated build output, historical docs/changelogs |
| `generated_code_inclusion` | `sampled`; `crates/icydb-schema-derive/src` quote targets and macro-time validation inspected as schema-node consumers |
| `test_surface_inclusion` | `sampled`; in-crate tests inspected where they are the only visible consumer |

## Step Status

| Step [M] | Status [C] | Evidence Artifact [M/C] | Comparability Impact [C] |
| ---- | ---- | ---- | ---- |
| STEP 0 | PASS | metadata table above | comparable refresh |
| STEP 1 | PASS | root exports, node facade exports, derive/build consumer scans | none |
| STEP 2 | PASS | stale-signal scan for dead-code, compatibility, fallback, shim, deprecated, `EntityModel`, `IndexModel` | none |
| STEP 3 | PASS | schema authority drift review across build, derive, and core proposal/runtime consumers | duplicate macro/runtime memory validation noted |
| STEP 4 | PASS | low-consumer public helper review | new public memory-policy helpers/constants included |
| STEP 5 | PASS | generated-boundary quote target review in `icydb-schema-derive` | none |
| STEP 6 | PASS | cfg/test-surface scan | none |
| STEP 7 | PASS | removal safety plan for narrowable candidates | none |
| STEP 8 | PASS | risk bucket table | duplicate-policy bucket increased |

## Footprint Summary

| Metric [M] | Value [D] | Notes [C] |
| ---- | ----: | ---- |
| total schema LOC | 3,962 | `wc -l crates/icydb-schema/src/**/*.rs crates/icydb-schema/src/*.rs` |
| public/scoped-public declarations | 271 | unchanged from previous run |
| largest file | 469 LOC | `node/canister.rs` |
| next largest files | 310 / 308 LOC | `node/index.rs`, `node/store.rs` |
| in-crate unit tests | 18 | all pass |

## Reachable Surface Inventory Summary

| Item [M] | Kind [M] | Path [M] | Visibility [M] | Feature/Cfg [M] | Consumer Evidence [M/C] | Surface Class [C] | Owner [C] | Risk [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| crate modules | root exports | `crates/icydb-schema/src/lib.rs:1` | `pub mod build/error/node/types/validate/visit` | none | facade re-export via `icydb::schema`; build/derive use `build` and `node` | mixed | schema/build boundary | Medium |
| schema prelude | `pub mod prelude` | `crates/icydb-schema/src/lib.rs:25` | public, with `schema_read` crate-only | none | node modules and derive helper imports rely on grouped node/type/error exports | live-generated-boundary | schema derive boundary | Low |
| node facade | module re-exports | `crates/icydb-schema/src/node/mod.rs:35` | `pub use *` over concrete schema nodes | none | derive emits `::icydb::schema::node::*`; core proposal/fingerprint consume index/field metadata | live-generated-boundary | macro/schema AST | Low |
| build registry | global schema access | `crates/icydb-schema/src/build/mod.rs:23` | `schema_write`/`get_schema` public, `schema_read` crate-only | none | derive registers nodes through `schema_write`; `icydb-build` reads validated schema through `get_schema` | live-generated-boundary | build codegen | Low |
| validation namespace | module/function | `crates/icydb-schema/src/validate/mod.rs:3`, `crates/icydb-schema/src/validate/naming.rs:7` | public module and public function | none | only called by crate-private `validate_schema`; no in-tree external consumers | overexposed-internal | schema validation | Medium |
| memory policy constants | constants | `crates/icydb-schema/src/node/mod.rs:57` | `APP_MEMORY_ID_MIN/MAX` public | none | schema runtime validation uses them; derive has independent duplicate constants | duplicate-policy-surface | schema memory validation | Medium |
| stable-key helpers | functions | `crates/icydb-schema/src/node/mod.rs:195` | public | none | schema runtime validation uses helpers; derive duplicates segment canonicality helper | duplicate-policy-surface | schema memory validation | Medium |
| schema graph helpers | methods | `crates/icydb-schema/src/node/schema.rs:28`, `:156`, `:168` | public methods | none | `get_type`, `check_node_as`, `get_node_values` have no in-tree consumers outside their own declarations | overexposed-internal | schema graph | Medium |
| visitor validation helper | struct/methods | `crates/icydb-schema/src/visit.rs:40` | public `ValidateVisitor` | none | only `validate/mod.rs` constructs it; `Visitor` trait itself is used by node traversal | overexposed-internal | schema validation traversal | Medium |
| stable memory allocation details | DTO + constructor/accessors | `crates/icydb-schema/src/node/store.rs:139` | public type and methods | none | `icydb-build` uses allocation accessors for `memory_id`/`stable_key`; constructor, schema metadata accessors, and identity comparator are only in crate/tests | overexposed-internal | stable memory codegen | Medium |
| tests | test modules | `crates/icydb-schema/src/**` | `#[cfg(test)]` | test only | 18 unit tests exercise error, memory, index, and naming behavior | live-test-support | crate tests | Low |

## Dead / Stale Candidate Table

| Candidate [M] | File [M] | Lines [M] | Signal [M] | Current Consumers [M/C] | Surface Class [C] | Authority Reason [C] | Deletion Confidence [C] | Risk If Removed [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| public validation namespace | `crates/icydb-schema/src/validate/mod.rs`, `crates/icydb-schema/src/validate/naming.rs` | `3`, `7` | public helper has only internal call path | `validate_schema -> validate_global -> naming::validate_entity_naming`; no external in-tree references | overexposed-internal | validation is invoked through `build::get_schema`, not direct facade calls | medium | breaking unknown out-of-tree direct users of `icydb::schema::validate::naming` |
| duplicated app-memory range constants | `crates/icydb-schema/src/node/mod.rs`, `crates/icydb-schema-derive/src/validate/memory.rs` | `57-58`, `2-3` | two crates carry `100..=254` independently | schema validation and derive validation both enforce the same range | duplicate-policy-surface | one app memory policy should own the range value; currently schema and derive can drift | medium | derive-time and runtime validation messages/behavior can diverge |
| duplicated stable-key segment canonicality | `crates/icydb-schema/src/node/mod.rs`, `crates/icydb-schema-derive/src/validate/memory.rs` | `195-201`, `33-39` | same helper body in schema and derive | derive validates macro inputs; schema validates generated nodes | duplicate-policy-surface | canonical stable-key syntax is one policy but implemented twice | medium | compile-time accepts could differ from runtime schema validation |
| public stable-key full validator helper | `crates/icydb-schema/src/node/mod.rs` | `203-218` | public helper has no in-tree external consumer | only internal schema validation/tests use it | overexposed-internal | full stable-key validation is schema validation policy, not a facade need identified by this audit | medium | possible out-of-tree validation helper users |
| public stable-memory key formatter | `crates/icydb-schema/src/node/store.rs` | `192` | public helper has only internal schema-node consumers | `Store::allocation`, `Canister::commit_stable_key`; derive duplicates commit-key format string | duplicate-policy-surface | stable key format is shared policy but not centralized across derive/schema | medium | future key-format drift between generated constants and schema node accessors |
| unused schema type-erasure helper | `crates/icydb-schema/src/node/schema.rs` | `28-43` | no references to `get_type()` | none found in `crates/*` | orphaned-helper | no current generated/runtime authority role found | medium | possible out-of-tree schema introspection users |
| unused schema graph convenience methods | `crates/icydb-schema/src/node/schema.rs` | `156-172` | no references to `check_node_as` or `get_node_values` | none found in `crates/*` | overexposed-internal | `cast_node`, `get_nodes`, and `filter_nodes` are the active codegen/validation accessors | medium | possible out-of-tree schema introspection users |
| public `ValidateVisitor` concrete type | `crates/icydb-schema/src/visit.rs` | `40-107` | concrete validation visitor exposed although only internal validation constructs it | `validate/mod.rs` only | overexposed-internal | visitor traversal remains live; the concrete validating visitor does not need facade reachability | medium | possible out-of-tree diagnostics using schema visitor directly |
| allocation metadata constructor/accessors | `crates/icydb-schema/src/node/store.rs` | `139-188` | public DTO exposes schema-version/fingerprint metadata that current codegen does not consume | build consumes `memory_id`/`stable_key`; tests consume constructor and `same_identity_as` | overexposed-internal | current codegen authority is stable memory id/key; accepted runtime schema owns schema version/fingerprint | low | public type is returned by public `Store` methods; narrowing needs owner decision |

No `stale-compatibility`, `stale-generated-fallback`, deprecated shim, or direct `EntityModel`/`IndexModel` runtime fallback signal was found in `crates/icydb-schema/src`.

## Runtime Authority Drift Findings

| Area [C] | Runtime Authority [C] | Alternate Authority Found? [C] | Evidence [M/C] | Allowed Role? [C] | Finding [C] | Risk [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| schema acceptance | accepted runtime schema snapshots | No | `icydb-schema` builds proposal-time node graph; `icydb-core` consumes generated model metadata to propose/accept schema | yes | no runtime fallback authority in `icydb-schema` | Low |
| generated models | proposal/codegen metadata only | No | no `EntityModel` or `IndexModel` symbols in `crates/icydb-schema/src` | yes | no generated-model reconstruction path | Low |
| stable memory id ownership | `ic-memory` runtime registry plus schema/codegen declarations | Partial | schema and derive enforce app range/canonical key rules; generated code declares via `ic_memory_range!`, `ic_memory_declaration!`, `ic_memory_key!` | yes, but duplicated | `icydb-schema` must still validate IcyDB's generated allocation config, but derive/schema copies should not drift | Medium |
| SQL DDL | catalog-native mutation in core | No | `icydb-schema` has no SQL DDL lowering code | yes | out of scope for schema crate | Low |
| endpoint names | generated `__icydb_*` names | No | `icydb-schema` has no endpoint name metadata or override code | yes | no endpoint-name shim surface | Low |
| persisted decoding | core codecs remain bounded/fallible | No | `icydb-schema` contains schema AST/build metadata, not persisted decoders | yes | no drift found | Low |

## Complexity Tied To Dead Surface

| Module [M] | Complexity Signal [M] | Dead-Surface Link [C] | Public/Hidden Items [M] | Current Consumers [M/C] | Shrink Action [C] | Expected Blast Radius [C] | Risk [C] |
| ---- | ---- | ---- | ----: | ---- | ---- | ---- | ---- |
| `node::mod` memory policy | constants/helpers mixed into node facade | public `APP_MEMORY_ID_*` and stable-key helpers are not consumed externally; derive duplicates similar logic | 4 public declarations plus crate-private validators | schema runtime validation, derive-side copy | move policy to one shared helper module or make schema helpers crate-private and derive reuse where feasible | schema + derive validation | Medium |
| `node::store` / derive canister | stable-key format in two places | schema `stable_memory_key()` and derive commit-key `format!` duplicate the key grammar | 1 public formatter plus derive string format | store/canister schema nodes, generated `CanisterKind` impl | centralize formatter or add a guard test proving derive/schema key parity | derive/schema boundary | Medium |
| `validate` | public submodule for one internal pass | direct validation entrypoint is private; submodule leaks globally | 2 public declarations | only `validate_schema` calls naming pass | make module private and function `pub(crate)`/private | schema crate plus facade API surface | Medium |
| `node::schema` | type-erasure and graph convenience methods | some public helpers have no visible consumers | 1 public enum, 1 public graph type, several methods | build uses `get_nodes`/`filter_nodes`; validation uses `cast_node`; listed helpers unused | owner decision, then delete unused helpers | possible out-of-tree schema introspection | Medium |
| `visit` | concrete validating visitor exported | public concrete visitor only supports internal validation run | 1 public enum, 1 public trait, 1 public struct | only validation module uses `ValidateVisitor`; traversal traits live | narrow concrete `ValidateVisitor` before narrowing `Visitor` | crate-private validation path | Medium |
| `node::store` allocation DTO | stable allocation DTO carries unused metadata accessors | constructor/schema metadata accessors are not consumed by build | 3 public allocation role/type/helper surfaces | build consumes returned allocation id/key | owner decision; keep id/key accessors, narrow constructor/test-only helpers if approved | codegen and tests | Medium |

## Facade / Generated-Boundary Findings

| Surface [M] | Boundary Type [C] | Generated Consumer Evidence [M/C] | Could Narrow? [C] | Required Replacement [C] | Deletion Confidence [C] | Risk [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| `schema::node::*` | generated code target | derive emits `::icydb::schema::node::{Def, Field, Index, Store, Canister, SchemaNode, ...}` | no broad narrowing without derive rewrite | none | low | High if removed |
| `schema::build::schema_write` | generated code registration | derive emits `::icydb::schema::build::schema_write().insert_node(...)` | no | none | low | High if removed |
| `schema::build::get_schema` | build codegen input | `icydb-build` calls `get_schema()` before codegen | no | none | low | High if removed |
| `schema::node::stable_key_segment_is_canonical` | public helper | derive does not use it; derive has its own helper | yes | crate-private helper or shared validation module | medium | Low in-tree, unknown external |
| `schema::node::APP_MEMORY_ID_*` | public constants | derive does not use them; derive has its own constants | yes | shared constants or derive import | medium | Low in-tree, unknown external |
| `schema::validate::naming` | facade-leaked validation helper | no generated consumer found | yes | private call from `validate_global` | medium | Low in-tree, unknown external |
| `schema::visit::ValidateVisitor` | concrete diagnostics helper | no generated consumer found | yes | crate-private validating visitor | medium | Low in-tree, unknown external |

## Feature / Diagnostics / Test Surface Review

| Surface [M] | Feature/Cfg [M] | Production Consumer? [M/C] | Test/Diagnostics Consumer? [M/C] | Visibility Could Narrow? [C] | Action [C] | Risk [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| in-crate test modules | `#[cfg(test)]` | no | yes, 18 unit tests | no production impact | keep | Low |
| `#[expect(clippy::too_many_arguments)]` test helper | `#[cfg(test)]` | no | yes, naming tests | no production impact | keep | Low |
| memory validation tests | `#[cfg(test)]` | no | yes, canister memory/id/key tests | no production impact | keep; add derive/schema parity guard if duplication remains | Low |
| feature-gated diagnostics | none found | N/A | N/A | N/A | none | Low |
| `#[doc(hidden)]` / `__macro` exports | none in `icydb-schema` | N/A | N/A | N/A | none | Low |

## Removal Safety Plan

| Candidate [M] | Action [C] | Owner Boundary [C] | Required Proof [C] | Focused Validation [C] | Wasm Raw Bytes Relevant? [C] | Follow-Up Required? [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| duplicated memory range constants | consolidate or guard | schema derive/runtime validation | prove derive-time and runtime validation use the same `100..=254` source or have a parity test | `cargo test -p icydb-schema`; derive trybuild/macro validation tests | no | yes |
| duplicated stable-key grammar | consolidate or guard | schema derive/runtime validation | prove macro-time segment validation and runtime key validation cannot drift; ensure `canic.` rejection stays runtime/generation-owned as intended | `cargo test -p icydb-schema`; macro validation tests | no | yes |
| `stable_memory_key` formatter duplication | consolidate or guard | stable key generation | derive `COMMIT_STABLE_KEY` and `Canister::commit_stable_key()` must produce identical keys | schema canister tests plus generated canister compile | no | yes |
| public validation namespace | narrow visibility | schema validation/build | change `pub mod validate` to private if no facade policy reason; make naming function private or `pub(crate)` | `cargo test -p icydb-schema`; `cargo check -p icydb-schema-derive -p icydb-build` once adjacent core compiles | no | yes |
| `SchemaNode::get_type` | delete or make crate-private | schema graph | prove no generated output or build consumer requires it | same as above plus `rg get_type` | no | yes |
| `Schema::check_node_as` / `get_node_values` | delete or make crate-private | schema graph | prove no generated output or build consumer requires them | same as above plus focused consumer scan | no | yes |
| public `ValidateVisitor` concrete type | make crate-private | schema validation traversal | keep `Visitor` trait only if external traversal remains desired | `cargo test -p icydb-schema` | no | yes |
| allocation metadata constructor/accessors | owner decision before change | stable memory codegen | decide whether schema version/fingerprint are future-facing public metadata or internal-only allocation bookkeeping | `cargo check -p icydb-build`; schema allocation tests | no | yes |

## Risk Score

Overall risk index: `4/10`.

| Bucket [C] | Count [D] | Highest Risk [C] | Notes [C] |
| ---- | ----: | ---- | ---- |
| stale compatibility | 0 | Low | no legacy/compat/fallback/shim/deprecated signals found |
| stale generated fallback | 0 | Low | no `EntityModel`/`IndexModel` runtime fallback in crate |
| orphaned helper | 1 | Medium | `SchemaNode::get_type` has no in-tree consumer |
| overexposed internal | 5 | Medium | validation namespace, graph helpers, concrete visitor, allocation metadata, full stable-key helper |
| duplicate policy surface | 3 | Medium | memory ID range, stable-key segment grammar, stable-key formatting |
| unclear | 1 | Medium | allocation metadata may be future-facing or accidental public surface |

## Verification Readout

| Check [M] | Result [C] | Notes [C] |
| ---- | ---- | ---- |
| stale-signal scan | PASS | no dead-code allowances, compatibility/fallback/shim/deprecated markers, hidden exports, or model-fallback symbols found in `icydb-schema/src` |
| public-surface scan | PASS | 271 public or scoped-public declarations found in `crates/icydb-schema/src` |
| generated-boundary scan | PASS | derive/build consumers account for core node/build surfaces; duplicate memory policy noted |
| `cargo check -p icydb-schema` | PASS | crate compiles |
| `cargo test -p icydb-schema` | PASS | 18 unit tests, doc tests empty |
| `cargo check -p icydb-schema-derive -p icydb-build` | BLOCKED | current dirty `icydb-core` fails first on missing `INDEX_COMPONENT_MAX_SIZE`, `INDEX_PRIMARY_KEY_MAX_SIZE`, and `CompactStoreKeyDecodeError` variants in `db/key_taxonomy.rs` |

## Follow-Up Actions

1. Consolidate or guard the duplicated schema/derive memory validation policy (`APP_MEMORY_ID_MIN/MAX`, reserved ID checks, and stable-key segment grammar).
2. Add a derive/schema parity check for commit stable-key formatting if the derive-side literal `format!("icydb.{}.commit.control.v1", ...)` remains separate from `stable_memory_key()`.
3. Narrow the validation namespace if no owner wants `icydb::schema::validate::naming` as a public pre-1.0 facade.
4. Review unused schema graph helpers (`get_type`, `check_node_as`, `get_node_values`) for deletion in a small follow-up patch.
5. Decide whether `StableMemoryAllocation` schema-version/fingerprint metadata is future public API or internal-only allocation bookkeeping.
6. Consider making `ValidateVisitor` crate-private while keeping the `Visitor` trait public only if direct schema traversal is intentionally supported.
