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
| `baseline_report` | `N/A` |
| `comparability_status` | `non-comparable`; first focused `icydb-schema` run under CSH-1.0 |
| `code_snapshot` | `e66d20ca1` |
| `in_scope_roots` | `crates/icydb-schema/src` |
| `excluded_roots` | `target`, generated build output, historical docs/changelogs |
| `generated_code_inclusion` | `sampled`; `crates/icydb-schema-derive/src` quote targets inspected for schema-node consumers |
| `test_surface_inclusion` | `sampled`; in-crate tests inspected where they are the only visible consumer |

## Step Status

| Step [M] | Status [C] | Evidence Artifact [M/C] | Comparability Impact [C] |
| ---- | ---- | ---- | ---- |
| STEP 0 | PASS | metadata table above | non-comparable baseline |
| STEP 1 | PASS | root exports, node facade exports, derive/build consumer scans | none beyond first-run status |
| STEP 2 | PASS | stale-signal scan for dead-code, compatibility, fallback, shim, deprecated, `EntityModel`, `IndexModel` | none |
| STEP 3 | PASS | schema authority drift review across build, derive, and core proposal/runtime consumers | none |
| STEP 4 | PASS | low-consumer public helper review | none |
| STEP 5 | PASS | generated-boundary quote target review in `icydb-schema-derive` | none |
| STEP 6 | PASS | cfg/test-surface scan | none |
| STEP 7 | PASS | removal safety plan for narrowable candidates | none |
| STEP 8 | PASS | risk bucket table | none |

## Reachable Surface Inventory Summary

| Item [M] | Kind [M] | Path [M] | Visibility [M] | Feature/Cfg [M] | Consumer Evidence [M/C] | Surface Class [C] | Owner [C] | Risk [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| crate modules | root exports | `crates/icydb-schema/src/lib.rs:1` | `pub mod build/error/node/types/validate/visit` | none | facade re-export via `icydb::schema`; build/derive use `build` and `node` | mixed | schema/build boundary | Medium |
| schema prelude | `pub mod prelude` | `crates/icydb-schema/src/lib.rs:25` | public, with `schema_read` crate-only | none | node modules and derive helper imports rely on grouped node/type/error exports | live-generated-boundary | schema derive boundary | Low |
| node facade | module re-exports | `crates/icydb-schema/src/node/mod.rs:35` | `pub use *` over concrete schema nodes | none | derive emits `::icydb::schema::node::*`; core proposal/fingerprint consume index/field metadata | live-generated-boundary | macro/schema AST | Low |
| build registry | global schema access | `crates/icydb-schema/src/build/mod.rs:23` | `schema_write`/`get_schema` public, `schema_read` crate-only | none | derive registers nodes through `schema_write`; `icydb-build` reads validated schema through `get_schema` | live-generated-boundary | build codegen | Low |
| validation namespace | module/function | `crates/icydb-schema/src/validate/mod.rs:3`, `crates/icydb-schema/src/validate/naming.rs:7` | public module and public function | none | only called by crate-private `validate_schema`; no in-tree external consumers | overexposed-internal | schema validation | Medium |
| schema graph helpers | methods | `crates/icydb-schema/src/node/schema.rs:28`, `:156`, `:168` | public methods | none | `get_type`, `check_node_as`, `get_node_values` have no in-tree consumers outside their own declarations | overexposed-internal | schema graph | Medium |
| visitor validation helper | struct/methods | `crates/icydb-schema/src/visit.rs:40` | public `ValidateVisitor` | none | only `validate/mod.rs` constructs it; `Visitor` trait itself is used by node traversal | overexposed-internal | schema validation traversal | Medium |
| stable memory allocation details | DTO + constructor/accessors | `crates/icydb-schema/src/node/store.rs:138` | public type and methods | none | `icydb-build` uses allocation accessors for `memory_id`/`stable_key`; constructor, schema metadata accessors, and identity comparator are only in crate/tests | overexposed-internal | stable memory codegen | Medium |
| tests | test modules | `crates/icydb-schema/src/**` | `#[cfg(test)]` | test only | 18 unit tests exercise error, memory, index, and naming behavior | live-test-support | crate tests | Low |

## Dead / Stale Candidate Table

| Candidate [M] | File [M] | Lines [M] | Signal [M] | Current Consumers [M/C] | Surface Class [C] | Authority Reason [C] | Deletion Confidence [C] | Risk If Removed [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| public validation namespace | `crates/icydb-schema/src/validate/mod.rs`, `crates/icydb-schema/src/validate/naming.rs` | `3`, `7` | public helper has only internal call path | `validate_schema -> validate_global -> naming::validate_entity_naming`; no external in-tree references | overexposed-internal | validation is invoked through `build::get_schema`, not direct facade calls | medium | breaking unknown out-of-tree direct users of `icydb::schema::validate::naming` |
| unused schema type-erasure helper | `crates/icydb-schema/src/node/schema.rs` | `28-43` | no references to `get_type()` | none found in `crates/*` | orphaned-helper | no current generated/runtime authority role found | medium | possible out-of-tree schema introspection users |
| unused schema graph convenience methods | `crates/icydb-schema/src/node/schema.rs` | `156-172` | no references to `check_node_as` or `get_node_values` | none found in `crates/*` | overexposed-internal | `cast_node`, `get_nodes`, and `filter_nodes` are the active codegen/validation accessors | medium | possible out-of-tree schema introspection users |
| public `ValidateVisitor` concrete type | `crates/icydb-schema/src/visit.rs` | `40-107` | concrete validation visitor exposed although only internal validation constructs it | `validate/mod.rs` only | overexposed-internal | visitor traversal remains live; the concrete validating visitor does not need facade reachability | medium | possible out-of-tree diagnostics using schema visitor directly |
| allocation metadata constructor/accessors | `crates/icydb-schema/src/node/store.rs` | `138-188` | public DTO exposes schema-version/fingerprint metadata that current codegen does not consume | build consumes `memory_id`/`stable_key`; tests consume constructor and `same_identity_as` | overexposed-internal | current codegen authority is stable memory id/key; accepted runtime schema owns schema version/fingerprint | low | public type is returned by public `Store` methods; narrowing needs owner decision |

No `stale-compatibility`, `stale-generated-fallback`, deprecated shim, or direct `EntityModel`/`IndexModel` runtime fallback signal was found in `crates/icydb-schema/src`.

## Runtime Authority Drift Findings

| Area [C] | Runtime Authority [C] | Alternate Authority Found? [C] | Evidence [M/C] | Allowed Role? [C] | Finding [C] | Risk [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| schema acceptance | accepted runtime schema snapshots | No | `icydb-schema` builds proposal-time node graph; `icydb-core` consumes generated model metadata to propose/accept schema | yes | no runtime fallback authority in `icydb-schema` | Low |
| generated models | proposal/codegen metadata only | No | no `EntityModel` or `IndexModel` symbols in `crates/icydb-schema/src` | yes | no generated-model reconstruction path | Low |
| SQL DDL | catalog-native mutation in core | No | `icydb-schema` has no SQL DDL lowering code | yes | out of scope for schema crate | Low |
| endpoint names | generated `__icydb_*` names | No | `icydb-schema` has no endpoint name metadata or override code | yes | no endpoint-name shim surface | Low |
| persisted decoding | core codecs remain bounded/fallible | No | `icydb-schema` contains schema AST/build metadata, not persisted decoders | yes | no drift found | Low |

## Complexity Tied To Dead Surface

| Module [M] | Complexity Signal [M] | Dead-Surface Link [C] | Public/Hidden Items [M] | Current Consumers [M/C] | Shrink Action [C] | Expected Blast Radius [C] | Risk [C] |
| ---- | ---- | ---- | ----: | ---- | ---- | ---- | ---- |
| `validate` | public submodule for one internal pass | direct validation entrypoint is private; submodule leaks globally | 2 public declarations | only `validate_schema` calls naming pass | make module private and function `pub(crate)`/private | schema crate plus facade API surface | Medium |
| `node::schema` | type-erasure and graph convenience methods | some public helpers have no visible consumers | 1 public enum, 1 public graph type, several methods | build uses `get_nodes`/`filter_nodes`; validation uses `cast_node`; listed helpers unused | owner decision, then delete unused helpers | possible out-of-tree schema introspection | Medium |
| `visit` | concrete validating visitor exported | public concrete visitor only supports internal validation run | 1 public enum, 1 public trait, 1 public struct | only validation module uses `ValidateVisitor`; traversal traits live | narrow concrete `ValidateVisitor` before narrowing `Visitor` | crate-private validation path | Medium |
| `node::store` | stable allocation DTO carries unused metadata accessors | constructor/schema metadata accessors are not consumed by build | 3 public allocation role/type/helper surfaces | build consumes returned allocation id/key | owner decision; keep id/key accessors, narrow constructor/test-only helpers if approved | codegen and tests | Medium |

## Facade / Generated-Boundary Findings

| Surface [M] | Boundary Type [C] | Generated Consumer Evidence [M/C] | Could Narrow? [C] | Required Replacement [C] | Deletion Confidence [C] | Risk [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| `schema::node::*` | generated code target | derive emits `::icydb::schema::node::{Def, Field, Index, Store, SchemaNode, ...}` | no broad narrowing without derive rewrite | none | low | High if removed |
| `schema::build::schema_write` | generated code registration | derive emits `::icydb::schema::build::schema_write().insert_node(...)` | no | none | low | High if removed |
| `schema::build::get_schema` | build codegen input | `icydb-build` calls `get_schema()` before codegen | no | none | low | High if removed |
| `schema::validate::naming` | facade-leaked validation helper | no generated consumer found | yes | private call from `validate_global` | medium | Low in-tree, unknown external |
| `schema::visit::ValidateVisitor` | concrete diagnostics helper | no generated consumer found | yes | crate-private validating visitor | medium | Low in-tree, unknown external |

## Feature / Diagnostics / Test Surface Review

| Surface [M] | Feature/Cfg [M] | Production Consumer? [M/C] | Test/Diagnostics Consumer? [M/C] | Visibility Could Narrow? [C] | Action [C] | Risk [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| in-crate test modules | `#[cfg(test)]` | no | yes, 18 unit tests | no production impact | keep | Low |
| `#[expect(clippy::too_many_arguments)]` test helper | `#[cfg(test)]` | no | yes, naming tests | no production impact | keep | Low |
| feature-gated diagnostics | none found | N/A | N/A | N/A | none | Low |
| `#[doc(hidden)]` / `__macro` exports | none in `icydb-schema` | N/A | N/A | N/A | none | Low |

## Removal Safety Plan

| Candidate [M] | Action [C] | Owner Boundary [C] | Required Proof [C] | Focused Validation [C] | Wasm Raw Bytes Relevant? [C] | Follow-Up Required? [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| public validation namespace | narrow visibility | schema validation/build | change `pub mod validate` to private if no facade policy reason; make naming function private or `pub(crate)` | `cargo test -p icydb-schema`; `cargo check -p icydb-schema-derive -p icydb-build` | no | yes |
| `SchemaNode::get_type` | delete or make crate-private | schema graph | prove no generated output or build consumer requires it | same as above plus `rg get_type` | no | yes |
| `Schema::check_node_as` / `get_node_values` | delete or make crate-private | schema graph | prove no generated output or build consumer requires them | same as above plus focused consumer scan | no | yes |
| public `ValidateVisitor` concrete type | make crate-private | schema validation traversal | keep `Visitor` trait only if external traversal remains desired | `cargo test -p icydb-schema` | no | yes |
| allocation metadata constructor/accessors | owner decision before change | stable memory codegen | decide whether schema version/fingerprint are future-facing public metadata or internal-only | `cargo check -p icydb-build`; schema allocation tests | no | yes |

## Risk Score

Overall risk index: `3/10`.

| Bucket [C] | Count [D] | Highest Risk [C] | Notes [C] |
| ---- | ----: | ---- | ---- |
| stale compatibility | 0 | Low | no legacy/compat/fallback/shim/deprecated signals found |
| stale generated fallback | 0 | Low | no `EntityModel`/`IndexModel` runtime fallback in crate |
| orphaned helper | 1 | Medium | `SchemaNode::get_type` has no in-tree consumer |
| overexposed internal | 4 | Medium | validation namespace, graph helpers, concrete visitor, allocation metadata |
| duplicate surface | 0 | Low | no parallel route/storage/schema API owner found |
| unclear | 1 | Medium | allocation metadata may be future-facing or accidental public surface |

## Verification Readout

| Check [M] | Result [C] | Notes [C] |
| ---- | ---- | ---- |
| stale-signal scan | PASS | no dead-code allowances, compatibility/fallback/shim/deprecated markers, hidden exports, or model-fallback symbols found |
| public-surface scan | PASS | 271 public or scoped-public declarations found in `crates/icydb-schema/src` |
| generated-boundary scan | PASS | derive/build consumers account for core node/build surfaces |
| `cargo check -p icydb-schema` | PASS | crate compiles |
| `cargo test -p icydb-schema` | PASS | 18 unit tests, doc tests empty |
| `cargo check -p icydb-schema-derive -p icydb-build` | PASS | generated-boundary adjacent crates compile |

## Follow-Up Actions

1. Narrow the validation namespace if no owner wants `icydb::schema::validate::naming` as a public pre-1.0 facade.
2. Review unused schema graph helpers (`get_type`, `check_node_as`, `get_node_values`) for deletion in a small follow-up patch.
3. Decide whether `StableMemoryAllocation` schema-version/fingerprint metadata is future public API or internal-only allocation bookkeeping.
4. Consider making `ValidateVisitor` crate-private while keeping the `Visitor` trait public only if direct schema traversal is intentionally supported.
