# Complexity / Surface Hygiene Audit - icydb-build - 2026-05-21

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
| `comparability_status` | `non-comparable`; first focused `icydb-build` run under CSH-1.1 |
| `code_snapshot` | `11b77ad92` plus dirty worktree |
| `in_scope_roots` | `crates/icydb-build/src`, `crates/icydb-build/Cargo.toml`, `crates/icydb-build/README.md`, sampled canister build-script consumers |
| `excluded_roots` | `target`, generated build output, historical changelog entries |
| `generated_code_inclusion` | `sampled`; generated endpoint names and macro expansion targets inspected from renderers |
| `test_surface_inclusion` | `full`; crate has zero unit/doc tests today |

## Step Status

| Step [M] | Status [C] | Evidence Artifact [M/C] | Comparability Impact [C] |
| ---- | ---- | ---- | ---- |
| STEP 0 | PASS | metadata table above | first CSH-1.1 run |
| STEP 1 | PASS | crate-root public API, hidden generated glue, and build-script consumer scan | non-comparable with prior informal cleanup |
| STEP 2 | PASS | stale-signal scan for dead-code, compatibility, fallback, shim, deprecated, `EntityModel`, and `IndexModel` | none |
| STEP 3 | PASS | generated endpoint and accepted-schema codegen authority review | none |
| STEP 4 | PASS | SQL/schema/store renderer complexity reviewed against retained generated surface | none |
| STEP 5 | PASS | facade/generated-boundary review for `build_with_options!`, `BuildOptions`, and `__icydb_*` renderers | none |
| STEP 6 | PASS | sql/metrics/snapshot/schema option switches and generated cfg branches reviewed | none |
| STEP 7 | PASS | no high- or medium-confidence removal candidate found | none |
| STEP 8 | PASS | risk score table below | none |

## Footprint Summary

| Metric [M] | Value [D] | Notes [C] |
| ---- | ----: | ---- |
| total inspected LOC | 1,030 | source, README, and manifest |
| source LOC | 992 | `lib.rs` plus `db/{mod,schema,sql,store}.rs` |
| largest source file | 338 | `db/sql.rs`; generated SQL surface renderer |
| public API declarations | 3 | `generate_with_options`, `BuildOptions`, `build_with_options!` |
| crate-private generated render hubs | 5 | actor builder, store, SQL, schema, metrics/snapshot renderers |
| unit/doc tests | 0 | build crate is validated through downstream build-script consumers |

## Reachable Surface And Retention Inventory Summary

| Item [M] | Kind [M] | Path [M] | Visibility [M] | Feature/Cfg [M] | Consumer Evidence [M/C] | Authority Reason [C] | Surface Class [C] | Owner [C] | Risk [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| `generate_with_options` | function | `crates/icydb-build/src/lib.rs:13` | public | build script | `build_with_options!` expands in downstream canister build scripts and calls this through `::icydb::build` | generated actor glue must be renderable from downstream build scripts | live-generated-boundary | build codegen | Low |
| `BuildOptions` | struct + builder/accessors | `crates/icydb-build/src/lib.rs:33` | public | build script | 8 canister build scripts construct options from `icydb-config-build` switches | config-owned booleans must cross into codegen without making config parsing a build-crate concern | live-generated-boundary | build/config seam | Low |
| `build_with_options!` | macro | `crates/icydb-build/src/lib.rs:162` | exported macro | build script | all current canister build scripts use the macro through `icydb::build_with_options!` | Cargo `OUT_DIR` actor emission is the stable generated-code entrypoint | live-generated-boundary | build codegen | Low |
| `ActorBuilder` | struct + impl | `crates/icydb-build/src/lib.rs:189` | crate-private | none | root generator and renderer modules use it | one canister's validated schema metadata must be shared across render surfaces | live-generated-boundary | build codegen | Low |
| store renderer | module helpers | `crates/icydb-build/src/db/store.rs` | private / `pub(super)` | generated actor | `db::generate` calls `generate_store_wiring`; output emits memory declarations, store registry, `core_db`, and `db` | generated actor glue owns stable memory registration and session accessors | live-generated-boundary | store/session glue | Low |
| SQL renderer | token bundle + helpers | `crates/icydb-build/src/db/sql.rs` | private / `pub(super)` | `feature = "sql"` in generated output | enabled by `BuildOptions`; output emits fixed `__icydb_query`, `__icydb_ddl`, and fixture endpoints | SQL endpoint surface is config-gated generated actor glue, not dynamic runtime dispatch | live-generated-boundary | SQL canister glue | Low |
| schema renderer | token bundle | `crates/icydb-build/src/db/schema.rs` | private / `pub(super)` | generated actor | enabled by `BuildOptions`; output calls `db().try_describe_entity` for concrete entity types | only codegen knows the concrete entity list for schema/schema-check endpoints | live-generated-boundary | schema endpoint glue | Low |
| snapshot renderer | function | `crates/icydb-build/src/lib.rs:253` | private | generated actor | enabled by `BuildOptions`; output calls hidden facade snapshot helper | generated actor exports fixed snapshot endpoint when config enables it | live-generated-boundary | observability glue | Low |
| metrics renderer | function | `crates/icydb-build/src/lib.rs:268` | private | generated actor | enabled by `BuildOptions`; output calls facade metrics report/reset helpers | generated actor exports fixed metrics endpoints when config enables them | live-diagnostics | observability glue | Low |
| generated lint suppressions | generated attributes | `db/store.rs`, `db/sql.rs` | emitted code only | `feature = "sql"` / zero-entity cases | `#[allow(dead_code)]`, `unused_mut`, and clippy allowances appear only inside emitted actor code | generated code must compile across optional endpoint and zero-entity combinations | live-generated-boundary | generated-code lint shaping | Low |

## Dead / Stale Candidate Table

| Candidate [M] | File [M] | Lines [M] | Signal [M] | Current Consumers [M/C] | Authority Reason [C] | Surface Class [C] | Deletion Confidence [C] | Risk If Removed [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| codegen `expect` / `unwrap` boundaries | `crates/icydb-build/src/lib.rs`, generated store registration | `15`, `16`, `174`; emitted store init | panic/expect signal | build script and generated actor bootstrap | invalid schema or `OUT_DIR` is a codegen/build boundary; store registration is generated from validated schema metadata | live-generated-boundary | low | converting to a broad runtime API would blur the current build-time failure contract |
| generated SQL helper `#[allow(dead_code)]` attributes | `crates/icydb-build/src/db/sql.rs` | emitted helper blocks | dead-code allowance signal | generated actor code under partial SQL options | helper functions are conditionally needed by different config combinations and must stay valid in zero-entity canisters | live-generated-boundary | low | removing allowances can reintroduce clippy failures in generated canisters |
| generated store registry lint allowances | `crates/icydb-build/src/db/store.rs` | emitted `STORE_REGISTRY` block | lint allowance signal | generated actor code across zero/multiple-store cases | emitted registry shape intentionally stays stable across generated canister shapes | live-generated-boundary | low | removing allowances risks generated-code clippy noise without shrinking runtime surface |

No stale compatibility branch, stale generated-model runtime fallback, endpoint name override shim, orphaned helper, or duplicate runtime authority was found in `icydb-build`.

## Runtime Authority Drift Findings

| Area [C] | Runtime Authority [C] | Alternate Authority Found? [C] | Evidence [M/C] | Allowed Role? [C] | Finding [C] | Risk [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| canister/schema lookup | validated schema graph from `icydb-schema::build::get_schema` | No | `generate_with_options` loads validated schema, then resolves one `Canister` node | Yes | build crate does not reconstruct schema authority | Low |
| store memory allocation | schema `Store`/`Canister` allocation metadata | No | store renderer emits `ic_memory_range`, commit declaration, and store `ic_memory_key!` declarations from schema node methods | Yes | memory ownership is explicit generated glue, not schema-order local assignment in build crate | Low |
| SQL DDL | core catalog-native DDL execution | No | generated `__icydb_ddl` dispatches to `db().execute_sql_ddl::<Entity>()` | Yes | SQL renderer is frontend glue only | Low |
| schema reports | accepted schema via session methods | No | schema renderer calls `try_describe_entity` and wraps generated/accepted comparison in `EntitySchemaCheckDescription` | Yes | generated model remains comparison/proposal input only | Low |
| generated endpoint naming | fixed `__icydb_*` names | No | renderer emits verbatim names with no endpoint `name = ...` override | Yes | matches current generated endpoint rule | Low |

## Facade / Generated Boundary Findings

| Surface [M] | Boundary Type [C] | Generated Consumer Evidence [M/C] | Could Narrow? [C] | Required Replacement [C] | Deletion Confidence [C] | Risk [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| `icydb::build::BuildOptions` | public facade via `icydb` re-export | downstream build scripts configure every current generated endpoint switch | No | none | low | Low |
| `build_with_options!` | exported macro | canister build scripts use macro to write `actor.rs` | No | new build-script API would be a behavior/API change | low | Low |
| `generate_with_options` | public macro target | macro expansion must call it from downstream crate context | No | macro-local render implementation is not practical across crate boundary | low | Low |
| `__icydb_query`, `__icydb_ddl`, fixtures, schema, snapshot, metrics | generated endpoint surface | token renderers emit fixed endpoint names from config switches | No | endpoint generation would need to move to a different generated-code owner | low | Low |
| `core_db` / `db` | generated actor helpers | generated actor and user code call these canister-local helpers | No | user-facing `db()` helper would disappear | low | Low |

## Feature / Diagnostics / Test Surface Review

| Surface [M] | Feature/Cfg [M] | Production Consumer? [M/C] | Test/Diagnostics Consumer? [M/C] | Visibility Could Narrow? [C] | Action [C] | Risk [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| SQL endpoint switches | generated `#[cfg(feature = "sql")]` | yes, canister SQL/DDL/fixture endpoints | integration tests call generated endpoints | No | retain | Low |
| metrics endpoint switches | generated endpoint option | optional production observability | CLI and tests decode metrics payloads through facade | No | retain | Low |
| snapshot endpoint switch | generated endpoint option | optional production observability | CLI snapshot path depends on fixed endpoint | No | retain | Low |
| schema/schema-check endpoint switch | generated endpoint option | optional production observability | CLI schema commands depend on fixed endpoint | No | retain | Low |
| tests in `icydb-build` | none | N/A | N/A | N/A | no action | Low |

## Removal Safety Plan

| Candidate [M] | Action [C] | Owner Boundary [C] | Required Proof [C] | Focused Validation [C] | Wasm Raw Bytes Relevant? [C] | Follow-Up Required? [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| none | no removal | build generated-code boundary | current retention reasons are explicit | `cargo check/test/clippy -p icydb-build` | no | no |

## Risk Score

| Bucket [C] | Count [D] | Highest Risk [C] | Notes [C] |
| ---- | ----: | ---- | ---- |
| stale compatibility | 0 | N/A | none found |
| stale generated fallback | 0 | N/A | no generated-model runtime reconstruction in this crate |
| orphaned helper | 0 | N/A | private helpers all feed current renderers |
| overexposed internal | 0 | N/A | public build API is required by macro/downstream build-script boundaries |
| duplicate surface | 0 | N/A | `BuildOptions` mirrors config switches but does not duplicate config parsing |
| unclear | 0 | N/A | no owner decision needed |

Dead-surface pressure score: **1/10**. The remaining complexity is generated-code rendering complexity, not obsolete surface retention.

## Verification Readout

| Check [M] | Result [M/C] |
| ---- | ---- |
| public/generated surface inventory via `rg` | pass |
| stale-signal scan via `rg` | pass; signals are generated-code lint/codegen-boundary allowances with current authority |
| canister build-script consumer scan | pass |
| `cargo check -p icydb-build` | pass |
| `cargo test -p icydb-build` | pass; 0 unit/doc tests |
| `cargo clippy -p icydb-build --all-targets -- -D warnings` | pass |

## Follow-Up Actions

None for CSH-1.1. A future non-CSH readability pass could move snapshot and metrics renderers out of `lib.rs`, but that is ordinary module organization, not dead-surface cleanup.
