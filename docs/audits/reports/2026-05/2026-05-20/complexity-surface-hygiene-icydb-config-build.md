# Complexity / Surface Hygiene Audit - icydb-config-build - 2026-05-20

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
| `comparability_status` | `non-comparable`; first focused `icydb-config-build` run under CSH-1.0 |
| `code_snapshot` | `ce24f4323` plus dirty worktree |
| `in_scope_roots` | `crates/icydb-config-build/src`, `crates/icydb-config-build/Cargo.toml`, canister `build.rs` consumers |
| `excluded_roots` | `target`, generated build output, historical docs/changelogs |
| `generated_code_inclusion` | `sampled`; canister `build.rs` users and generated-config include sites inspected |
| `test_surface_inclusion` | `full`; crate unit tests inspected |

## Step Status

| Step [M] | Status [C] | Evidence Artifact [M/C] | Comparability Impact [C] |
| ---- | ---- | ---- | ---- |
| STEP 0 | PASS | metadata table above | first focused baseline |
| STEP 1 | PASS | public model/function/error inventory plus canister and CLI consumer scan | none beyond first-run status |
| STEP 2 | PASS | stale-signal scan for dead-code, compatibility, fallback, shim, deprecated, `EntityModel`, `IndexModel` | none after cleanup |
| STEP 3 | PASS | host-only config authority and generated-code boundary review | stale generated constants path removed |
| STEP 4 | PASS | generated-file write path and ignored build-script validation parameter reviewed | stale complexity removed |
| STEP 5 | PASS | generated boundary include/use scan | no `icydb_config.rs` include consumer found |
| STEP 6 | PASS | unit-test and CLI diagnostics consumer review | none |
| STEP 7 | PASS | removal safety plan executed for high-signal stale surface | none |
| STEP 8 | PASS | risk bucket table | none |

## Footprint Summary

| Metric [M] | Value [D] | Notes [C] |
| ---- | ----: | ---- |
| source LOC | 757 | split across `emit.rs`, `error.rs`, `lib.rs`, `model.rs`, `parse.rs`, `resolve.rs` |
| Cargo manifest LOC | 17 | unchanged |
| public/scoped-public declaration lines | 36 | includes crate-private constructors, parser helpers, and resolver helpers |
| public error variants | 6 | generated-file write errors removed |
| build-script consumers | 8 | canister `build.rs` files call `emit_config_for_build_script()` |
| host-tool consumers | 1 | `icydb-cli` uses resolved config loading/reporting |
| unit tests | 8 | renderer-only generated constants test removed with renderer |

## Reachable Surface Inventory Summary

| Item [M] | Kind [M] | Path [M] | Visibility [M] | Feature/Cfg [M] | Consumer Evidence [M/C] | Surface Class [C] | Owner [C] | Risk [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| build-script emit API | function | `crates/icydb-config-build/src/emit.rs:14` | public | build-script use | 8 canister build scripts call it and use returned config to set `BuildOptions` | live-generated-boundary | build script config authority | Low |
| resolved config model | struct + accessors | `crates/icydb-config-build/src/model.rs:8` | public type, private fields | none | `icydb-cli/src/config.rs` stores and reports path/config | live-authority | host config tools | Low |
| generated config model | struct + accessors | `crates/icydb-config-build/src/model.rs:36` | public type, private fields | none | canister build scripts and CLI read endpoint switches | live-generated-boundary | build script config authority | Low |
| generated canister config | struct + accessors | `crates/icydb-config-build/src/model.rs:110` | public type, private fields | none | CLI reports per-canister switch state through `canisters()` | live-authority | host config tools | Low |
| config loading errors | enum | `crates/icydb-config-build/src/error.rs:7` | public | none | build scripts and CLI stringify errors | live-diagnostics | config diagnostics | Low |
| resolved-load API | function | `crates/icydb-config-build/src/parse.rs:37` | public | none | `icydb-cli` loads config summaries/checks through this API | live-authority | config parser/validator | Low |
| raw parse/file-load helpers | functions | `crates/icydb-config-build/src/parse.rs:15`, `:22` | `#[cfg(test)] pub(crate)` / `pub(crate)` | string parser test-only; file loader crate-internal | unit tests exercise raw parse; emit and resolved-load use file loader | live-internal | config parser/validator | Low |
| resolver helpers | struct/function | `crates/icydb-config-build/src/resolve.rs:8`, `:13` | crate-private | none | build-script emit path and resolved-load API use same resolver | live-internal | config resolution | Low |
| raw TOML DTOs | private structs | `crates/icydb-config-build/src/parse.rs:197` | private | none | serde parse only | live-internal | config parser | Low |
| unit tests | test module | `crates/icydb-config-build/src/lib.rs:20` | `#[cfg(test)]` | test only | parser, validator, and path resolution covered | live-test-support | crate tests | Low |

## Dead / Stale Candidate Table

| Candidate [M] | File [M] | Lines [M] | Signal [M] | Current Consumers [M/C] | Surface Class [C] | Authority Reason [C] | Deletion Confidence [C] | Risk If Removed [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| generated `OUT_DIR/icydb_config.rs` constants path | former `crates/icydb-config-build/src/lib.rs` | removed | repo search found no `icydb_config.rs` include/read site outside this crate and old design docs | none in live crates | stale-compatibility | current canister build scripts consume returned `GeneratedIcydbConfig` and pass switches into `icydb::build::BuildOptions` | applied | public renderer/writer API was removed under pre-1.0 hard-cut policy |
| ignored `known_canisters` parameter on build-script emit API | former `emit_config_for_canister` | removed | parameter was named `_known_canisters` and not used | all 8 canister build scripts passed singleton lists | duplicate-surface | parse/load APIs still own known-canister validation for host tools/tests; build-script emit intentionally loads the visible workspace config | applied | canister build scripts updated to parameterless `emit_config_for_build_script()` |

No remaining `stale-compatibility`, `stale-generated-fallback`, deprecated shim, direct `EntityModel`/`IndexModel` runtime fallback, or persisted-decoding surface was found in `icydb-config-build`.

## Runtime Authority Drift Findings

| Area [C] | Runtime Authority [C] | Alternate Authority Found? [C] | Evidence [M/C] | Allowed Role? [C] | Finding [C] | Risk [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| config parsing | host-only `icydb-config-build` | No | `toml` dependency is isolated to build/CLI paths; runtime crates do not import parser | yes | parser ownership is clean | Low |
| endpoint generation switches | `GeneratedIcydbConfig` passed into `BuildOptions` | No | canister build scripts call `emit_config_for_build_script()` then set `BuildOptions`; generated constants path is gone | yes | one live config delivery path remains | Low |
| schema/generated models | accepted runtime schema snapshots | No | crate has no `EntityModel`/`IndexModel` or schema reconstruction code | yes | no runtime schema fallback | Low |
| SQL DDL | catalog-native core mutation | No | crate only gates endpoint generation | yes | no SQL mutation authority drift | Low |
| endpoint names | generated `__icydb_*` names | No | config has endpoint-family booleans only, no endpoint-name override | yes | no endpoint-name shim | Low |
| persisted decoding | core codecs | No | config parser does not decode persisted runtime data | yes | out of scope | Low |

## Complexity Tied To Dead Surface

| Module [M] | Complexity Signal [M] | Dead-Surface Link [C] | Public/Hidden Items [M] | Current Consumers [M/C] | Shrink Action [C] | Expected Blast Radius [C] | Risk [C] |
| ---- | ---- | ---- | ----: | ---- | ---- | ---- | ---- |
| `emit.rs` | build-script cargo rerun hints plus visible config loading | none remaining | 1 public function | 8 canister build scripts | none | N/A | Low |
| `parse.rs` | TOML DTOs, validation, canister normalization | none; parser remains strict and private DTOs stay local | 1 public function, 2 scoped helpers | CLI uses resolved-load; tests use raw string parser; build-script emit uses crate-private file loader | none | N/A | Low |
| `model.rs` | endpoint-switch accessors | no stale fields found | 3 public model types | CLI report and canister build scripts | none | N/A | Low |
| `resolve.rs` | shared config search path | no public leak | 1 crate-private struct, 1 crate-private function | parse and emit modules | none | N/A | Low |

## Facade / Generated-Boundary Findings

| Surface [M] | Boundary Type [C] | Generated Consumer Evidence [M/C] | Could Narrow? [C] | Required Replacement [C] | Deletion Confidence [C] | Risk [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| `emit_config_for_build_script` | build-script boundary | 8 canister `build.rs` files call it | no | none | low | High if removed |
| `GeneratedIcydbConfig` accessors | build-script/CLI boundary | build scripts set `BuildOptions`; CLI reports config state | no | none | low | High if removed |
| `GeneratedCanisterConfig` accessors | CLI report boundary | `render_config_report` iterates `config.canisters()` | no | none | low | Medium if removed |
| `load_resolved_icydb_toml` | host tool boundary | CLI uses resolved load for show/check/endpoint guards | no | none | low | Medium if removed |
| raw parse/file-load helpers | internal/test boundary | no external crate-root export; only tests and sibling modules use them | already narrowed | none | high | Low |

## Feature / Diagnostics / Test Surface Review

| Surface [M] | Feature/Cfg [M] | Production Consumer? [M/C] | Test/Diagnostics Consumer? [M/C] | Visibility Could Narrow? [C] | Action [C] | Risk [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| crate unit tests | `#[cfg(test)]` | no | yes, 8 unit tests | no production impact | keep | Low |
| config diagnostics | public `ConfigBuildError` | build scripts and CLI stringify errors | yes | no obvious narrowing after generated-write variants were removed | keep | Low |
| feature-gated diagnostics | none | N/A | N/A | N/A | none | Low |
| hidden exports/macros | none | N/A | N/A | N/A | none | Low |

## Removal Safety Plan

| Candidate [M] | Action [C] | Owner Boundary [C] | Required Proof [C] | Focused Validation [C] | Wasm Raw Bytes Relevant? [C] | Follow-Up Required? [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| generated `OUT_DIR/icydb_config.rs` write path | removed | build-script config boundary | repo-wide search found no live include/read site; canister build scripts compile through returned config model | `cargo test -p icydb-config-build`; `cargo check -p icydb-cli`; all canister package checks | no | no |
| ignored build-script known-canisters parameter | removed with API rename | build-script validation policy | parse/load still support known-canister validation; build-script emit keeps workspace-visible config loading | same as above | no | no |

## Risk Score

Overall risk index: `1/10`.

| Bucket [C] | Count [D] | Highest Risk [C] | Notes [C] |
| ---- | ----: | ---- | ---- |
| stale compatibility | 0 | Low | stale generated constants path was removed |
| stale generated fallback | 0 | Low | no generated model runtime fallback or reconstruction path |
| orphaned helper | 0 | Low | no helper without a current owner found |
| overexposed internal | 0 | Low | main models have private fields and accessor-only shape |
| duplicate surface | 0 | Low | returned config model is now the single in-tree build-script delivery path |
| unclear | 0 | Low | build-script known-canister ambiguity removed with parameterless emit API |

## Verification Readout

| Check [M] | Result [C] | Notes [C] |
| ---- | ---- | ---- |
| stale-signal scan | PASS | no compatibility/fallback/shim/deprecated/model symbols found in live source |
| public-surface scan | PASS | public model, error, resolved-load, and build-script emit surfaces accounted for; raw parser helpers narrowed |
| generated-boundary scan | PASS | canister build scripts consume returned config; no `icydb_config.rs` include site found |
| `cargo test -p icydb-config-build` | PASS | 8 unit tests, doc tests empty |
| `cargo check -p icydb-config-build` | PASS | crate compiles |
| `cargo check -p icydb-cli` | PASS | host-tool consumer compiles |
| canister package checks | PASS | demo, test SQL, and all audit canister build scripts compile |

## Follow-Up Actions

None.
