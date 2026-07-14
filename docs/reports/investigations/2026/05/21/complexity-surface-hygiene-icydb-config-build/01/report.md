# Complexity / Surface Hygiene Audit - icydb-config-build - 2026-05-21

## Run Metadata

| Field [M/C] | Value |
| ---- | ---- |
| `method_version` | `CSH-1.1` |
| `surface_taxonomy` | `ST-1` |
| `authority_taxonomy` | `AT-1` |
| `deletion_confidence_model` | `DC-1` |
| `compatibility_policy` | `pre-1.0-hard-cut` |
| `wasm_signal_rule` | `raw-wasm-primary` |
| `baseline_report` | `docs/audits/reports/2026-05/2026-05-20/complexity-surface-hygiene-icydb-config-build.md` |
| `comparability_status` | `non-comparable`; prior run used `CSH-1.0`, this run backfills mandatory retention justification under `CSH-1.1` |
| `code_snapshot` | `11b77ad92` plus dirty worktree |
| `in_scope_roots` | `crates/icydb-config-build/src`, `crates/icydb-config-build/Cargo.toml`, sampled canister build-script consumers, `icydb-cli` config consumer |
| `excluded_roots` | `target`, generated build output, historical changelog entries |
| `generated_code_inclusion` | `sampled`; canister `build.rs` consumers inspected |
| `test_surface_inclusion` | `full`; crate unit tests inspected as parser/resolver coverage |

## Step Status

| Step [M] | Status [C] | Evidence Artifact [M/C] | Comparability Impact [C] |
| ---- | ---- | ---- | ---- |
| STEP 0 | PASS | metadata table above | non-comparable with CSH-1.0 baseline |
| STEP 1 | PASS | public re-export, model, parser, resolver, and consumer inventory | retention reasons backfilled |
| STEP 2 | PASS | stale-signal scan for dead-code, compatibility, fallback, shim, deprecated, `EntityModel`, and `IndexModel` | none |
| STEP 3 | PASS | host-only config parsing and generated endpoint switch authority reviewed | none |
| STEP 4 | PASS | crate size and parser/resolver complexity reviewed against live boundaries | none |
| STEP 5 | PASS | build-script and CLI facade boundaries reviewed | none |
| STEP 6 | PASS | unit-test and diagnostics surface reviewed | none |
| STEP 7 | PASS | no high- or medium-confidence removal candidate found | none |
| STEP 8 | PASS | risk score table below | none |

## Footprint Summary

| Metric [M] | Value [D] | Notes [C] |
| ---- | ----: | ---- |
| total inspected LOC | 803 | source and manifest |
| source LOC | 786 | `emit.rs`, `error.rs`, `lib.rs`, `model.rs`, `parse.rs`, `resolve.rs`, `tests.rs` |
| largest source file | 240 | `parse.rs`; strict TOML parse and validation |
| public crate-root re-exports | 4 | build-script emit API, error type, config models, resolved-load API |
| build-script consumers | 8 | current canister `build.rs` files call `emit_config_for_build_script()` |
| host-tool consumers | 1 | `icydb-cli` uses resolved config loading/reporting |
| unit tests | 8+ | parser, validation, and path resolution tests in `tests.rs` |

## Reachable Surface And Retention Inventory Summary

| Item [M] | Kind [M] | Path [M] | Visibility [M] | Feature/Cfg [M] | Consumer Evidence [M/C] | Authority Reason [C] | Surface Class [C] | Owner [C] | Risk [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| `emit_config_for_build_script` | function | `crates/icydb-config-build/src/emit.rs:14` | public | build script | canister `build.rs` files call it and convert returned switches into `icydb::build::BuildOptions` | canister build scripts need one host-only config entrypoint; runtime actor code must not parse TOML | live-generated-boundary | build config seam | Low |
| `ConfigBuildError` | enum | `crates/icydb-config-build/src/error.rs:7` | public | none | build scripts and CLI propagate/stringify config diagnostics | config parsing and discovery failures need path-aware diagnostics across build-script and host-tool boundaries | live-diagnostics | config diagnostics | Low |
| `ResolvedIcydbConfig` | struct + accessors | `crates/icydb-config-build/src/model.rs:8` | public type, private fields | none | `icydb-cli/src/config.rs` stores and renders resolved path plus config | CLI config show/check needs both source path and validated switch model | live-authority | host config tools | Low |
| `GeneratedIcydbConfig` | struct + accessors | `crates/icydb-config-build/src/model.rs:36` | public type, private fields | none | build scripts query endpoint switches; CLI reports canister config | one validated config model must feed both build endpoint generation and host reporting | live-generated-boundary | build config model | Low |
| `GeneratedCanisterConfig` | struct + accessors | `crates/icydb-config-build/src/model.rs:104` | public type, private fields | none | CLI iterates `GeneratedIcydbConfig::canisters()` and reads per-canister switches | host tools need per-canister reporting without exposing raw TOML DTOs | live-authority | host config model | Low |
| `GeneratedCanisterSqlConfig` | struct | `crates/icydb-config-build/src/model.rs:171` | crate-private | none | parser constructs it; public canister config exposes only semantic accessors | SQL switch grouping keeps raw booleans private while preserving public accessors | live-authority | config model internals | Low |
| `GeneratedCanisterMetricsConfig` | struct | `crates/icydb-config-build/src/model.rs:189` | crate-private | none | parser constructs it; public canister config exposes only semantic accessors | metrics switch grouping keeps raw booleans private while preserving public accessors | live-authority | config model internals | Low |
| `load_resolved_icydb_toml` | function | `crates/icydb-config-build/src/parse.rs:37` | public | host tool | `icydb-cli` config show/check paths call it with known-canister context where needed | host tools need read-only config discovery and validation without Cargo build-script side effects | live-authority | host config tools | Low |
| `parse_icydb_toml` | function | `crates/icydb-config-build/src/parse.rs:15` | `#[cfg(test)] pub(crate)` | tests | crate unit tests parse inline TOML fixtures | test-only parser keeps strict TOML validation covered without widening production API | live-test-support | parser tests | Low |
| `load_icydb_toml` | function | `crates/icydb-config-build/src/parse.rs:22` | crate-private | none | build-script emit and resolved-load API share file loading | one file-load path preserves identical validation for build scripts and host tools | live-authority | parser internals | Low |
| raw TOML DTOs | private structs | `crates/icydb-config-build/src/parse.rs:197` | private | none | serde deserialization only | raw TOML shape remains isolated from public validated config model | live-authority | parser internals | Low |
| canister normalization helpers | functions | `crates/icydb-config-build/src/parse.rs:82`, `:169`, `:187` | private | none | parser validation uses them; tests cover unknown/ambiguous canister handling | host config validation must reject ambiguous or unknown canister names when known-canister context is supplied | live-authority | parser validation | Low |
| `ResolvedConfigPath` and resolver | struct/function | `crates/icydb-config-build/src/resolve.rs:8`, `:13` | crate-private | none | build-script emit and resolved-load API share path resolution | config discovery order must be identical for build scripts and CLI config commands | live-authority | config resolution | Low |
| `CONFIG_FILE_NAME` / `CONFIG_PATH_ENV` | constants | `crates/icydb-config-build/src/lib.rs:18`, `:19` | crate-private | none | resolver and emit path use them; tests reference file name | avoids duplicate discovery/env literals inside the crate | live-authority | config resolution | Low |
| crate unit tests | module | `crates/icydb-config-build/src/tests.rs` | `#[cfg(test)]` | tests | package tests exercise parser/resolver contracts | protects strict host config validation and discovery behavior | live-test-support | config tests | Low |

## Dead / Stale Candidate Table

| Candidate [M] | File [M] | Lines [M] | Signal [M] | Current Consumers [M/C] | Authority Reason [C] | Surface Class [C] | Deletion Confidence [C] | Risk If Removed [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| `expect` in parser tests | `crates/icydb-config-build/src/tests.rs` | multiple | `expect` signal | unit tests only | tests use `expect` to make fixture intent explicit; no production panic surface | live-test-support | low | changing these would be style churn, not surface reduction |
| `known_canisters` validation lane | `crates/icydb-config-build/src/parse.rs` | `15`, `22`, `37`, `53`, `68`, `169` | validation branch | CLI config checks and tests; build-script emit passes `&[]` intentionally | host tools need optional known-canister validation, while build scripts load visible config without generated canister discovery | live-authority | low | deleting it would weaken `icydb config check` diagnostics |

No stale compatibility branch, generated-model runtime fallback, endpoint-name override shim, orphaned helper, duplicate public delivery path, or direct `EntityModel` / `IndexModel` authority was found in `icydb-config-build`.

## Runtime Authority Drift Findings

| Area [C] | Runtime Authority [C] | Alternate Authority Found? [C] | Evidence [M/C] | Allowed Role? [C] | Finding [C] | Risk [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| config parsing | host-only `icydb-config-build` | No | parser is consumed only by build scripts and CLI; runtime/generated actor code receives booleans through `BuildOptions` | Yes | TOML authority does not leak into runtime crates | Low |
| endpoint switches | generated actor `BuildOptions` | No | canister build scripts convert `GeneratedIcydbConfig` into build options | Yes | config crate gates generated surfaces but does not render actor glue | Low |
| accepted schema | core/runtime accepted schema snapshots | No | crate has no schema graph, `EntityModel`, or `IndexModel` reconstruction | Yes | no runtime schema authority drift | Low |
| SQL DDL | catalog-native schema mutation in core | No | crate contains only endpoint-family booleans | Yes | SQL config does not become DDL mutation authority | Low |
| endpoint names | fixed generated `__icydb_*` names | No | config model has switch booleans, no endpoint name strings | Yes | no configurable endpoint-name lane | Low |

## Complexity Tied To Dead Surface

| Module [M] | Complexity Signal [M] | Dead-Surface Link [C] | Public/Hidden Items [M] | Current Consumers [M/C] | Shrink Action [C] | Expected Blast Radius [C] | Risk [C] |
| ---- | ---- | ---- | ----: | ---- | ---- | ---- | ---- |
| `parse.rs` | strict TOML DTOs, normalization, optional known-canister validation | none; this is current host config authority | 1 public function, 2 scoped helpers, private DTOs | CLI config commands, build-script emit, tests | none | N/A | Low |
| `model.rs` | endpoint-family accessor surface | none; private fields keep raw shape narrow | 3 public model types, 2 crate-private grouped configs | CLI reports and canister build scripts | none | N/A | Low |
| `emit.rs` | Cargo rerun hints plus config discovery | none; build-script boundary is live | 1 public function | canister `build.rs` files | none | N/A | Low |
| `resolve.rs` | workspace-root search and env override handling | none; shared discovery path avoids duplicate behavior | 1 crate-private struct, 1 crate-private function | emit and resolved-load paths | none | N/A | Low |

## Facade / Generated Boundary Findings

| Surface [M] | Boundary Type [C] | Generated Consumer Evidence [M/C] | Could Narrow? [C] | Required Replacement [C] | Deletion Confidence [C] | Risk [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| `emit_config_for_build_script` | build-script facade | current canister `build.rs` files call it directly | No | every canister build script would need a replacement config discovery API | low | High |
| `GeneratedIcydbConfig` switch accessors | build-script and CLI facade | build scripts set `BuildOptions`; CLI reports endpoint switches | No | duplicated build-script/CLI config models or direct TOML parsing | low | High |
| `GeneratedCanisterConfig` | CLI reporting facade | `canisters()` exposes values for config tables | No | CLI-specific projection type or broader raw TOML exposure | low | Medium |
| `load_resolved_icydb_toml` | host-tool facade | `icydb-cli` config commands use it | No | CLI would need to own TOML parsing/discovery | low | Medium |
| raw parser/resolver helpers | internal implementation | no crate-root export | Already narrow | none | high | Low |

## Feature / Diagnostics / Test Surface Review

| Surface [M] | Feature/Cfg [M] | Production Consumer? [M/C] | Test/Diagnostics Consumer? [M/C] | Visibility Could Narrow? [C] | Action [C] | Risk [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| `ConfigBuildError` variants | none | build scripts and CLI | unit tests match selected variants | No; public error type is facade diagnostics | retain | Low |
| `parse_icydb_toml` | `#[cfg(test)]` | no | crate tests | Already test-only | retain | Low |
| raw TOML DTOs | none | parser only | indirectly through tests | Already private | retain | Low |
| generated output | N/A | no generated output owned by this crate | N/A | N/A | none | Low |

## Removal Safety Plan

| Candidate [M] | Action [C] | Owner Boundary [C] | Required Proof [C] | Focused Validation [C] | Wasm Raw Bytes Relevant? [C] | Follow-Up Required? [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| none | no removal | host config/build-script boundary | current retention reasons are explicit | `cargo check/test/clippy -p icydb-config-build` | no | no |

## Risk Score

| Bucket [C] | Count [D] | Highest Risk [C] | Notes [C] |
| ---- | ----: | ---- | ---- |
| stale compatibility | 0 | N/A | none found |
| stale generated fallback | 0 | N/A | no generated-model runtime reconstruction in this crate |
| orphaned helper | 0 | N/A | private helpers feed current parse/resolve/model paths |
| overexposed internal | 0 | N/A | public models have private fields and semantic accessors |
| duplicate surface | 0 | N/A | one config delivery model feeds build scripts and CLI |
| unclear | 0 | N/A | no owner decision required |

Dead-surface pressure score: **1/10**. The remaining complexity is strict host config parsing and build/CLI facade support, not obsolete retained surface.

## Verification Readout

| Check [M] | Result [M/C] |
| ---- | ---- |
| public surface inventory via `rg` | pass |
| stale-signal scan via `rg` | pass; only test `expect` and live known-canister validation lane found |
| build-script consumer scan | pass |
| CLI config consumer scan | pass |
| `cargo check -p icydb-config-build` | pass |
| `cargo test -p icydb-config-build` | pass; 9 unit tests, 0 doc tests |
| `cargo clippy -p icydb-config-build --all-targets -- -D warnings` | pass |
| `git diff --check` | pass |

## Follow-Up Actions

None for CSH-1.1. Future config feature work should keep this crate host-only and continue to expose semantic endpoint switches rather than raw TOML DTOs.
