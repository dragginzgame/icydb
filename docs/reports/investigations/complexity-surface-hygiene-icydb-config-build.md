> Historical assessment, not current-code qualification. Raw output and superseded runs
> were retired from the working tree under Shared 0.30.4. Git history retains them.
> Current follow-up authority is GitHub issues; selected results below retain their
> original source, method and limitations, not a fresh measurement.

# Complexity / Surface Hygiene Audit - icydb-config-build - 2026-05-21

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

## Runtime Authority Drift Findings

| Area [C] | Runtime Authority [C] | Alternate Authority Found? [C] | Evidence [M/C] | Allowed Role? [C] | Finding [C] | Risk [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| config parsing | host-only `icydb-config-build` | No | parser is consumed only by build scripts and CLI; runtime/generated actor code receives booleans through `BuildOptions` | Yes | TOML authority does not leak into runtime crates | Low |
| endpoint switches | generated actor `BuildOptions` | No | canister build scripts convert `GeneratedIcydbConfig` into build options | Yes | config crate gates generated surfaces but does not render actor glue | Low |
| accepted schema | core/runtime accepted schema snapshots | No | crate has no schema graph, `EntityModel`, or `IndexModel` reconstruction | Yes | no runtime schema authority drift | Low |
| SQL DDL | catalog-native schema mutation in core | No | crate contains only endpoint-family booleans | Yes | SQL config does not become DDL mutation authority | Low |
| endpoint names | fixed generated `__icydb_*` names | No | config model has switch booleans, no endpoint name strings | Yes | no configurable endpoint-name lane | Low |

## Facade / Generated Boundary Findings

| Surface [M] | Boundary Type [C] | Generated Consumer Evidence [M/C] | Could Narrow? [C] | Required Replacement [C] | Deletion Confidence [C] | Risk [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| `emit_config_for_build_script` | build-script facade | current canister `build.rs` files call it directly | No | every canister build script would need a replacement config discovery API | low | High |
| `GeneratedIcydbConfig` switch accessors | build-script and CLI facade | build scripts set `BuildOptions`; CLI reports endpoint switches | No | duplicated build-script/CLI config models or direct TOML parsing | low | High |
| `GeneratedCanisterConfig` | CLI reporting facade | `canisters()` exposes values for config tables | No | CLI-specific projection type or broader raw TOML exposure | low | Medium |
| `load_resolved_icydb_toml` | host-tool facade | `icydb-cli` config commands use it | No | CLI would need to own TOML parsing/discovery | low | Medium |
| raw parser/resolver helpers | internal implementation | no crate-root export | Already narrow | none | high | Low |

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

[Original report and recorded commands](https://github.com/dragginzgame/icydb/blob/5bf0845a043448b0ed07d39f294da0663a16fefe/docs/reports/investigations/2026/05/21/complexity-surface-hygiene-icydb-config-build/01/report.md).
