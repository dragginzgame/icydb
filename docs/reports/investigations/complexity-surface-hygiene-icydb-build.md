> Historical assessment, not current-code qualification. Raw output and superseded runs
> were retired from the working tree under Shared 0.30.4. Git history retains them.
> Current follow-up authority is GitHub issues; selected results below retain their
> original source, method and limitations, not a fresh measurement.

# Complexity / Surface Hygiene Audit - icydb-build - 2026-05-21

## Footprint Summary

| Metric [M] | Value [D] | Notes [C] |
| ---- | ----: | ---- |
| total inspected LOC | 1,030 | source, README, and manifest |
| source LOC | 992 | `lib.rs` plus `db/{mod,schema,sql,store}.rs` |
| largest source file | 338 | `db/sql/mod.rs`; generated SQL surface renderer |
| public API declarations | 3 | `generate_with_options`, `BuildOptions`, `build_with_options!` |
| crate-private generated render hubs | 5 | actor builder, store, SQL, schema, metrics/snapshot renderers |
| unit/doc tests | 0 | build crate is validated through downstream build-script consumers |

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

[Original report and recorded commands](https://github.com/dragginzgame/icydb/blob/5bf0845a043448b0ed07d39f294da0663a16fefe/docs/reports/investigations/2026/05/21/complexity-surface-hygiene-icydb-build/01/report.md).
