> Historical assessment, not current-code qualification. Raw output and superseded runs
> were retired from the working tree under Shared 0.30.4. Git history retains them.
> Current follow-up authority is GitHub issues; selected results below retain their
> original source, method and limitations, not a fresh measurement.

# Complexity / Surface Hygiene Audit - icydb - 2026-05-21

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

## Runtime Authority Drift Findings

| Area [C] | Runtime Authority [C] | Alternate Authority Found? [C] | Evidence [M/C] | Allowed Role? [C] | Finding [C] | Risk [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| accepted schema | accepted schema snapshots in `icydb-core` sessions | No | facade `model` exports are hidden/generated-boundary only; SQL/schema endpoints call session/core methods | Yes | facade does not reconstruct runtime authority from generated models | Low |
| SQL DDL | catalog-native core mutation runners | No | facade SQL module converts and renders already-executed core SQL outputs | Yes | SQL facade is endpoint/result surface, not mutation authority | Low |
| generated endpoints | fixed `__icydb_*` generated names | No | build and derive crates target `__macro`, `__reexports`, `db`, `metrics`, and fixed endpoint names | Yes | low-level generated persistence/value plumbing now uses `__macro`; no endpoint `name = ...` override lane in facade | Low |
| primary-key storage vocabulary | compact key taxonomy in core | No after patch | `StorageKey` removed from `icydb::value`; core still retains internal decoded compatibility type | Yes | facade no longer advertises legacy storage-key value surface | Low |
| metrics/snapshot/schema | generated endpoint gates and CLI DTOs | No | hidden metrics and public DB DTOs are consumed by generated endpoints/CLI | Yes | observability DTOs remain diagnostics surfaces only | Low |

## Facade / Generated Boundary Findings

| Surface [M] | Boundary Type [C] | Generated Consumer Evidence [M/C] | Could Narrow? [C] | Required Replacement [C] | Deletion Confidence [C] | Risk [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| `__macro` | generated wiring lane | derive/build crates emit direct `::icydb::__macro::*` references | No | coordinated generated-code retargeting | low | High |
| `__reexports` | generated dependency lane | generated code uses facade paths for `canic_cdk`, `serde`, `ctor`, `ic_memory`, `icydb_derive` | No | users would need explicit transitive dependencies or derive code would need direct crate paths | low | High |
| `__macro::PersistedRow`, slot traits, stores | hidden generated storage lane | derive-generated persistence impls now target `__macro::{PersistedRow, SlotReader, SlotWriter, InternalError}` | No immediate need | none | low | Low |
| `__macro::EntityValue` | generated entity trait lane | schema derive now emits `::icydb::__macro::EntityValue` | No | none | low | Low |
| `model::*` | generated model lane | schema derive emits `MODEL` constants using model DTOs | No | schema derive architecture change | low | Medium |
| SQL rendering DTOs | endpoint/tooling surface | generated SQL endpoints return `SqlQueryResult`; CLI renders it | No | duplicate DTOs or CLI/core coupling | low | Medium |

## Follow-Up Actions

None for this facade slice. Future generated-code cleanup can still reduce direct `db` references for SQL/session DTOs if a concrete stale surface appears, but no current deletion candidate remains under CSH-1.1.

[Original report and recorded commands](https://github.com/dragginzgame/icydb/blob/5bf0845a043448b0ed07d39f294da0663a16fefe/docs/reports/investigations/2026/05/21/complexity-surface-hygiene-icydb/01/report.md).
