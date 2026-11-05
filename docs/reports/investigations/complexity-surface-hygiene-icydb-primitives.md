> Historical assessment, not current-code qualification. Raw output and superseded runs
> were retired from the working tree under Shared 0.30.4. Git history retains them.
> Current follow-up authority is GitHub issues; selected results below retain their
> original source, method and limitations, not a fresh measurement.

# Complexity / Surface Hygiene Audit - icydb-primitives - 2026-05-20

## Footprint Summary

| Metric [M] | Value [D] | Notes [C] |
| ---- | ----: | ---- |
| total source LOC | 458 | `lib.rs` 178 LOC, `macros.rs` 280 LOC |
| README LOC | 12 | short crate-purpose statement |
| public/scoped-public declarations | 25 | includes exported macro declarations |
| dependencies | 0 | no normal dependencies |
| unit/doc tests | 0 | compile-only validation today |

## Reachable Surface Inventory Summary

| Item [M] | Kind [M] | Path [M] | Visibility [M] | Consumer Evidence [M/C] | Surface Class [C] | Owner [C] | Risk [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| scalar kind enum | enum | `crates/icydb-primitives/src/lib.rs:11` | public | schema maps `Primitive` to `ScalarKind`; core projects registry entries into `Value` semantics | live-authority | scalar capability registry | Low |
| scalar metadata accessors | type + methods | `crates/icydb-primitives/src/lib.rs:99` | public type, private fields, public accessors | returned by `ScalarKind::metadata()` and used through kind helper methods | live-authority | scalar capability registry | Low |
| coercion family enum | enum | `crates/icydb-primitives/src/lib.rs:167` | public | core maps families through `scalar_registry_family!` | live-authority | scalar coercion routing | Low |
| scalar registry macro | macro | `crates/icydb-primitives/src/macros.rs:245` | `#[macro_export]` | `icydb-core/src/scalar_registry.rs` consumes it through core adapter macros | live-generated-boundary | scalar capability registry | Low |
| registry entries macro | macro | `crates/icydb-primitives/src/macros.rs:2` | `#[macro_export]` | only called by `scalar_kind_registry!` in-tree | overexposed-helper | scalar capability registry | Medium |
| all scalar kinds constant | const | `crates/icydb-primitives/src/lib.rs:178` | public | no in-tree consumer found | future-facing-public | scalar capability registry | Low/Medium |
| README | docs | `crates/icydb-primitives/README.md:1` | packaged docs | correctly describes canonical scalar registry role | live-docs | crate package surface | Low |

## Authority Drift Findings

| Area [C] | Intended Authority [C] | Alternate Authority Found? [C] | Evidence [M/C] | Allowed Role? [C] | Finding [C] | Risk [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| scalar capability registry | `icydb-primitives::ScalarKind` metadata | Partial | schema delegates arithmetic, storage-key encoding, and ordering to `ScalarKind`; core projects the primitive registry into `Value` semantics | yes | primitives is the live shared source for major scalar capabilities | Low |
| schema trait generation policy | schema `Primitive` helpers | Yes | schema still owns copy/hash/remainder/numeric group helper logic | yes | adjacent duplication is currently schema-specific rather than stale primitive code | Medium |
| core value semantics | core scalar registry adapter | No | core uses `icydb_primitives::scalar_kind_registry!` and maps families/value patterns locally | yes | primitives does not own runtime `Value` layout | Low |
| generated models | proposal/codegen metadata only | No | primitives has no generated model APIs | yes | no runtime fallback authority | Low |
| persisted decoding | core codecs remain bounded/fallible | No | primitives contains metadata only | yes | out of scope | Low |

## Facade / Generated-Boundary Findings

| Surface [M] | Boundary Type [C] | Consumer Evidence [M/C] | Could Narrow? [C] | Required Replacement [C] | Deletion Confidence [C] | Risk [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| `ScalarKind` | public metadata API | schema `Primitive` mapping, potential tooling | no | none | low | High if removed |
| `ScalarMetadata` | public return type | returned from `ScalarKind::metadata()` | no | none | low | Medium if removed |
| `ScalarCoercionFamily` | public metadata API | core family adapter | no | none | low | Medium if removed |
| `scalar_kind_registry!` | macro registry API | core scalar registry adapter | no | none | low | High if removed |
| `scalar_kind_registry_entries!` | lower-level macro helper | no direct in-tree consumer | maybe | wrapper macro should remain enough for core | medium | Medium external risk |
| `ALL_SCALAR_KINDS` | public iteration API | no in-tree consumer | maybe | callers could use `scalar_kind_registry!` or local array | low | Low/Medium external risk |

## Verification Readout

| Check [M] | Result [C] | Notes [C] |
| ---- | ---- | ---- |
| stale-signal scan | PASS | no dead-code allowances beyond intentional clippy expectation; no compatibility/fallback/shim/deprecated/model symbols |
| public-surface scan | PASS | 25 public/scoped-public declarations including macros |
| generated-boundary scan | PASS | schema/core consumers account for primary scalar registry surface |
| `cargo check -p icydb-primitives` | PASS | crate compiles |
| `cargo test -p icydb-primitives` | PASS | 0 unit tests, 0 doc tests |
| `cargo check -p icydb-schema` | PASS | schema consumer compiles |
| `cargo check -p icydb-core` | PASS | core registry consumer compiles |

## Follow-Up Actions

1. Decide whether `scalar_kind_registry_entries!` is intended public API or should be `#[doc(hidden)]` helper surface behind `scalar_kind_registry!`.
2. Add small primitive invariant tests if `ALL_SCALAR_KINDS` remains public: length matches registry, contains every `ScalarKind` once, and metadata access works for every entry.
3. Document which scalar capability bits belong in primitives versus schema-only trait-generation policy before moving any of the remaining schema `Primitive` helpers.

[Original report and recorded commands](https://github.com/dragginzgame/icydb/blob/5bf0845a043448b0ed07d39f294da0663a16fefe/docs/reports/investigations/2026/05/20/complexity-surface-hygiene-icydb-primitives/01/report.md).
