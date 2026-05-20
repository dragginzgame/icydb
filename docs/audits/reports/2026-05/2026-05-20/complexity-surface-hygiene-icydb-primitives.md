# Complexity / Surface Hygiene Audit - icydb-primitives - 2026-05-20

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
| `comparability_status` | `non-comparable`; first focused `icydb-primitives` run under CSH-1.0 |
| `code_snapshot` | `ce24f4323` plus dirty worktree |
| `in_scope_roots` | `crates/icydb-primitives/src`, `crates/icydb-primitives/README.md` |
| `excluded_roots` | `target`, generated build output, historical docs/changelogs |
| `generated_code_inclusion` | `sampled`; schema and core scalar-registry consumers inspected |
| `test_surface_inclusion` | `full`; crate has no unit or doc tests |

## Step Status

| Step [M] | Status [C] | Evidence Artifact [M/C] | Comparability Impact [C] |
| ---- | ---- | ---- | ---- |
| STEP 0 | PASS | metadata table above | first focused baseline |
| STEP 1 | PASS | public enum/metadata/macro inventory | none beyond first-run status |
| STEP 2 | PASS | stale-signal scan for dead-code, compatibility, fallback, shim, deprecated, `EntityModel`, `IndexModel` | none |
| STEP 3 | PASS | scalar metadata authority review across schema and core consumers | consumer-side duplication noted |
| STEP 4 | PASS | low-consumer public macro/constant review | none |
| STEP 5 | PASS | generated-boundary scan through `icydb-schema` / `icydb-core` scalar registry adapters | none |
| STEP 6 | PASS | cfg/test-surface scan | no tests in crate |
| STEP 7 | PASS | removal safety plan for narrowable candidates | none |
| STEP 8 | PASS | risk bucket table | none |

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

## Dead / Stale Candidate Table

| Candidate [M] | File [M] | Lines [M] | Signal [M] | Current Consumers [M/C] | Surface Class [C] | Authority Reason [C] | Deletion Confidence [C] | Risk If Removed [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| lower-level exported registry entries macro | `crates/icydb-primitives/src/macros.rs` | `2-242` | exported helper has no direct in-tree consumer outside wrapper macro | `scalar_kind_registry!` only | overexposed-helper | `scalar_kind_registry!` is the intended public projection macro; direct entry macro use is not needed in-tree | medium | unknown external macro consumers; macro export changes are breaking |
| public scalar-kind iteration constant | `crates/icydb-primitives/src/lib.rs` | `178` | no in-tree consumer found | none | future-facing-public | useful as canonical iteration surface, but currently unproven by local tests/consumers | low | possible external tooling uses the list |
| consumer-side scalar capability duplication | `crates/icydb-schema/src/types.rs` | `166-263` | schema has local helpers beyond direct `ScalarKind` delegation | derive uses `supports_remainder`, `supports_copy`, `supports_hash`, `supports_numeric_value`, `supports_ord`, and type-class helpers | adjacent-duplication | primitives owns shared scalar capability metadata, but not every schema trait-generation policy bit today | medium | moving too much into primitives could blur schema-only trait-generation policy |

No `stale-compatibility`, `stale-generated-fallback`, deprecated shim, or direct `EntityModel`/`IndexModel` runtime fallback signal was found in `icydb-primitives`.

## Authority Drift Findings

| Area [C] | Intended Authority [C] | Alternate Authority Found? [C] | Evidence [M/C] | Allowed Role? [C] | Finding [C] | Risk [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| scalar capability registry | `icydb-primitives::ScalarKind` metadata | Partial | schema delegates arithmetic, storage-key encoding, and ordering to `ScalarKind`; core projects the primitive registry into `Value` semantics | yes | primitives is the live shared source for major scalar capabilities | Low |
| schema trait generation policy | schema `Primitive` helpers | Yes | schema still owns copy/hash/remainder/numeric group helper logic | yes | adjacent duplication is currently schema-specific rather than stale primitive code | Medium |
| core value semantics | core scalar registry adapter | No | core uses `icydb_primitives::scalar_kind_registry!` and maps families/value patterns locally | yes | primitives does not own runtime `Value` layout | Low |
| generated models | proposal/codegen metadata only | No | primitives has no generated model APIs | yes | no runtime fallback authority | Low |
| persisted decoding | core codecs remain bounded/fallible | No | primitives contains metadata only | yes | out of scope | Low |

## Complexity Tied To Dead Surface

| Module [M] | Complexity Signal [M] | Dead-Surface Link [C] | Public/Hidden Items [M] | Current Consumers [M/C] | Shrink Action [C] | Expected Blast Radius [C] | Risk [C] |
| ---- | ---- | ---- | ----: | ---- | ---- | ---- | ---- |
| `macros.rs` | 280 LOC registry macro DSL | lower-level `scalar_kind_registry_entries!` is exported only to support wrapper macro | 2 exported macros, 2 crate-local adapter macros | core consumes wrapper macro only | consider `#[doc(hidden)]` on the entries macro or keep as explicit advanced API | external macro callers | Medium |
| `lib.rs` scalar helpers | 21 scalar kinds with 8 boolean capability bits | no tests prove registry invariants or list length/uniqueness | 3 public types, 1 public constant | schema/core compile against it | add small invariant tests before future scalar additions | no runtime blast radius | Low |
| adjacent schema helpers | schema still owns some scalar policy helpers | not dead in primitives, but shared capability ownership boundary is not fully documented | N/A | derive uses schema helpers | document which bits are primitives-owned versus schema-only trait-generation policy | schema derive | Medium |

## Facade / Generated-Boundary Findings

| Surface [M] | Boundary Type [C] | Consumer Evidence [M/C] | Could Narrow? [C] | Required Replacement [C] | Deletion Confidence [C] | Risk [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| `ScalarKind` | public metadata API | schema `Primitive` mapping, potential tooling | no | none | low | High if removed |
| `ScalarMetadata` | public return type | returned from `ScalarKind::metadata()` | no | none | low | Medium if removed |
| `ScalarCoercionFamily` | public metadata API | core family adapter | no | none | low | Medium if removed |
| `scalar_kind_registry!` | macro registry API | core scalar registry adapter | no | none | low | High if removed |
| `scalar_kind_registry_entries!` | lower-level macro helper | no direct in-tree consumer | maybe | wrapper macro should remain enough for core | medium | Medium external risk |
| `ALL_SCALAR_KINDS` | public iteration API | no in-tree consumer | maybe | callers could use `scalar_kind_registry!` or local array | low | Low/Medium external risk |

## Feature / Diagnostics / Test Surface Review

| Surface [M] | Feature/Cfg [M] | Production Consumer? [M/C] | Test/Diagnostics Consumer? [M/C] | Visibility Could Narrow? [C] | Action [C] | Risk [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| crate tests | none | N/A | none | N/A | add invariant tests if scalar registry grows | Low |
| `#[expect(clippy::struct_excessive_bools)]` | production type annotation | yes | N/A | no | keep; metadata is intentionally bit-oriented | Low |
| feature-gated diagnostics | none | N/A | N/A | N/A | none | Low |
| hidden exports | none | N/A | N/A | N/A | consider hiding lower entries macro from docs if retained as helper | Low |

## Removal Safety Plan

| Candidate [M] | Action [C] | Owner Boundary [C] | Required Proof [C] | Focused Validation [C] | Wasm Raw Bytes Relevant? [C] | Follow-Up Required? [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| `scalar_kind_registry_entries!` direct public surface | mark hidden or intentionally document | scalar registry macro API | prove only wrapper macro is required by in-tree consumers | `cargo check -p icydb-core -p icydb-schema`; macro docs/trybuild if added | no | optional |
| `ALL_SCALAR_KINDS` | keep with tests or remove before 1.0 | scalar iteration API | decide if public iteration is intended; if kept, test order/length coverage | `cargo test -p icydb-primitives` | no | optional |
| schema-side scalar helper duplication | document or migrate specific bits | schema trait-generation vs shared scalar metadata | classify `supports_remainder`, copy/hash, and numeric group helpers as schema-only or move capability bits into primitives | schema derive tests; `cargo check -p icydb-schema -p icydb-core` | no | yes |

## Risk Score

Overall risk index: `2/10`.

| Bucket [C] | Count [D] | Highest Risk [C] | Notes [C] |
| ---- | ----: | ---- | ---- |
| stale compatibility | 0 | Low | no legacy/compat/fallback/shim/deprecated signals found |
| stale generated fallback | 0 | Low | no model fallback or runtime reconstruction path |
| orphaned helper | 0 | Low | lower entries macro is helper surface, not dead code |
| overexposed internal | 1 | Medium | lower-level exported macro |
| duplicate surface | 1 | Medium | adjacent schema helper duplication, not primitive-local stale code |
| unclear | 1 | Low/Medium | `ALL_SCALAR_KINDS` is future-facing but unused in-tree |

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
