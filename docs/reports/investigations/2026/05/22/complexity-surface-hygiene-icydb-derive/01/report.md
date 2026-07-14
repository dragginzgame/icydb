# Complexity / Surface Hygiene Audit - icydb-derive - 2026-05-22

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
| `comparability_status` | `non-comparable`; first focused `icydb-derive` run under CSH-1.1 |
| `code_snapshot` | `11b77ad92` plus dirty worktree |
| `in_scope_roots` | `crates/icydb-derive/src`, `crates/icydb-derive/Cargo.toml`, `crates/icydb-derive/README.md`, sampled macro-test/schema consumers |
| `excluded_roots` | `target`, generated build output, unrelated crate audits |
| `generated_code_inclusion` | `direct`; this crate emits generated impls and was inspected as generated-code authority |
| `test_surface_inclusion` | `sampled`; downstream schema macro tests and facade retarget checks considered |

## Step Status

| Step [M] | Status [C] | Evidence Artifact [M/C] | Comparability Impact [C] |
| ---- | ---- | ---- | ---- |
| STEP 0 | PASS | metadata table above | first CSH-1.1 run |
| STEP 1 | PASS | crate-root proc-macro entrypoint inventory and helper module scan | non-comparable |
| STEP 2 | PASS | stale-signal scan for `StorageKey`, `::icydb::db`, storage traits, compatibility, fallback, shim, and dead-code markers | no stale public generated path remains |
| STEP 3 | PASS | generated persistence/value authority review | none |
| STEP 4 | PASS | module-size and helper ownership review | none |
| STEP 5 | PASS | facade/export boundary reviewed between proc-macro entrypoints and private generator helpers | helper visibility narrowed |
| STEP 6 | PASS | test/diagnostics exposure reviewed through downstream macro tests | none |
| STEP 7 | PASS | one low-risk helper visibility cleanup applied | none |
| STEP 8 | PASS | risk score table below | none |

## Footprint Summary

| Metric [M] | Value [D] | Notes [C] |
| ---- | ----: | ---- |
| total inspected LOC | 741 | source plus manifest and README |
| source LOC | 709 | proc-macro entrypoints plus generator helpers |
| largest source file | 229 | `ops.rs`; arithmetic newtype derive generator |
| proc-macro entrypoints | 14 | all intentionally public through `#[proc_macro_derive]` |
| private generator modules | 6 | `display`, `field_projection`, `inner`, `newtype`, `ops`, `persisted_row` |

## Reachable Surface And Retention Inventory Summary

| Item [M] | Kind [M] | Path [M] | Visibility [M] | Consumer Evidence [M/C] | Authority Reason [C] | Surface Class [C] | Owner [C] | Risk [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| arithmetic derives | proc macros | `crates/icydb-derive/src/lib.rs` | public proc-macro exports | facade re-exports and user macro tests | derive entrypoints are the crate's public reason to exist | live-generated-boundary | derive facade | Low |
| `FieldProjection` derive | proc macro | `crates/icydb-derive/src/lib.rs`, `field_projection.rs` | public entrypoint, crate-private helper | generated value projection tests | generated impl targets `::icydb::__macro::FieldProjection` | live-generated-boundary | projection generator | Low |
| `PersistedRow` derive | proc macro | `crates/icydb-derive/src/lib.rs`, `persisted_row.rs` | public entrypoint, crate-private helper | generated persistence tests and schema macro tests | generated impl targets hidden `::icydb::__macro` storage contracts | live-generated-boundary | persistence generator | Low |
| `Display` and `Inner` derives | proc macros | `display.rs`, `inner.rs` | public entrypoints, crate-private helpers | facade tests and downstream derive users | simple newtype convenience macros | live-user-surface | derive facade | Low |
| `newtype` parser | helper module | `newtype.rs` | crate-private | `display`, `inner`, and `ops` modules | shared parser removes duplicated tuple-newtype checks | live-internal-helper | derive internals | Low |

## Dead / Stale Candidate Table

| Candidate [M] | File [M] | Signal [M] | Current Consumers [M/C] | Authority Reason [C] | Surface Class [C] | Deletion Confidence [C] | Action [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| module-level `derive_*` helpers as plain `pub` | generator modules | private module helper visibility wider than needed | only `lib.rs` in this crate calls them | proc-macro entrypoints are the public boundary; generator helpers are internal implementation | overexposed-internal | high | narrowed to `pub(crate)` |
| generated `::icydb::db::*` persistence paths | `persisted_row.rs` | low-level storage traits formerly routed through DB facade | no live post-retarget consumer | generated persistence plumbing belongs behind `::icydb::__macro`, not normal DB facade | stale-generated-boundary | high | already retargeted to `::icydb::__macro` in the working slice |

No dead derive entrypoint, unused dependency, generated-model runtime authority drift, or compatibility shim was found.

## Runtime Authority Drift Findings

| Area [C] | Runtime Authority [C] | Alternate Authority Found? [C] | Evidence [M/C] | Allowed Role? [C] | Finding [C] | Risk [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| persistence layout | accepted schema/runtime storage contracts | No | `PersistedRow` derive emits slot read/write bridge only and delegates field storage to `PersistedFieldSlotCodec` | Yes | derive is mechanical generated wiring, not runtime authority | Low |
| value projection | facade hidden generated trait lane | No | `FieldProjection` derive targets `::icydb::__macro::FieldProjection` and `Value` | Yes | value projection surface stays hidden from normal user APIs | Low |
| arithmetic/newtype helpers | user-facing trait impl generation | No | derives emit impls for `::icydb::traits::*` newtype traits | Yes | no storage or schema authority involved | Low |

## Complexity Tied To Dead Surface

| Module [M] | Complexity Signal [M] | Dead-Surface Link [C] | Current Consumers [M/C] | Shrink Action [C] | Risk [C] |
| ---- | ---- | ---- | ---- | ---- | ---- |
| `ops.rs` | largest file, many similar derive helpers | none; generated arithmetic impls are live | proc-macro entrypoints | none beyond helper visibility narrowing | Low |
| `persisted_row.rs` | low-level generated storage bridge | stale facade path already retargeted to `__macro` | `PersistedRow` derive users | retain; generated boundary is intentional | Low |
| `field_projection.rs` | generated value projection | none after `__macro` targeting | `FieldProjection` derive users | retain | Low |

## Removal Safety Plan

| Candidate [M] | Action [C] | Owner Boundary [C] | Required Proof [C] | Focused Validation [C] | Follow-Up Required? [C] |
| ---- | ---- | ---- | ---- | ---- | ---- |
| plain `pub` generator helpers | narrowed to `pub(crate)` | proc-macro crate internals | modules are private and only crate root calls helpers | `cargo check -p icydb-derive`; clippy; downstream macro tests | no |

## Risk Score

| Bucket [C] | Count [D] | Highest Risk [C] | Notes [C] |
| ---- | ----: | ---- | ---- |
| stale compatibility | 0 | N/A | none found |
| stale generated fallback | 0 | N/A | no runtime generated-model authority in this crate |
| orphaned helper | 0 | N/A | helpers are called from proc-macro entrypoints |
| overexposed internal | 1 | Low | helper visibility narrowed |
| duplicate surface | 0 | N/A | no duplicate derive ownership found |
| unclear | 0 | N/A | no owner decision required |

Dead-surface pressure score: **1/10**. Remaining complexity is live generated-code emission.

## Verification Readout

| Check [M] | Result [M/C] |
| ---- | ---- |
| proc-macro entrypoint inventory via `rg` | pass |
| stale-signal scan via `rg` | pass; no live `::icydb::db::*` generated storage path remains |
| helper visibility review | pass; generator helpers narrowed to `pub(crate)` |
| dependency review | pass; manifest contains only `proc-macro2`, `quote`, and `syn` |
| focused validation | pass; `cargo check/test/clippy -p icydb-derive`, downstream schema macro tests, formatting, and diff whitespace checks |

## Follow-Up Actions

None for `icydb-derive`. The larger generated-schema hygiene work belongs in `icydb-schema-derive`, not this crate.
