# Complexity / Surface Hygiene Audit - icydb-utils - 2026-05-22

## Run Metadata

| Field [M/C] | Value |
| ---- | ---- |
| `method_version` | `CSH-1.2` |
| `surface_taxonomy` | `ST-1` |
| `authority_taxonomy` | `AT-1` |
| `deletion_confidence_model` | `DC-1` |
| `compatibility_policy` | `pre-1.0-hard-cut` |
| `wasm_signal_rule` | `raw-wasm-primary` |
| `baseline_report` | prior informal `icydb-utils` cleanup in `0.161.1`; no CSH-1.2 baseline |
| `comparability_status` | `non-comparable`; first stricter CSH-1.2 pass |
| `code_snapshot` | `11b77ad92` plus dirty worktree |
| `in_scope_roots` | `crates/icydb-utils/src`, `crates/icydb-utils/Cargo.toml`, in-tree consumers |
| `excluded_roots` | `target`, historical changelogs, unrelated dirty files |
| `generated_code_inclusion` | `sampled`; schema-derive consumers inspected |
| `test_surface_inclusion` | `included`; in-crate tests are the main behavioral proof for case conversion helpers |

## Step Status

| Step [M] | Status [C] | Evidence Artifact [M/C] | Comparability Impact [C] |
| ---- | ---- | ---- | ---- |
| STEP 0 | PASS | metadata table above | first CSH-1.2 run |
| STEP 1 | PASS | crate-root exports, case enum, `Casing`, and `to_snake_case` inventory | non-comparable |
| STEP 2 | PASS | stale-signal scan plus enum-variant consumer scan | new validator consumers added for formerly unused variants |
| STEP 3 | PASS | runtime/schema-derive consumer review | none |
| STEP 4 | PASS | crate-boundary and single-purpose helper review | none |
| STEP 5 | N/A | no `icydb` facade or generated `__macro` boundary in this crate | none |
| STEP 6 | PASS | in-crate tests and downstream validator/sanitizer consumers reviewed | none |
| STEP 7 | PASS | removal safety plan below | none |
| STEP 8 | PASS | risk score table below | none |

## Reachable Surface And Retention Inventory Summary

| Item [M] | Kind [M] | Path [M] | Visibility [M] | Feature/Cfg [M] | Consumer Evidence [M/C] | Consumer Should Exist? [C] | Authority Reason [C] | Surface Class [C] | Owner [C] | Disposition [C] | Risk [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| `Case` | enum | `crates/icydb-utils/src/case/mod.rs` | public | none | `icydb` validators/sanitizers and `icydb-schema-derive` node validation/generation import it | Yes | shared text-case vocabulary prevents drift between runtime validation/sanitization and schema macro naming | `live-authority` | case conversion helper crate | `RETAIN WITH OWNER` | Low |
| `Casing` | trait | `crates/icydb-utils/src/case/mod.rs` | public | none | runtime validators/sanitizers and derive parsers call `to_case` / `is_case` | Yes | shared conversion entrypoint gives runtime and macro code one behavior source | `live-authority` | case conversion helper crate | `RETAIN WITH OWNER` | Low |
| `to_snake_case` | function | `crates/icydb-utils/src/case/snake.rs` | public re-export | none | `icydb-core` identity naming and constant-case helper call it | Yes | core identity normalization and derive/runtime case conversion share one snake-case implementation | `live-authority` | case conversion helper crate | `RETAIN WITH OWNER` | Low |
| `constant`, `snake`, `title` helpers | private modules | `crates/icydb-utils/src/case/*` | private | tests | called by `Casing` and in-crate tests | Yes | implementation support for retained case variants | `live-authority` | case conversion helper crate | `RETAIN WITH OWNER` | Low |

## Dead / Stale Candidate Table

| Candidate [M] | File [M] | Lines [M] | Signal [M] | Current Consumers [M/C] | Consumer Should Exist? [C] | Authority Reason [C] | Surface Class [C] | Deletion Confidence [C] | Disposition [C] | Risk If Removed [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| None | N/A | N/A | former `Case::Camel`, `Case::Sentence`, and `Case::UpperKebab` variants now have runtime validator consumers | Yes | text-case validation surface added in `icydb` | `live-authority` | N/A | `RETAIN WITH OWNER` | Low |

## Runtime Authority Drift Findings

| Area [C] | Runtime Authority [C] | Alternate Authority Found? [C] | Evidence [M/C] | Allowed Role? [C] | Finding [C] | Risk [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| runtime text validators/sanitizers | `icydb` base validator/sanitizer surfaces | No | validators/sanitizers import `Case` and `Casing` directly | Yes | shared helper crate remains a narrow utility owner, not runtime authority drift | Low |
| schema macro naming | `icydb-schema-derive` generated metadata/parser validation | No | derive uses `Case::Constant`, `Case::Snake`, `Case::UpperCamel`, and `Case::UpperSnake` | Yes | helper crate provides deterministic naming behavior only | Low |
| core identity normalization | `icydb-core` identity module | No | core imports only `to_snake_case` | Yes | retained direct function avoids duplicating snake-case behavior in core | Low |

## Removal Safety Plan

| Candidate [M] | Action [C] | Disposition [C] | Owner Boundary [C] | Required Proof [C] | Focused Validation [C] | Wasm Raw Bytes Relevant? [C] | Follow-Up Trigger [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| crate existence | retain as shared case owner | `RETAIN WITH OWNER` | runtime validators/sanitizers plus schema derive naming | consumer scan proves use from `icydb`, `icydb-core`, and `icydb-schema-derive` | package/workspace compile | no | revisit only if either runtime or derive stops using shared case conversion |

## Risk Score

| Bucket [C] | Count [D] | Highest Risk [C] | Notes [C] |
| ---- | ----: | ---- | ---- |
| stale compatibility | 0 | N/A | no compatibility paths found |
| stale generated fallback | 0 | N/A | no generated-model fallback surface |
| orphaned helper | 0 | N/A | formerly unused case variants now have validator consumers |
| overexposed internal | 0 | N/A | crate root is narrow after prior 0.161.1 cleanup |
| duplicate surface | 0 | N/A | no duplicate case implementation found in inspected consumers |
| unclear | 0 | N/A | no owner decision needed |

Dead-surface pressure score: **0/10** after adding validator consumers for the formerly unused case variants. The crate remains justified as the shared owner of case conversion used by runtime validators/sanitizers, schema derive, and core identity normalization.

## Disposition Summary

| Disposition [C] | Count [D] | Items [C] |
| ---- | ----: | ---- |
| `RETAIN WITH OWNER` | 4 | `Case`, `Casing`, `to_snake_case`, private case helpers |
| `DEFER WITH TRIGGER` | 1 | crate boundary; revisit only if runtime or derive stops sharing case conversion |

## Verification Readout

| Check [M] | Result [M/C] |
| ---- | ---- |
| file/source inventory | pass |
| consumer scan | pass |
| stale-signal scan | pass |
| focused validation | pass; focused macro validation tests, `cargo check -p icydb -p icydb-schema-tests`, `cargo clippy -p icydb -p icydb-schema-tests --all-targets -- -D warnings`, formatting, and diff whitespace checks |

## Follow-Up Actions

None for this crate unless future work removes either the runtime or derive use of shared case conversion.
