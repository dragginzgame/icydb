# Module Surface Hardening Audit - types::ulid - 2026-06-08

## Run Metadata

| Field [M/C] | Value |
| ---- | ---- |
| `method_version` | `MSH-2.0` |
| `surface_taxonomy` | `ST-1` |
| `authority_taxonomy` | `AT-1` |
| `deletion_confidence_model` | `DC-1` |
| `compatibility_policy` | `pre-1.0-hard-cut` |
| `wasm_signal_rule` | `raw-wasm-primary` |
| `hot_path_risk_model` | `HP-1` |
| `proof_policy` | `read-only-first` |
| `baseline_report` | N/A; first MSH-2.0 pass for `types::ulid` |
| `comparability_status` | `non-comparable`; no prior MSH-2.0 module baseline |
| `code_snapshot` | `77328a199` plus local MSH cleanup/report changes |
| `target module` | `crates/icydb-core/src/types/ulid` |
| `owning crate` | `icydb-core` |
| `expected hotness` | `warm`, with `encode-decode-hot` / `wasm-sensitive` byte paths |
| `in_scope_roots` | `crates/icydb-core/src/types/ulid/mod.rs`, `crates/icydb-core/src/types/ulid/generator.rs`, `crates/icydb-core/src/types/ulid/tests.rs`, selected direct consumers |
| `excluded_roots` | `target`, historical docs, unrelated modules except focused consumers |
| `generated_code_inclusion` | sampled; schema derive generated insert and composite key byte paths inspected |
| `test_surface_inclusion` | included for module tests; sampled/count-based for broad in-tree fixture consumers |
| `patch_mode` | `implementation-requested` |

## Step Status

| Step [M] | Status [C] | Evidence Artifact [M/C] | Comparability Impact [C] |
| ---- | ---- | ---- | ---- |
| STEP 0 | PASS | metadata table above | first target run |
| STEP 1 | PASS | public item scan plus re-export scan through `icydb_core::types::*` and `icydb::types` | none |
| STEP 2 | PASS | stale-signal scan for `dead_code`, `legacy`, `compat`, `fallback`, `shim`, `deprecated`, generated-model terms | none |
| STEP 3 | PASS | primary-key, persisted scalar, cursor token, SQL generated insert, and schema derive consumers inspected | none |
| STEP 4 | PASS | private generator and one-caller helpers reviewed | none |
| STEP 5 | PASS | public facade exposure reviewed through `icydb::types` | none |
| STEP 6 | PASS | module tests plus broad deterministic-fixture consumer counts reviewed | none |
| STEP 7 | PASS | removal/narrowing plan plus applied generation-error split below | none |
| STEP 8 | PASS | risk score table below | none |

## Reachable Surface And Retention Inventory Summary

| Item [M] | Kind [M] | Path [M] | Visibility [M] | Feature/Cfg [M] | Consumer Evidence [M/C] | Consumer Should Exist? [C] | Authority Reason [C] | Surface Class [C] | Owner [C] | Disposition [C] | Risk [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| `Ulid` | struct | `crates/icydb-core/src/types/ulid/mod.rs` | public via `icydb_core::types::*` and `icydb::types` | none | typed primary keys, `Value::Ulid`, persisted scalar decode, cursor token decode, SQL insert generation | Yes | canonical runtime scalar for ULID keys and persisted scalar values | `live-authority` | core scalar type layer | `RETAIN WITH OWNER` | Medium |
| `Ulid::generate` | function | `crates/icydb-core/src/types/ulid/mod.rs` | public | none | SQL generated insert uses it; schema derive admits only explicit `Ulid::generate` for generated insert fields | Yes | current write-time generated key authority | `live-authority` | core scalar type layer plus SQL write generated-field path | `RETAIN WITH OWNER` | Medium |
| `Ulid::from_bytes` / `to_bytes` | functions | `crates/icydb-core/src/types/ulid/mod.rs` | public | none | persisted scalar, structural field, cursor token, key taxonomy, `EntityKeyBytes` all use canonical 16-byte payloads | Yes | zero-allocation canonical byte representation for storage, cursor, key, and value paths | `live-authority` | core scalar type layer | `RETAIN HOT PATH` | Medium |
| `Ulid::try_from_bytes` / `TryFrom<&[u8]>` | function / impl | `crates/icydb-core/src/types/ulid/mod.rs` | public | none | module-owned fallible slice decode; mirrors other fixed-width primitive decode surfaces | Yes | bounded fallible decode surface for external dynamic bytes | `live-authority` | core scalar type layer | `RETAIN WITH OWNER` | Low |
| `Ulid::nil`, `MIN`, `MAX` | constants/functions | `crates/icydb-core/src/types/ulid/mod.rs` | public | none | generator default uses `nil`; boundary constants are standard scalar sentinels | Yes | scalar boundary/sentinel vocabulary without allocation or parsing | `live-authority` | core scalar type layer | `RETAIN WITH OWNER` | Low |
| `Ulid::from_timestamp_and_randomness` | function | `crates/icydb-core/src/types/ulid/mod.rs` | private | none | called only by private ULID generator after cross-module tests moved to deterministic public fixtures | Yes | private implementation support for monotonic generated-key construction | `live-authority` | ULID generator | `RETAIN WITH OWNER` | Low |
| `Ulid::from_u128` | function | `crates/icydb-core/src/types/ulid/mod.rs` | `#[cfg(test)] pub(crate)` | test only | broad in-tree use is deterministic tests/fixtures; no production authority found | Yes | crate-local deterministic test fixture support without production facade exposure | `live-test-support` | core test support | `RETAIN WITH OWNER` | Low |
| `UlidParseError` | enum | `crates/icydb-core/src/types/ulid/mod.rs` | public | none | `FromStr` exposes only invalid string parse failure | Yes | public parse failure taxonomy now matches reachable parse behavior | `live-authority` | core scalar type layer | `RETAIN WITH OWNER` | Low |
| `UlidGenerationError` | enum | `crates/icydb-core/src/types/ulid/generator.rs` | private | none | private generator returns overflow/randomness failures before public `generate` panics | Yes | generation failure taxonomy is internal until/unless a public fallible generator is intentionally added | `live-authority` | ULID generator | `RETAIN WITH OWNER` | Low |
| `UlidDecodeError` | enum | `crates/icydb-core/src/types/ulid/mod.rs` | public | none | returned by `try_from_bytes` and `TryFrom<&[u8]>` | Yes | typed, bounded fixed-width decode error | `live-authority` | core scalar type layer | `RETAIN WITH OWNER` | Low |
| `types::ulid::generator` | private module | `crates/icydb-core/src/types/ulid/generator.rs` | private module, `pub(super)` entrypoint | `wasm32` sequence branch; non-wasm random branch | called only by `Ulid::try_generate`; tests cover monotonic/native/w wasm paths | Yes | process-local monotonic generated key authority | `live-authority` | ULID generator | `RETAIN WITH OWNER` | Medium |

## Dead / Stale Candidate Table

| Candidate [M] | File [M] | Lines [M] | Signal [M] | Current Consumers [M/C] | Consumer Should Exist? [C] | Authority Reason [C] | Surface Class [C] | Deletion Confidence [C] | Disposition [C] | Risk If Removed [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| `Ulid::from_timestamp_and_randomness` public visibility | `crates/icydb-core/src/types/ulid/mod.rs` | former public constructor | production use is private generator; public facade made generator construction externally visible | fixed in this cleanup slice | Generator yes; broad public visibility no proven need | construction is now private implementation support for monotonic generation | `overexposed-internal` | `high` | `NARROW NOW` | validated by focused compile/tests |
| `Ulid::from_u128` public visibility | `crates/icydb-core/src/types/ulid/mod.rs` | former public constructor | broad in-tree use is deterministic fixture construction; equivalent production route exists via `from_bytes(n.to_be_bytes())` | fixed in this cleanup slice | Tests need deterministic IDs, but production surface does not need to be the owner | fixture support is now `#[cfg(test)] pub(crate)` | `duplicate-surface` | `high` | `NARROW NOW` | validated by focused compile/tests |
| `UlidError::{GeneratorOverflow, RandomnessUnavailable}` | `crates/icydb-core/src/types/ulid/mod.rs`, `crates/icydb-core/src/types/ulid/generator.rs` | former public error variants | private generation errors leaked through the public parse error enum; no public `try_generate` exists | fixed in this cleanup slice | Private generator keeps typed failures; public parse consumers now only see `UlidParseError` | split completed | `overexposed-internal` | `high` | `DELETE NOW` | validated by focused compile/tests |

No remaining deferred ULID surface finding from this pass. The deterministic
fixture constructor is retained as test-only crate-local support.

## Runtime Authority Drift Findings

| Area [C] | Runtime Authority [C] | Alternate Authority Found? [C] | Evidence [M/C] | Allowed Role? [C] | Finding [C] | Risk [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| generated insert | accepted schema write policy plus SQL write path | No | schema derive admits `Ulid::generate`; SQL write path materializes `Value::Ulid(Ulid::generate())` | Yes | no generated-model runtime fallback found | Low |
| storage bytes | accepted schema scalar codec and key codecs | No | persisted scalar, structural field, cursor token, and key taxonomy decode from exact 16-byte payloads | Yes | byte authority remains catalog/runtime-native | Low |
| deterministic fixture constructors | tests | No | `from_u128` supports many tests but is now `#[cfg(test)] pub(crate)`; timestamp/random construction was narrowed to the private generator | Yes | deterministic construction no longer widens production facade | Low |
| generation failure taxonomy | private generator | No | generation failures now use private `UlidGenerationError`; `FromStr` returns public `UlidParseError` | Yes | public parse error enum no longer carries private generator failures | Low |

## Hot Path / Wasm Regression Gate

| Code Unit [M] | Hotness [C] | Proposed Cleanup [C] | Optimization Risk [C] | Required Proof [C] | Disposition [C] |
| ---- | ---- | ---- | ---- | ---- | ---- |
| `Ulid::from_bytes` / `to_bytes`, persisted scalar, cursor, and key decode paths | `encode-decode-hot`, `wasm-sensitive` | none; do not route through string parsing or heap allocation for cleanup | string conversion would add allocation and parsing to success paths | raw wasm/instruction proof if changed | `RETAIN HOT PATH` |
| `types::ulid::generator` | `warm`, `wasm-sensitive` | possible error-type split only | error taxonomy split should not change success-path generation shape, but broad refactors could | focused core tests; raw wasm check only if generation logic changes | `RETAIN WITH OWNER` |
| deterministic constructors | `test-only` for `from_u128`; private generator for timestamp/random construction | narrowed both constructors out of public production facade | low runtime risk; main cost is compile/test coverage for fixture access | compile plus focused ULID/key/persisted-row tests | `NARROW NOW` |

## Risk Score

| Bucket [C] | Count [D] | Highest Risk [C] | Notes [C] |
| ---- | ----: | ---- | ---- |
| stale compatibility | 0 | N/A | no compatibility branches found |
| stale generated fallback | 0 | N/A | no generated-model runtime fallback found |
| orphaned helper | 0 | N/A | private generator helpers have current owner |
| overexposed internal | 0 | N/A | timestamp/random public constructor and generation error variants were removed from public surface |
| duplicate surface | 0 | N/A | `from_u128` is now test-only crate-local fixture support |
| unclear | 0 | N/A | facade API cleanup trigger is clear enough to defer |

Dead-surface pressure score: **0/10**. The module is fundamentally justified as
the core ULID scalar and key type. The audited public overexposure findings
were narrowed out of the production facade.

## Disposition Summary

| Disposition [C] | Count [D] | Items [C] |
| ---- | ----: | ---- |
| `RETAIN WITH OWNER` | 9 | `Ulid`, `Ulid::generate`, `Ulid::try_from_bytes`, test-only `Ulid::from_u128`, private `Ulid::from_timestamp_and_randomness`, `UlidDecodeError`, `UlidParseError`, `UlidGenerationError`, private generator |
| `RETAIN HOT PATH` | 1 | canonical byte encode/decode surface |
| `DEFER WITH TRIGGER` | 0 | none |
| `NARROW NOW` | 2 | `Ulid::from_timestamp_and_randomness` made private; `Ulid::from_u128` made `#[cfg(test)] pub(crate)` |
| `DELETE NOW` | 1 | generation-only public `UlidError` variants removed by the cleanup slice |

## Verification Readout

| Check [M] | Result [M/C] |
| ---- | ---- |
| file/source inventory | pass |
| public/re-export scan | pass |
| stale-signal scan | pass; no `allow(dead_code)`, stale compatibility, fallback, shim, deprecated, or generated-model reconstruction signal in target module |
| consumer scan | pass; direct production consumers and broad deterministic-fixture counts inspected |
| focused validation | pass; core compile, facade compile, SQL-feature ULID test filter, SQL-feature diagnostics fixture test, SQL-feature direct persisted-row codec tests, and SQL-feature core-library clippy completed after cleanup |
| no-feature lib-test filter | blocked; unrelated SQL-gated test/support imports currently keep `cargo test -p icydb-core types::ulid --lib` from compiling without `--features sql` |

## Follow-Up Actions

None from this pass. Done in the cleanup slice: `Ulid::from_timestamp_and_randomness` is private, `Ulid::from_u128` is test-only crate-local fixture support, public parse failure is `UlidParseError`, and private generation failure is `UlidGenerationError`.
