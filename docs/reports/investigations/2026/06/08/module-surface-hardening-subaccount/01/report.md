# Module Surface Hardening Audit - types::subaccount - 2026-06-08

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
| `baseline_report` | N/A; first MSH-2.0 pass for `types::subaccount` |
| `comparability_status` | `non-comparable`; no prior MSH-2.0 module baseline |
| `code_snapshot` | `9a33ccd9a` plus local MSH cleanup/report changes |
| `target module` | `crates/icydb-core/src/types/subaccount.rs` |
| `owning crate` | `icydb-core` |
| `expected hotness` | `warm`, with `encode-decode-hot` / `wasm-sensitive` fixed-width byte paths |
| `in_scope_roots` | `crates/icydb-core/src/types/subaccount.rs`, key taxonomy byte paths, account conversion, cursor/value/persisted scalar decode, schema-derive defaults |
| `excluded_roots` | `target`, historical docs, unrelated modules except focused consumers |
| `generated_code_inclusion` | sampled; schema derive default generation and accepted field-kind paths inspected |
| `test_surface_inclusion` | included for direct `Subaccount` tests and broad in-tree fixture constructor users |
| `patch_mode` | `implementation-requested` |

## Step Status

| Step [M] | Status [C] | Evidence Artifact [M/C] | Comparability Impact [C] |
| ---- | ---- | ---- | ---- |
| STEP 0 | PASS | metadata table above | first target run |
| STEP 1 | PASS | public item scan plus direct facade consumers through `icydb_core::types::*` and `icydb::types` | none |
| STEP 2 | PASS | stale-signal scan for duplicate constructors, orphan conversions, generated-model terms, and fixed-width byte paths | none |
| STEP 3 | PASS | primary-key, account, persisted scalar, cursor token, SQL/generated-default, and schema derive consumers inspected | none |
| STEP 4 | PASS | fixed-width encode/decode helpers reviewed for hot path retention | none |
| STEP 5 | PASS | public facade exposure reviewed through `icydb::types` | none |
| STEP 6 | PASS | test fixtures and module tests reviewed; duplicate constructor references moved to canonical constructor | none |
| STEP 7 | PASS | removal/narrowing plan applied below | none |
| STEP 8 | PASS | risk score table below | none |

## Reachable Surface And Retention Inventory Summary

| Item [M] | Kind [M] | Path [M] | Visibility [M] | Feature/Cfg [M] | Consumer Evidence [M/C] | Consumer Should Exist? [C] | Authority Reason [C] | Surface Class [C] | Owner [C] | Disposition [C] | Risk [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| `Subaccount` | struct | `crates/icydb-core/src/types/subaccount.rs` | public via `icydb_core::types::*` and `icydb::types` | none | `Account`, `Value::Subaccount`, primary keys, cursor tokens, persisted scalar values, schema defaults | Yes | canonical runtime scalar for fixed-width ICRC subaccounts and key/value storage | `live-authority` | core scalar type layer | `RETAIN WITH OWNER` | Medium |
| `Subaccount::from_array` | function | `crates/icydb-core/src/types/subaccount.rs` | public | none | account decode, cursor decode, primary-key decode, schema derive defaults, generated fixtures/tests | Yes | canonical fixed-width constructor with compile-time length enforcement | `live-authority` | core scalar type layer | `RETAIN WITH OWNER` | Medium |
| `Subaccount::to_array` / `to_bytes` | functions | `crates/icydb-core/src/types/subaccount.rs` | public | none | account encode, ordered index encoding, predicate/hash/value storage paths, entity key bytes | Yes | canonical 32-byte representation without allocation or parsing | `live-authority` | core scalar type layer | `RETAIN HOT PATH` | Medium |
| `Subaccount::MIN` / `MAX` | constants | `crates/icydb-core/src/types/subaccount.rs` | public | none | account/key tests, account encode default subaccount, boundary fixtures | Yes | fixed-width scalar boundary/sentinel vocabulary | `live-authority` | core scalar type layer | `RETAIN WITH OWNER` | Low |
| `Subaccount::as_slice` | function | `crates/icydb-core/src/types/subaccount.rs` | `pub(crate)` | none | key taxonomy compact primary-key encoding and length diagnostics only | Yes, crate-internal only | zero-copy crate-local key encoding helper; no public facade authority | `live-authority` | key taxonomy byte path | `NARROW NOW` | Low |
| `From<Principal> for Subaccount` | trait impl | `crates/icydb-core/src/types/subaccount.rs` | public impl | none | identity/account helper conversion and ICRC-style subaccount derivation | Yes | documented principal-derived subaccount construction | `live-authority` | core scalar type layer | `RETAIN WITH OWNER` | Low |
| runtime traits | trait impls | `crates/icydb-core/src/types/subaccount.rs` | public trait behavior | none | `Value`, field validation/sanitization, model/runtime scalar paths | Yes | scalar participates in existing runtime value contracts | `live-authority` | core scalar type layer | `RETAIN WITH OWNER` | Low |

## Dead / Stale Candidate Table

| Candidate [M] | File [M] | Lines [M] | Signal [M] | Current Consumers [M/C] | Consumer Should Exist? [C] | Authority Reason [C] | Surface Class [C] | Deletion Confidence [C] | Disposition [C] | Risk If Removed [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| `Subaccount::new` | `crates/icydb-core/src/types/subaccount.rs` | former public constructor | exact duplicate of `from_array`; broad use was test fixture spelling | fixed in this cleanup slice | No; `from_array` already names the fixed-width constructor and is used by generated/default decode paths | duplicate public spelling added facade surface without separate semantics | `duplicate-surface` | `high` | `DELETE NOW` | validated by focused compile/tests |
| `Subaccount::to_ulid` | `crates/icydb-core/src/types/subaccount.rs` | former public conversion | no in-tree consumers; lossy/implicit interpretation of lower 16 bytes as a ULID | fixed in this cleanup slice | No current owner found | conversion encodes an arbitrary convention outside subaccount storage/key authority | `orphaned-helper` | `high` | `DELETE NOW` | validated by focused compile/tests |
| public `Subaccount::as_slice` | `crates/icydb-core/src/types/subaccount.rs` | former public helper | only crate-local key taxonomy consumers need borrowed bytes | fixed in this cleanup slice | Public no; crate-local yes | public borrowed-byte access duplicated `to_bytes`/`to_array` without external ownership | `overexposed-internal` | `high` | `NARROW NOW` | validated by focused compile/tests |

No remaining deferred Subaccount surface finding from this pass. The scalar
itself remains public because accepted schemas, values, account identifiers,
primary keys, and persisted byte codecs all depend on it as runtime authority.

## Runtime Authority Drift Findings

| Area [C] | Runtime Authority [C] | Alternate Authority Found? [C] | Evidence [M/C] | Allowed Role? [C] | Finding [C] | Risk [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| account identity | `Account` plus accepted subaccount bytes | No | account encode/decode uses 32-byte subaccount arrays and `Subaccount::MIN` for absent subaccount encoding | Yes | no generated-model runtime fallback found | Low |
| primary-key storage | compact key taxonomy and accepted schema field kinds | No | `PrimaryKeyComponent::Subaccount` encodes borrowed bytes and decodes through `from_array` | Yes | byte authority remains catalog/runtime-native | Low |
| persisted values | structural value storage and scalar slot codec | No | `Value::Subaccount` decode/encode uses exact 32-byte payloads | Yes | storage codecs remain bounded and fallible | Low |
| generated defaults | schema derive | No | generated default expressions use `Subaccount::from_array` | Yes | generated model emits scalar construction only; it is not runtime mutation authority | Low |
| arbitrary ULID projection | none | Yes, former `to_ulid` | no consumers or schema authority found | No | removed orphan conversion | Low |

## Hot Path / Wasm Regression Gate

| Code Unit [M] | Hotness [C] | Proposed Cleanup [C] | Optimization Risk [C] | Required Proof [C] | Disposition [C] |
| ---- | ---- | ---- | ---- | ---- | ---- |
| `Subaccount::to_array` / `to_bytes`, account encode, key taxonomy, cursor/value/persisted scalar decode | `encode-decode-hot`, `wasm-sensitive` | none to byte representation; retain direct fixed-width copies | string/hex conversion or heap allocation would regress success paths | focused core/facade compile plus SQL-feature scalar tests; raw wasm proof only if byte layout changes | `RETAIN HOT PATH` |
| `Subaccount::as_slice` | `encode-decode-hot`, crate-local | visibility narrowed only | low; generated code unchanged and key taxonomy still borrows bytes directly | core/facade compile and SQL-feature scalar tests | `NARROW NOW` |
| duplicate fixture constructor | `test-only` | remove `new`; use `from_array` | low; only fixture spelling changes | SQL-feature lib-test target compile | `DELETE NOW` |

## Risk Score

| Bucket [C] | Count [D] | Highest Risk [C] | Notes [C] |
| ---- | ----: | ---- | ---- |
| stale compatibility | 0 | N/A | no compatibility branches found |
| stale generated fallback | 0 | N/A | generated code uses accepted scalar construction only |
| orphaned helper | 0 | N/A | former `to_ulid` removed |
| overexposed internal | 0 | N/A | borrowed-byte helper is now crate-local |
| duplicate surface | 0 | N/A | former `new` duplicate removed |
| unclear | 0 | N/A | all cleanup candidates had direct owner/no-owner evidence |

Dead-surface pressure score: **0/10**. The module is justified as the
Subaccount scalar and fixed-width byte type. The audited dead and overexposed
helpers were removed or narrowed out of the public production facade.

## Disposition Summary

| Disposition [C] | Count [D] | Items [C] |
| ---- | ----: | ---- |
| `RETAIN WITH OWNER` | 5 | `Subaccount`, `Subaccount::from_array`, `Subaccount::MIN` / `MAX`, `From<Principal> for Subaccount`, runtime traits |
| `RETAIN HOT PATH` | 1 | canonical 32-byte encode/decode surface |
| `DEFER WITH TRIGGER` | 0 | none |
| `NARROW NOW` | 1 | `Subaccount::as_slice` made `pub(crate)` |
| `DELETE NOW` | 2 | duplicate `Subaccount::new`; orphaned `Subaccount::to_ulid` |

## Verification Readout

| Check [M] | Result [M/C] |
| ---- | ---- |
| file/source inventory | pass |
| public/re-export scan | pass |
| stale-signal scan | pass; no `allow(dead_code)`, stale compatibility, fallback, shim, deprecated, or generated-model reconstruction signal in target module |
| consumer scan | pass; direct production consumers and broad fixture constructor users inspected |
| focused validation | pass; core compile, facade compile, SQL-feature Subaccount test filter, and SQL-feature core-library clippy completed after cleanup |
| no-feature lib-test filter | not rerun for this slice; previous ULID pass found unrelated SQL-gated test/support imports blocking no-feature lib-test compilation |

## Follow-Up Actions

None from this pass. Done in the cleanup slice: `Subaccount::new` was removed,
`Subaccount::to_ulid` was removed, public `Subaccount::as_slice` was narrowed to
crate-local visibility, and tests now use `Subaccount::from_array`.
