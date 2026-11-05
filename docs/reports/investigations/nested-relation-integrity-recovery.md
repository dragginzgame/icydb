> Historical assessment, not current-code qualification. Raw output and superseded runs
> were retired from the working tree under Shared 0.30.4. Git history retains them.
> Current follow-up authority is GitHub issues; selected results below retain their
> original source, method and limitations, not a fresh measurement.

# Nested-Relation Integrity And Recovery Audit

## Run Metadata And Scope

- Date: 2026-09-05 UTC.

- Method: `NREL-1.0` with `DOMAIN-1`; bounded investigation using the applicable
  storage/recovery audit obligations, plus nested-relation limit and reverse-edge
  contracts from the 0.253 design.

- Snapshot: `2e156dce34adedbf9981e473f1f16c3ebf267752`;
  tree `7eb88137093e4cd5ecb97a852967002480ee0416`.

- Source worktree: clean throughout the audit; only new report evidence added.
  [Source manifest](https://github.com/dragginzgame/icydb/blob/5bf0845a043448b0ed07d39f294da0663a16fefe/docs/reports/investigations/2026/09/05/nested-relation-integrity-recovery/01/artifacts/source-snapshot.json) records inspected file hashes.

- Baseline: `N/A`; first run of this bounded investigation. The 0.253 landing
  receipts were context, not reused execution evidence.

- Comparability: non-comparable, no earlier identical scoped run or measured
  size/performance delta claimed.

- Trigger: user-requested follow-up to published 0.253 and audit-tooling work.

- Selected: repeated list/set/map projection, raw/lookup/reverse budgets,
  source replacement/deletion, same-batch target/source operations, shared
  commit preparation, interrupted recovery, and journal/schema idempotence.

## Verdict

`FAIL`: the unique-target lookup cap counts successful validations rather than
all distinct validation requests. A separate nested-recovery proof gap remains.

All 27 selected repository tests passed: 25 core tests and two real-canister
tests. Two isolated probe checks also passed, one reproducing the counter defect.
Those successful test exits do not turn the demonstrated contract violation
into a passing audit. No row loss, orphaned reverse edge, or unsafe publication
was observed in the executed scenarios; untested nested interruptions are not
claimed safe.

## Verification Readout

Cargo preflight used `CARGO_HOME=/home/adam/projects/icydb/.cache/cargo/icydb`
and `CARGO_TARGET_DIR=/home/adam/projects/icydb/target/icydb`, Rust 1.97.1,
and the locked dependency graph. Core selection was `-p icydb-core --lib
--features sql`. Integration selection was `-p icydb-testing-integration --test
nested_relation_measurement_contract`; that package enables its declared facade
features. Tests were listed before execution; none were ignored.

The core binary produced by the Cargo preflight was
`target/icydb/debug/deps/icydb_core-841b397c2d7e2115`. Its listed selections were
run directly with `RUST_TEST_THREADS=8`, avoiding contention with the independent
integration-target build. The binary and selection were not substituted.

| Selection/check | Result | Executed evidence |
| --- | --- | --- |
| `cargo test --locked -p icydb-core --lib --features sql db::relation::reverse_index::tests -- --list`, then the produced core binary with `db::relation::reverse_index::tests --nocapture` | PASS | 18 listed; 18 passed, 0 failed, 0 ignored |
| Core binary `mixed_entity_recovery_after_ --list`, then `mixed_entity_recovery_after_ --nocapture` | PASS | All five maintained cut points listed and passed; 0 failed/ignored |
| Core binary `exact_controls_append_replay_retire_and_reopen --list`, then the same filter with `--nocapture` | PASS | 1 listed and passed; 0 failed/ignored |
| Core binary `journaled_schema_candidate_replay_and_fold_are_idempotent --list`, then the same filter with `--nocapture` | PASS | 1 listed and passed; 0 failed/ignored |
| `cargo test --locked -p icydb-testing-integration --test nested_relation_measurement_contract repeated_actor_ -- --list` | PASS | Exactly two matching tests listed |
| Same integration selection with `-- --test-threads=1 --nocapture`, through the repository PocketIC wrapper | PASS | 2 passed, 0 failed/ignored; 3 unrelated tests filtered out; 35 seconds |
| `rustc --edition=2024 --test artifacts/lookup-budget-probe.rs -o <temporary-binary>`, then `--list` and `--nocapture` | PASS | 2 isolated checks; successful-hit control and confirmed miss-accounting defect; not repository runtime tests |
| Source-body comparison for the isolated probe | PASS | `validate_target_once` body identical to inspected production method |

[Original report and recorded commands](https://github.com/dragginzgame/icydb/blob/5bf0845a043448b0ed07d39f294da0663a16fefe/docs/reports/investigations/2026/09/05/nested-relation-integrity-recovery/01/report.md).
