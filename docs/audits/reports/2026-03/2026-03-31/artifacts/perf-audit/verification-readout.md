# Perf Audit Verification Readout

- method tag: `PERF-0.3-quickstart-pocketic-surface-sampling-expanded`
- status: `PARTIAL`
- comparability: `non-comparable`
- authoritative instruction rows: `present for 25 measured quickstart canister scenarios`

## Commands

- `cargo check -p icydb-core` -> PASS
- `cargo test -p canister_quickstart --features sql -- --nocapture` -> PASS
- `POCKET_IC_BIN=/tmp/pocket-ic-server-13.0.0/pocket-ic cargo test -p icydb-testing-integration --test sql_canister sql_canister_perf_harness_reports_positive_instruction_samples -- --nocapture` -> PASS

## Notes

- This run improves the quickstart numeric baseline with grouped `HAVING`,
  grouped/global `EXPLAIN`, grouped invalid-cursor rejection, fluent invalid-cursor
  rejection, paged load, grouped continuation, metadata-lane, and computed-projection rows.
- The audit remains partial because it still lacks phase-isolated totals,
  fluent grouped-builder totals, and cursor signature-mismatch coverage.
