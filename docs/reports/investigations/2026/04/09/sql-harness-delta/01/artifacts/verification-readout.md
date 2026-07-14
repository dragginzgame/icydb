# Verification Readout

- current-tree command:
  - `CARGO_TARGET_DIR=/tmp/icydb-bench-current-target POCKET_IC_BIN=/tmp/pocket-ic-server-13.0.0/pocket-ic cargo test -p icydb-testing-integration --test sql_canister sql_canister_perf_harness_reports_positive_instruction_samples -- --nocapture`
- current-tree status:
  - passed
- baseline command:
  - `CARGO_TARGET_DIR=/tmp/icydb-bench-head-target POCKET_IC_BIN=/tmp/pocket-ic-server-13.0.0/pocket-ic cargo test -p icydb-testing-integration --test sql_canister sql_canister_perf_harness_reports_positive_instruction_samples -- --nocapture`
- baseline status:
  - passed
- baseline revision:
  - `31e27185fb4b746c7023a2b28186cf6bfd9aef95`
- extraction notes:
  - both successful runs emitted `134` scenario rows
  - the JSON arrays were extracted directly from the checked-in perf harness output
  - the full comparison table was written to `sql-harness-delta.tsv`
