# Verification Readout

- fresh broad matrix command:
  - `POCKET_IC_BIN=/tmp/pocket-ic-server-13.0.0/pocket-ic cargo test -p icydb-testing-integration --test sql_canister sql_canister_perf_harness_reports_positive_instruction_samples -- --nocapture`
- fresh broad matrix status:
  - passed
- focused operation-repeat rerun command:
  - `POCKET_IC_BIN=/tmp/pocket-ic-server-13.0.0/pocket-ic cargo test -p icydb-testing-integration --test sql_canister sql_canister_perf_operation_repeat_benchmarks_are_segregated -- --nocapture`
- focused operation-repeat rerun status:
  - not rerun in this refresh
- notes:
  - `demo_rpg-samples.json` is a fresh current-tree broad matrix from the successful rerun
  - `operation-repeat-samples.json` remains the earlier same-day focused matrix and was preserved as reference while this pass focused on the broad surface matrix plus the shared scalar execute path
