# Verification Readout

- fresh broad matrix command:
  - `POCKET_IC_BIN=/tmp/pocket-ic-server-13.0.0/pocket-ic cargo test -p icydb-testing-integration --test sql_canister sql_canister_perf_harness_reports_positive_instruction_samples -- --nocapture`
- fresh broad matrix status:
  - passed
- focused operation-repeat rerun command:
  - `POCKET_IC_BIN=/tmp/pocket-ic-server-13.0.0/pocket-ic cargo test -p icydb-testing-integration --test sql_canister sql_canister_perf_operation_repeat_benchmarks_are_segregated -- --nocapture`
- focused operation-repeat rerun status:
  - failed during quickstart canister build
- focused operation-repeat rerun blocker:
  - Cargo attempted a fresh crates.io resolution and failed on `canic-cdk = "^0.22.3"` because the current index only exposed `0.22.2`, `0.22.1`, and `0.22.0`
- notes:
  - `quickstart-samples.json` is a fresh current-tree broad matrix from the successful rerun
  - `operation-repeat-samples.json` remains the earlier same-day focused matrix and was preserved as reference, not refreshed as part of this rerun
