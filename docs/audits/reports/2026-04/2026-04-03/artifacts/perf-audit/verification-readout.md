# Verification Readout

- command:
  - `POCKET_IC_BIN=/tmp/pocket-ic-server-13.0.0/pocket-ic cargo test -p icydb-testing-integration --test sql_canister sql_canister_perf_operation_repeat_benchmarks_are_segregated -- --nocapture`
- status:
  - passed
- notes:
  - every scenario used one fresh quickstart canister install plus one fresh fixture load
  - `x100` remained stable enough to keep in the focused matrix for all four operation families
  - delete used the dedicated `FluentDeletePerfUserCount` path so repeated delete did not depend on finite fixture-row exhaustion
