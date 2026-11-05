> Historical assessment, not current-code qualification. Raw output and superseded runs
> were retired from the working tree under Shared 0.30.4. Git history retains them.
> Current follow-up authority is GitHub issues; selected results below retain their
> original source, method and limitations, not a fresh measurement.

# Toko Miner direct-batch cycle accounting

2026-09-30. Investigation of downstream ICYDB-039 against the retained
Canic 0.110.48 / IcyDB 0.261.18 Wasms. IcyDB's checkout is released
`c16083c1d` (0.261.19); no runtime change is indicated by this finding.

## Finding

The failing assertion treats a decrease in spendable cycle balance as consumed
cycles. Pending execution and callback prepayments make that assumption false:
settling earlier work can increase the balance while consuming additional cycles.
The exact failing interval reconciles with the replica's independently queried,
monotonic consumed-cycle counters. No clamp, retry or extra scheduling delay is
needed to measure consumption.

The public Canic fixture supplies more than its configured installation amount.
Its root sends 1,000,000,000,000 cycles when creating each child; the installer
then adds 100,000,000,000,000. The replica records the 500,000,000,000 creation
charge in the child's consumed-cycle metrics. Thus balances above 100 trillion
do not imply unexpected funding. The starting accounting total is 101 trillion.

## Exact inputs and source owners

- Toko Miner source: `c9587fc58b5a1b445fb5ce3b85fc05a154d7995f`.

- Release build: `1f94b7223dfb06871ca72fa16402e11705d486b8348b05d6c15acd96df77bf7a`.

- Game Shard raw Wasm: **10,540,490 bytes**, SHA-256
  `aab5cc17d59fbc2bbc8e6cd7288b7a08e7cbd48a5b5707483d361e1ade09e28b`.

- PocketIC server 16.0.0 binary SHA-256:
  `69e324bdb68d32d878b7a9504b1379f08f8d1921272bacb065b0fabb3d0f3792`.

- [PocketIC 16 release](https://github.com/dfinity/pocketic/releases/tag/16.0.0)
  identifies replica source `fc21803c3c3a8dd452b3b58b959751c41fecb89c`.

- Canic [`create_canister`](https://github.com/dragginzgame/canic/blob/v0.110.48/canisters/test/sharding_root_stub/src/lib.rs)
  owns the creation payment; its
  [managed fixture](https://github.com/dragginzgame/canic/blob/v0.110.48/crates/canic/src/testing/managed_component_group/mod.rs)
  owns the separate installation credit.

- Replica [`refund_unused_execution_cycles`](https://github.com/dfinity/ic/blob/fc21803c3c3a8dd452b3b58b959751c41fecb89c/rs/cycles_account_manager/src/cycles_account_manager.rs)
  and [`refund_cycles`](https://github.com/dfinity/ic/blob/fc21803c3c3a8dd452b3b58b959751c41fecb89c/rs/replicated_state/src/canister_state/system_state.rs)
  own refunds and consumed-counter settlement.

- [`get_canister_metrics`](https://github.com/dfinity/ic/blob/fc21803c3c3a8dd452b3b58b959751c41fecb89c/rs/execution_environment/src/canister_manager.rs)
  selects the monotonic counters; the
  [subnet query handler](https://github.com/dfinity/ic/blob/fc21803c3c3a8dd452b3b58b959751c41fecb89c/rs/execution_environment/src/query_handler/subnet_query.rs)
  exposes them without executing another canister update.

[Original report and recorded commands](https://github.com/dragginzgame/icydb/blob/5bf0845a043448b0ed07d39f294da0663a16fefe/docs/reports/investigations/2026/09/30/toko-cost7/report.md).
