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

## Reproduction and attribution

The original single direct-batch test was compiled in an isolated Toko Miner
snapshot under `/tmp/icydb-cost7-investigation`. Host `Instant` measurements and
`host_elapsed_ms` were removed before execution. The timed-cadence test was never
selected. The original scheduling, checked balance subtraction, gameplay
conservation, replay, rejection and instruction-observation checks were retained.

It reproduced the exact reported failure at the first stationary, size-one
trial's `cost-7`:

| Quantity | Before | After | Change |
| --- | ---: | ---: | ---: |
| Spendable balance | 100,264,448,335,502 | 100,425,550,802,242 | +161,102,466,740 |
| Consumed-cycle counter total | 569,239,308,498 | 574,449,197,758 | +5,209,889,260 |
| Unsettled prepayments, derived | 166,312,356,000 | 0 | −166,312,356,000 |

The counter observations come from a subsequent run with read-only management
`canister_metrics` queries. It reproduces the same before/after balances without
adding ticks. The derived prepayment column is
`101,000,000,000,000 - balance - consumed_total`, not the storage-reservation
`reserved_cycles` field. The accounting equation is exact:

```text
166,312,356,000 prepayments released
  - 5,209,889,260 cycles consumed
= 161,102,466,740 spendable balance increase
```

A separate diagnostic withheld `cost-7` and advanced 48 replica rounds without
advancing virtual time. The increase occurred before submitting that request:

| Round | Balance | Consumed total | Derived unsettled prepayments |
| --- | ---: | ---: | ---: |
| 0 | 100,264,448,335,502 | 569,239,308,498 | 166,312,356,000 |
| 1 | 100,264,448,335,502 | 569,239,308,498 | 166,312,356,000 |
| 2 | 100,299,574,982,695 | 574,117,661,305 | 126,307,356,000 |
| 3 | 100,425,853,713,980 | 574,146,286,020 | 0 |
| 4 | 100,425,417,569,049 | 574,582,430,951 | 0 |
| 5 | 100,425,395,811,190 | 574,604,188,810 | 0 |
| 6–48 | 100,424,891,985,067 | 575,108,014,933 | 0 |

Consumed totals were queried through round 6; balances remained unchanged through
round 48. After this diagnostic drain, `cost-7` consumed 302,831,739 cycles and
the balance assertion instead failed at `cost-12`. Draining only the first
failing boundary moves the failure and is not the proposed correction.

## Measurement correction and limits

Use the existing read-only management `canister_metrics` endpoint, routed to the
target canister's subnet with a controller caller. Sum its consumed-cycle
categories with checked arithmetic, then subtract the earlier total from the
later total. Apply this consistently to batch, setup, checkpoint-tail, idle,
retry and rejection intervals. Keep raw balances as diagnostics, separately
labelled from consumption. Keep monotonic-counter and gameplay assertions.

These are replica-reported **nominal consumed cycles**, not instruction-price
estimates. Their totals reconcile exactly with actual balances for this fixture;
do not assume the same conversion for another subnet configuration. Instruction
and transmission costs enter the counters when their prepayments settle.
Consequently an interval includes background work settled within it, including
work started earlier. It is not exclusive attribution to `execute_actions`.

The scratch candidate preserves the original scheduling and uses these counters
at every existing cycle boundary. It has no drain loop, delay, fallback or
saturating subtraction. Its patch is retained alongside this report as an
investigation artefact, not applied to the concurrently edited Toko Miner tree:
[counter probe patch](counter-probe.patch.txt).

Validation **passed**: all eight stationary/walking trials at batch sizes
1, 8, 16 and 32, totalling 512 actions. Inventory/locker conservation, exact
replay, rejection, commit/action counts and instruction observations remain
asserted. The initial balance-based reproduction and diagnostic drain failed as
described above. Full repository suites, live deployment and capacity
qualification were not run.

Downstream follow-up: land the counter measurement in Toko Miner's maintained
qualification harness, update its report consumers/labels to the explicit
nominal-cycle and settlement-interval contract, and run its owned release gate.
The retained patch is the exact tested diagnostic candidate, including probe
logging; its old latency-description label and broad interval field names still
need editorial cleanup before adoption. This investigation does not close live
capacity acceptance or make a new release ready.

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

Local complete logs: `/tmp/icydb-cost7-baseline.log`,
`/tmp/icydb-cost7-drain.log`, `/tmp/icydb-cost7-metrics.log`, and
`/tmp/icydb-cost7-counters.log`. The final machine report is
`/tmp/icydb-cost7-counters.json`.

Production complexity and raw Wasm deltas are zero. No production types or enum
variants were removed. This is an accounting diagnosis, not a claimed cycle or
instruction saving. Only disposable PocketIC instances were created for these
probes; no existing local or deployed network was restarted.
