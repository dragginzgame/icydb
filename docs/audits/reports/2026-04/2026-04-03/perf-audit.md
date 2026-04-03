# Query Instruction Footprint Audit - 2026-04-03

## Report Preamble

- scope: focused PocketIC operation-repeat benchmark for segregated `select`, `insert`, `update`, and `delete`
- definition path: `docs/audits/recurring/crosscutting/crosscutting-perf-audit.md`
- compared baseline report path: `docs/audits/reports/2026-03/2026-03-31/perf-audit.md`
- code snapshot identifier: `working tree`
- method tag/version: `PERF-0.4-quickstart-pocketic-operation-repeat`
- comparability status: `partial`
  - this run is comparable within the focused `x1` / `x10` / `x100` operation-repeat matrix
  - it is not a drop-in replacement for the broader March 31 surface matrix
- auditor: `Codex`
- run timestamp (UTC): `2026-04-03T08:07:59Z`
- execution environment: `PocketIC + quickstart test canister`
- entities in scope: `User`
- surfaces in scope:
  - generated SQL dispatch select
  - typed SQL dispatch select
  - typed single insert
  - typed single update
  - fluent single delete count-only

## Initial Read

This report persists one focused repeat benchmark matrix so the project can
track how the four common operation families scale when the same operation is
executed `x1`, `x10`, and `x100` times inside one quickstart canister query.

The benchmark is intentionally narrow:

- every scenario uses one fresh canister install plus one fresh fixture load
- repeated executions happen inside one wasm query call via the quickstart
  `sql_perf(...)` harness
- `select`, `insert`, `update`, and `delete` are reported separately instead of
  being mixed into one larger surface summary
- delete uses one dedicated `perf-delete-user` path so repeated runs do not
  depend on exhausting the default fixture rows

## Operation Matrix

| Operation | Surface | x1 avg | x10 avg | x100 avg | x10 total | x100 total |
| ---- | ---- | ----: | ----: | ----: | ----: | ----: |
| `select` | generated dispatch | `597,431` | `596,632` | `596,916` | `5,966,324` | `59,691,656` |
| `select` | typed dispatch | `621,186` | `620,360` | `619,738` | `6,203,608` | `61,973,871` |
| `insert` | typed single-row | `492,029` | `573,845` | `632,727` | `5,738,456` | `63,272,799` |
| `update` | typed single-row | `764,562` | `873,410` | `930,727` | `8,734,108` | `93,072,754` |
| `delete` | fluent count-only | `708,869` | `708,496` | `708,956` | `7,084,967` | `70,895,610` |

## Structural Read

- `select` is effectively flat across repetition on both dispatch surfaces.
- `delete` is also effectively flat across repetition on the dedicated
  count-only path.
- `insert` grows materially with repetition inside one query call:
  `492,029 -> 632,727` (`+28.6%`) from `x1` to `x100`.
- `update` grows even more:
  `764,562 -> 930,727` (`+21.7%`) from `x1` to `x100`.

That means the current repeated-run growth is concentrated in write mutation
paths, not in read selection or delete-count execution.

## Operation Skew

- typed select stays about `23k` above generated select at every repeat width:
  - `x1`: `621,186` vs `597,431` (`+4.0%`)
  - `x10`: `620,360` vs `596,632` (`+4.0%`)
  - `x100`: `619,738` vs `596,916` (`+3.8%`)
- delete count-only remains substantially cheaper than update:
  - `x1`: `708,869` vs `764,562`
  - `x100`: `708,956` vs `930,727`
- insert remains the cheapest mutation path in the matrix even after repetition:
  - `x100`: `632,727` vs delete `708,956` vs update `930,727`

## Artifacts

- focused samples:
  - `docs/audits/reports/2026-04/2026-04-03/artifacts/perf-audit/operation-repeat-samples.json`
- verification notes:
  - `docs/audits/reports/2026-04/2026-04-03/artifacts/perf-audit/verification-readout.md`

## Next Read

This matrix is useful as a small regression guard for the four common operation
families. The next useful follow-up would be either:

- fold these rows into the broader normalized perf manifest as an additional
  benchmark family, or
- add equivalent `x10` / `x100` focused repeats for grouped and aggregate SQL
  once the write-path work is stable enough that query-path noise becomes more
  important again.
