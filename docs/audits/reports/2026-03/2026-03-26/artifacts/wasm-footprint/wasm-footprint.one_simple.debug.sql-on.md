# Recurring Audit - Wasm Footprint (2026-03-26)

## Report Preamble

- scope: recurring wasm footprint audit for `one_simple` with profile `debug` and SQL variant `sql-on`
- compared baseline report path: `docs/audits/reports/2026-03/2026-03-24/wasm-footprint.md`
- code snapshot identifier: `16c600ba`
- method tag/version: `WASM-1.0`
- comparability status: `comparable`

## Checklist Results

| Requirement | Status | Evidence |
| --- | --- | --- |
| Wasm size artifacts captured | PASS | size report + summary artifacts written |
| Twiggy top breakdown generated | PASS | top text/csv artifacts written |
| Twiggy dominator breakdown generated | PASS | dominator text artifact written |
| Twiggy monomorphization breakdown generated | PASS | 0 ┊          0.00% ┊     0 ┊ 0.00% ┊ Σ [0 Total Rows] |
| Baseline delta availability | PASS | baseline artifact loaded |

PASS=5, PARTIAL=0, FAIL=0

## Size Snapshot

| Metric | Previous | Current | Delta |
| --- | ---: | ---: | ---: |
| dfx-built `.wasm` | 3,542,734 | 3,330,340 | -212,394 |
| dfx-built deterministic `.wasm.gz` | 959,924 | 908,812 | -51,112 |
| dfx-shrunk `.wasm` | 2,879,061 | 2,716,655 | -162,406 |
| dfx-shrunk `.wasm.gz` | 876,449 | 829,785 | -46,664 |

## Structural Snapshot (ic-wasm)

| Metric | dfx-built | dfx-shrunk |
| --- | ---: | ---: |
| Function count | 4,412 | 4,412 |
| Callback count | 1 | 1 |
| Data section count | 2 | 2 |
| Data section bytes | 367,528 | 367,528 |
| Exported methods | 7 | 7 |

## Twiggy Top Offenders (Shallow Size)

| Rank | Item | Shallow Bytes | Shallow % |
| ---: | --- | ---: | ---: |
| 1 | data[0] | 367,398 | 13.52% |
| 2 | code[0] | 33,627 | 1.24% |
| 3 | code[1] | 23,312 | 0.86% |
| 4 | code[2] | 18,665 | 0.69% |
| 5 | code[4] | 11,903 | 0.44% |
| 6 | code[5] | 10,639 | 0.39% |
| 7 | code[6] | 10,214 | 0.38% |
| 8 | code[7] | 8,981 | 0.33% |
| 9 | code[8] | 8,653 | 0.32% |
| 10 | code[3] | 8,644 | 0.32% |

## Twiggy Retained Hotspots

| Rank | Item | Retained Bytes | Retained % |
| ---: | --- | ---: | ---: |
| 1 | table[0] | 552,999 | 20.36% |
| 2 | elem[0] | 552,993 | 20.36% |
| 3 | export "canister_query query" | 464,824 | 17.11% |
| 4 | code[4318] | 464,800 | 17.11% |
| 5 | code[4325] | 464,790 | 17.11% |
| 6 | code[430] | 464,783 | 17.11% |
| 7 | code[25] | 446,084 | 16.42% |
| 8 | data[0] | 367,398 | 13.52% |
| 9 | code[116] | 201,376 | 7.41% |
| 10 | code[880] | 141,114 | 5.19% |

## Artifacts

- `docs/audits/reports/2026-03/2026-03-26/artifacts/wasm-footprint/wasm-footprint.one_simple.debug.sql-on.size-report.json`
- `docs/audits/reports/2026-03/2026-03-26/artifacts/wasm-footprint/wasm-footprint.one_simple.debug.sql-on.size-summary.md`
- `docs/audits/reports/2026-03/2026-03-26/artifacts/wasm-footprint/wasm-footprint.one_simple.debug.sql-on.twiggy-top.txt`
- `docs/audits/reports/2026-03/2026-03-26/artifacts/wasm-footprint/wasm-footprint.one_simple.debug.sql-on.twiggy-top.csv`
- `docs/audits/reports/2026-03/2026-03-26/artifacts/wasm-footprint/wasm-footprint.one_simple.debug.sql-on.twiggy-dominators.txt`
- `docs/audits/reports/2026-03/2026-03-26/artifacts/wasm-footprint/wasm-footprint.one_simple.debug.sql-on.twiggy-retained.csv`
- `docs/audits/reports/2026-03/2026-03-26/artifacts/wasm-footprint/wasm-footprint.one_simple.debug.sql-on.twiggy-monos.txt`

## Follow-Up Actions

- No follow-up actions required for this run.

## Verification Readout

- `bash scripts/ci/wasm-size-report.sh` -> PASS
- `twiggy top -n 40` -> PASS
- `twiggy top --retained -n 40` -> PASS
- `twiggy dominators -r 160` -> PASS
- `twiggy monos` -> PASS
