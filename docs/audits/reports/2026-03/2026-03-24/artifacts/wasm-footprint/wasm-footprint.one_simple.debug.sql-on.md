# Recurring Audit - Wasm Footprint (2026-03-24)

## Report Preamble

- scope: recurring wasm footprint audit for `one_simple` with profile `debug` and SQL variant `sql-on`
- compared baseline report path: `docs/audits/reports/2026-03/2026-03-20/wasm-footprint.md`
- code snapshot identifier: `5a1d34bd`
- method tag/version: `WASM-1.0`
- comparability status: `non-comparable (baseline report exists but baseline size artifact is missing)`

## Checklist Results

| Requirement | Status | Evidence |
| --- | --- | --- |
| Wasm size artifacts captured | PASS | size report + summary artifacts written |
| Twiggy top breakdown generated | PASS | top text/csv artifacts written |
| Twiggy dominator breakdown generated | PASS | dominator text artifact written |
| Twiggy monomorphization breakdown generated | PASS | 0 ┊          0.00% ┊     0 ┊ 0.00% ┊ Σ [0 Total Rows] |
| Baseline delta availability | PARTIAL | baseline artifact missing at expected scoped artifacts path |

PASS=4, PARTIAL=1, FAIL=0

## Size Snapshot

| Metric | Previous | Current | Delta |
| --- | ---: | ---: | ---: |
| dfx-built `.wasm` | N/A | 3,542,734 | N/A |
| dfx-built deterministic `.wasm.gz` | N/A | 959,924 | N/A |
| dfx-shrunk `.wasm` | N/A | 2,879,061 | N/A |
| dfx-shrunk `.wasm.gz` | N/A | 876,449 | N/A |

## Structural Snapshot (ic-wasm)

| Metric | dfx-built | dfx-shrunk |
| --- | ---: | ---: |
| Function count | 4,703 | 4,703 |
| Callback count | 1 | 1 |
| Data section count | 2 | 2 |
| Data section bytes | 367,808 | 367,808 |
| Exported methods | 7 | 7 |

## Twiggy Top Offenders (Shallow Size)

| Rank | Item | Shallow Bytes | Shallow % |
| ---: | --- | ---: | ---: |
| 1 | data[0] | 367,678 | 12.77% |
| 2 | code[0] | 18,665 | 0.65% |
| 3 | code[2] | 11,824 | 0.41% |
| 4 | code[3] | 10,639 | 0.37% |
| 5 | code[4] | 10,208 | 0.35% |
| 6 | code[5] | 8,981 | 0.31% |
| 7 | code[7] | 8,653 | 0.30% |
| 8 | code[6] | 8,584 | 0.30% |
| 9 | code[1] | 8,276 | 0.29% |
| 10 | code[9] | 8,152 | 0.28% |

## Twiggy Retained Hotspots

| Rank | Item | Retained Bytes | Retained % |
| ---: | --- | ---: | ---: |
| 1 | export "canister_query query" | 579,353 | 20.12% |
| 2 | code[4605] | 579,329 | 20.12% |
| 3 | code[4614] | 579,319 | 20.12% |
| 4 | code[467] | 579,312 | 20.12% |
| 5 | code[131] | 560,153 | 19.46% |
| 6 | table[0] | 533,770 | 18.54% |
| 7 | elem[0] | 533,764 | 18.54% |
| 8 | data[0] | 367,678 | 12.77% |
| 9 | code[392] | 165,214 | 5.74% |
| 10 | code[46] | 156,055 | 5.42% |

## Artifacts

- `docs/audits/reports/2026-03/2026-03-24/artifacts/wasm-footprint/wasm-footprint.one_simple.debug.sql-on.size-report.json`
- `docs/audits/reports/2026-03/2026-03-24/artifacts/wasm-footprint/wasm-footprint.one_simple.debug.sql-on.size-summary.md`
- `docs/audits/reports/2026-03/2026-03-24/artifacts/wasm-footprint/wasm-footprint.one_simple.debug.sql-on.twiggy-top.txt`
- `docs/audits/reports/2026-03/2026-03-24/artifacts/wasm-footprint/wasm-footprint.one_simple.debug.sql-on.twiggy-top.csv`
- `docs/audits/reports/2026-03/2026-03-24/artifacts/wasm-footprint/wasm-footprint.one_simple.debug.sql-on.twiggy-dominators.txt`
- `docs/audits/reports/2026-03/2026-03-24/artifacts/wasm-footprint/wasm-footprint.one_simple.debug.sql-on.twiggy-retained.csv`
- `docs/audits/reports/2026-03/2026-03-24/artifacts/wasm-footprint/wasm-footprint.one_simple.debug.sql-on.twiggy-monos.txt`

## Follow-Up Actions

- owner boundary: `wasm-audit history`; action: preserve size artifacts for baseline reports so trend deltas remain comparable; target report date/run: next `wasm-footprint` run.

## Verification Readout

- `bash scripts/ci/wasm-size-report.sh` -> PASS
- `twiggy top -n 40` -> PASS
- `twiggy top --retained -n 40` -> PASS
- `twiggy dominators -r 160` -> PASS
- `twiggy monos` -> PASS
