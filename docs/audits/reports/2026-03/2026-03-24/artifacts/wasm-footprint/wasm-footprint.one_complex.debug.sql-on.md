# Recurring Audit - Wasm Footprint (2026-03-24)

## Report Preamble

- scope: recurring wasm footprint audit for `one_complex` with profile `debug` and SQL variant `sql-on`
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
| dfx-built `.wasm` | N/A | 3,545,805 | N/A |
| dfx-built deterministic `.wasm.gz` | N/A | 961,296 | N/A |
| dfx-shrunk `.wasm` | N/A | 2,882,418 | N/A |
| dfx-shrunk `.wasm.gz` | N/A | 877,471 | N/A |

## Structural Snapshot (ic-wasm)

| Metric | dfx-built | dfx-shrunk |
| --- | ---: | ---: |
| Function count | 4,703 | 4,703 |
| Callback count | 1 | 1 |
| Data section count | 2 | 2 |
| Data section bytes | 370,880 | 370,880 |
| Exported methods | 7 | 7 |

## Twiggy Top Offenders (Shallow Size)

| Rank | Item | Shallow Bytes | Shallow % |
| ---: | --- | ---: | ---: |
| 1 | data[0] | 370,750 | 12.86% |
| 2 | code[0] | 18,665 | 0.65% |
| 3 | code[2] | 11,824 | 0.41% |
| 4 | code[3] | 10,639 | 0.37% |
| 5 | code[4] | 10,208 | 0.35% |
| 6 | code[5] | 8,981 | 0.31% |
| 7 | code[7] | 8,653 | 0.30% |
| 8 | code[6] | 8,584 | 0.30% |
| 9 | code[1] | 8,276 | 0.29% |
| 10 | code[9] | 8,159 | 0.28% |

## Twiggy Retained Hotspots

| Rank | Item | Retained Bytes | Retained % |
| ---: | --- | ---: | ---: |
| 1 | export "canister_query query" | 579,389 | 20.10% |
| 2 | code[4605] | 579,365 | 20.10% |
| 3 | code[4612] | 579,355 | 20.10% |
| 4 | code[467] | 579,348 | 20.10% |
| 5 | code[131] | 560,183 | 19.43% |
| 6 | table[0] | 533,851 | 18.52% |
| 7 | elem[0] | 533,845 | 18.52% |
| 8 | data[0] | 370,750 | 12.86% |
| 9 | code[392] | 165,229 | 5.73% |
| 10 | code[46] | 156,068 | 5.41% |

## Artifacts

- `docs/audits/reports/2026-03/2026-03-24/artifacts/wasm-footprint/wasm-footprint.one_complex.debug.sql-on.size-report.json`
- `docs/audits/reports/2026-03/2026-03-24/artifacts/wasm-footprint/wasm-footprint.one_complex.debug.sql-on.size-summary.md`
- `docs/audits/reports/2026-03/2026-03-24/artifacts/wasm-footprint/wasm-footprint.one_complex.debug.sql-on.twiggy-top.txt`
- `docs/audits/reports/2026-03/2026-03-24/artifacts/wasm-footprint/wasm-footprint.one_complex.debug.sql-on.twiggy-top.csv`
- `docs/audits/reports/2026-03/2026-03-24/artifacts/wasm-footprint/wasm-footprint.one_complex.debug.sql-on.twiggy-dominators.txt`
- `docs/audits/reports/2026-03/2026-03-24/artifacts/wasm-footprint/wasm-footprint.one_complex.debug.sql-on.twiggy-retained.csv`
- `docs/audits/reports/2026-03/2026-03-24/artifacts/wasm-footprint/wasm-footprint.one_complex.debug.sql-on.twiggy-monos.txt`

## Follow-Up Actions

- owner boundary: `wasm-audit history`; action: preserve size artifacts for baseline reports so trend deltas remain comparable; target report date/run: next `wasm-footprint` run.

## Verification Readout

- `bash scripts/ci/wasm-size-report.sh` -> PASS
- `twiggy top -n 40` -> PASS
- `twiggy top --retained -n 40` -> PASS
- `twiggy dominators -r 160` -> PASS
- `twiggy monos` -> PASS
