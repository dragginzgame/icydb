# Recurring Audit - Wasm Footprint (2026-06-06)

## Report Preamble

- scope: recurring wasm footprint audit for `ten_complex` with profile `wasm-release` and SQL variant `sql-on`
- compared baseline report path: `docs/audits/reports/2026-04/2026-04-27/wasm-footprint.md`
- code snapshot identifier: `2b97a0d33`
- method tag/version: `WASM-1.0`
- comparability status: `non-comparable (baseline report exists but compatible ICP size artifact is missing)`

## Checklist Results

| Requirement | Status | Evidence |
| --- | --- | --- |
| Wasm size artifacts captured | PASS | size report + summary artifacts written |
| Twiggy top breakdown generated | PASS | top text/csv artifacts written |
| Twiggy dominator breakdown generated | PASS | dominator text artifact written |
| Twiggy monomorphization breakdown generated | PASS | 0 ┊          0.00% ┊     0 ┊ 0.00% ┊ Σ [0 Total Rows] |
| Baseline delta availability | PARTIAL | baseline artifact missing at `docs/audits/reports/2026-04/2026-04-27/artifacts/wasm-footprint/wasm-footprint.ten_complex.wasm-release.sql-on.size-report.json` |

PASS=4, PARTIAL=1, FAIL=0

## Size Snapshot

| Metric | Previous | Current | Delta |
| --- | ---: | ---: | ---: |
| icp-built `.wasm` | N/A | 2,676,342 | N/A |
| icp-built deterministic `.wasm.gz` | N/A | 854,714 | N/A |
| icp-shrunk `.wasm` | N/A | 2,492,449 | N/A |
| icp-shrunk `.wasm.gz` | N/A | 812,186 | N/A |

## Structural Snapshot (ic-wasm)

| Metric | icp-built | icp-shrunk |
| --- | ---: | ---: |
| Function count | 5,841 | 5,841 |
| Callback count | 1 | 1 |
| Data section count | 3 | 3 |
| Data section bytes | 187,348 | 187,348 |
| Exported methods | 3 | 3 |

## Twiggy Top Offenders (Shallow Size)

| Rank | Item | Shallow Bytes | Shallow % |
| ---: | --- | ---: | ---: |
| 1 | data[0] | 187,082 | 7.51% |
| 2 | code[0] | 34,152 | 1.37% |
| 3 | code[1] | 31,464 | 1.26% |
| 4 | code[2] | 27,594 | 1.11% |
| 5 | code[3] | 19,951 | 0.80% |
| 6 | code[4] | 17,783 | 0.71% |
| 7 | code[5] | 15,294 | 0.61% |
| 8 | code[7] | 14,368 | 0.58% |
| 9 | code[6] | 14,134 | 0.57% |
| 10 | code[9] | 13,335 | 0.54% |

## Twiggy Retained Hotspots

| Rank | Item | Retained Bytes | Retained % |
| ---: | --- | ---: | ---: |
| 1 | export "canister_query query_ten_complex_fluent" | 659,972 | 26.48% |
| 2 | code[5819] | 659,929 | 26.48% |
| 3 | code[14] | 659,920 | 26.48% |
| 4 | code[12] | 368,003 | 14.76% |
| 5 | code[418] | 213,550 | 8.57% |
| 6 | code[28] | 212,407 | 8.52% |
| 7 | code[26] | 192,058 | 7.71% |
| 8 | data[0] | 187,082 | 7.51% |
| 9 | code[27] | 181,272 | 7.27% |
| 10 | table[0] | 174,838 | 7.01% |

## Artifacts

- `docs/audits/reports/2026-06/2026-06-06/artifacts/wasm-footprint/wasm-footprint.ten_complex.wasm-release.sql-on.size-report.json`
- `docs/audits/reports/2026-06/2026-06-06/artifacts/wasm-footprint/wasm-footprint.ten_complex.wasm-release.sql-on.size-summary.md`
- `docs/audits/reports/2026-06/2026-06-06/artifacts/wasm-footprint/wasm-footprint.ten_complex.wasm-release.sql-on.twiggy-top.txt`
- `docs/audits/reports/2026-06/2026-06-06/artifacts/wasm-footprint/wasm-footprint.ten_complex.wasm-release.sql-on.twiggy-top.csv`
- `docs/audits/reports/2026-06/2026-06-06/artifacts/wasm-footprint/wasm-footprint.ten_complex.wasm-release.sql-on.twiggy-dominators.txt`
- `docs/audits/reports/2026-06/2026-06-06/artifacts/wasm-footprint/wasm-footprint.ten_complex.wasm-release.sql-on.twiggy-retained.csv`
- `docs/audits/reports/2026-06/2026-06-06/artifacts/wasm-footprint/wasm-footprint.ten_complex.wasm-release.sql-on.twiggy-monos.txt`

## Follow-Up Actions

- owner boundary: `wasm-audit history`; action: preserve size artifacts for baseline reports so trend deltas remain comparable; target report date/run: next `wasm-footprint` run.

## Verification Readout

- `bash scripts/ci/wasm-size-report.sh` -> PASS
- `twiggy top -n 40` -> PASS
- `twiggy top --retained -n 40` -> PASS
- `twiggy dominators -r 160` -> PASS
- `twiggy monos` -> PASS
