# Recurring Audit - Wasm Footprint (2026-06-08)

## Report Preamble

- scope: recurring wasm footprint audit for `one_complex` with profile `wasm-release` and SQL variant `sql-on`
- compared baseline report path: `docs/audits/reports/2026-06/2026-06-06/wasm-footprint.md`
- code snapshot identifier: `449d796ac`
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
| icp-built `.wasm` | 2,649,374 | 2,642,585 | -6,789 |
| icp-built deterministic `.wasm.gz` | 852,454 | 850,627 | -1,827 |
| icp-shrunk `.wasm` | 2,467,165 | 2,461,214 | -5,951 |
| icp-shrunk `.wasm.gz` | 809,131 | 807,894 | -1,237 |

## Structural Snapshot (ic-wasm)

| Metric | icp-built | icp-shrunk |
| --- | ---: | ---: |
| Function count | 5,800 | 5,800 |
| Callback count | 1 | 1 |
| Data section count | 3 | 3 |
| Data section bytes | 187,044 | 187,044 |
| Exported methods | 3 | 3 |

## Twiggy Top Offenders (Shallow Size)

| Rank | Item | Shallow Bytes | Shallow % |
| ---: | --- | ---: | ---: |
| 1 | data[0] | 186,778 | 7.59% |
| 2 | code[0] | 34,152 | 1.39% |
| 3 | code[1] | 31,464 | 1.28% |
| 4 | code[2] | 27,594 | 1.12% |
| 5 | code[3] | 19,951 | 0.81% |
| 6 | code[4] | 17,784 | 0.72% |
| 7 | code[5] | 15,294 | 0.62% |
| 8 | code[7] | 14,370 | 0.58% |
| 9 | code[6] | 14,134 | 0.57% |
| 10 | code[9] | 13,335 | 0.54% |

## Twiggy Retained Hotspots

| Rank | Item | Retained Bytes | Retained % |
| ---: | --- | ---: | ---: |
| 1 | export "canister_query query_one_complex_fluent" | 659,245 | 26.79% |
| 2 | code[5778] | 659,202 | 26.78% |
| 3 | code[14] | 659,193 | 26.78% |
| 4 | code[12] | 368,003 | 14.95% |
| 5 | code[406] | 213,550 | 8.68% |
| 6 | code[28] | 212,407 | 8.63% |
| 7 | code[26] | 192,058 | 7.80% |
| 8 | data[0] | 186,778 | 7.59% |
| 9 | code[27] | 181,272 | 7.37% |
| 10 | code[81] | 169,010 | 6.87% |

## Artifacts

- `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.one_complex.wasm-release.sql-on.size-report.json`
- `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.one_complex.wasm-release.sql-on.size-summary.md`
- `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.one_complex.wasm-release.sql-on.twiggy-top.txt`
- `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.one_complex.wasm-release.sql-on.twiggy-top.csv`
- `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.one_complex.wasm-release.sql-on.twiggy-dominators.txt`
- `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.one_complex.wasm-release.sql-on.twiggy-retained.csv`
- `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.one_complex.wasm-release.sql-on.twiggy-monos.txt`

## Follow-Up Actions

- No follow-up actions required for this run.

## Verification Readout

- `bash scripts/ci/wasm-size-report.sh` -> PASS
- `twiggy top -n 40` -> PASS
- `twiggy top --retained -n 40` -> PASS
- `twiggy dominators -r 160` -> PASS
- `twiggy monos` -> PASS
