# Recurring Audit - Wasm Footprint (2026-06-08)

## Report Preamble

- scope: recurring wasm footprint audit for `ten_complex` with profile `wasm-release` and SQL variant `sql-on`
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
| icp-built `.wasm` | 2,676,342 | 2,669,553 | -6,789 |
| icp-built deterministic `.wasm.gz` | 854,714 | 852,213 | -2,501 |
| icp-shrunk `.wasm` | 2,492,449 | 2,486,498 | -5,951 |
| icp-shrunk `.wasm.gz` | 812,186 | 810,401 | -1,785 |

## Structural Snapshot (ic-wasm)

| Metric | icp-built | icp-shrunk |
| --- | ---: | ---: |
| Function count | 5,821 | 5,821 |
| Callback count | 1 | 1 |
| Data section count | 3 | 3 |
| Data section bytes | 188,836 | 188,836 |
| Exported methods | 3 | 3 |

## Twiggy Top Offenders (Shallow Size)

| Rank | Item | Shallow Bytes | Shallow % |
| ---: | --- | ---: | ---: |
| 1 | data[0] | 188,570 | 7.58% |
| 2 | code[0] | 34,152 | 1.37% |
| 3 | code[1] | 31,464 | 1.27% |
| 4 | code[2] | 27,594 | 1.11% |
| 5 | code[3] | 19,951 | 0.80% |
| 6 | code[4] | 17,783 | 0.72% |
| 7 | code[5] | 15,294 | 0.62% |
| 8 | code[7] | 14,368 | 0.58% |
| 9 | code[6] | 14,134 | 0.57% |
| 10 | code[9] | 13,335 | 0.54% |

## Twiggy Retained Hotspots

| Rank | Item | Retained Bytes | Retained % |
| ---: | --- | ---: | ---: |
| 1 | export "canister_query query_ten_complex_fluent" | 659,235 | 26.51% |
| 2 | code[5799] | 659,192 | 26.51% |
| 3 | code[14] | 659,183 | 26.51% |
| 4 | code[12] | 368,003 | 14.80% |
| 5 | code[416] | 213,550 | 8.59% |
| 6 | code[28] | 212,407 | 8.54% |
| 7 | code[26] | 192,058 | 7.72% |
| 8 | data[0] | 188,570 | 7.58% |
| 9 | code[27] | 181,272 | 7.29% |
| 10 | table[0] | 174,635 | 7.02% |

## Artifacts

- `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.ten_complex.wasm-release.sql-on.size-report.json`
- `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.ten_complex.wasm-release.sql-on.size-summary.md`
- `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.ten_complex.wasm-release.sql-on.twiggy-top.txt`
- `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.ten_complex.wasm-release.sql-on.twiggy-top.csv`
- `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.ten_complex.wasm-release.sql-on.twiggy-dominators.txt`
- `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.ten_complex.wasm-release.sql-on.twiggy-retained.csv`
- `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.ten_complex.wasm-release.sql-on.twiggy-monos.txt`

## Follow-Up Actions

- No follow-up actions required for this run.

## Verification Readout

- `bash scripts/ci/wasm-size-report.sh` -> PASS
- `twiggy top -n 40` -> PASS
- `twiggy top --retained -n 40` -> PASS
- `twiggy dominators -r 160` -> PASS
- `twiggy monos` -> PASS
