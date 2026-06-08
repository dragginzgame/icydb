# Recurring Audit - Wasm Footprint (2026-06-08)

## Report Preamble

- scope: recurring wasm footprint audit for `ten_complex` with profile `wasm-release` and SQL variant `sql-on`
- compared baseline report path: `docs/audits/reports/2026-06/2026-06-06/wasm-footprint.md`
- code snapshot identifier: `1043f2d10`
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
| icp-built `.wasm` | 2,676,342 | 2,620,081 | -56,261 |
| icp-built deterministic `.wasm.gz` | 854,714 | 837,276 | -17,438 |
| icp-shrunk `.wasm` | 2,492,449 | 2,440,406 | -52,043 |
| icp-shrunk `.wasm.gz` | 812,186 | 795,289 | -16,897 |

## Structural Snapshot (ic-wasm)

| Metric | icp-built | icp-shrunk |
| --- | ---: | ---: |
| Function count | 5,815 | 5,815 |
| Callback count | 1 | 1 |
| Data section count | 3 | 3 |
| Data section bytes | 185,580 | 185,580 |
| Exported methods | 3 | 3 |

## Twiggy Top Offenders (Shallow Size)

| Rank | Item | Shallow Bytes | Shallow % |
| ---: | --- | ---: | ---: |
| 1 | data[0] | 185,314 | 7.59% |
| 2 | code[0] | 33,635 | 1.38% |
| 3 | code[1] | 32,386 | 1.33% |
| 4 | code[2] | 27,593 | 1.13% |
| 5 | code[3] | 20,378 | 0.84% |
| 6 | code[4] | 17,791 | 0.73% |
| 7 | code[5] | 15,294 | 0.63% |
| 8 | code[7] | 14,256 | 0.58% |
| 9 | code[6] | 14,213 | 0.58% |
| 10 | code[8] | 13,140 | 0.54% |

## Twiggy Retained Hotspots

| Rank | Item | Retained Bytes | Retained % |
| ---: | --- | ---: | ---: |
| 1 | export "canister_query query_ten_complex_fluent" | 652,292 | 26.73% |
| 2 | code[5793] | 652,249 | 26.73% |
| 3 | code[13] | 652,240 | 26.73% |
| 4 | code[10] | 368,003 | 15.08% |
| 5 | code[411] | 213,550 | 8.75% |
| 6 | code[26] | 212,407 | 8.70% |
| 7 | code[25] | 192,058 | 7.87% |
| 8 | data[0] | 185,314 | 7.59% |
| 9 | code[32] | 180,118 | 7.38% |
| 10 | table[0] | 172,349 | 7.06% |

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
