# Recurring Audit - Wasm Footprint (2026-06-08)

## Report Preamble

- scope: recurring wasm footprint audit for `one_complex` with profile `wasm-release` and SQL variant `sql-on`
- compared baseline report path: `docs/audits/reports/2026-06/2026-06-06/wasm-footprint.md`
- code snapshot identifier: `16b63b730`
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
| icp-built `.wasm` | 2,649,374 | 2,608,054 | -41,320 |
| icp-built deterministic `.wasm.gz` | 852,454 | 844,251 | -8,203 |
| icp-shrunk `.wasm` | 2,467,165 | 2,430,070 | -37,095 |
| icp-shrunk `.wasm.gz` | 809,131 | 801,280 | -7,851 |

## Structural Snapshot (ic-wasm)

| Metric | icp-built | icp-shrunk |
| --- | ---: | ---: |
| Function count | 5,794 | 5,794 |
| Callback count | 1 | 1 |
| Data section count | 3 | 3 |
| Data section bytes | 183,460 | 183,460 |
| Exported methods | 3 | 3 |

## Twiggy Top Offenders (Shallow Size)

| Rank | Item | Shallow Bytes | Shallow % |
| ---: | --- | ---: | ---: |
| 1 | data[0] | 183,194 | 7.54% |
| 2 | code[0] | 34,152 | 1.41% |
| 3 | code[1] | 31,464 | 1.29% |
| 4 | code[2] | 27,593 | 1.14% |
| 5 | code[3] | 19,951 | 0.82% |
| 6 | code[4] | 17,784 | 0.73% |
| 7 | code[5] | 15,294 | 0.63% |
| 8 | code[7] | 14,370 | 0.59% |
| 9 | code[6] | 14,134 | 0.58% |
| 10 | code[8] | 13,335 | 0.55% |

## Twiggy Retained Hotspots

| Rank | Item | Retained Bytes | Retained % |
| ---: | --- | ---: | ---: |
| 1 | export "canister_query query_one_complex_fluent" | 659,184 | 27.13% |
| 2 | code[5772] | 659,141 | 27.12% |
| 3 | code[13] | 659,132 | 27.12% |
| 4 | code[11] | 368,003 | 15.14% |
| 5 | code[402] | 213,550 | 8.79% |
| 6 | code[27] | 212,407 | 8.74% |
| 7 | code[25] | 192,058 | 7.90% |
| 8 | data[0] | 183,194 | 7.54% |
| 9 | code[26] | 181,271 | 7.46% |
| 10 | code[78] | 169,009 | 6.95% |

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
