# Recurring Audit - Wasm Footprint (2026-06-08)

## Report Preamble

- scope: recurring wasm footprint audit for `one_simple` with profile `wasm-release` and SQL variant `sql-on`
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
| icp-built `.wasm` | 2,623,214 | 2,616,425 | -6,789 |
| icp-built deterministic `.wasm.gz` | 843,777 | 841,397 | -2,380 |
| icp-shrunk `.wasm` | 2,442,434 | 2,436,483 | -5,951 |
| icp-shrunk `.wasm.gz` | 801,233 | 799,718 | -1,515 |

## Structural Snapshot (ic-wasm)

| Metric | icp-built | icp-shrunk |
| --- | ---: | ---: |
| Function count | 5,761 | 5,761 |
| Callback count | 1 | 1 |
| Data section count | 3 | 3 |
| Data section bytes | 184,740 | 184,740 |
| Exported methods | 3 | 3 |

## Twiggy Top Offenders (Shallow Size)

| Rank | Item | Shallow Bytes | Shallow % |
| ---: | --- | ---: | ---: |
| 1 | data[0] | 184,474 | 7.57% |
| 2 | code[0] | 34,152 | 1.40% |
| 3 | code[1] | 31,464 | 1.29% |
| 4 | code[2] | 27,594 | 1.13% |
| 5 | code[3] | 19,951 | 0.82% |
| 6 | code[4] | 17,727 | 0.73% |
| 7 | code[5] | 15,294 | 0.63% |
| 8 | code[7] | 14,370 | 0.59% |
| 9 | code[6] | 14,134 | 0.58% |
| 10 | code[9] | 13,335 | 0.55% |

## Twiggy Retained Hotspots

| Rank | Item | Retained Bytes | Retained % |
| ---: | --- | ---: | ---: |
| 1 | export "canister_query query_one_simple_fluent" | 640,223 | 26.28% |
| 2 | code[5739] | 640,181 | 26.27% |
| 3 | code[14] | 640,172 | 26.27% |
| 4 | code[12] | 368,119 | 15.11% |
| 5 | code[403] | 213,550 | 8.76% |
| 6 | code[27] | 212,407 | 8.72% |
| 7 | code[25] | 192,058 | 7.88% |
| 8 | data[0] | 184,474 | 7.57% |
| 9 | code[26] | 181,272 | 7.44% |
| 10 | code[80] | 169,010 | 6.94% |

## Artifacts

- `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.one_simple.wasm-release.sql-on.size-report.json`
- `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.one_simple.wasm-release.sql-on.size-summary.md`
- `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.one_simple.wasm-release.sql-on.twiggy-top.txt`
- `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.one_simple.wasm-release.sql-on.twiggy-top.csv`
- `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.one_simple.wasm-release.sql-on.twiggy-dominators.txt`
- `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.one_simple.wasm-release.sql-on.twiggy-retained.csv`
- `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.one_simple.wasm-release.sql-on.twiggy-monos.txt`

## Follow-Up Actions

- No follow-up actions required for this run.

## Verification Readout

- `bash scripts/ci/wasm-size-report.sh` -> PASS
- `twiggy top -n 40` -> PASS
- `twiggy top --retained -n 40` -> PASS
- `twiggy dominators -r 160` -> PASS
- `twiggy monos` -> PASS
