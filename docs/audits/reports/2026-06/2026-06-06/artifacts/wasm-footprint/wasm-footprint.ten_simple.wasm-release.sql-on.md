# Recurring Audit - Wasm Footprint (2026-06-06)

## Report Preamble

- scope: recurring wasm footprint audit for `ten_simple` with profile `wasm-release` and SQL variant `sql-on`
- compared baseline report path: `docs/audits/reports/2026-04/2026-04-27/wasm-footprint.md`
- code snapshot identifier: `cb2b898a5`
- method tag/version: `WASM-1.0`
- comparability status: `non-comparable (baseline report exists but compatible ICP size artifact is missing)`

## Checklist Results

| Requirement | Status | Evidence |
| --- | --- | --- |
| Wasm size artifacts captured | PASS | size report + summary artifacts written |
| Twiggy top breakdown generated | PASS | top text/csv artifacts written |
| Twiggy dominator breakdown generated | PASS | dominator text artifact written |
| Twiggy monomorphization breakdown generated | PASS | 0 ┊          0.00% ┊     0 ┊ 0.00% ┊ Σ [0 Total Rows] |
| Baseline delta availability | PARTIAL | baseline artifact missing at `docs/audits/reports/2026-04/2026-04-27/artifacts/wasm-footprint/wasm-footprint.ten_simple.wasm-release.sql-on.size-report.json` |

PASS=4, PARTIAL=1, FAIL=0

## Size Snapshot

| Metric | Previous | Current | Delta |
| --- | ---: | ---: | ---: |
| icp-built `.wasm` | N/A | 2,654,510 | N/A |
| icp-built deterministic `.wasm.gz` | N/A | 848,289 | N/A |
| icp-shrunk `.wasm` | N/A | 2,471,772 | N/A |
| icp-shrunk `.wasm.gz` | N/A | 806,189 | N/A |

## Structural Snapshot (ic-wasm)

| Metric | icp-built | icp-shrunk |
| --- | ---: | ---: |
| Function count | 5,817 | 5,817 |
| Callback count | 1 | 1 |
| Data section count | 3 | 3 |
| Data section bytes | 185,172 | 185,172 |
| Exported methods | 3 | 3 |

## Twiggy Top Offenders (Shallow Size)

| Rank | Item | Shallow Bytes | Shallow % |
| ---: | --- | ---: | ---: |
| 1 | data[0] | 184,906 | 7.48% |
| 2 | code[0] | 34,152 | 1.38% |
| 3 | code[1] | 31,471 | 1.27% |
| 4 | code[2] | 27,594 | 1.12% |
| 5 | code[3] | 19,718 | 0.80% |
| 6 | code[4] | 17,727 | 0.72% |
| 7 | code[5] | 15,294 | 0.62% |
| 8 | code[7] | 14,370 | 0.58% |
| 9 | code[6] | 14,134 | 0.57% |
| 10 | code[9] | 13,376 | 0.54% |

## Twiggy Retained Hotspots

| Rank | Item | Retained Bytes | Retained % |
| ---: | --- | ---: | ---: |
| 1 | export "canister_query query_ten_simple_fluent" | 643,808 | 26.05% |
| 2 | code[5795] | 643,766 | 26.04% |
| 3 | code[14] | 643,757 | 26.04% |
| 4 | code[12] | 368,119 | 14.89% |
| 5 | code[416] | 213,550 | 8.64% |
| 6 | code[27] | 212,407 | 8.59% |
| 7 | code[25] | 192,058 | 7.77% |
| 8 | data[0] | 184,906 | 7.48% |
| 9 | code[26] | 181,272 | 7.33% |
| 10 | table[0] | 175,073 | 7.08% |

## Artifacts

- `docs/audits/reports/2026-06/2026-06-06/artifacts/wasm-footprint/wasm-footprint.ten_simple.wasm-release.sql-on.size-report.json`
- `docs/audits/reports/2026-06/2026-06-06/artifacts/wasm-footprint/wasm-footprint.ten_simple.wasm-release.sql-on.size-summary.md`
- `docs/audits/reports/2026-06/2026-06-06/artifacts/wasm-footprint/wasm-footprint.ten_simple.wasm-release.sql-on.twiggy-top.txt`
- `docs/audits/reports/2026-06/2026-06-06/artifacts/wasm-footprint/wasm-footprint.ten_simple.wasm-release.sql-on.twiggy-top.csv`
- `docs/audits/reports/2026-06/2026-06-06/artifacts/wasm-footprint/wasm-footprint.ten_simple.wasm-release.sql-on.twiggy-dominators.txt`
- `docs/audits/reports/2026-06/2026-06-06/artifacts/wasm-footprint/wasm-footprint.ten_simple.wasm-release.sql-on.twiggy-retained.csv`
- `docs/audits/reports/2026-06/2026-06-06/artifacts/wasm-footprint/wasm-footprint.ten_simple.wasm-release.sql-on.twiggy-monos.txt`

## Follow-Up Actions

- owner boundary: `wasm-audit history`; action: preserve size artifacts for baseline reports so trend deltas remain comparable; target report date/run: next `wasm-footprint` run.

## Verification Readout

- `bash scripts/ci/wasm-size-report.sh` -> PASS
- `twiggy top -n 40` -> PASS
- `twiggy top --retained -n 40` -> PASS
- `twiggy dominators -r 160` -> PASS
- `twiggy monos` -> PASS
