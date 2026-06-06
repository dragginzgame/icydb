# Recurring Audit - Wasm Footprint (2026-06-06)

## Report Preamble

- scope: recurring wasm footprint audit for `one_sql_query` with profile `wasm-release` and SQL variant `sql-on`
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
| Baseline delta availability | PARTIAL | baseline artifact missing at expected scoped artifacts path |

PASS=4, PARTIAL=1, FAIL=0

## Size Snapshot

| Metric | Previous | Current | Delta |
| --- | ---: | ---: | ---: |
| icp-built `.wasm` | N/A | 3,170,336 | N/A |
| icp-built deterministic `.wasm.gz` | N/A | 1,071,475 | N/A |
| icp-shrunk `.wasm` | N/A | 2,955,294 | N/A |
| icp-shrunk `.wasm.gz` | N/A | 1,023,008 | N/A |

## Structural Snapshot (ic-wasm)

| Metric | icp-built | icp-shrunk |
| --- | ---: | ---: |
| Function count | 6,918 | 6,918 |
| Callback count | 1 | 1 |
| Data section count | 3 | 3 |
| Data section bytes | 241,476 | 241,476 |
| Exported methods | 2 | 2 |

## Twiggy Top Offenders (Shallow Size)

| Rank | Item | Shallow Bytes | Shallow % |
| ---: | --- | ---: | ---: |
| 1 | data[0] | 241,210 | 8.16% |
| 2 | code[0] | 34,152 | 1.16% |
| 3 | code[1] | 32,375 | 1.10% |
| 4 | code[2] | 31,012 | 1.05% |
| 5 | code[3] | 27,594 | 0.93% |
| 6 | code[4] | 25,498 | 0.86% |
| 7 | code[6] | 18,713 | 0.63% |
| 8 | code[7] | 18,274 | 0.62% |
| 9 | code[5] | 18,189 | 0.62% |
| 10 | code[8] | 17,653 | 0.60% |

## Twiggy Retained Hotspots

| Rank | Item | Retained Bytes | Retained % |
| ---: | --- | ---: | ---: |
| 1 | export "canister_query query_one_sql" | 1,115,868 | 37.76% |
| 2 | code[6891] | 1,115,836 | 37.76% |
| 3 | code[7] | 1,115,827 | 37.76% |
| 4 | code[15] | 368,111 | 12.46% |
| 5 | code[8] | 256,242 | 8.67% |
| 6 | data[0] | 241,210 | 8.16% |
| 7 | code[491] | 213,550 | 7.23% |
| 8 | code[33] | 212,407 | 7.19% |
| 9 | code[4] | 202,056 | 6.84% |
| 10 | code[32] | 192,058 | 6.50% |

## Artifacts

- `docs/audits/reports/2026-06/2026-06-06/artifacts/wasm-footprint/wasm-footprint.one_sql_query.wasm-release.sql-on.size-report.json`
- `docs/audits/reports/2026-06/2026-06-06/artifacts/wasm-footprint/wasm-footprint.one_sql_query.wasm-release.sql-on.size-summary.md`
- `docs/audits/reports/2026-06/2026-06-06/artifacts/wasm-footprint/wasm-footprint.one_sql_query.wasm-release.sql-on.twiggy-top.txt`
- `docs/audits/reports/2026-06/2026-06-06/artifacts/wasm-footprint/wasm-footprint.one_sql_query.wasm-release.sql-on.twiggy-top.csv`
- `docs/audits/reports/2026-06/2026-06-06/artifacts/wasm-footprint/wasm-footprint.one_sql_query.wasm-release.sql-on.twiggy-dominators.txt`
- `docs/audits/reports/2026-06/2026-06-06/artifacts/wasm-footprint/wasm-footprint.one_sql_query.wasm-release.sql-on.twiggy-retained.csv`
- `docs/audits/reports/2026-06/2026-06-06/artifacts/wasm-footprint/wasm-footprint.one_sql_query.wasm-release.sql-on.twiggy-monos.txt`

## Follow-Up Actions

- owner boundary: `wasm-audit history`; action: preserve size artifacts for baseline reports so trend deltas remain comparable; target report date/run: next `wasm-footprint` run.

## Verification Readout

- `bash scripts/ci/wasm-size-report.sh` -> PASS
- `twiggy top -n 40` -> PASS
- `twiggy top --retained -n 40` -> PASS
- `twiggy dominators -r 160` -> PASS
- `twiggy monos` -> PASS
