# Recurring Audit - Wasm Footprint (2026-06-08)

## Report Preamble

- scope: recurring wasm footprint audit for `one_sql_query` with profile `wasm-release` and SQL variant `sql-on`
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
| icp-built `.wasm` | 3,170,336 | 3,162,448 | -7,888 |
| icp-built deterministic `.wasm.gz` | 1,071,475 | 1,070,720 | -755 |
| icp-shrunk `.wasm` | 2,955,294 | 2,948,284 | -7,010 |
| icp-shrunk `.wasm.gz` | 1,023,008 | 1,021,717 | -1,291 |

## Structural Snapshot (ic-wasm)

| Metric | icp-built | icp-shrunk |
| --- | ---: | ---: |
| Function count | 6,903 | 6,903 |
| Callback count | 1 | 1 |
| Data section count | 3 | 3 |
| Data section bytes | 241,252 | 241,252 |
| Exported methods | 2 | 2 |

## Twiggy Top Offenders (Shallow Size)

| Rank | Item | Shallow Bytes | Shallow % |
| ---: | --- | ---: | ---: |
| 1 | data[0] | 240,986 | 8.17% |
| 2 | code[0] | 34,152 | 1.16% |
| 3 | code[1] | 32,375 | 1.10% |
| 4 | code[2] | 31,012 | 1.05% |
| 5 | code[3] | 27,594 | 0.94% |
| 6 | code[4] | 25,498 | 0.86% |
| 7 | code[5] | 22,655 | 0.77% |
| 8 | code[7] | 18,713 | 0.63% |
| 9 | code[6] | 18,189 | 0.62% |
| 10 | code[8] | 17,653 | 0.60% |

## Twiggy Retained Hotspots

| Rank | Item | Retained Bytes | Retained % |
| ---: | --- | ---: | ---: |
| 1 | export "canister_query query_one_sql" | 1,115,258 | 37.83% |
| 2 | code[6876] | 1,115,226 | 37.83% |
| 3 | code[168] | 1,115,216 | 37.83% |
| 4 | code[5] | 1,111,435 | 37.70% |
| 5 | code[15] | 368,111 | 12.49% |
| 6 | code[8] | 256,238 | 8.69% |
| 7 | data[0] | 240,986 | 8.17% |
| 8 | code[490] | 213,550 | 7.24% |
| 9 | code[33] | 212,407 | 7.20% |
| 10 | code[4] | 202,054 | 6.85% |

## Artifacts

- `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.one_sql_query.wasm-release.sql-on.size-report.json`
- `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.one_sql_query.wasm-release.sql-on.size-summary.md`
- `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.one_sql_query.wasm-release.sql-on.twiggy-top.txt`
- `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.one_sql_query.wasm-release.sql-on.twiggy-top.csv`
- `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.one_sql_query.wasm-release.sql-on.twiggy-dominators.txt`
- `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.one_sql_query.wasm-release.sql-on.twiggy-retained.csv`
- `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.one_sql_query.wasm-release.sql-on.twiggy-monos.txt`

## Follow-Up Actions

- No follow-up actions required for this run.

## Verification Readout

- `bash scripts/ci/wasm-size-report.sh` -> PASS
- `twiggy top -n 40` -> PASS
- `twiggy top --retained -n 40` -> PASS
- `twiggy dominators -r 160` -> PASS
- `twiggy monos` -> PASS
