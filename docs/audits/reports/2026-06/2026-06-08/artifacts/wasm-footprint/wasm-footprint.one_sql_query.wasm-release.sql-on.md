# Recurring Audit - Wasm Footprint (2026-06-08)

## Report Preamble

- scope: recurring wasm footprint audit for `one_sql_query` with profile `wasm-release` and SQL variant `sql-on`
- compared baseline report path: `docs/audits/reports/2026-06/2026-06-06/wasm-footprint.md`
- code snapshot identifier: `4ce80891e`
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
| icp-built `.wasm` | 3,170,336 | 3,128,202 | -42,134 |
| icp-built deterministic `.wasm.gz` | 1,071,475 | 1,051,596 | -19,879 |
| icp-shrunk `.wasm` | 2,955,294 | 2,915,803 | -39,491 |
| icp-shrunk `.wasm.gz` | 1,023,008 | 1,006,452 | -16,556 |

## Structural Snapshot (ic-wasm)

| Metric | icp-built | icp-shrunk |
| --- | ---: | ---: |
| Function count | 6,861 | 6,861 |
| Callback count | 1 | 1 |
| Data section count | 3 | 3 |
| Data section bytes | 239,524 | 239,524 |
| Exported methods | 1 | 1 |

## Twiggy Top Offenders (Shallow Size)

| Rank | Item | Shallow Bytes | Shallow % |
| ---: | --- | ---: | ---: |
| 1 | data[0] | 239,258 | 8.21% |
| 2 | code[0] | 33,635 | 1.15% |
| 3 | code[1] | 31,925 | 1.09% |
| 4 | code[2] | 30,722 | 1.05% |
| 5 | code[3] | 27,594 | 0.95% |
| 6 | code[4] | 24,676 | 0.85% |
| 7 | code[5] | 20,066 | 0.69% |
| 8 | code[6] | 19,154 | 0.66% |
| 9 | code[7] | 18,570 | 0.64% |
| 10 | code[8] | 17,160 | 0.59% |

## Twiggy Retained Hotspots

| Rank | Item | Retained Bytes | Retained % |
| ---: | --- | ---: | ---: |
| 1 | export "canister_query query_one_sql" | 1,119,505 | 38.39% |
| 2 | code[6835] | 1,119,473 | 38.39% |
| 3 | code[5] | 1,118,030 | 38.34% |
| 4 | code[15] | 368,111 | 12.62% |
| 5 | code[8] | 257,798 | 8.84% |
| 6 | data[0] | 239,258 | 8.21% |
| 7 | code[484] | 213,550 | 7.32% |
| 8 | code[32] | 212,407 | 7.28% |
| 9 | code[4] | 196,024 | 6.72% |
| 10 | code[31] | 192,058 | 6.59% |

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
