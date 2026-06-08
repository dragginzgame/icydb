# Recurring Audit - Wasm Footprint (2026-06-08)

## Report Preamble

- scope: recurring wasm footprint audit for `one_sql_query` with profile `wasm-release` and SQL variant `sql-on`
- compared baseline report path: `docs/audits/reports/2026-06/2026-06-06/wasm-footprint.md`
- code snapshot identifier: `95088970b`
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
| icp-built `.wasm` | 3,170,336 | 3,147,348 | -22,988 |
| icp-built deterministic `.wasm.gz` | 1,071,475 | 1,061,452 | -10,023 |
| icp-shrunk `.wasm` | 2,955,294 | 2,933,300 | -21,994 |
| icp-shrunk `.wasm.gz` | 1,023,008 | 1,012,434 | -10,574 |

## Structural Snapshot (ic-wasm)

| Metric | icp-built | icp-shrunk |
| --- | ---: | ---: |
| Function count | 6,900 | 6,900 |
| Callback count | 1 | 1 |
| Data section count | 3 | 3 |
| Data section bytes | 240,780 | 240,780 |
| Exported methods | 2 | 2 |

## Twiggy Top Offenders (Shallow Size)

| Rank | Item | Shallow Bytes | Shallow % |
| ---: | --- | ---: | ---: |
| 1 | data[0] | 240,514 | 8.20% |
| 2 | code[0] | 33,635 | 1.15% |
| 3 | code[1] | 31,925 | 1.09% |
| 4 | code[2] | 30,722 | 1.05% |
| 5 | code[3] | 27,594 | 0.94% |
| 6 | code[4] | 24,701 | 0.84% |
| 7 | code[5] | 24,007 | 0.82% |
| 8 | code[6] | 19,154 | 0.65% |
| 9 | code[7] | 18,570 | 0.63% |
| 10 | code[8] | 17,160 | 0.59% |

## Twiggy Retained Hotspots

| Rank | Item | Retained Bytes | Retained % |
| ---: | --- | ---: | ---: |
| 1 | export "canister_query query_one_sql" | 1,106,998 | 37.74% |
| 2 | code[6873] | 1,106,966 | 37.74% |
| 3 | code[164] | 1,106,956 | 37.74% |
| 4 | code[5] | 1,103,175 | 37.61% |
| 5 | code[15] | 368,111 | 12.55% |
| 6 | code[8] | 255,084 | 8.70% |
| 7 | data[0] | 240,514 | 8.20% |
| 8 | code[487] | 213,550 | 7.28% |
| 9 | code[32] | 212,407 | 7.24% |
| 10 | code[4] | 195,563 | 6.67% |

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
