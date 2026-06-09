# Recurring Audit - Wasm Footprint (2026-06-09)

## Report Preamble

- scope: recurring wasm footprint audit for `one_sql_query` with profile `wasm-release` and SQL variant `sql-on`
- compared baseline report path: `docs/audits/reports/2026-06/2026-06-08/wasm-footprint.md`
- code snapshot identifier: `c95398ed5`
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
| icp-built `.wasm` | 3,129,276 | 3,100,379 | -28,897 |
| icp-built deterministic `.wasm.gz` | 1,052,868 | 1,045,951 | -6,917 |
| icp-shrunk `.wasm` | 2,916,794 | 2,889,405 | -27,389 |
| icp-shrunk `.wasm.gz` | 1,006,633 | 998,123 | -8,510 |

## Structural Snapshot (ic-wasm)

| Metric | icp-built | icp-shrunk |
| --- | ---: | ---: |
| Function count | 6,849 | 6,849 |
| Callback count | 1 | 1 |
| Data section count | 3 | 3 |
| Data section bytes | 233,364 | 233,364 |
| Exported methods | 1 | 1 |

## Twiggy Top Offenders (Shallow Size)

| Rank | Item | Shallow Bytes | Shallow % |
| ---: | --- | ---: | ---: |
| 1 | data[0] | 233,098 | 8.07% |
| 2 | code[0] | 33,635 | 1.16% |
| 3 | code[1] | 31,925 | 1.10% |
| 4 | code[2] | 30,722 | 1.06% |
| 5 | code[3] | 27,594 | 0.96% |
| 6 | code[4] | 24,676 | 0.85% |
| 7 | code[5] | 20,113 | 0.70% |
| 8 | code[6] | 19,154 | 0.66% |
| 9 | code[7] | 18,570 | 0.64% |
| 10 | code[8] | 17,160 | 0.59% |

## Twiggy Retained Hotspots

| Rank | Item | Retained Bytes | Retained % |
| ---: | --- | ---: | ---: |
| 1 | export "canister_query query_one_sql" | 1,119,796 | 38.76% |
| 2 | code[6823] | 1,119,764 | 38.75% |
| 3 | code[5] | 1,118,321 | 38.70% |
| 4 | code[15] | 368,111 | 12.74% |
| 5 | code[8] | 257,798 | 8.92% |
| 6 | data[0] | 233,098 | 8.07% |
| 7 | code[482] | 213,550 | 7.39% |
| 8 | code[32] | 212,407 | 7.35% |
| 9 | code[4] | 196,024 | 6.78% |
| 10 | code[31] | 192,058 | 6.65% |

## Artifacts

- `docs/audits/reports/2026-06/2026-06-09/artifacts/wasm-footprint/wasm-footprint.one_sql_query.wasm-release.sql-on.size-report.json`
- `docs/audits/reports/2026-06/2026-06-09/artifacts/wasm-footprint/wasm-footprint.one_sql_query.wasm-release.sql-on.size-summary.md`
- `docs/audits/reports/2026-06/2026-06-09/artifacts/wasm-footprint/wasm-footprint.one_sql_query.wasm-release.sql-on.twiggy-top.txt`
- `docs/audits/reports/2026-06/2026-06-09/artifacts/wasm-footprint/wasm-footprint.one_sql_query.wasm-release.sql-on.twiggy-top.csv`
- `docs/audits/reports/2026-06/2026-06-09/artifacts/wasm-footprint/wasm-footprint.one_sql_query.wasm-release.sql-on.twiggy-dominators.txt`
- `docs/audits/reports/2026-06/2026-06-09/artifacts/wasm-footprint/wasm-footprint.one_sql_query.wasm-release.sql-on.twiggy-retained.csv`
- `docs/audits/reports/2026-06/2026-06-09/artifacts/wasm-footprint/wasm-footprint.one_sql_query.wasm-release.sql-on.twiggy-monos.txt`

## Follow-Up Actions

- No follow-up actions required for this run.

## Verification Readout

- `bash scripts/ci/wasm-size-report.sh` -> PASS
- `twiggy top -n 40` -> PASS
- `twiggy top --retained -n 40` -> PASS
- `twiggy dominators -r 160` -> PASS
- `twiggy monos` -> PASS
