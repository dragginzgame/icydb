# Recurring Audit - Wasm Footprint (2026-06-09)

## Report Preamble

- scope: recurring wasm footprint audit for `one_sql_query` with profile `wasm-release` and SQL variant `sql-on`
- compared baseline report path: `docs/audits/reports/2026-06/2026-06-08/wasm-footprint.md`
- code snapshot identifier: `a43cb9272`
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
| icp-built `.wasm` | 3,129,276 | 3,075,660 | -53,616 |
| icp-built deterministic `.wasm.gz` | 1,052,868 | 1,037,173 | -15,695 |
| icp-shrunk `.wasm` | 2,916,794 | 2,865,976 | -50,818 |
| icp-shrunk `.wasm.gz` | 1,006,633 | 989,886 | -16,747 |

## Structural Snapshot (ic-wasm)

| Metric | icp-built | icp-shrunk |
| --- | ---: | ---: |
| Function count | 6,829 | 6,829 |
| Callback count | 1 | 1 |
| Data section count | 3 | 3 |
| Data section bytes | 221,340 | 221,340 |
| Exported methods | 1 | 1 |

## Twiggy Top Offenders (Shallow Size)

| Rank | Item | Shallow Bytes | Shallow % |
| ---: | --- | ---: | ---: |
| 1 | data[0] | 221,074 | 7.71% |
| 2 | code[0] | 33,635 | 1.17% |
| 3 | code[1] | 31,925 | 1.11% |
| 4 | code[2] | 30,722 | 1.07% |
| 5 | code[3] | 27,594 | 0.96% |
| 6 | code[4] | 24,676 | 0.86% |
| 7 | code[6] | 19,562 | 0.68% |
| 8 | code[5] | 19,154 | 0.67% |
| 9 | code[7] | 18,570 | 0.65% |
| 10 | code[8] | 17,160 | 0.60% |

## Twiggy Retained Hotspots

| Rank | Item | Retained Bytes | Retained % |
| ---: | --- | ---: | ---: |
| 1 | export "canister_query query_one_sql" | 1,117,011 | 38.97% |
| 2 | code[6803] | 1,116,979 | 38.97% |
| 3 | code[6] | 1,115,536 | 38.92% |
| 4 | code[15] | 368,111 | 12.84% |
| 5 | code[8] | 257,798 | 9.00% |
| 6 | data[0] | 221,074 | 7.71% |
| 7 | code[481] | 213,550 | 7.45% |
| 8 | code[31] | 212,407 | 7.41% |
| 9 | code[4] | 196,003 | 6.84% |
| 10 | code[30] | 192,058 | 6.70% |

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
