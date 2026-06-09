# Recurring Audit - Wasm Footprint (2026-06-09)

## Report Preamble

- scope: recurring wasm footprint audit for `one_sql_query` with profile `wasm-release` and SQL variant `sql-on`
- compared baseline report path: `docs/audits/reports/2026-06/2026-06-08/wasm-footprint.md`
- code snapshot identifier: `83dc6bcad`
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
| icp-built `.wasm` | 3,129,276 | 3,079,077 | -50,199 |
| icp-built deterministic `.wasm.gz` | 1,052,868 | 1,039,362 | -13,506 |
| icp-shrunk `.wasm` | 2,916,794 | 2,869,384 | -47,410 |
| icp-shrunk `.wasm.gz` | 1,006,633 | 990,753 | -15,880 |

## Structural Snapshot (ic-wasm)

| Metric | icp-built | icp-shrunk |
| --- | ---: | ---: |
| Function count | 6,829 | 6,829 |
| Callback count | 1 | 1 |
| Data section count | 3 | 3 |
| Data section bytes | 224,684 | 224,684 |
| Exported methods | 1 | 1 |

## Twiggy Top Offenders (Shallow Size)

| Rank | Item | Shallow Bytes | Shallow % |
| ---: | --- | ---: | ---: |
| 1 | data[0] | 224,418 | 7.82% |
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
| 1 | export "canister_query query_one_sql" | 1,117,075 | 38.93% |
| 2 | code[6803] | 1,117,043 | 38.93% |
| 3 | code[6] | 1,115,600 | 38.88% |
| 4 | code[15] | 368,111 | 12.83% |
| 5 | code[8] | 257,798 | 8.98% |
| 6 | data[0] | 224,418 | 7.82% |
| 7 | code[481] | 213,550 | 7.44% |
| 8 | code[31] | 212,407 | 7.40% |
| 9 | code[4] | 196,015 | 6.83% |
| 10 | code[30] | 192,058 | 6.69% |

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
