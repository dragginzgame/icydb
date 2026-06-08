# Recurring Audit - Wasm Footprint (2026-06-08)

## Report Preamble

- scope: recurring wasm footprint audit for `one_fluent_query` with profile `wasm-release` and SQL variant `sql-on`
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
| icp-built `.wasm` | 2,564,390 | 2,530,582 | -33,808 |
| icp-built deterministic `.wasm.gz` | 828,531 | 813,522 | -15,009 |
| icp-shrunk `.wasm` | 2,389,137 | 2,357,536 | -31,601 |
| icp-shrunk `.wasm.gz` | 786,522 | 772,414 | -14,108 |

## Structural Snapshot (ic-wasm)

| Metric | icp-built | icp-shrunk |
| --- | ---: | ---: |
| Function count | 5,647 | 5,647 |
| Callback count | 1 | 1 |
| Data section count | 3 | 3 |
| Data section bytes | 181,204 | 181,204 |
| Exported methods | 1 | 1 |

## Twiggy Top Offenders (Shallow Size)

| Rank | Item | Shallow Bytes | Shallow % |
| ---: | --- | ---: | ---: |
| 1 | data[0] | 180,938 | 7.67% |
| 2 | code[0] | 33,635 | 1.43% |
| 3 | code[1] | 32,386 | 1.37% |
| 4 | code[2] | 27,593 | 1.17% |
| 5 | code[3] | 20,379 | 0.86% |
| 6 | code[4] | 17,855 | 0.76% |
| 7 | code[5] | 17,202 | 0.73% |
| 8 | code[7] | 15,856 | 0.67% |
| 9 | code[6] | 15,294 | 0.65% |
| 10 | code[9] | 14,255 | 0.60% |

## Twiggy Retained Hotspots

| Rank | Item | Retained Bytes | Retained % |
| ---: | --- | ---: | ---: |
| 1 | export "canister_query query_one_fluent" | 640,539 | 27.17% |
| 2 | code[5628] | 640,504 | 27.17% |
| 3 | code[7] | 639,061 | 27.11% |
| 4 | code[12] | 368,119 | 15.61% |
| 5 | code[5] | 257,940 | 10.94% |
| 6 | code[388] | 213,550 | 9.06% |
| 7 | code[25] | 212,407 | 9.01% |
| 8 | code[24] | 192,058 | 8.15% |
| 9 | data[0] | 180,938 | 7.67% |
| 10 | table[0] | 176,628 | 7.49% |

## Artifacts

- `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.one_fluent_query.wasm-release.sql-on.size-report.json`
- `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.one_fluent_query.wasm-release.sql-on.size-summary.md`
- `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.one_fluent_query.wasm-release.sql-on.twiggy-top.txt`
- `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.one_fluent_query.wasm-release.sql-on.twiggy-top.csv`
- `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.one_fluent_query.wasm-release.sql-on.twiggy-dominators.txt`
- `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.one_fluent_query.wasm-release.sql-on.twiggy-retained.csv`
- `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.one_fluent_query.wasm-release.sql-on.twiggy-monos.txt`

## Follow-Up Actions

- No follow-up actions required for this run.

## Verification Readout

- `bash scripts/ci/wasm-size-report.sh` -> PASS
- `twiggy top -n 40` -> PASS
- `twiggy top --retained -n 40` -> PASS
- `twiggy dominators -r 160` -> PASS
- `twiggy monos` -> PASS
