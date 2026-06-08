# Recurring Audit - Wasm Footprint (2026-06-08)

## Report Preamble

- scope: recurring wasm footprint audit for `one_fluent_query` with profile `wasm-release` and SQL variant `sql-on`
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
| icp-built `.wasm` | 2,564,390 | 2,557,632 | -6,758 |
| icp-built deterministic `.wasm.gz` | 828,531 | 826,238 | -2,293 |
| icp-shrunk `.wasm` | 2,389,137 | 2,383,215 | -5,922 |
| icp-shrunk `.wasm.gz` | 786,522 | 785,132 | -1,390 |

## Structural Snapshot (ic-wasm)

| Metric | icp-built | icp-shrunk |
| --- | ---: | ---: |
| Function count | 5,686 | 5,686 |
| Callback count | 1 | 1 |
| Data section count | 3 | 3 |
| Data section bytes | 180,516 | 180,516 |
| Exported methods | 2 | 2 |

## Twiggy Top Offenders (Shallow Size)

| Rank | Item | Shallow Bytes | Shallow % |
| ---: | --- | ---: | ---: |
| 1 | data[0] | 180,250 | 7.56% |
| 2 | code[0] | 34,152 | 1.43% |
| 3 | code[1] | 31,464 | 1.32% |
| 4 | code[2] | 27,593 | 1.16% |
| 5 | code[3] | 19,952 | 0.84% |
| 6 | code[5] | 17,727 | 0.74% |
| 7 | code[4] | 17,695 | 0.74% |
| 8 | code[6] | 15,294 | 0.64% |
| 9 | code[8] | 14,883 | 0.62% |
| 10 | code[9] | 14,370 | 0.60% |

## Twiggy Retained Hotspots

| Rank | Item | Retained Bytes | Retained % |
| ---: | --- | ---: | ---: |
| 1 | export "canister_query query_one_fluent" | 643,659 | 27.01% |
| 2 | code[5666] | 643,624 | 27.01% |
| 3 | code[8] | 643,615 | 27.01% |
| 4 | code[13] | 368,119 | 15.45% |
| 5 | code[4] | 256,382 | 10.76% |
| 6 | code[392] | 213,550 | 8.96% |
| 7 | code[26] | 212,407 | 8.91% |
| 8 | code[25] | 192,058 | 8.06% |
| 9 | data[0] | 180,250 | 7.56% |
| 10 | code[75] | 170,676 | 7.16% |

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
