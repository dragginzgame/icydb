# Recurring Audit - Wasm Footprint (2026-06-09)

## Report Preamble

- scope: recurring wasm footprint audit for `one_fluent_query` with profile `wasm-release` and SQL variant `sql-on`
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
| icp-built `.wasm` | 2,531,292 | 2,502,409 | -28,883 |
| icp-built deterministic `.wasm.gz` | 812,703 | 806,535 | -6,168 |
| icp-shrunk `.wasm` | 2,358,229 | 2,330,854 | -27,375 |
| icp-shrunk `.wasm.gz` | 772,595 | 764,698 | -7,897 |

## Structural Snapshot (ic-wasm)

| Metric | icp-built | icp-shrunk |
| --- | ---: | ---: |
| Function count | 5,630 | 5,630 |
| Callback count | 1 | 1 |
| Data section count | 3 | 3 |
| Data section bytes | 175,196 | 175,196 |
| Exported methods | 1 | 1 |

## Twiggy Top Offenders (Shallow Size)

| Rank | Item | Shallow Bytes | Shallow % |
| ---: | --- | ---: | ---: |
| 1 | data[0] | 174,930 | 7.50% |
| 2 | code[0] | 33,635 | 1.44% |
| 3 | code[1] | 32,386 | 1.39% |
| 4 | code[2] | 27,593 | 1.18% |
| 5 | code[3] | 20,379 | 0.87% |
| 6 | code[4] | 17,855 | 0.77% |
| 7 | code[5] | 17,202 | 0.74% |
| 8 | code[7] | 15,917 | 0.68% |
| 9 | code[6] | 15,294 | 0.66% |
| 10 | code[9] | 14,255 | 0.61% |

## Twiggy Retained Hotspots

| Rank | Item | Retained Bytes | Retained % |
| ---: | --- | ---: | ---: |
| 1 | export "canister_query query_one_fluent" | 640,411 | 27.48% |
| 2 | code[5611] | 640,376 | 27.47% |
| 3 | code[7] | 638,933 | 27.41% |
| 4 | code[12] | 368,119 | 15.79% |
| 5 | code[5] | 257,940 | 11.07% |
| 6 | code[386] | 213,550 | 9.16% |
| 7 | code[25] | 212,407 | 9.11% |
| 8 | code[24] | 192,058 | 8.24% |
| 9 | data[0] | 174,930 | 7.50% |
| 10 | table[0] | 174,529 | 7.49% |

## Artifacts

- `docs/audits/reports/2026-06/2026-06-09/artifacts/wasm-footprint/wasm-footprint.one_fluent_query.wasm-release.sql-on.size-report.json`
- `docs/audits/reports/2026-06/2026-06-09/artifacts/wasm-footprint/wasm-footprint.one_fluent_query.wasm-release.sql-on.size-summary.md`
- `docs/audits/reports/2026-06/2026-06-09/artifacts/wasm-footprint/wasm-footprint.one_fluent_query.wasm-release.sql-on.twiggy-top.txt`
- `docs/audits/reports/2026-06/2026-06-09/artifacts/wasm-footprint/wasm-footprint.one_fluent_query.wasm-release.sql-on.twiggy-top.csv`
- `docs/audits/reports/2026-06/2026-06-09/artifacts/wasm-footprint/wasm-footprint.one_fluent_query.wasm-release.sql-on.twiggy-dominators.txt`
- `docs/audits/reports/2026-06/2026-06-09/artifacts/wasm-footprint/wasm-footprint.one_fluent_query.wasm-release.sql-on.twiggy-retained.csv`
- `docs/audits/reports/2026-06/2026-06-09/artifacts/wasm-footprint/wasm-footprint.one_fluent_query.wasm-release.sql-on.twiggy-monos.txt`

## Follow-Up Actions

- No follow-up actions required for this run.

## Verification Readout

- `bash scripts/ci/wasm-size-report.sh` -> PASS
- `twiggy top -n 40` -> PASS
- `twiggy top --retained -n 40` -> PASS
- `twiggy dominators -r 160` -> PASS
- `twiggy monos` -> PASS
