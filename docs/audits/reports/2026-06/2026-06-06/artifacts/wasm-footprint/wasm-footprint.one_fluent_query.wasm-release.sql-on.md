# Recurring Audit - Wasm Footprint (2026-06-06)

## Report Preamble

- scope: recurring wasm footprint audit for `one_fluent_query` with profile `wasm-release` and SQL variant `sql-on`
- compared baseline report path: `docs/audits/reports/2026-04/2026-04-27/wasm-footprint.md`
- code snapshot identifier: `cb2b898a5`
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
| icp-built `.wasm` | N/A | 2,568,374 | N/A |
| icp-built deterministic `.wasm.gz` | N/A | 829,997 | N/A |
| icp-shrunk `.wasm` | N/A | 2,392,849 | N/A |
| icp-shrunk `.wasm.gz` | N/A | 788,140 | N/A |

## Structural Snapshot (ic-wasm)

| Metric | icp-built | icp-shrunk |
| --- | ---: | ---: |
| Function count | 5,719 | 5,719 |
| Callback count | 1 | 1 |
| Data section count | 3 | 3 |
| Data section bytes | 179,028 | 179,028 |
| Exported methods | 2 | 2 |

## Twiggy Top Offenders (Shallow Size)

| Rank | Item | Shallow Bytes | Shallow % |
| ---: | --- | ---: | ---: |
| 1 | data[0] | 178,762 | 7.47% |
| 2 | code[0] | 34,152 | 1.43% |
| 3 | code[1] | 31,471 | 1.32% |
| 4 | code[2] | 27,593 | 1.15% |
| 5 | code[3] | 19,719 | 0.82% |
| 6 | code[5] | 17,727 | 0.74% |
| 7 | code[4] | 17,695 | 0.74% |
| 8 | code[7] | 15,780 | 0.66% |
| 9 | code[6] | 15,294 | 0.64% |
| 10 | code[9] | 14,368 | 0.60% |

## Twiggy Retained Hotspots

| Rank | Item | Retained Bytes | Retained % |
| ---: | --- | ---: | ---: |
| 1 | export "canister_query query_one_fluent" | 647,420 | 27.06% |
| 2 | code[5699] | 647,385 | 27.05% |
| 3 | code[7] | 647,376 | 27.05% |
| 4 | code[13] | 368,119 | 15.38% |
| 5 | code[4] | 256,384 | 10.71% |
| 6 | code[395] | 213,550 | 8.92% |
| 7 | code[26] | 212,407 | 8.88% |
| 8 | code[25] | 192,058 | 8.03% |
| 9 | data[0] | 178,762 | 7.47% |
| 10 | code[75] | 170,678 | 7.13% |

## Artifacts

- `docs/audits/reports/2026-06/2026-06-06/artifacts/wasm-footprint/wasm-footprint.one_fluent_query.wasm-release.sql-on.size-report.json`
- `docs/audits/reports/2026-06/2026-06-06/artifacts/wasm-footprint/wasm-footprint.one_fluent_query.wasm-release.sql-on.size-summary.md`
- `docs/audits/reports/2026-06/2026-06-06/artifacts/wasm-footprint/wasm-footprint.one_fluent_query.wasm-release.sql-on.twiggy-top.txt`
- `docs/audits/reports/2026-06/2026-06-06/artifacts/wasm-footprint/wasm-footprint.one_fluent_query.wasm-release.sql-on.twiggy-top.csv`
- `docs/audits/reports/2026-06/2026-06-06/artifacts/wasm-footprint/wasm-footprint.one_fluent_query.wasm-release.sql-on.twiggy-dominators.txt`
- `docs/audits/reports/2026-06/2026-06-06/artifacts/wasm-footprint/wasm-footprint.one_fluent_query.wasm-release.sql-on.twiggy-retained.csv`
- `docs/audits/reports/2026-06/2026-06-06/artifacts/wasm-footprint/wasm-footprint.one_fluent_query.wasm-release.sql-on.twiggy-monos.txt`

## Follow-Up Actions

- owner boundary: `wasm-audit history`; action: preserve size artifacts for baseline reports so trend deltas remain comparable; target report date/run: next `wasm-footprint` run.

## Verification Readout

- `bash scripts/ci/wasm-size-report.sh` -> PASS
- `twiggy top -n 40` -> PASS
- `twiggy top --retained -n 40` -> PASS
- `twiggy dominators -r 160` -> PASS
- `twiggy monos` -> PASS
