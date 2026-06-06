# Recurring Audit - Wasm Footprint (2026-06-06)

## Report Preamble

- scope: recurring wasm footprint audit for `one_fluent_query` with profile `wasm-release` and SQL variant `sql-on`
- compared baseline report path: `docs/audits/reports/2026-04/2026-04-27/wasm-footprint.md`
- code snapshot identifier: `2b97a0d33`
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
| icp-built `.wasm` | N/A | 2,564,390 | N/A |
| icp-built deterministic `.wasm.gz` | N/A | 828,531 | N/A |
| icp-shrunk `.wasm` | N/A | 2,389,137 | N/A |
| icp-shrunk `.wasm.gz` | N/A | 786,522 | N/A |

## Structural Snapshot (ic-wasm)

| Metric | icp-built | icp-shrunk |
| --- | ---: | ---: |
| Function count | 5,705 | 5,705 |
| Callback count | 1 | 1 |
| Data section count | 3 | 3 |
| Data section bytes | 178,900 | 178,900 |
| Exported methods | 2 | 2 |

## Twiggy Top Offenders (Shallow Size)

| Rank | Item | Shallow Bytes | Shallow % |
| ---: | --- | ---: | ---: |
| 1 | data[0] | 178,634 | 7.48% |
| 2 | code[0] | 34,152 | 1.43% |
| 3 | code[1] | 31,464 | 1.32% |
| 4 | code[2] | 27,593 | 1.15% |
| 5 | code[3] | 19,952 | 0.84% |
| 6 | code[5] | 17,727 | 0.74% |
| 7 | code[4] | 17,695 | 0.74% |
| 8 | code[7] | 15,780 | 0.66% |
| 9 | code[6] | 15,294 | 0.64% |
| 10 | code[9] | 14,370 | 0.60% |

## Twiggy Retained Hotspots

| Rank | Item | Retained Bytes | Retained % |
| ---: | --- | ---: | ---: |
| 1 | export "canister_query query_one_fluent" | 644,504 | 26.98% |
| 2 | code[5685] | 644,469 | 26.97% |
| 3 | code[7] | 644,460 | 26.97% |
| 4 | code[13] | 368,119 | 15.41% |
| 5 | code[4] | 256,383 | 10.73% |
| 6 | code[394] | 213,550 | 8.94% |
| 7 | code[26] | 212,407 | 8.89% |
| 8 | code[25] | 192,058 | 8.04% |
| 9 | data[0] | 178,634 | 7.48% |
| 10 | code[75] | 170,677 | 7.14% |

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
