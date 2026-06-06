# Recurring Audit - Wasm Footprint (2026-06-06)

## Report Preamble

- scope: recurring wasm footprint audit for `minimal` with profile `wasm-release` and SQL variant `sql-on`
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
| Baseline delta availability | PARTIAL | baseline artifact missing at `docs/audits/reports/2026-04/2026-04-27/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.size-report.json` |

PASS=4, PARTIAL=1, FAIL=0

## Size Snapshot

| Metric | Previous | Current | Delta |
| --- | ---: | ---: | ---: |
| icp-built `.wasm` | N/A | 370,058 | N/A |
| icp-built deterministic `.wasm.gz` | N/A | 132,701 | N/A |
| icp-shrunk `.wasm` | N/A | 339,468 | N/A |
| icp-shrunk `.wasm.gz` | N/A | 125,278 | N/A |

## Structural Snapshot (ic-wasm)

| Metric | icp-built | icp-shrunk |
| --- | ---: | ---: |
| Function count | 1,028 | 1,028 |
| Callback count | 1 | 1 |
| Data section count | 3 | 3 |
| Data section bytes | 39,952 | 39,952 |
| Exported methods | 2 | 2 |

## Twiggy Top Offenders (Shallow Size)

| Rank | Item | Shallow Bytes | Shallow % |
| ---: | --- | ---: | ---: |
| 1 | data[0] | 39,890 | 11.75% |
| 2 | code[0] | 19,259 | 5.67% |
| 3 | code[1] | 16,079 | 4.74% |
| 4 | code[2] | 12,402 | 3.65% |
| 5 | code[3] | 8,589 | 2.53% |
| 6 | code[4] | 7,601 | 2.24% |
| 7 | code[5] | 6,723 | 1.98% |
| 8 | code[6] | 6,531 | 1.92% |
| 9 | code[7] | 5,390 | 1.59% |
| 10 | code[8] | 4,793 | 1.41% |

## Twiggy Retained Hotspots

| Rank | Item | Retained Bytes | Retained % |
| ---: | --- | ---: | ---: |
| 1 | export "canister_query __icydb_metrics" | 118,922 | 35.03% |
| 2 | code[1014] | 118,888 | 35.02% |
| 3 | code[0] | 118,879 | 35.02% |
| 4 | table[0] | 64,973 | 19.14% |
| 5 | elem[0] | 64,967 | 19.14% |
| 6 | data[0] | 39,890 | 11.75% |
| 7 | code[1] | 37,104 | 10.93% |
| 8 | code[4] | 23,455 | 6.91% |
| 9 | code[22] | 17,267 | 5.09% |
| 10 | code[2] | 12,795 | 3.77% |

## Artifacts

- `docs/audits/reports/2026-06/2026-06-06/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.size-report.json`
- `docs/audits/reports/2026-06/2026-06-06/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.size-summary.md`
- `docs/audits/reports/2026-06/2026-06-06/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.twiggy-top.txt`
- `docs/audits/reports/2026-06/2026-06-06/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.twiggy-top.csv`
- `docs/audits/reports/2026-06/2026-06-06/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.twiggy-dominators.txt`
- `docs/audits/reports/2026-06/2026-06-06/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.twiggy-retained.csv`
- `docs/audits/reports/2026-06/2026-06-06/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.twiggy-monos.txt`

## Follow-Up Actions

- owner boundary: `wasm-audit history`; action: preserve size artifacts for baseline reports so trend deltas remain comparable; target report date/run: next `wasm-footprint` run.

## Verification Readout

- `bash scripts/ci/wasm-size-report.sh` -> PASS
- `twiggy top -n 40` -> PASS
- `twiggy top --retained -n 40` -> PASS
- `twiggy dominators -r 160` -> PASS
- `twiggy monos` -> PASS
