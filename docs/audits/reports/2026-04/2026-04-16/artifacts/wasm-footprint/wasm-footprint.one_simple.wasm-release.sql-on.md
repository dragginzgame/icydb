# Recurring Audit - Wasm Footprint (2026-04-16)

## Report Preamble

- scope: recurring wasm footprint audit for `one_simple` with profile `wasm-release` and SQL variant `sql-on`
- compared baseline report path: `docs/audits/reports/2026-04/2026-04-03/wasm-footprint.md`
- code snapshot identifier: `ab418057d`
- method tag/version: `WASM-1.0`
- comparability status: `non-comparable (baseline report exists but baseline size artifact is missing)`

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
| dfx-built `.wasm` | N/A | 934,130 | N/A |
| dfx-built deterministic `.wasm.gz` | N/A | 327,369 | N/A |
| dfx-shrunk `.wasm` | N/A | 871,645 | N/A |
| dfx-shrunk `.wasm.gz` | N/A | 311,354 | N/A |

## Structural Snapshot (ic-wasm)

| Metric | dfx-built | dfx-shrunk |
| --- | ---: | ---: |
| Function count | 2,280 | 2,280 |
| Callback count | 1 | 1 |
| Data section count | 2 | 2 |
| Data section bytes | 133,112 | 133,112 |
| Exported methods | 6 | 6 |

## Twiggy Top Offenders (Shallow Size)

| Rank | Item | Shallow Bytes | Shallow % |
| ---: | --- | ---: | ---: |
| 1 | data[0] | 132,962 | 15.25% |
| 2 | code[0] | 26,812 | 3.08% |
| 3 | code[1] | 23,040 | 2.64% |
| 4 | code[2] | 16,910 | 1.94% |
| 5 | code[3] | 14,882 | 1.71% |
| 6 | code[4] | 12,554 | 1.44% |
| 7 | code[5] | 9,457 | 1.08% |
| 8 | code[6] | 5,490 | 0.63% |
| 9 | code[7] | 5,473 | 0.63% |
| 10 | code[9] | 5,430 | 0.62% |

## Twiggy Retained Hotspots

| Rank | Item | Retained Bytes | Retained % |
| ---: | --- | ---: | ---: |
| 1 | table[0] | 288,010 | 33.04% |
| 2 | elem[0] | 288,004 | 33.04% |
| 3 | data[0] | 132,962 | 15.25% |
| 4 | export "canister_query icydb_snapshot" | 99,433 | 11.41% |
| 5 | code[2241] | 99,400 | 11.40% |
| 6 | code[17] | 99,391 | 11.40% |
| 7 | code[0] | 93,492 | 10.73% |
| 8 | export "canister_query icydb_metrics" | 89,298 | 10.24% |
| 9 | code[2240] | 89,266 | 10.24% |
| 10 | code[3] | 89,257 | 10.24% |

## Artifacts

- `docs/audits/reports/2026-04/2026-04-16/artifacts/wasm-footprint/wasm-footprint.one_simple.wasm-release.sql-on.size-report.json`
- `docs/audits/reports/2026-04/2026-04-16/artifacts/wasm-footprint/wasm-footprint.one_simple.wasm-release.sql-on.size-summary.md`
- `docs/audits/reports/2026-04/2026-04-16/artifacts/wasm-footprint/wasm-footprint.one_simple.wasm-release.sql-on.twiggy-top.txt`
- `docs/audits/reports/2026-04/2026-04-16/artifacts/wasm-footprint/wasm-footprint.one_simple.wasm-release.sql-on.twiggy-top.csv`
- `docs/audits/reports/2026-04/2026-04-16/artifacts/wasm-footprint/wasm-footprint.one_simple.wasm-release.sql-on.twiggy-dominators.txt`
- `docs/audits/reports/2026-04/2026-04-16/artifacts/wasm-footprint/wasm-footprint.one_simple.wasm-release.sql-on.twiggy-retained.csv`
- `docs/audits/reports/2026-04/2026-04-16/artifacts/wasm-footprint/wasm-footprint.one_simple.wasm-release.sql-on.twiggy-monos.txt`

## Follow-Up Actions

- owner boundary: `wasm-audit history`; action: preserve size artifacts for baseline reports so trend deltas remain comparable; target report date/run: next `wasm-footprint` run.

## Verification Readout

- `bash scripts/ci/wasm-size-report.sh` -> PASS
- `twiggy top -n 40` -> PASS
- `twiggy top --retained -n 40` -> PASS
- `twiggy dominators -r 160` -> PASS
- `twiggy monos` -> PASS
