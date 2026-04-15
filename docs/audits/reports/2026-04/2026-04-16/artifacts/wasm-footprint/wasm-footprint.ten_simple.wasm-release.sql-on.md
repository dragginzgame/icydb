# Recurring Audit - Wasm Footprint (2026-04-16)

## Report Preamble

- scope: recurring wasm footprint audit for `ten_simple` with profile `wasm-release` and SQL variant `sql-on`
- compared baseline report path: `docs/audits/reports/2026-04/2026-04-03/wasm-footprint.md`
- code snapshot identifier: `b93b44407`
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
| dfx-built `.wasm` | N/A | 957,521 | N/A |
| dfx-built deterministic `.wasm.gz` | N/A | 331,686 | N/A |
| dfx-shrunk `.wasm` | N/A | 893,361 | N/A |
| dfx-shrunk `.wasm.gz` | N/A | 315,703 | N/A |

## Structural Snapshot (ic-wasm)

| Metric | dfx-built | dfx-shrunk |
| --- | ---: | ---: |
| Function count | 2,317 | 2,317 |
| Callback count | 1 | 1 |
| Data section count | 2 | 2 |
| Data section bytes | 136,184 | 136,184 |
| Exported methods | 6 | 6 |

## Twiggy Top Offenders (Shallow Size)

| Rank | Item | Shallow Bytes | Shallow % |
| ---: | --- | ---: | ---: |
| 1 | data[0] | 136,034 | 15.23% |
| 2 | code[0] | 26,812 | 3.00% |
| 3 | code[1] | 23,074 | 2.58% |
| 4 | code[2] | 16,910 | 1.89% |
| 5 | code[3] | 14,882 | 1.67% |
| 6 | code[4] | 12,633 | 1.41% |
| 7 | code[5] | 9,457 | 1.06% |
| 8 | code[6] | 6,775 | 0.76% |
| 9 | code[7] | 5,490 | 0.61% |
| 10 | code[8] | 5,473 | 0.61% |

## Twiggy Retained Hotspots

| Rank | Item | Retained Bytes | Retained % |
| ---: | --- | ---: | ---: |
| 1 | table[0] | 306,465 | 34.30% |
| 2 | elem[0] | 306,459 | 34.30% |
| 3 | data[0] | 136,034 | 15.23% |
| 4 | export "canister_query icydb_snapshot" | 99,433 | 11.13% |
| 5 | code[2278] | 99,400 | 11.13% |
| 6 | code[17] | 99,391 | 11.13% |
| 7 | code[0] | 93,492 | 10.47% |
| 8 | export "canister_query icydb_metrics" | 89,298 | 10.00% |
| 9 | code[2277] | 89,266 | 9.99% |
| 10 | code[3] | 89,257 | 9.99% |

## Artifacts

- `docs/audits/reports/2026-04/2026-04-16/artifacts/wasm-footprint/wasm-footprint.ten_simple.wasm-release.sql-on.size-report.json`
- `docs/audits/reports/2026-04/2026-04-16/artifacts/wasm-footprint/wasm-footprint.ten_simple.wasm-release.sql-on.size-summary.md`
- `docs/audits/reports/2026-04/2026-04-16/artifacts/wasm-footprint/wasm-footprint.ten_simple.wasm-release.sql-on.twiggy-top.txt`
- `docs/audits/reports/2026-04/2026-04-16/artifacts/wasm-footprint/wasm-footprint.ten_simple.wasm-release.sql-on.twiggy-top.csv`
- `docs/audits/reports/2026-04/2026-04-16/artifacts/wasm-footprint/wasm-footprint.ten_simple.wasm-release.sql-on.twiggy-dominators.txt`
- `docs/audits/reports/2026-04/2026-04-16/artifacts/wasm-footprint/wasm-footprint.ten_simple.wasm-release.sql-on.twiggy-retained.csv`
- `docs/audits/reports/2026-04/2026-04-16/artifacts/wasm-footprint/wasm-footprint.ten_simple.wasm-release.sql-on.twiggy-monos.txt`

## Follow-Up Actions

- owner boundary: `wasm-audit history`; action: preserve size artifacts for baseline reports so trend deltas remain comparable; target report date/run: next `wasm-footprint` run.

## Verification Readout

- `bash scripts/ci/wasm-size-report.sh` -> PASS
- `twiggy top -n 40` -> PASS
- `twiggy top --retained -n 40` -> PASS
- `twiggy dominators -r 160` -> PASS
- `twiggy monos` -> PASS
