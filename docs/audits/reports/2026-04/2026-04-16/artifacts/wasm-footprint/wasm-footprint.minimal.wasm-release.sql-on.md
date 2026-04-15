# Recurring Audit - Wasm Footprint (2026-04-16)

## Report Preamble

- scope: recurring wasm footprint audit for `minimal` with profile `wasm-release` and SQL variant `sql-on`
- compared baseline report path: `docs/audits/reports/2026-04/2026-04-03/wasm-footprint.md`
- code snapshot identifier: `b93b44407`
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
| dfx-built `.wasm` | 779,081 | 710,014 | -69,067 |
| dfx-built deterministic `.wasm.gz` | 273,045 | 249,544 | -23,501 |
| dfx-shrunk `.wasm` | 722,820 | 659,919 | -62,901 |
| dfx-shrunk `.wasm.gz` | 256,632 | 234,526 | -22,106 |

## Structural Snapshot (ic-wasm)

| Metric | dfx-built | dfx-shrunk |
| --- | ---: | ---: |
| Function count | 1,910 | 1,910 |
| Callback count | 1 | 1 |
| Data section count | 2 | 2 |
| Data section bytes | 102,280 | 102,280 |
| Exported methods | 6 | 6 |

## Twiggy Top Offenders (Shallow Size)

| Rank | Item | Shallow Bytes | Shallow % |
| ---: | --- | ---: | ---: |
| 1 | data[0] | 102,130 | 15.48% |
| 2 | code[0] | 28,092 | 4.26% |
| 3 | code[1] | 16,996 | 2.58% |
| 4 | code[2] | 14,861 | 2.25% |
| 5 | code[3] | 9,427 | 1.43% |
| 6 | code[4] | 5,488 | 0.83% |
| 7 | code[5] | 5,473 | 0.83% |
| 8 | code[6] | 4,919 | 0.75% |
| 9 | code[9] | 4,584 | 0.69% |
| 10 | code[8] | 4,553 | 0.69% |

## Twiggy Retained Hotspots

| Rank | Item | Retained Bytes | Retained % |
| ---: | --- | ---: | ---: |
| 1 | export "canister_query icydb_snapshot" | 160,263 | 24.29% |
| 2 | code[1872] | 160,230 | 24.28% |
| 3 | code[12] | 160,221 | 24.28% |
| 4 | code[0] | 154,298 | 23.38% |
| 5 | table[0] | 108,326 | 16.42% |
| 6 | elem[0] | 108,320 | 16.41% |
| 7 | data[0] | 102,130 | 15.48% |
| 8 | export "canister_query icydb_metrics" | 89,725 | 13.60% |
| 9 | code[1871] | 89,693 | 13.59% |
| 10 | code[2] | 89,684 | 13.59% |

## Artifacts

- `docs/audits/reports/2026-04/2026-04-16/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.size-report.json`
- `docs/audits/reports/2026-04/2026-04-16/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.size-summary.md`
- `docs/audits/reports/2026-04/2026-04-16/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.twiggy-top.txt`
- `docs/audits/reports/2026-04/2026-04-16/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.twiggy-top.csv`
- `docs/audits/reports/2026-04/2026-04-16/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.twiggy-dominators.txt`
- `docs/audits/reports/2026-04/2026-04-16/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.twiggy-retained.csv`
- `docs/audits/reports/2026-04/2026-04-16/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.twiggy-monos.txt`

## Follow-Up Actions

- No follow-up actions required for this run.

## Verification Readout

- `bash scripts/ci/wasm-size-report.sh` -> PASS
- `twiggy top -n 40` -> PASS
- `twiggy top --retained -n 40` -> PASS
- `twiggy dominators -r 160` -> PASS
- `twiggy monos` -> PASS
