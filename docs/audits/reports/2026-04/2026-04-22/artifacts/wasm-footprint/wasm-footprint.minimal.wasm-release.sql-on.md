# Recurring Audit - Wasm Footprint (2026-04-22)

## Report Preamble

- scope: recurring wasm footprint audit for `minimal` with profile `wasm-release` and SQL variant `sql-on`
- compared baseline report path: `docs/audits/reports/2026-04/2026-04-16/wasm-footprint.md`
- code snapshot identifier: `b43bba078`
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
| dfx-built `.wasm` | 709,693 | 683,357 | -26,336 |
| dfx-built deterministic `.wasm.gz` | 248,863 | 240,772 | -8,091 |
| dfx-shrunk `.wasm` | 659,648 | 633,590 | -26,058 |
| dfx-shrunk `.wasm.gz` | 234,463 | 226,298 | -8,165 |

## Structural Snapshot (ic-wasm)

| Metric | dfx-built | dfx-shrunk |
| --- | ---: | ---: |
| Function count | 1,906 | 1,906 |
| Callback count | 1 | 1 |
| Data section count | 2 | 2 |
| Data section bytes | 101,904 | 101,904 |
| Exported methods | 6 | 6 |

## Twiggy Top Offenders (Shallow Size)

| Rank | Item | Shallow Bytes | Shallow % |
| ---: | --- | ---: | ---: |
| 1 | data[0] | 101,754 | 16.06% |
| 2 | code[0] | 25,532 | 4.03% |
| 3 | code[1] | 16,096 | 2.54% |
| 4 | code[2] | 14,262 | 2.25% |
| 5 | code[3] | 8,596 | 1.36% |
| 6 | code[4] | 5,516 | 0.87% |
| 7 | code[5] | 4,794 | 0.76% |
| 8 | code[6] | 4,583 | 0.72% |
| 9 | code[8] | 4,429 | 0.70% |
| 10 | code[10] | 4,248 | 0.67% |

## Twiggy Retained Hotspots

| Rank | Item | Retained Bytes | Retained % |
| ---: | --- | ---: | ---: |
| 1 | export "canister_query icydb_snapshot" | 145,743 | 23.00% |
| 2 | code[1868] | 145,710 | 23.00% |
| 3 | code[12] | 145,701 | 23.00% |
| 4 | code[0] | 140,065 | 22.11% |
| 5 | table[0] | 106,385 | 16.79% |
| 6 | elem[0] | 106,379 | 16.79% |
| 7 | data[0] | 101,754 | 16.06% |
| 8 | export "canister_query icydb_metrics" | 86,000 | 13.57% |
| 9 | code[1867] | 85,968 | 13.57% |
| 10 | code[2] | 85,959 | 13.57% |

## Artifacts

- `docs/audits/reports/2026-04/2026-04-22/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.size-report.json`
- `docs/audits/reports/2026-04/2026-04-22/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.size-summary.md`
- `docs/audits/reports/2026-04/2026-04-22/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.twiggy-top.txt`
- `docs/audits/reports/2026-04/2026-04-22/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.twiggy-top.csv`
- `docs/audits/reports/2026-04/2026-04-22/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.twiggy-dominators.txt`
- `docs/audits/reports/2026-04/2026-04-22/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.twiggy-retained.csv`
- `docs/audits/reports/2026-04/2026-04-22/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.twiggy-monos.txt`

## Follow-Up Actions

- No follow-up actions required for this run.

## Verification Readout

- `bash scripts/ci/wasm-size-report.sh` -> PASS
- `twiggy top -n 40` -> PASS
- `twiggy top --retained -n 40` -> PASS
- `twiggy dominators -r 160` -> PASS
- `twiggy monos` -> PASS
