# Recurring Audit - Wasm Footprint (2026-04-27)

## Report Preamble

- scope: recurring wasm footprint audit for `minimal` with profile `wasm-release` and SQL variant `sql-on`
- compared baseline report path: `docs/audits/reports/2026-04/2026-04-22/wasm-footprint.md`
- code snapshot identifier: `9890a4d7a`
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
| dfx-built `.wasm` | 683,357 | 686,706 | +3,349 |
| dfx-built deterministic `.wasm.gz` | 240,772 | 242,274 | +1,502 |
| dfx-shrunk `.wasm` | 633,590 | 636,717 | +3,127 |
| dfx-shrunk `.wasm.gz` | 226,298 | 227,645 | +1,347 |

## Structural Snapshot (ic-wasm)

| Metric | dfx-built | dfx-shrunk |
| --- | ---: | ---: |
| Function count | 1,914 | 1,914 |
| Callback count | 1 | 1 |
| Data section count | 2 | 2 |
| Data section bytes | 102,248 | 102,248 |
| Exported methods | 6 | 6 |

## Twiggy Top Offenders (Shallow Size)

| Rank | Item | Shallow Bytes | Shallow % |
| ---: | --- | ---: | ---: |
| 1 | data[0] | 102,098 | 16.04% |
| 2 | code[0] | 26,137 | 4.10% |
| 3 | code[1] | 16,096 | 2.53% |
| 4 | code[2] | 14,222 | 2.23% |
| 5 | code[3] | 8,598 | 1.35% |
| 6 | code[4] | 5,516 | 0.87% |
| 7 | code[5] | 4,794 | 0.75% |
| 8 | code[6] | 4,583 | 0.72% |
| 9 | code[8] | 4,429 | 0.70% |
| 10 | code[12] | 4,248 | 0.67% |

## Twiggy Retained Hotspots

| Rank | Item | Retained Bytes | Retained % |
| ---: | --- | ---: | ---: |
| 1 | export "canister_query icydb_snapshot" | 147,974 | 23.24% |
| 2 | code[1876] | 147,941 | 23.23% |
| 3 | code[13] | 147,932 | 23.23% |
| 4 | code[0] | 142,296 | 22.35% |
| 5 | table[0] | 106,470 | 16.72% |
| 6 | elem[0] | 106,464 | 16.72% |
| 7 | data[0] | 102,098 | 16.04% |
| 8 | export "canister_query icydb_metrics" | 85,962 | 13.50% |
| 9 | code[1875] | 85,930 | 13.50% |
| 10 | code[2] | 85,921 | 13.49% |

## Artifacts

- `docs/audits/reports/2026-04/2026-04-27/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.size-report.json`
- `docs/audits/reports/2026-04/2026-04-27/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.size-summary.md`
- `docs/audits/reports/2026-04/2026-04-27/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.twiggy-top.txt`
- `docs/audits/reports/2026-04/2026-04-27/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.twiggy-top.csv`
- `docs/audits/reports/2026-04/2026-04-27/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.twiggy-dominators.txt`
- `docs/audits/reports/2026-04/2026-04-27/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.twiggy-retained.csv`
- `docs/audits/reports/2026-04/2026-04-27/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.twiggy-monos.txt`

## Follow-Up Actions

- No follow-up actions required for this run.

## Verification Readout

- `bash scripts/ci/wasm-size-report.sh` -> PASS
- `twiggy top -n 40` -> PASS
- `twiggy top --retained -n 40` -> PASS
- `twiggy dominators -r 160` -> PASS
- `twiggy monos` -> PASS
