# Recurring Audit - Wasm Footprint (2026-04-22)

## Report Preamble

- scope: recurring wasm footprint audit for `one_simple` with profile `wasm-release` and SQL variant `sql-on`
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
| dfx-built `.wasm` | 934,130 | 894,532 | -39,598 |
| dfx-built deterministic `.wasm.gz` | 327,369 | 315,713 | -11,656 |
| dfx-shrunk `.wasm` | 871,645 | 832,381 | -39,264 |
| dfx-shrunk `.wasm.gz` | 311,354 | 300,012 | -11,342 |

## Structural Snapshot (ic-wasm)

| Metric | dfx-built | dfx-shrunk |
| --- | ---: | ---: |
| Function count | 2,276 | 2,276 |
| Callback count | 1 | 1 |
| Data section count | 2 | 2 |
| Data section bytes | 132,864 | 132,864 |
| Exported methods | 6 | 6 |

## Twiggy Top Offenders (Shallow Size)

| Rank | Item | Shallow Bytes | Shallow % |
| ---: | --- | ---: | ---: |
| 1 | data[0] | 132,714 | 15.94% |
| 2 | code[0] | 24,286 | 2.92% |
| 3 | code[1] | 20,561 | 2.47% |
| 4 | code[2] | 16,005 | 1.92% |
| 5 | code[3] | 14,282 | 1.72% |
| 6 | code[4] | 12,054 | 1.45% |
| 7 | code[5] | 8,626 | 1.04% |
| 8 | code[6] | 5,518 | 0.66% |
| 9 | code[10] | 4,861 | 0.58% |
| 10 | code[9] | 4,803 | 0.58% |

## Twiggy Retained Hotspots

| Rank | Item | Retained Bytes | Retained % |
| ---: | --- | ---: | ---: |
| 1 | table[0] | 272,985 | 32.80% |
| 2 | elem[0] | 272,979 | 32.79% |
| 3 | data[0] | 132,714 | 15.94% |
| 4 | export "canister_query icydb_snapshot" | 92,724 | 11.14% |
| 5 | code[2238] | 92,691 | 11.14% |
| 6 | code[17] | 92,682 | 11.13% |
| 7 | code[0] | 87,064 | 10.46% |
| 8 | export "canister_query icydb_metrics" | 85,595 | 10.28% |
| 9 | code[2237] | 85,563 | 10.28% |
| 10 | code[3] | 85,554 | 10.28% |

## Artifacts

- `docs/audits/reports/2026-04/2026-04-22/artifacts/wasm-footprint/wasm-footprint.one_simple.wasm-release.sql-on.size-report.json`
- `docs/audits/reports/2026-04/2026-04-22/artifacts/wasm-footprint/wasm-footprint.one_simple.wasm-release.sql-on.size-summary.md`
- `docs/audits/reports/2026-04/2026-04-22/artifacts/wasm-footprint/wasm-footprint.one_simple.wasm-release.sql-on.twiggy-top.txt`
- `docs/audits/reports/2026-04/2026-04-22/artifacts/wasm-footprint/wasm-footprint.one_simple.wasm-release.sql-on.twiggy-top.csv`
- `docs/audits/reports/2026-04/2026-04-22/artifacts/wasm-footprint/wasm-footprint.one_simple.wasm-release.sql-on.twiggy-dominators.txt`
- `docs/audits/reports/2026-04/2026-04-22/artifacts/wasm-footprint/wasm-footprint.one_simple.wasm-release.sql-on.twiggy-retained.csv`
- `docs/audits/reports/2026-04/2026-04-22/artifacts/wasm-footprint/wasm-footprint.one_simple.wasm-release.sql-on.twiggy-monos.txt`

## Follow-Up Actions

- No follow-up actions required for this run.

## Verification Readout

- `bash scripts/ci/wasm-size-report.sh` -> PASS
- `twiggy top -n 40` -> PASS
- `twiggy top --retained -n 40` -> PASS
- `twiggy dominators -r 160` -> PASS
- `twiggy monos` -> PASS
