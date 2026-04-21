# Recurring Audit - Wasm Footprint (2026-04-22)

## Report Preamble

- scope: recurring wasm footprint audit for `ten_simple` with profile `wasm-release` and SQL variant `sql-on`
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
| dfx-built `.wasm` | 951,725 | 910,376 | -41,349 |
| dfx-built deterministic `.wasm.gz` | 329,226 | 317,463 | -11,763 |
| dfx-shrunk `.wasm` | 887,923 | 846,925 | -40,998 |
| dfx-shrunk `.wasm.gz` | 313,001 | 302,117 | -10,884 |

## Structural Snapshot (ic-wasm)

| Metric | dfx-built | dfx-shrunk |
| --- | ---: | ---: |
| Function count | 2,302 | 2,302 |
| Callback count | 1 | 1 |
| Data section count | 2 | 2 |
| Data section bytes | 134,528 | 134,528 |
| Exported methods | 6 | 6 |

## Twiggy Top Offenders (Shallow Size)

| Rank | Item | Shallow Bytes | Shallow % |
| ---: | --- | ---: | ---: |
| 1 | data[0] | 134,378 | 15.87% |
| 2 | code[0] | 24,286 | 2.87% |
| 3 | code[1] | 20,561 | 2.43% |
| 4 | code[2] | 16,005 | 1.89% |
| 5 | code[3] | 14,282 | 1.69% |
| 6 | code[4] | 12,054 | 1.42% |
| 7 | code[5] | 8,626 | 1.02% |
| 8 | code[6] | 5,518 | 0.65% |
| 9 | code[9] | 4,803 | 0.57% |
| 10 | code[8] | 4,718 | 0.56% |

## Twiggy Retained Hotspots

| Rank | Item | Retained Bytes | Retained % |
| ---: | --- | ---: | ---: |
| 1 | table[0] | 285,865 | 33.75% |
| 2 | elem[0] | 285,859 | 33.75% |
| 3 | data[0] | 134,378 | 15.87% |
| 4 | export "canister_query icydb_snapshot" | 92,724 | 10.95% |
| 5 | code[2264] | 92,691 | 10.94% |
| 6 | code[16] | 92,682 | 10.94% |
| 7 | code[0] | 87,064 | 10.28% |
| 8 | export "canister_query icydb_metrics" | 85,595 | 10.11% |
| 9 | code[2263] | 85,563 | 10.10% |
| 10 | code[3] | 85,554 | 10.10% |

## Artifacts

- `docs/audits/reports/2026-04/2026-04-22/artifacts/wasm-footprint/wasm-footprint.ten_simple.wasm-release.sql-on.size-report.json`
- `docs/audits/reports/2026-04/2026-04-22/artifacts/wasm-footprint/wasm-footprint.ten_simple.wasm-release.sql-on.size-summary.md`
- `docs/audits/reports/2026-04/2026-04-22/artifacts/wasm-footprint/wasm-footprint.ten_simple.wasm-release.sql-on.twiggy-top.txt`
- `docs/audits/reports/2026-04/2026-04-22/artifacts/wasm-footprint/wasm-footprint.ten_simple.wasm-release.sql-on.twiggy-top.csv`
- `docs/audits/reports/2026-04/2026-04-22/artifacts/wasm-footprint/wasm-footprint.ten_simple.wasm-release.sql-on.twiggy-dominators.txt`
- `docs/audits/reports/2026-04/2026-04-22/artifacts/wasm-footprint/wasm-footprint.ten_simple.wasm-release.sql-on.twiggy-retained.csv`
- `docs/audits/reports/2026-04/2026-04-22/artifacts/wasm-footprint/wasm-footprint.ten_simple.wasm-release.sql-on.twiggy-monos.txt`

## Follow-Up Actions

- No follow-up actions required for this run.

## Verification Readout

- `bash scripts/ci/wasm-size-report.sh` -> PASS
- `twiggy top -n 40` -> PASS
- `twiggy top --retained -n 40` -> PASS
- `twiggy dominators -r 160` -> PASS
- `twiggy monos` -> PASS
