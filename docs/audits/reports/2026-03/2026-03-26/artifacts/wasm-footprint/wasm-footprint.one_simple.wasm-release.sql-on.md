# Recurring Audit - Wasm Footprint (2026-03-26)

## Report Preamble

- scope: recurring wasm footprint audit for `one_simple` with profile `wasm-release` and SQL variant `sql-on`
- compared baseline report path: `docs/audits/reports/2026-03/2026-03-24/wasm-footprint.md`
- code snapshot identifier: `16c600ba`
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
| dfx-built `.wasm` | 1,482,348 | 1,400,600 | -81,748 |
| dfx-built deterministic `.wasm.gz` | 541,295 | 508,157 | -33,138 |
| dfx-shrunk `.wasm` | 1,383,715 | 1,307,229 | -76,486 |
| dfx-shrunk `.wasm.gz` | 507,627 | 482,146 | -25,481 |

## Structural Snapshot (ic-wasm)

| Metric | dfx-built | dfx-shrunk |
| --- | ---: | ---: |
| Function count | 3,335 | 3,335 |
| Callback count | 1 | 1 |
| Data section count | 2 | 2 |
| Data section bytes | 186,732 | 186,732 |
| Exported methods | 7 | 7 |

## Twiggy Top Offenders (Shallow Size)

| Rank | Item | Shallow Bytes | Shallow % |
| ---: | --- | ---: | ---: |
| 1 | data[0] | 186,598 | 14.27% |
| 2 | code[0] | 22,749 | 1.74% |
| 3 | code[1] | 18,437 | 1.41% |
| 4 | code[2] | 17,848 | 1.37% |
| 5 | code[3] | 15,418 | 1.18% |
| 6 | code[4] | 14,575 | 1.11% |
| 7 | code[5] | 12,223 | 0.94% |
| 8 | code[7] | 11,239 | 0.86% |
| 9 | code[8] | 10,831 | 0.83% |
| 10 | code[9] | 10,358 | 0.79% |

## Twiggy Retained Hotspots

| Rank | Item | Retained Bytes | Retained % |
| ---: | --- | ---: | ---: |
| 1 | table[0] | 287,503 | 21.99% |
| 2 | elem[0] | 287,497 | 21.99% |
| 3 | export "canister_query query" | 205,255 | 15.70% |
| 4 | code[3296] | 205,231 | 15.70% |
| 5 | code[4] | 205,222 | 15.70% |
| 6 | data[0] | 186,598 | 14.27% |
| 7 | code[1343] | 82,650 | 6.32% |
| 8 | code[0] | 82,480 | 6.31% |
| 9 | code[33] | 68,695 | 5.26% |
| 10 | code[1] | 57,088 | 4.37% |

## Artifacts

- `docs/audits/reports/2026-03/2026-03-26/artifacts/wasm-footprint/wasm-footprint.one_simple.wasm-release.sql-on.size-report.json`
- `docs/audits/reports/2026-03/2026-03-26/artifacts/wasm-footprint/wasm-footprint.one_simple.wasm-release.sql-on.size-summary.md`
- `docs/audits/reports/2026-03/2026-03-26/artifacts/wasm-footprint/wasm-footprint.one_simple.wasm-release.sql-on.twiggy-top.txt`
- `docs/audits/reports/2026-03/2026-03-26/artifacts/wasm-footprint/wasm-footprint.one_simple.wasm-release.sql-on.twiggy-top.csv`
- `docs/audits/reports/2026-03/2026-03-26/artifacts/wasm-footprint/wasm-footprint.one_simple.wasm-release.sql-on.twiggy-dominators.txt`
- `docs/audits/reports/2026-03/2026-03-26/artifacts/wasm-footprint/wasm-footprint.one_simple.wasm-release.sql-on.twiggy-retained.csv`
- `docs/audits/reports/2026-03/2026-03-26/artifacts/wasm-footprint/wasm-footprint.one_simple.wasm-release.sql-on.twiggy-monos.txt`

## Follow-Up Actions

- No follow-up actions required for this run.

## Verification Readout

- `bash scripts/ci/wasm-size-report.sh` -> PASS
- `twiggy top -n 40` -> PASS
- `twiggy top --retained -n 40` -> PASS
- `twiggy dominators -r 160` -> PASS
- `twiggy monos` -> PASS
