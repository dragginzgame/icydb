# Recurring Audit - Wasm Footprint (2026-03-15)

## Report Preamble

- scope: recurring wasm footprint audit for `minimal` with profile `wasm-release`
- compared baseline report path: `docs/audits/reports/2026-03/2026-03-15/wasm-footprint.md`
- code snapshot identifier: `29026378`
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
| dfx-built `.wasm` | 1,500,533 | 1,500,576 | +43 |
| dfx-built deterministic `.wasm.gz` | 521,857 | 522,240 | +383 |
| dfx-shrunk `.wasm` | 1,403,593 | 1,403,634 | +41 |
| dfx-shrunk `.wasm.gz` | 488,906 | 489,046 | +140 |

## Structural Snapshot (ic-wasm)

| Metric | dfx-built | dfx-shrunk |
| --- | ---: | ---: |
| Function count | 3,529 | 3,529 |
| Callback count | 1 | 1 |
| Data section count | 2 | 2 |
| Data section bytes | 176,472 | 176,472 |
| Exported methods | 7 | 7 |

## Twiggy Top Offenders (Shallow Size)

| Rank | Item | Shallow Bytes | Shallow % |
| ---: | --- | ---: | ---: |
| 1 | data[0] | 176,338 | 12.56% |
| 2 | code[1] | 22,007 | 1.57% |
| 3 | code[0] | 21,558 | 1.54% |
| 4 | code[2] | 18,807 | 1.34% |
| 5 | code[3] | 17,057 | 1.22% |
| 6 | code[4] | 15,890 | 1.13% |
| 7 | code[5] | 11,893 | 0.85% |
| 8 | code[7] | 10,878 | 0.77% |
| 9 | code[6] | 10,757 | 0.77% |
| 10 | code[8] | 10,492 | 0.75% |

## Twiggy Retained Hotspots

| Rank | Item | Retained Bytes | Retained % |
| ---: | --- | ---: | ---: |
| 1 | export "canister_query query" | 419,776 | 29.91% |
| 2 | code[3491] | 419,752 | 29.90% |
| 3 | code[79] | 419,743 | 29.90% |
| 4 | code[0] | 416,282 | 29.66% |
| 5 | code[47] | 181,981 | 12.96% |
| 6 | data[0] | 176,338 | 12.56% |
| 7 | code[44] | 152,167 | 10.84% |
| 8 | code[3] | 150,552 | 10.73% |
| 9 | code[1] | 140,196 | 9.99% |
| 10 | table[0] | 138,250 | 9.85% |

## Artifacts

- `docs/audits/reports/2026-03/2026-03-15/artifacts/wasm-footprint/wasm-footprint-2.minimal.wasm-release.size-report.json`
- `docs/audits/reports/2026-03/2026-03-15/artifacts/wasm-footprint/wasm-footprint-2.minimal.wasm-release.size-summary.md`
- `docs/audits/reports/2026-03/2026-03-15/artifacts/wasm-footprint/wasm-footprint-2.minimal.wasm-release.twiggy-top.txt`
- `docs/audits/reports/2026-03/2026-03-15/artifacts/wasm-footprint/wasm-footprint-2.minimal.wasm-release.twiggy-top.csv`
- `docs/audits/reports/2026-03/2026-03-15/artifacts/wasm-footprint/wasm-footprint-2.minimal.wasm-release.twiggy-dominators.txt`
- `docs/audits/reports/2026-03/2026-03-15/artifacts/wasm-footprint/wasm-footprint-2.minimal.wasm-release.twiggy-retained.csv`
- `docs/audits/reports/2026-03/2026-03-15/artifacts/wasm-footprint/wasm-footprint-2.minimal.wasm-release.twiggy-monos.txt`

## Follow-Up Actions

- No follow-up actions required for this run.

## Verification Readout

- `bash scripts/ci/wasm-size-report.sh` -> PASS
- `twiggy top -n 40` -> PASS
- `twiggy top --retained -n 40` -> PASS
- `twiggy dominators -r 160` -> PASS
- `twiggy monos` -> PASS
