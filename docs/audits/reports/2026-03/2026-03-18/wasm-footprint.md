# Recurring Audit - Wasm Footprint (2026-03-18)

## Report Preamble

- scope: recurring wasm footprint audit for `minimal` with profile `wasm-release` and SQL variant `sql-on`
- compared baseline report path: `docs/audits/reports/2026-03/2026-03-15/wasm-footprint-2.md`
- code snapshot identifier: `d22618f7`
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
| dfx-built `.wasm` | 1,500,576 | 1,664,527 | +163,951 |
| dfx-built deterministic `.wasm.gz` | 522,240 | 575,807 | +53,567 |
| dfx-shrunk `.wasm` | 1,403,634 | 1,554,923 | +151,289 |
| dfx-shrunk `.wasm.gz` | 489,046 | 538,235 | +49,189 |

## Structural Snapshot (ic-wasm)

| Metric | dfx-built | dfx-shrunk |
| --- | ---: | ---: |
| Function count | 3,895 | 3,895 |
| Callback count | 1 | 1 |
| Data section count | 2 | 2 |
| Data section bytes | 185,460 | 185,460 |
| Exported methods | 7 | 7 |

## Twiggy Top Offenders (Shallow Size)

| Rank | Item | Shallow Bytes | Shallow % |
| ---: | --- | ---: | ---: |
| 1 | data[0] | 185,326 | 11.92% |
| 2 | code[0] | 22,099 | 1.42% |
| 3 | code[1] | 21,434 | 1.38% |
| 4 | code[2] | 20,242 | 1.30% |
| 5 | code[3] | 17,848 | 1.15% |
| 6 | code[4] | 14,877 | 0.96% |
| 7 | code[5] | 13,664 | 0.88% |
| 8 | code[6] | 13,479 | 0.87% |
| 9 | code[7] | 12,974 | 0.83% |
| 10 | code[8] | 12,249 | 0.79% |

## Twiggy Retained Hotspots

| Rank | Item | Retained Bytes | Retained % |
| ---: | --- | ---: | ---: |
| 1 | table[0] | 702,047 | 45.15% |
| 2 | elem[0] | 702,041 | 45.15% |
| 3 | code[1] | 210,733 | 13.55% |
| 4 | data[0] | 185,326 | 11.92% |
| 5 | code[111] | 76,894 | 4.95% |
| 6 | code[379] | 74,504 | 4.79% |
| 7 | code[16] | 73,832 | 4.75% |
| 8 | code[52] | 52,275 | 3.36% |
| 9 | code[10] | 51,905 | 3.34% |
| 10 | code[789] | 44,210 | 2.84% |

## Artifacts

- `docs/audits/reports/2026-03/2026-03-18/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.size-report.json`
- `docs/audits/reports/2026-03/2026-03-18/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.size-summary.md`
- `docs/audits/reports/2026-03/2026-03-18/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.twiggy-top.txt`
- `docs/audits/reports/2026-03/2026-03-18/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.twiggy-top.csv`
- `docs/audits/reports/2026-03/2026-03-18/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.twiggy-dominators.txt`
- `docs/audits/reports/2026-03/2026-03-18/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.twiggy-retained.csv`
- `docs/audits/reports/2026-03/2026-03-18/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.twiggy-monos.txt`

## Follow-Up Actions

- No follow-up actions required for this run.

## Verification Readout

- `bash scripts/ci/wasm-size-report.sh` -> PASS
- `twiggy top -n 40` -> PASS
- `twiggy top --retained -n 40` -> PASS
- `twiggy dominators -r 160` -> PASS
- `twiggy monos` -> PASS
