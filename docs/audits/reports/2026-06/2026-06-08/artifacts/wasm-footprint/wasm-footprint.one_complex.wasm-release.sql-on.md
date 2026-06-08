# Recurring Audit - Wasm Footprint (2026-06-08)

## Report Preamble

- scope: recurring wasm footprint audit for `one_complex` with profile `wasm-release` and SQL variant `sql-on`
- compared baseline report path: `docs/audits/reports/2026-06/2026-06-06/wasm-footprint.md`
- code snapshot identifier: `e2a9534b3`
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
| icp-built `.wasm` | 2,649,374 | 2,595,849 | -53,525 |
| icp-built deterministic `.wasm.gz` | 852,454 | 834,753 | -17,701 |
| icp-shrunk `.wasm` | 2,467,165 | 2,417,833 | -49,332 |
| icp-shrunk `.wasm.gz` | 809,131 | 793,199 | -15,932 |

## Structural Snapshot (ic-wasm)

| Metric | icp-built | icp-shrunk |
| --- | ---: | ---: |
| Function count | 5,795 | 5,795 |
| Callback count | 1 | 1 |
| Data section count | 3 | 3 |
| Data section bytes | 184,292 | 184,292 |
| Exported methods | 3 | 3 |

## Twiggy Top Offenders (Shallow Size)

| Rank | Item | Shallow Bytes | Shallow % |
| ---: | --- | ---: | ---: |
| 1 | data[0] | 184,026 | 7.61% |
| 2 | code[0] | 33,635 | 1.39% |
| 3 | code[1] | 32,386 | 1.34% |
| 4 | code[2] | 27,593 | 1.14% |
| 5 | code[3] | 20,378 | 0.84% |
| 6 | code[4] | 17,792 | 0.74% |
| 7 | code[5] | 15,294 | 0.63% |
| 8 | code[7] | 14,258 | 0.59% |
| 9 | code[6] | 14,213 | 0.59% |
| 10 | code[8] | 13,140 | 0.54% |

## Twiggy Retained Hotspots

| Rank | Item | Retained Bytes | Retained % |
| ---: | --- | ---: | ---: |
| 1 | export "canister_query query_one_complex_fluent" | 652,592 | 26.99% |
| 2 | code[5773] | 652,549 | 26.99% |
| 3 | code[13] | 652,540 | 26.99% |
| 4 | code[10] | 368,003 | 15.22% |
| 5 | code[401] | 213,550 | 8.83% |
| 6 | code[26] | 212,407 | 8.79% |
| 7 | code[25] | 192,058 | 7.94% |
| 8 | data[0] | 184,026 | 7.61% |
| 9 | code[32] | 180,118 | 7.45% |
| 10 | code[77] | 169,007 | 6.99% |

## Artifacts

- `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.one_complex.wasm-release.sql-on.size-report.json`
- `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.one_complex.wasm-release.sql-on.size-summary.md`
- `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.one_complex.wasm-release.sql-on.twiggy-top.txt`
- `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.one_complex.wasm-release.sql-on.twiggy-top.csv`
- `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.one_complex.wasm-release.sql-on.twiggy-dominators.txt`
- `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.one_complex.wasm-release.sql-on.twiggy-retained.csv`
- `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.one_complex.wasm-release.sql-on.twiggy-monos.txt`

## Follow-Up Actions

- No follow-up actions required for this run.

## Verification Readout

- `bash scripts/ci/wasm-size-report.sh` -> PASS
- `twiggy top -n 40` -> PASS
- `twiggy top --retained -n 40` -> PASS
- `twiggy dominators -r 160` -> PASS
- `twiggy monos` -> PASS
