# Recurring Audit - Wasm Footprint (2026-06-09)

## Report Preamble

- scope: recurring wasm footprint audit for `ten_complex` with profile `wasm-release` and SQL variant `sql-on`
- compared baseline report path: `docs/audits/reports/2026-06/2026-06-08/wasm-footprint.md`
- code snapshot identifier: `c95398ed5`
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
| icp-built `.wasm` | 2,600,068 | 2,571,060 | -29,008 |
| icp-built deterministic `.wasm.gz` | 829,482 | 822,508 | -6,974 |
| icp-shrunk `.wasm` | 2,422,108 | 2,394,610 | -27,498 |
| icp-shrunk `.wasm.gz` | 788,861 | 781,128 | -7,733 |

## Structural Snapshot (ic-wasm)

| Metric | icp-built | icp-shrunk |
| --- | ---: | ---: |
| Function count | 5,746 | 5,746 |
| Callback count | 1 | 1 |
| Data section count | 3 | 3 |
| Data section bytes | 180,060 | 180,060 |
| Exported methods | 2 | 2 |

## Twiggy Top Offenders (Shallow Size)

| Rank | Item | Shallow Bytes | Shallow % |
| ---: | --- | ---: | ---: |
| 1 | data[0] | 179,794 | 7.51% |
| 2 | code[0] | 33,635 | 1.40% |
| 3 | code[1] | 32,386 | 1.35% |
| 4 | code[2] | 27,593 | 1.15% |
| 5 | code[3] | 20,378 | 0.85% |
| 6 | code[4] | 17,787 | 0.74% |
| 7 | code[5] | 15,294 | 0.64% |
| 8 | code[7] | 14,256 | 0.60% |
| 9 | code[6] | 14,213 | 0.59% |
| 10 | code[8] | 13,140 | 0.55% |

## Twiggy Retained Hotspots

| Rank | Item | Retained Bytes | Retained % |
| ---: | --- | ---: | ---: |
| 1 | export "canister_query query_ten_complex_fluent" | 651,486 | 27.21% |
| 2 | code[5725] | 651,443 | 27.20% |
| 3 | code[13] | 651,434 | 27.20% |
| 4 | code[10] | 368,003 | 15.37% |
| 5 | code[404] | 213,550 | 8.92% |
| 6 | code[25] | 212,407 | 8.87% |
| 7 | table[0] | 196,347 | 8.20% |
| 8 | elem[0] | 196,341 | 8.20% |
| 9 | code[24] | 192,058 | 8.02% |
| 10 | code[32] | 180,117 | 7.52% |

## Artifacts

- `docs/audits/reports/2026-06/2026-06-09/artifacts/wasm-footprint/wasm-footprint.ten_complex.wasm-release.sql-on.size-report.json`
- `docs/audits/reports/2026-06/2026-06-09/artifacts/wasm-footprint/wasm-footprint.ten_complex.wasm-release.sql-on.size-summary.md`
- `docs/audits/reports/2026-06/2026-06-09/artifacts/wasm-footprint/wasm-footprint.ten_complex.wasm-release.sql-on.twiggy-top.txt`
- `docs/audits/reports/2026-06/2026-06-09/artifacts/wasm-footprint/wasm-footprint.ten_complex.wasm-release.sql-on.twiggy-top.csv`
- `docs/audits/reports/2026-06/2026-06-09/artifacts/wasm-footprint/wasm-footprint.ten_complex.wasm-release.sql-on.twiggy-dominators.txt`
- `docs/audits/reports/2026-06/2026-06-09/artifacts/wasm-footprint/wasm-footprint.ten_complex.wasm-release.sql-on.twiggy-retained.csv`
- `docs/audits/reports/2026-06/2026-06-09/artifacts/wasm-footprint/wasm-footprint.ten_complex.wasm-release.sql-on.twiggy-monos.txt`

## Follow-Up Actions

- No follow-up actions required for this run.

## Verification Readout

- `bash scripts/ci/wasm-size-report.sh` -> PASS
- `twiggy top -n 40` -> PASS
- `twiggy top --retained -n 40` -> PASS
- `twiggy dominators -r 160` -> PASS
- `twiggy monos` -> PASS
