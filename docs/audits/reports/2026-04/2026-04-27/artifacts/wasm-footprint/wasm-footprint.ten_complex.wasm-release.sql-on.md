# Recurring Audit - Wasm Footprint (2026-04-27)

## Report Preamble

- scope: recurring wasm footprint audit for `ten_complex` with profile `wasm-release` and SQL variant `sql-on`
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
| dfx-built `.wasm` | 911,478 | 919,680 | +8,202 |
| dfx-built deterministic `.wasm.gz` | 317,651 | 321,900 | +4,249 |
| dfx-shrunk `.wasm` | 848,027 | 855,516 | +7,489 |
| dfx-shrunk `.wasm.gz` | 302,446 | 305,813 | +3,367 |

## Structural Snapshot (ic-wasm)

| Metric | dfx-built | dfx-shrunk |
| --- | ---: | ---: |
| Function count | 2,333 | 2,333 |
| Callback count | 1 | 1 |
| Data section count | 2 | 2 |
| Data section bytes | 136,424 | 136,424 |
| Exported methods | 6 | 6 |

## Twiggy Top Offenders (Shallow Size)

| Rank | Item | Shallow Bytes | Shallow % |
| ---: | --- | ---: | ---: |
| 1 | data[0] | 136,274 | 15.93% |
| 2 | code[0] | 24,867 | 2.91% |
| 3 | code[1] | 22,798 | 2.66% |
| 4 | code[2] | 16,005 | 1.87% |
| 5 | code[3] | 14,244 | 1.66% |
| 6 | code[4] | 10,791 | 1.26% |
| 7 | code[5] | 8,626 | 1.01% |
| 8 | code[6] | 5,518 | 0.64% |
| 9 | code[9] | 4,803 | 0.56% |
| 10 | code[8] | 4,757 | 0.56% |

## Twiggy Retained Hotspots

| Rank | Item | Retained Bytes | Retained % |
| ---: | --- | ---: | ---: |
| 1 | table[0] | 289,687 | 33.86% |
| 2 | elem[0] | 289,681 | 33.86% |
| 3 | data[0] | 136,274 | 15.93% |
| 4 | export "canister_query icydb_snapshot" | 95,091 | 11.12% |
| 5 | code[2295] | 95,058 | 11.11% |
| 6 | code[16] | 95,049 | 11.11% |
| 7 | code[0] | 89,431 | 10.45% |
| 8 | export "canister_query icydb_metrics" | 85,555 | 10.00% |
| 9 | code[2294] | 85,523 | 10.00% |
| 10 | code[3] | 85,514 | 10.00% |

## Artifacts

- `docs/audits/reports/2026-04/2026-04-27/artifacts/wasm-footprint/wasm-footprint.ten_complex.wasm-release.sql-on.size-report.json`
- `docs/audits/reports/2026-04/2026-04-27/artifacts/wasm-footprint/wasm-footprint.ten_complex.wasm-release.sql-on.size-summary.md`
- `docs/audits/reports/2026-04/2026-04-27/artifacts/wasm-footprint/wasm-footprint.ten_complex.wasm-release.sql-on.twiggy-top.txt`
- `docs/audits/reports/2026-04/2026-04-27/artifacts/wasm-footprint/wasm-footprint.ten_complex.wasm-release.sql-on.twiggy-top.csv`
- `docs/audits/reports/2026-04/2026-04-27/artifacts/wasm-footprint/wasm-footprint.ten_complex.wasm-release.sql-on.twiggy-dominators.txt`
- `docs/audits/reports/2026-04/2026-04-27/artifacts/wasm-footprint/wasm-footprint.ten_complex.wasm-release.sql-on.twiggy-retained.csv`
- `docs/audits/reports/2026-04/2026-04-27/artifacts/wasm-footprint/wasm-footprint.ten_complex.wasm-release.sql-on.twiggy-monos.txt`

## Follow-Up Actions

- No follow-up actions required for this run.

## Verification Readout

- `bash scripts/ci/wasm-size-report.sh` -> PASS
- `twiggy top -n 40` -> PASS
- `twiggy top --retained -n 40` -> PASS
- `twiggy dominators -r 160` -> PASS
- `twiggy monos` -> PASS
