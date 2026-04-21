# Recurring Audit - Wasm Footprint (2026-04-22)

## Report Preamble

- scope: recurring wasm footprint audit for `ten_complex` with profile `wasm-release` and SQL variant `sql-on`
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
| dfx-built `.wasm` | 952,699 | 911,478 | -41,221 |
| dfx-built deterministic `.wasm.gz` | 329,512 | 317,651 | -11,861 |
| dfx-shrunk `.wasm` | 888,897 | 848,027 | -40,870 |
| dfx-shrunk `.wasm.gz` | 313,253 | 302,446 | -10,807 |

## Structural Snapshot (ic-wasm)

| Metric | dfx-built | dfx-shrunk |
| --- | ---: | ---: |
| Function count | 2,302 | 2,302 |
| Callback count | 1 | 1 |
| Data section count | 2 | 2 |
| Data section bytes | 135,680 | 135,680 |
| Exported methods | 6 | 6 |

## Twiggy Top Offenders (Shallow Size)

| Rank | Item | Shallow Bytes | Shallow % |
| ---: | --- | ---: | ---: |
| 1 | data[0] | 135,530 | 15.98% |
| 2 | code[0] | 24,286 | 2.86% |
| 3 | code[1] | 20,561 | 2.42% |
| 4 | code[2] | 16,005 | 1.89% |
| 5 | code[3] | 14,282 | 1.68% |
| 6 | code[4] | 12,054 | 1.42% |
| 7 | code[5] | 8,626 | 1.02% |
| 8 | code[6] | 5,518 | 0.65% |
| 9 | code[9] | 4,803 | 0.57% |
| 10 | code[8] | 4,718 | 0.56% |

## Twiggy Retained Hotspots

| Rank | Item | Retained Bytes | Retained % |
| ---: | --- | ---: | ---: |
| 1 | table[0] | 285,815 | 33.70% |
| 2 | elem[0] | 285,809 | 33.70% |
| 3 | data[0] | 135,530 | 15.98% |
| 4 | export "canister_query icydb_snapshot" | 92,724 | 10.93% |
| 5 | code[2264] | 92,691 | 10.93% |
| 6 | code[16] | 92,682 | 10.93% |
| 7 | code[0] | 87,064 | 10.27% |
| 8 | export "canister_query icydb_metrics" | 85,595 | 10.09% |
| 9 | code[2263] | 85,563 | 10.09% |
| 10 | code[3] | 85,554 | 10.09% |

## Artifacts

- `docs/audits/reports/2026-04/2026-04-22/artifacts/wasm-footprint/wasm-footprint.ten_complex.wasm-release.sql-on.size-report.json`
- `docs/audits/reports/2026-04/2026-04-22/artifacts/wasm-footprint/wasm-footprint.ten_complex.wasm-release.sql-on.size-summary.md`
- `docs/audits/reports/2026-04/2026-04-22/artifacts/wasm-footprint/wasm-footprint.ten_complex.wasm-release.sql-on.twiggy-top.txt`
- `docs/audits/reports/2026-04/2026-04-22/artifacts/wasm-footprint/wasm-footprint.ten_complex.wasm-release.sql-on.twiggy-top.csv`
- `docs/audits/reports/2026-04/2026-04-22/artifacts/wasm-footprint/wasm-footprint.ten_complex.wasm-release.sql-on.twiggy-dominators.txt`
- `docs/audits/reports/2026-04/2026-04-22/artifacts/wasm-footprint/wasm-footprint.ten_complex.wasm-release.sql-on.twiggy-retained.csv`
- `docs/audits/reports/2026-04/2026-04-22/artifacts/wasm-footprint/wasm-footprint.ten_complex.wasm-release.sql-on.twiggy-monos.txt`

## Follow-Up Actions

- No follow-up actions required for this run.

## Verification Readout

- `bash scripts/ci/wasm-size-report.sh` -> PASS
- `twiggy top -n 40` -> PASS
- `twiggy top --retained -n 40` -> PASS
- `twiggy dominators -r 160` -> PASS
- `twiggy monos` -> PASS
