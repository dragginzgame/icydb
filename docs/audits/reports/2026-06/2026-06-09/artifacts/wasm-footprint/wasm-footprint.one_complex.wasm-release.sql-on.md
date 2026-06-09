# Recurring Audit - Wasm Footprint (2026-06-09)

## Report Preamble

- scope: recurring wasm footprint audit for `one_complex` with profile `wasm-release` and SQL variant `sql-on`
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
| icp-built `.wasm` | 2,574,928 | 2,545,920 | -29,008 |
| icp-built deterministic `.wasm.gz` | 827,504 | 820,673 | -6,831 |
| icp-shrunk `.wasm` | 2,398,649 | 2,371,151 | -27,498 |
| icp-shrunk `.wasm.gz` | 786,254 | 778,220 | -8,034 |

## Structural Snapshot (ic-wasm)

| Metric | icp-built | icp-shrunk |
| --- | ---: | ---: |
| Function count | 5,725 | 5,725 |
| Callback count | 1 | 1 |
| Data section count | 3 | 3 |
| Data section bytes | 178,268 | 178,268 |
| Exported methods | 2 | 2 |

## Twiggy Top Offenders (Shallow Size)

| Rank | Item | Shallow Bytes | Shallow % |
| ---: | --- | ---: | ---: |
| 1 | data[0] | 178,002 | 7.51% |
| 2 | code[0] | 33,635 | 1.42% |
| 3 | code[1] | 32,386 | 1.37% |
| 4 | code[2] | 27,593 | 1.16% |
| 5 | code[3] | 20,378 | 0.86% |
| 6 | code[4] | 17,788 | 0.75% |
| 7 | code[5] | 15,294 | 0.65% |
| 8 | code[7] | 14,258 | 0.60% |
| 9 | code[6] | 14,213 | 0.60% |
| 10 | code[8] | 13,140 | 0.55% |

## Twiggy Retained Hotspots

| Rank | Item | Retained Bytes | Retained % |
| ---: | --- | ---: | ---: |
| 1 | export "canister_query query_one_complex_fluent" | 651,496 | 27.48% |
| 2 | code[5704] | 651,453 | 27.47% |
| 3 | code[13] | 651,444 | 27.47% |
| 4 | code[10] | 368,003 | 15.52% |
| 5 | code[394] | 213,550 | 9.01% |
| 6 | code[25] | 212,407 | 8.96% |
| 7 | code[24] | 192,058 | 8.10% |
| 8 | code[32] | 180,117 | 7.60% |
| 9 | data[0] | 178,002 | 7.51% |
| 10 | table[0] | 174,755 | 7.37% |

## Artifacts

- `docs/audits/reports/2026-06/2026-06-09/artifacts/wasm-footprint/wasm-footprint.one_complex.wasm-release.sql-on.size-report.json`
- `docs/audits/reports/2026-06/2026-06-09/artifacts/wasm-footprint/wasm-footprint.one_complex.wasm-release.sql-on.size-summary.md`
- `docs/audits/reports/2026-06/2026-06-09/artifacts/wasm-footprint/wasm-footprint.one_complex.wasm-release.sql-on.twiggy-top.txt`
- `docs/audits/reports/2026-06/2026-06-09/artifacts/wasm-footprint/wasm-footprint.one_complex.wasm-release.sql-on.twiggy-top.csv`
- `docs/audits/reports/2026-06/2026-06-09/artifacts/wasm-footprint/wasm-footprint.one_complex.wasm-release.sql-on.twiggy-dominators.txt`
- `docs/audits/reports/2026-06/2026-06-09/artifacts/wasm-footprint/wasm-footprint.one_complex.wasm-release.sql-on.twiggy-retained.csv`
- `docs/audits/reports/2026-06/2026-06-09/artifacts/wasm-footprint/wasm-footprint.one_complex.wasm-release.sql-on.twiggy-monos.txt`

## Follow-Up Actions

- No follow-up actions required for this run.

## Verification Readout

- `bash scripts/ci/wasm-size-report.sh` -> PASS
- `twiggy top -n 40` -> PASS
- `twiggy top --retained -n 40` -> PASS
- `twiggy dominators -r 160` -> PASS
- `twiggy monos` -> PASS
