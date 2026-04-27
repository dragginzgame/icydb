# Recurring Audit - Wasm Footprint (2026-04-27)

## Report Preamble

- scope: recurring wasm footprint audit for `one_simple` with profile `wasm-release` and SQL variant `sql-on`
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
| dfx-built `.wasm` | 894,532 | 903,953 | +9,421 |
| dfx-built deterministic `.wasm.gz` | 315,713 | 320,104 | +4,391 |
| dfx-shrunk `.wasm` | 832,381 | 841,039 | +8,658 |
| dfx-shrunk `.wasm.gz` | 300,012 | 303,771 | +3,759 |

## Structural Snapshot (ic-wasm)

| Metric | dfx-built | dfx-shrunk |
| --- | ---: | ---: |
| Function count | 2,307 | 2,307 |
| Callback count | 1 | 1 |
| Data section count | 2 | 2 |
| Data section bytes | 133,736 | 133,736 |
| Exported methods | 6 | 6 |

## Twiggy Top Offenders (Shallow Size)

| Rank | Item | Shallow Bytes | Shallow % |
| ---: | --- | ---: | ---: |
| 1 | data[0] | 133,586 | 15.88% |
| 2 | code[0] | 24,867 | 2.96% |
| 3 | code[1] | 22,798 | 2.71% |
| 4 | code[2] | 16,005 | 1.90% |
| 5 | code[3] | 14,244 | 1.69% |
| 6 | code[4] | 10,791 | 1.28% |
| 7 | code[5] | 8,626 | 1.03% |
| 8 | code[6] | 5,518 | 0.66% |
| 9 | code[9] | 5,449 | 0.65% |
| 10 | code[10] | 4,803 | 0.57% |

## Twiggy Retained Hotspots

| Rank | Item | Retained Bytes | Retained % |
| ---: | --- | ---: | ---: |
| 1 | table[0] | 277,899 | 33.04% |
| 2 | elem[0] | 277,893 | 33.04% |
| 3 | data[0] | 133,586 | 15.88% |
| 4 | export "canister_query icydb_snapshot" | 95,091 | 11.31% |
| 5 | code[2269] | 95,058 | 11.30% |
| 6 | code[17] | 95,049 | 11.30% |
| 7 | code[0] | 89,431 | 10.63% |
| 8 | export "canister_query icydb_metrics" | 85,555 | 10.17% |
| 9 | code[2268] | 85,523 | 10.17% |
| 10 | code[3] | 85,514 | 10.17% |

## Artifacts

- `docs/audits/reports/2026-04/2026-04-27/artifacts/wasm-footprint/wasm-footprint.one_simple.wasm-release.sql-on.size-report.json`
- `docs/audits/reports/2026-04/2026-04-27/artifacts/wasm-footprint/wasm-footprint.one_simple.wasm-release.sql-on.size-summary.md`
- `docs/audits/reports/2026-04/2026-04-27/artifacts/wasm-footprint/wasm-footprint.one_simple.wasm-release.sql-on.twiggy-top.txt`
- `docs/audits/reports/2026-04/2026-04-27/artifacts/wasm-footprint/wasm-footprint.one_simple.wasm-release.sql-on.twiggy-top.csv`
- `docs/audits/reports/2026-04/2026-04-27/artifacts/wasm-footprint/wasm-footprint.one_simple.wasm-release.sql-on.twiggy-dominators.txt`
- `docs/audits/reports/2026-04/2026-04-27/artifacts/wasm-footprint/wasm-footprint.one_simple.wasm-release.sql-on.twiggy-retained.csv`
- `docs/audits/reports/2026-04/2026-04-27/artifacts/wasm-footprint/wasm-footprint.one_simple.wasm-release.sql-on.twiggy-monos.txt`

## Follow-Up Actions

- No follow-up actions required for this run.

## Verification Readout

- `bash scripts/ci/wasm-size-report.sh` -> PASS
- `twiggy top -n 40` -> PASS
- `twiggy top --retained -n 40` -> PASS
- `twiggy dominators -r 160` -> PASS
- `twiggy monos` -> PASS
