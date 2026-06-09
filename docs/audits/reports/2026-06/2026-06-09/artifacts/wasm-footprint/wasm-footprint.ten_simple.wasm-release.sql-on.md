# Recurring Audit - Wasm Footprint (2026-06-09)

## Report Preamble

- scope: recurring wasm footprint audit for `ten_simple` with profile `wasm-release` and SQL variant `sql-on`
- compared baseline report path: `docs/audits/reports/2026-06/2026-06-08/wasm-footprint.md`
- code snapshot identifier: `a43cb9272`
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
| icp-built `.wasm` | 2,576,302 | 2,524,544 | -51,758 |
| icp-built deterministic `.wasm.gz` | 821,445 | 808,444 | -13,001 |
| icp-shrunk `.wasm` | 2,399,770 | 2,350,872 | -48,898 |
| icp-shrunk `.wasm.gz` | 781,843 | 767,016 | -14,827 |

## Structural Snapshot (ic-wasm)

| Metric | icp-built | icp-shrunk |
| --- | ---: | ---: |
| Function count | 5,686 | 5,686 |
| Callback count | 1 | 1 |
| Data section count | 3 | 3 |
| Data section bytes | 167,788 | 167,788 |
| Exported methods | 2 | 2 |

## Twiggy Top Offenders (Shallow Size)

| Rank | Item | Shallow Bytes | Shallow % |
| ---: | --- | ---: | ---: |
| 1 | data[0] | 167,522 | 7.13% |
| 2 | code[0] | 33,635 | 1.43% |
| 3 | code[1] | 32,386 | 1.38% |
| 4 | code[2] | 27,593 | 1.17% |
| 5 | code[3] | 20,378 | 0.87% |
| 6 | code[4] | 17,840 | 0.76% |
| 7 | code[5] | 15,294 | 0.65% |
| 8 | code[7] | 14,257 | 0.61% |
| 9 | code[6] | 14,213 | 0.60% |
| 10 | code[8] | 13,053 | 0.56% |

## Twiggy Retained Hotspots

| Rank | Item | Retained Bytes | Retained % |
| ---: | --- | ---: | ---: |
| 1 | export "canister_query query_ten_simple_fluent" | 632,210 | 26.89% |
| 2 | code[5665] | 632,168 | 26.89% |
| 3 | code[13] | 632,159 | 26.89% |
| 4 | code[9] | 368,119 | 15.66% |
| 5 | code[400] | 213,550 | 9.08% |
| 6 | code[23] | 212,407 | 9.04% |
| 7 | table[0] | 194,480 | 8.27% |
| 8 | elem[0] | 194,474 | 8.27% |
| 9 | code[22] | 192,058 | 8.17% |
| 10 | code[30] | 180,116 | 7.66% |

## Artifacts

- `docs/audits/reports/2026-06/2026-06-09/artifacts/wasm-footprint/wasm-footprint.ten_simple.wasm-release.sql-on.size-report.json`
- `docs/audits/reports/2026-06/2026-06-09/artifacts/wasm-footprint/wasm-footprint.ten_simple.wasm-release.sql-on.size-summary.md`
- `docs/audits/reports/2026-06/2026-06-09/artifacts/wasm-footprint/wasm-footprint.ten_simple.wasm-release.sql-on.twiggy-top.txt`
- `docs/audits/reports/2026-06/2026-06-09/artifacts/wasm-footprint/wasm-footprint.ten_simple.wasm-release.sql-on.twiggy-top.csv`
- `docs/audits/reports/2026-06/2026-06-09/artifacts/wasm-footprint/wasm-footprint.ten_simple.wasm-release.sql-on.twiggy-dominators.txt`
- `docs/audits/reports/2026-06/2026-06-09/artifacts/wasm-footprint/wasm-footprint.ten_simple.wasm-release.sql-on.twiggy-retained.csv`
- `docs/audits/reports/2026-06/2026-06-09/artifacts/wasm-footprint/wasm-footprint.ten_simple.wasm-release.sql-on.twiggy-monos.txt`

## Follow-Up Actions

- No follow-up actions required for this run.

## Verification Readout

- `bash scripts/ci/wasm-size-report.sh` -> PASS
- `twiggy top -n 40` -> PASS
- `twiggy top --retained -n 40` -> PASS
- `twiggy dominators -r 160` -> PASS
- `twiggy monos` -> PASS
