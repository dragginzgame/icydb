# Recurring Audit - Wasm Footprint (2026-06-09)

## Report Preamble

- scope: recurring wasm footprint audit for `ten_simple` with profile `wasm-release` and SQL variant `sql-on`
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
| icp-built `.wasm` | 2,576,302 | 2,547,419 | -28,883 |
| icp-built deterministic `.wasm.gz` | 821,445 | 815,058 | -6,387 |
| icp-shrunk `.wasm` | 2,399,770 | 2,372,396 | -27,374 |
| icp-shrunk `.wasm.gz` | 781,843 | 773,757 | -8,086 |

## Structural Snapshot (ic-wasm)

| Metric | icp-built | icp-shrunk |
| --- | ---: | ---: |
| Function count | 5,707 | 5,707 |
| Callback count | 1 | 1 |
| Data section count | 3 | 3 |
| Data section bytes | 177,756 | 177,756 |
| Exported methods | 2 | 2 |

## Twiggy Top Offenders (Shallow Size)

| Rank | Item | Shallow Bytes | Shallow % |
| ---: | --- | ---: | ---: |
| 1 | data[0] | 177,490 | 7.48% |
| 2 | code[0] | 33,635 | 1.42% |
| 3 | code[1] | 32,386 | 1.37% |
| 4 | code[2] | 27,593 | 1.16% |
| 5 | code[3] | 20,378 | 0.86% |
| 6 | code[4] | 17,855 | 0.75% |
| 7 | code[5] | 15,294 | 0.64% |
| 8 | code[7] | 14,258 | 0.60% |
| 9 | code[6] | 14,213 | 0.60% |
| 10 | code[8] | 13,140 | 0.55% |

## Twiggy Retained Hotspots

| Rank | Item | Retained Bytes | Retained % |
| ---: | --- | ---: | ---: |
| 1 | export "canister_query query_ten_simple_fluent" | 634,925 | 26.76% |
| 2 | code[5686] | 634,883 | 26.76% |
| 3 | code[13] | 634,874 | 26.76% |
| 4 | code[10] | 368,119 | 15.52% |
| 5 | code[401] | 213,550 | 9.00% |
| 6 | code[24] | 212,407 | 8.95% |
| 7 | table[0] | 196,474 | 8.28% |
| 8 | elem[0] | 196,468 | 8.28% |
| 9 | code[23] | 192,058 | 8.10% |
| 10 | code[31] | 180,116 | 7.59% |

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
