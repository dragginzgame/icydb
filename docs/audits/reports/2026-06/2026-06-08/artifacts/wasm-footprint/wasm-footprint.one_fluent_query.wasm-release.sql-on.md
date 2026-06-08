# Recurring Audit - Wasm Footprint (2026-06-08)

## Report Preamble

- scope: recurring wasm footprint audit for `one_fluent_query` with profile `wasm-release` and SQL variant `sql-on`
- compared baseline report path: `docs/audits/reports/2026-06/2026-06-06/wasm-footprint.md`
- code snapshot identifier: `1043f2d10`
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
| icp-built `.wasm` | 2,564,390 | 2,546,592 | -17,798 |
| icp-built deterministic `.wasm.gz` | 828,531 | 819,374 | -9,157 |
| icp-shrunk `.wasm` | 2,389,137 | 2,372,150 | -16,987 |
| icp-shrunk `.wasm.gz` | 786,522 | 777,113 | -9,409 |

## Structural Snapshot (ic-wasm)

| Metric | icp-built | icp-shrunk |
| --- | ---: | ---: |
| Function count | 5,685 | 5,685 |
| Callback count | 1 | 1 |
| Data section count | 3 | 3 |
| Data section bytes | 180,716 | 180,716 |
| Exported methods | 2 | 2 |

## Twiggy Top Offenders (Shallow Size)

| Rank | Item | Shallow Bytes | Shallow % |
| ---: | --- | ---: | ---: |
| 1 | data[0] | 180,450 | 7.61% |
| 2 | code[0] | 33,635 | 1.42% |
| 3 | code[1] | 32,386 | 1.37% |
| 4 | code[2] | 27,593 | 1.16% |
| 5 | code[3] | 20,379 | 0.86% |
| 6 | code[4] | 17,859 | 0.75% |
| 7 | code[5] | 17,202 | 0.73% |
| 8 | code[6] | 15,294 | 0.64% |
| 9 | code[9] | 14,613 | 0.62% |
| 10 | code[8] | 14,255 | 0.60% |

## Twiggy Retained Hotspots

| Rank | Item | Retained Bytes | Retained % |
| ---: | --- | ---: | ---: |
| 1 | export "canister_query query_one_fluent" | 639,032 | 26.94% |
| 2 | code[5665] | 638,997 | 26.94% |
| 3 | code[9] | 638,988 | 26.94% |
| 4 | code[12] | 368,119 | 15.52% |
| 5 | code[5] | 255,226 | 10.76% |
| 6 | code[391] | 213,550 | 9.00% |
| 7 | code[26] | 212,407 | 8.95% |
| 8 | code[25] | 192,058 | 8.10% |
| 9 | data[0] | 180,450 | 7.61% |
| 10 | code[74] | 170,674 | 7.19% |

## Artifacts

- `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.one_fluent_query.wasm-release.sql-on.size-report.json`
- `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.one_fluent_query.wasm-release.sql-on.size-summary.md`
- `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.one_fluent_query.wasm-release.sql-on.twiggy-top.txt`
- `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.one_fluent_query.wasm-release.sql-on.twiggy-top.csv`
- `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.one_fluent_query.wasm-release.sql-on.twiggy-dominators.txt`
- `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.one_fluent_query.wasm-release.sql-on.twiggy-retained.csv`
- `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.one_fluent_query.wasm-release.sql-on.twiggy-monos.txt`

## Follow-Up Actions

- No follow-up actions required for this run.

## Verification Readout

- `bash scripts/ci/wasm-size-report.sh` -> PASS
- `twiggy top -n 40` -> PASS
- `twiggy top --retained -n 40` -> PASS
- `twiggy dominators -r 160` -> PASS
- `twiggy monos` -> PASS
