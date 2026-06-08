# Recurring Audit - Wasm Footprint (2026-06-08)

## Report Preamble

- scope: recurring wasm footprint audit for `minimal` with profile `wasm-release` and SQL variant `sql-on`
- compared baseline report path: `docs/audits/reports/2026-06/2026-06-06/wasm-footprint.md`
- code snapshot identifier: `16b63b730`
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
| icp-built `.wasm` | 370,058 | 328,729 | -41,329 |
| icp-built deterministic `.wasm.gz` | 132,701 | 122,771 | -9,930 |
| icp-shrunk `.wasm` | 339,468 | 302,135 | -37,333 |
| icp-shrunk `.wasm.gz` | 125,278 | 116,166 | -9,112 |

## Structural Snapshot (ic-wasm)

| Metric | icp-built | icp-shrunk |
| --- | ---: | ---: |
| Function count | 1,008 | 1,008 |
| Callback count | 1 | 1 |
| Data section count | 3 | 3 |
| Data section bytes | 35,728 | 35,728 |
| Exported methods | 2 | 2 |

## Twiggy Top Offenders (Shallow Size)

| Rank | Item | Shallow Bytes | Shallow % |
| ---: | --- | ---: | ---: |
| 1 | data[0] | 35,666 | 11.80% |
| 2 | code[0] | 19,246 | 6.37% |
| 3 | code[1] | 16,079 | 5.32% |
| 4 | code[2] | 8,588 | 2.84% |
| 5 | code[3] | 5,390 | 1.78% |
| 6 | code[4] | 4,793 | 1.59% |
| 7 | code[5] | 4,583 | 1.52% |
| 8 | code[6] | 4,425 | 1.46% |
| 9 | code[7] | 4,044 | 1.34% |
| 10 | code[8] | 3,303 | 1.09% |

## Twiggy Retained Hotspots

| Rank | Item | Retained Bytes | Retained % |
| ---: | --- | ---: | ---: |
| 1 | export "canister_query __icydb_metrics" | 105,089 | 34.78% |
| 2 | code[994] | 105,055 | 34.77% |
| 3 | code[0] | 105,046 | 34.77% |
| 4 | table[0] | 64,788 | 21.44% |
| 5 | elem[0] | 64,782 | 21.44% |
| 6 | code[1] | 37,104 | 12.28% |
| 7 | data[0] | 35,666 | 11.80% |
| 8 | code[18] | 17,267 | 5.71% |
| 9 | code[2] | 12,644 | 4.18% |
| 10 | export "get_candid_pointer" | 12,536 | 4.15% |

## Artifacts

- `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.size-report.json`
- `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.size-summary.md`
- `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.twiggy-top.txt`
- `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.twiggy-top.csv`
- `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.twiggy-dominators.txt`
- `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.twiggy-retained.csv`
- `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.twiggy-monos.txt`

## Follow-Up Actions

- No follow-up actions required for this run.

## Verification Readout

- `bash scripts/ci/wasm-size-report.sh` -> PASS
- `twiggy top -n 40` -> PASS
- `twiggy top --retained -n 40` -> PASS
- `twiggy dominators -r 160` -> PASS
- `twiggy monos` -> PASS
