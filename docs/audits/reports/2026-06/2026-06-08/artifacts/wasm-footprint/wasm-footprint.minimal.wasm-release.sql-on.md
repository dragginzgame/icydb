# Recurring Audit - Wasm Footprint (2026-06-08)

## Report Preamble

- scope: recurring wasm footprint audit for `minimal` with profile `wasm-release` and SQL variant `sql-on`
- compared baseline report path: `docs/audits/reports/2026-06/2026-06-06/wasm-footprint.md`
- code snapshot identifier: `4ce80891e`
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
| icp-built `.wasm` | 370,058 | 312,605 | -57,453 |
| icp-built deterministic `.wasm.gz` | 132,701 | 116,529 | -16,172 |
| icp-shrunk `.wasm` | 339,468 | 287,347 | -52,121 |
| icp-shrunk `.wasm.gz` | 125,278 | 110,605 | -14,673 |

## Structural Snapshot (ic-wasm)

| Metric | icp-built | icp-shrunk |
| --- | ---: | ---: |
| Function count | 967 | 967 |
| Callback count | 1 | 1 |
| Data section count | 3 | 3 |
| Data section bytes | 35,600 | 35,600 |
| Exported methods | 1 | 1 |

## Twiggy Top Offenders (Shallow Size)

| Rank | Item | Shallow Bytes | Shallow % |
| ---: | --- | ---: | ---: |
| 1 | data[0] | 35,538 | 12.37% |
| 2 | code[0] | 22,629 | 7.88% |
| 3 | code[1] | 16,077 | 5.59% |
| 4 | code[2] | 8,587 | 2.99% |
| 5 | code[3] | 5,390 | 1.88% |
| 6 | code[4] | 4,793 | 1.67% |
| 7 | code[5] | 4,582 | 1.59% |
| 8 | code[6] | 4,446 | 1.55% |
| 9 | code[7] | 4,044 | 1.41% |
| 10 | code[8] | 3,303 | 1.15% |

## Twiggy Retained Hotspots

| Rank | Item | Retained Bytes | Retained % |
| ---: | --- | ---: | ---: |
| 1 | export "canister_query __icydb_metrics" | 129,942 | 45.22% |
| 2 | code[954] | 129,908 | 45.21% |
| 3 | code[0] | 125,346 | 43.62% |
| 4 | table[0] | 91,222 | 31.75% |
| 5 | elem[0] | 91,216 | 31.74% |
| 6 | code[1] | 38,586 | 13.43% |
| 7 | data[0] | 35,538 | 12.37% |
| 8 | code[14] | 17,267 | 6.01% |
| 9 | code[2] | 12,643 | 4.40% |
| 10 | code[28] | 11,925 | 4.15% |

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
