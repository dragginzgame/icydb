# Recurring Audit - Wasm Footprint (2026-03-31)

## Report Preamble

- scope: recurring wasm footprint audit for `one_simple` with profile `wasm-release` and SQL variant `sql-on`
- compared baseline report path: `docs/audits/reports/2026-03/2026-03-26/wasm-footprint.md`
- code snapshot identifier: `1356b3bc`
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
| dfx-built `.wasm` | 1,400,600 | 1,344,522 | -56,078 |
| dfx-built deterministic `.wasm.gz` | 508,157 | 490,239 | -17,918 |
| dfx-shrunk `.wasm` | 1,307,229 | 1,254,475 | -52,754 |
| dfx-shrunk `.wasm.gz` | 482,146 | 469,274 | -12,872 |

## Structural Snapshot (ic-wasm)

| Metric | dfx-built | dfx-shrunk |
| --- | ---: | ---: |
| Function count | 3,251 | 3,251 |
| Callback count | 1 | 1 |
| Data section count | 2 | 2 |
| Data section bytes | 173,044 | 173,044 |
| Exported methods | 7 | 7 |

## Twiggy Top Offenders (Shallow Size)

| Rank | Item | Shallow Bytes | Shallow % |
| ---: | --- | ---: | ---: |
| 1 | data[0] | 172,894 | 13.78% |
| 2 | code[0] | 23,041 | 1.84% |
| 3 | code[1] | 18,251 | 1.45% |
| 4 | code[2] | 17,844 | 1.42% |
| 5 | code[3] | 15,167 | 1.21% |
| 6 | code[4] | 14,507 | 1.16% |
| 7 | code[6] | 11,238 | 0.90% |
| 8 | code[7] | 10,831 | 0.86% |
| 9 | code[8] | 10,302 | 0.82% |
| 10 | code[9] | 9,784 | 0.78% |

## Twiggy Retained Hotspots

| Rank | Item | Retained Bytes | Retained % |
| ---: | --- | ---: | ---: |
| 1 | table[0] | 289,160 | 23.05% |
| 2 | elem[0] | 289,154 | 23.05% |
| 3 | export "canister_query query" | 196,158 | 15.64% |
| 4 | code[3212] | 196,134 | 15.63% |
| 5 | code[4] | 196,125 | 15.63% |
| 6 | data[0] | 172,894 | 13.78% |
| 7 | code[1292] | 83,738 | 6.68% |
| 8 | code[0] | 83,568 | 6.66% |
| 9 | code[31] | 68,456 | 5.46% |
| 10 | code[1] | 56,852 | 4.53% |

## Artifacts

- `docs/audits/reports/2026-03/2026-03-31/artifacts/wasm-footprint/wasm-footprint.one_simple.wasm-release.sql-on.size-report.json`
- `docs/audits/reports/2026-03/2026-03-31/artifacts/wasm-footprint/wasm-footprint.one_simple.wasm-release.sql-on.size-summary.md`
- `docs/audits/reports/2026-03/2026-03-31/artifacts/wasm-footprint/wasm-footprint.one_simple.wasm-release.sql-on.twiggy-top.txt`
- `docs/audits/reports/2026-03/2026-03-31/artifacts/wasm-footprint/wasm-footprint.one_simple.wasm-release.sql-on.twiggy-top.csv`
- `docs/audits/reports/2026-03/2026-03-31/artifacts/wasm-footprint/wasm-footprint.one_simple.wasm-release.sql-on.twiggy-dominators.txt`
- `docs/audits/reports/2026-03/2026-03-31/artifacts/wasm-footprint/wasm-footprint.one_simple.wasm-release.sql-on.twiggy-retained.csv`
- `docs/audits/reports/2026-03/2026-03-31/artifacts/wasm-footprint/wasm-footprint.one_simple.wasm-release.sql-on.twiggy-monos.txt`

## Follow-Up Actions

- No follow-up actions required for this run.

## Verification Readout

- `bash scripts/ci/wasm-size-report.sh` -> PASS
- `twiggy top -n 40` -> PASS
- `twiggy top --retained -n 40` -> PASS
- `twiggy dominators -r 160` -> PASS
- `twiggy monos` -> PASS
