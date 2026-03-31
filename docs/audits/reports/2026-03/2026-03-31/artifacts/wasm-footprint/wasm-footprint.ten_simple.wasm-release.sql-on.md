# Recurring Audit - Wasm Footprint (2026-03-31)

## Report Preamble

- scope: recurring wasm footprint audit for `ten_simple` with profile `wasm-release` and SQL variant `sql-on`
- compared baseline report path: `docs/audits/reports/2026-03/2026-03-26/wasm-footprint.md`
- code snapshot identifier: `1356b3bc`
- method tag/version: `WASM-1.0`
- comparability status: `non-comparable (baseline report exists but baseline size artifact is missing)`

## Checklist Results

| Requirement | Status | Evidence |
| --- | --- | --- |
| Wasm size artifacts captured | PASS | size report + summary artifacts written |
| Twiggy top breakdown generated | PASS | top text/csv artifacts written |
| Twiggy dominator breakdown generated | PASS | dominator text artifact written |
| Twiggy monomorphization breakdown generated | PASS | 0 ┊          0.00% ┊     0 ┊ 0.00% ┊ Σ [0 Total Rows] |
| Baseline delta availability | PARTIAL | baseline artifact missing at expected scoped artifacts path |

PASS=4, PARTIAL=1, FAIL=0

## Size Snapshot

| Metric | Previous | Current | Delta |
| --- | ---: | ---: | ---: |
| dfx-built `.wasm` | N/A | 1,362,740 | N/A |
| dfx-built deterministic `.wasm.gz` | N/A | 492,261 | N/A |
| dfx-shrunk `.wasm` | N/A | 1,271,399 | N/A |
| dfx-shrunk `.wasm.gz` | N/A | 470,988 | N/A |

## Structural Snapshot (ic-wasm)

| Metric | dfx-built | dfx-shrunk |
| --- | ---: | ---: |
| Function count | 3,284 | 3,284 |
| Callback count | 1 | 1 |
| Data section count | 2 | 2 |
| Data section bytes | 174,836 | 174,836 |
| Exported methods | 7 | 7 |

## Twiggy Top Offenders (Shallow Size)

| Rank | Item | Shallow Bytes | Shallow % |
| ---: | --- | ---: | ---: |
| 1 | data[0] | 174,686 | 13.74% |
| 2 | code[0] | 23,041 | 1.81% |
| 3 | code[1] | 18,251 | 1.44% |
| 4 | code[2] | 17,844 | 1.40% |
| 5 | code[3] | 15,167 | 1.19% |
| 6 | code[4] | 14,508 | 1.14% |
| 7 | code[6] | 11,238 | 0.88% |
| 8 | code[7] | 10,831 | 0.85% |
| 9 | code[8] | 10,302 | 0.81% |
| 10 | code[9] | 9,784 | 0.77% |

## Twiggy Retained Hotspots

| Rank | Item | Retained Bytes | Retained % |
| ---: | --- | ---: | ---: |
| 1 | table[0] | 303,154 | 23.84% |
| 2 | elem[0] | 303,148 | 23.84% |
| 3 | export "canister_query query" | 196,161 | 15.43% |
| 4 | code[3245] | 196,137 | 15.43% |
| 5 | code[4] | 196,128 | 15.43% |
| 6 | data[0] | 174,686 | 13.74% |
| 7 | code[0] | 83,568 | 6.57% |
| 8 | code[31] | 68,456 | 5.38% |
| 9 | code[1] | 56,852 | 4.47% |
| 10 | code[12] | 40,888 | 3.22% |

## Artifacts

- `docs/audits/reports/2026-03/2026-03-31/artifacts/wasm-footprint/wasm-footprint.ten_simple.wasm-release.sql-on.size-report.json`
- `docs/audits/reports/2026-03/2026-03-31/artifacts/wasm-footprint/wasm-footprint.ten_simple.wasm-release.sql-on.size-summary.md`
- `docs/audits/reports/2026-03/2026-03-31/artifacts/wasm-footprint/wasm-footprint.ten_simple.wasm-release.sql-on.twiggy-top.txt`
- `docs/audits/reports/2026-03/2026-03-31/artifacts/wasm-footprint/wasm-footprint.ten_simple.wasm-release.sql-on.twiggy-top.csv`
- `docs/audits/reports/2026-03/2026-03-31/artifacts/wasm-footprint/wasm-footprint.ten_simple.wasm-release.sql-on.twiggy-dominators.txt`
- `docs/audits/reports/2026-03/2026-03-31/artifacts/wasm-footprint/wasm-footprint.ten_simple.wasm-release.sql-on.twiggy-retained.csv`
- `docs/audits/reports/2026-03/2026-03-31/artifacts/wasm-footprint/wasm-footprint.ten_simple.wasm-release.sql-on.twiggy-monos.txt`

## Follow-Up Actions

- owner boundary: `wasm-audit history`; action: preserve size artifacts for baseline reports so trend deltas remain comparable; target report date/run: next `wasm-footprint` run.

## Verification Readout

- `bash scripts/ci/wasm-size-report.sh` -> PASS
- `twiggy top -n 40` -> PASS
- `twiggy top --retained -n 40` -> PASS
- `twiggy dominators -r 160` -> PASS
- `twiggy monos` -> PASS
