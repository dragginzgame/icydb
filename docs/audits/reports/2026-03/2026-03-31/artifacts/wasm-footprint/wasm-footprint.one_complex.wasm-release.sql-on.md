# Recurring Audit - Wasm Footprint (2026-03-31)

## Report Preamble

- scope: recurring wasm footprint audit for `one_complex` with profile `wasm-release` and SQL variant `sql-on`
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
| dfx-built `.wasm` | N/A | 1,346,964 | N/A |
| dfx-built deterministic `.wasm.gz` | N/A | 490,764 | N/A |
| dfx-shrunk `.wasm` | N/A | 1,256,836 | N/A |
| dfx-shrunk `.wasm.gz` | N/A | 469,842 | N/A |

## Structural Snapshot (ic-wasm)

| Metric | dfx-built | dfx-shrunk |
| --- | ---: | ---: |
| Function count | 3,254 | 3,254 |
| Callback count | 1 | 1 |
| Data section count | 2 | 2 |
| Data section bytes | 174,196 | 174,196 |
| Exported methods | 7 | 7 |

## Twiggy Top Offenders (Shallow Size)

| Rank | Item | Shallow Bytes | Shallow % |
| ---: | --- | ---: | ---: |
| 1 | data[0] | 174,046 | 13.85% |
| 2 | code[0] | 23,041 | 1.83% |
| 3 | code[1] | 18,251 | 1.45% |
| 4 | code[2] | 17,844 | 1.42% |
| 5 | code[3] | 15,167 | 1.21% |
| 6 | code[4] | 14,507 | 1.15% |
| 7 | code[6] | 11,238 | 0.89% |
| 8 | code[7] | 10,831 | 0.86% |
| 9 | code[8] | 10,302 | 0.82% |
| 10 | code[9] | 9,784 | 0.78% |

## Twiggy Retained Hotspots

| Rank | Item | Retained Bytes | Retained % |
| ---: | --- | ---: | ---: |
| 1 | table[0] | 289,234 | 23.01% |
| 2 | elem[0] | 289,228 | 23.01% |
| 3 | export "canister_query query" | 196,158 | 15.61% |
| 4 | code[3215] | 196,134 | 15.61% |
| 5 | code[4] | 196,125 | 15.60% |
| 6 | data[0] | 174,046 | 13.85% |
| 7 | code[1293] | 83,739 | 6.66% |
| 8 | code[0] | 83,568 | 6.65% |
| 9 | code[31] | 68,456 | 5.45% |
| 10 | code[1] | 56,852 | 4.52% |

## Artifacts

- `docs/audits/reports/2026-03/2026-03-31/artifacts/wasm-footprint/wasm-footprint.one_complex.wasm-release.sql-on.size-report.json`
- `docs/audits/reports/2026-03/2026-03-31/artifacts/wasm-footprint/wasm-footprint.one_complex.wasm-release.sql-on.size-summary.md`
- `docs/audits/reports/2026-03/2026-03-31/artifacts/wasm-footprint/wasm-footprint.one_complex.wasm-release.sql-on.twiggy-top.txt`
- `docs/audits/reports/2026-03/2026-03-31/artifacts/wasm-footprint/wasm-footprint.one_complex.wasm-release.sql-on.twiggy-top.csv`
- `docs/audits/reports/2026-03/2026-03-31/artifacts/wasm-footprint/wasm-footprint.one_complex.wasm-release.sql-on.twiggy-dominators.txt`
- `docs/audits/reports/2026-03/2026-03-31/artifacts/wasm-footprint/wasm-footprint.one_complex.wasm-release.sql-on.twiggy-retained.csv`
- `docs/audits/reports/2026-03/2026-03-31/artifacts/wasm-footprint/wasm-footprint.one_complex.wasm-release.sql-on.twiggy-monos.txt`

## Follow-Up Actions

- owner boundary: `wasm-audit history`; action: preserve size artifacts for baseline reports so trend deltas remain comparable; target report date/run: next `wasm-footprint` run.

## Verification Readout

- `bash scripts/ci/wasm-size-report.sh` -> PASS
- `twiggy top -n 40` -> PASS
- `twiggy top --retained -n 40` -> PASS
- `twiggy dominators -r 160` -> PASS
- `twiggy monos` -> PASS
