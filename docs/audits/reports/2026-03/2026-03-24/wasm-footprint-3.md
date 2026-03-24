# Recurring Audit - Wasm Footprint (2026-03-24)

## Report Preamble

- scope: recurring wasm footprint audit for `one_complex` with profile `wasm-release` and SQL variant `sql-on`
- compared baseline report path: `docs/audits/reports/2026-03/2026-03-24/wasm-footprint.md`
- code snapshot identifier: `3f453012`
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
| dfx-built `.wasm` | N/A | 1,483,380 | N/A |
| dfx-built deterministic `.wasm.gz` | N/A | 541,618 | N/A |
| dfx-shrunk `.wasm` | N/A | 1,384,746 | N/A |
| dfx-shrunk `.wasm.gz` | N/A | 507,891 | N/A |

## Structural Snapshot (ic-wasm)

| Metric | dfx-built | dfx-shrunk |
| --- | ---: | ---: |
| Function count | 3,528 | 3,528 |
| Callback count | 1 | 1 |
| Data section count | 2 | 2 |
| Data section bytes | 189,804 | 189,804 |
| Exported methods | 7 | 7 |

## Twiggy Top Offenders (Shallow Size)

| Rank | Item | Shallow Bytes | Shallow % |
| ---: | --- | ---: | ---: |
| 1 | data[0] | 189,670 | 13.70% |
| 2 | code[0] | 43,955 | 3.17% |
| 3 | code[1] | 25,465 | 1.84% |
| 4 | code[2] | 22,364 | 1.62% |
| 5 | code[3] | 17,848 | 1.29% |
| 6 | code[4] | 13,965 | 1.01% |
| 7 | code[5] | 12,393 | 0.89% |
| 8 | code[7] | 12,222 | 0.88% |
| 9 | code[6] | 12,180 | 0.88% |
| 10 | code[8] | 10,370 | 0.75% |

## Twiggy Retained Hotspots

| Rank | Item | Retained Bytes | Retained % |
| ---: | --- | ---: | ---: |
| 1 | export "canister_query query" | 263,879 | 19.06% |
| 2 | code[3489] | 263,855 | 19.05% |
| 3 | code[486] | 263,845 | 19.05% |
| 4 | code[1] | 263,361 | 19.02% |
| 5 | table[0] | 243,123 | 17.56% |
| 6 | elem[0] | 243,117 | 17.56% |
| 7 | data[0] | 189,670 | 13.70% |
| 8 | code[0] | 68,747 | 4.96% |
| 9 | code[2] | 54,662 | 3.95% |
| 10 | code[8] | 49,041 | 3.54% |

## Artifacts

- `docs/audits/reports/2026-03/2026-03-24/artifacts/wasm-footprint/wasm-footprint-3.one_complex.wasm-release.sql-on.size-report.json`
- `docs/audits/reports/2026-03/2026-03-24/artifacts/wasm-footprint/wasm-footprint-3.one_complex.wasm-release.sql-on.size-summary.md`
- `docs/audits/reports/2026-03/2026-03-24/artifacts/wasm-footprint/wasm-footprint-3.one_complex.wasm-release.sql-on.twiggy-top.txt`
- `docs/audits/reports/2026-03/2026-03-24/artifacts/wasm-footprint/wasm-footprint-3.one_complex.wasm-release.sql-on.twiggy-top.csv`
- `docs/audits/reports/2026-03/2026-03-24/artifacts/wasm-footprint/wasm-footprint-3.one_complex.wasm-release.sql-on.twiggy-dominators.txt`
- `docs/audits/reports/2026-03/2026-03-24/artifacts/wasm-footprint/wasm-footprint-3.one_complex.wasm-release.sql-on.twiggy-retained.csv`
- `docs/audits/reports/2026-03/2026-03-24/artifacts/wasm-footprint/wasm-footprint-3.one_complex.wasm-release.sql-on.twiggy-monos.txt`

## Follow-Up Actions

- owner boundary: `wasm-audit history`; action: preserve size artifacts for baseline reports so trend deltas remain comparable; target report date/run: next `wasm-footprint` run.

## Verification Readout

- `bash scripts/ci/wasm-size-report.sh` -> PASS
- `twiggy top -n 40` -> PASS
- `twiggy top --retained -n 40` -> PASS
- `twiggy dominators -r 160` -> PASS
- `twiggy monos` -> PASS
