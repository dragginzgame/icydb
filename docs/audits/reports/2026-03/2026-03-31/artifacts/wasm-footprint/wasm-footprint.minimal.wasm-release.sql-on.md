# Recurring Audit - Wasm Footprint (2026-03-31)

## Report Preamble

- scope: recurring wasm footprint audit for `minimal` with profile `wasm-release` and SQL variant `sql-on`
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
| dfx-built `.wasm` | 1,253,076 | 1,207,667 | -45,409 |
| dfx-built deterministic `.wasm.gz` | 460,468 | 445,424 | -15,044 |
| dfx-shrunk `.wasm` | 1,168,588 | 1,125,935 | -42,653 |
| dfx-shrunk `.wasm.gz` | 437,145 | 425,708 | -11,437 |

## Structural Snapshot (ic-wasm)

| Metric | dfx-built | dfx-shrunk |
| --- | ---: | ---: |
| Function count | 3,004 | 3,004 |
| Callback count | 1 | 1 |
| Data section count | 2 | 2 |
| Data section bytes | 167,016 | 167,016 |
| Exported methods | 7 | 7 |

## Twiggy Top Offenders (Shallow Size)

| Rank | Item | Shallow Bytes | Shallow % |
| ---: | --- | ---: | ---: |
| 1 | data[0] | 166,866 | 14.82% |
| 2 | code[0] | 18,473 | 1.64% |
| 3 | code[1] | 17,844 | 1.58% |
| 4 | code[2] | 15,292 | 1.36% |
| 5 | code[3] | 14,279 | 1.27% |
| 6 | code[5] | 10,302 | 0.91% |
| 7 | code[6] | 9,750 | 0.87% |
| 8 | code[7] | 9,457 | 0.84% |
| 9 | code[8] | 8,809 | 0.78% |
| 10 | code[9] | 8,786 | 0.78% |

## Twiggy Retained Hotspots

| Rank | Item | Retained Bytes | Retained % |
| ---: | --- | ---: | ---: |
| 1 | export "canister_query query" | 210,369 | 18.68% |
| 2 | code[2965] | 210,345 | 18.68% |
| 3 | code[3] | 210,336 | 18.68% |
| 4 | table[0] | 168,746 | 14.99% |
| 5 | elem[0] | 168,740 | 14.99% |
| 6 | data[0] | 166,866 | 14.82% |
| 7 | code[26] | 71,767 | 6.37% |
| 8 | code[0] | 60,173 | 5.34% |
| 9 | code[2] | 47,906 | 4.25% |
| 10 | code[9] | 40,885 | 3.63% |

## Artifacts

- `docs/audits/reports/2026-03/2026-03-31/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.size-report.json`
- `docs/audits/reports/2026-03/2026-03-31/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.size-summary.md`
- `docs/audits/reports/2026-03/2026-03-31/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.twiggy-top.txt`
- `docs/audits/reports/2026-03/2026-03-31/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.twiggy-top.csv`
- `docs/audits/reports/2026-03/2026-03-31/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.twiggy-dominators.txt`
- `docs/audits/reports/2026-03/2026-03-31/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.twiggy-retained.csv`
- `docs/audits/reports/2026-03/2026-03-31/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.twiggy-monos.txt`

## Follow-Up Actions

- No follow-up actions required for this run.

## Verification Readout

- `bash scripts/ci/wasm-size-report.sh` -> PASS
- `twiggy top -n 40` -> PASS
- `twiggy top --retained -n 40` -> PASS
- `twiggy dominators -r 160` -> PASS
- `twiggy monos` -> PASS
