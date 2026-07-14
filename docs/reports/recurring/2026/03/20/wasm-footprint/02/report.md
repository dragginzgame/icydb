# Recurring Audit - Wasm Footprint (2026-03-20)

## Report Preamble

- scope: recurring wasm footprint audit for `twenty` with profile `wasm-release` and SQL variant `sql-on`
- compared baseline report path: `docs/audits/reports/2026-03/2026-03-20/wasm-footprint.md`
- code snapshot identifier: `d8f08504`
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
| dfx-built `.wasm` | N/A | 3,589,973 | N/A |
| dfx-built deterministic `.wasm.gz` | N/A | 690,718 | N/A |
| dfx-shrunk `.wasm` | N/A | 3,380,740 | N/A |
| dfx-shrunk `.wasm.gz` | N/A | 630,604 | N/A |

## Structural Snapshot (ic-wasm)

| Metric | dfx-built | dfx-shrunk |
| --- | ---: | ---: |
| Function count | 5,695 | 5,695 |
| Callback count | 1 | 1 |
| Data section count | 2 | 2 |
| Data section bytes | 192,244 | 192,244 |
| Exported methods | 7 | 7 |

## Twiggy Top Offenders (Shallow Size)

| Rank | Item | Shallow Bytes | Shallow % |
| ---: | --- | ---: | ---: |
| 1 | data[0] | 192,110 | 5.68% |
| 2 | code[0] | 20,291 | 0.60% |
| 3 | code[1] | 17,848 | 0.53% |
| 4 | code[2] | 16,040 | 0.47% |
| 5 | code[23] | 14,900 | 0.44% |
| 6 | code[3] | 14,884 | 0.44% |
| 7 | code[4] | 14,884 | 0.44% |
| 8 | code[5] | 14,884 | 0.44% |
| 9 | code[6] | 14,884 | 0.44% |
| 10 | code[7] | 14,884 | 0.44% |

## Twiggy Retained Hotspots

| Rank | Item | Retained Bytes | Retained % |
| ---: | --- | ---: | ---: |
| 1 | table[0] | 2,519,015 | 74.51% |
| 2 | elem[0] | 2,519,009 | 74.51% |
| 3 | code[329] | 228,416 | 6.76% |
| 4 | code[570] | 226,413 | 6.70% |
| 5 | code[0] | 225,254 | 6.66% |
| 6 | data[0] | 192,110 | 5.68% |
| 7 | code[283] | 76,819 | 2.27% |
| 8 | code[775] | 74,465 | 2.20% |
| 9 | code[55] | 73,793 | 2.18% |
| 10 | code[30] | 49,168 | 1.45% |

## Artifacts

- `docs/audits/reports/2026-03/2026-03-20/artifacts/wasm-footprint/wasm-footprint-2.twenty.wasm-release.sql-on.size-report.json`
- `docs/audits/reports/2026-03/2026-03-20/artifacts/wasm-footprint/wasm-footprint-2.twenty.wasm-release.sql-on.size-summary.md`
- `docs/audits/reports/2026-03/2026-03-20/artifacts/wasm-footprint/wasm-footprint-2.twenty.wasm-release.sql-on.twiggy-top.txt`
- `docs/audits/reports/2026-03/2026-03-20/artifacts/wasm-footprint/wasm-footprint-2.twenty.wasm-release.sql-on.twiggy-top.csv`
- `docs/audits/reports/2026-03/2026-03-20/artifacts/wasm-footprint/wasm-footprint-2.twenty.wasm-release.sql-on.twiggy-dominators.txt`
- `docs/audits/reports/2026-03/2026-03-20/artifacts/wasm-footprint/wasm-footprint-2.twenty.wasm-release.sql-on.twiggy-retained.csv`
- `docs/audits/reports/2026-03/2026-03-20/artifacts/wasm-footprint/wasm-footprint-2.twenty.wasm-release.sql-on.twiggy-monos.txt`

## Follow-Up Actions

- owner boundary: `wasm-audit history`; action: preserve size artifacts for baseline reports so trend deltas remain comparable; target report date/run: next `wasm-footprint` run.

## Verification Readout

- `bash scripts/ci/wasm-size-report.sh` -> PASS
- `twiggy top -n 40` -> PASS
- `twiggy top --retained -n 40` -> PASS
- `twiggy dominators -r 160` -> PASS
- `twiggy monos` -> PASS
