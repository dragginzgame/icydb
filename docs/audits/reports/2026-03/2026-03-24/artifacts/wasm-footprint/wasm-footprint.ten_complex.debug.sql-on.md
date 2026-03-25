# Recurring Audit - Wasm Footprint (2026-03-24)

## Report Preamble

- scope: recurring wasm footprint audit for `ten_complex` with profile `debug` and SQL variant `sql-on`
- compared baseline report path: `docs/audits/reports/2026-03/2026-03-20/wasm-footprint.md`
- code snapshot identifier: `5a1d34bd`
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
| dfx-built `.wasm` | N/A | 3,620,345 | N/A |
| dfx-built deterministic `.wasm.gz` | N/A | 966,646 | N/A |
| dfx-shrunk `.wasm` | N/A | 2,935,278 | N/A |
| dfx-shrunk `.wasm.gz` | N/A | 881,459 | N/A |

## Structural Snapshot (ic-wasm)

| Metric | dfx-built | dfx-shrunk |
| --- | ---: | ---: |
| Function count | 4,823 | 4,823 |
| Callback count | 1 | 1 |
| Data section count | 2 | 2 |
| Data section bytes | 382,912 | 382,912 |
| Exported methods | 7 | 7 |

## Twiggy Top Offenders (Shallow Size)

| Rank | Item | Shallow Bytes | Shallow % |
| ---: | --- | ---: | ---: |
| 1 | data[0] | 382,782 | 13.04% |
| 2 | code[0] | 18,665 | 0.64% |
| 3 | code[2] | 11,824 | 0.40% |
| 4 | code[3] | 10,639 | 0.36% |
| 5 | code[4] | 10,208 | 0.35% |
| 6 | code[5] | 8,981 | 0.31% |
| 7 | code[6] | 8,653 | 0.29% |
| 8 | code[1] | 8,276 | 0.28% |
| 9 | code[8] | 8,152 | 0.28% |
| 10 | code[9] | 7,872 | 0.27% |

## Twiggy Retained Hotspots

| Rank | Item | Retained Bytes | Retained % |
| ---: | --- | ---: | ---: |
| 1 | export "canister_query query" | 578,105 | 19.70% |
| 2 | code[4725] | 578,081 | 19.69% |
| 3 | code[4734] | 578,071 | 19.69% |
| 4 | code[507] | 578,064 | 19.69% |
| 5 | table[0] | 578,020 | 19.69% |
| 6 | elem[0] | 578,014 | 19.69% |
| 7 | code[2651] | 560,800 | 19.11% |
| 8 | code[131] | 560,560 | 19.10% |
| 9 | data[0] | 382,782 | 13.04% |
| 10 | code[402] | 165,619 | 5.64% |

## Artifacts

- `docs/audits/reports/2026-03/2026-03-24/artifacts/wasm-footprint/wasm-footprint.ten_complex.debug.sql-on.size-report.json`
- `docs/audits/reports/2026-03/2026-03-24/artifacts/wasm-footprint/wasm-footprint.ten_complex.debug.sql-on.size-summary.md`
- `docs/audits/reports/2026-03/2026-03-24/artifacts/wasm-footprint/wasm-footprint.ten_complex.debug.sql-on.twiggy-top.txt`
- `docs/audits/reports/2026-03/2026-03-24/artifacts/wasm-footprint/wasm-footprint.ten_complex.debug.sql-on.twiggy-top.csv`
- `docs/audits/reports/2026-03/2026-03-24/artifacts/wasm-footprint/wasm-footprint.ten_complex.debug.sql-on.twiggy-dominators.txt`
- `docs/audits/reports/2026-03/2026-03-24/artifacts/wasm-footprint/wasm-footprint.ten_complex.debug.sql-on.twiggy-retained.csv`
- `docs/audits/reports/2026-03/2026-03-24/artifacts/wasm-footprint/wasm-footprint.ten_complex.debug.sql-on.twiggy-monos.txt`

## Follow-Up Actions

- owner boundary: `wasm-audit history`; action: preserve size artifacts for baseline reports so trend deltas remain comparable; target report date/run: next `wasm-footprint` run.

## Verification Readout

- `bash scripts/ci/wasm-size-report.sh` -> PASS
- `twiggy top -n 40` -> PASS
- `twiggy top --retained -n 40` -> PASS
- `twiggy dominators -r 160` -> PASS
- `twiggy monos` -> PASS
