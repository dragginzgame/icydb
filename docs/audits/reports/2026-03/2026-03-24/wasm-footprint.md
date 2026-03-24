# Recurring Audit - Wasm Footprint (2026-03-24)

## Report Preamble

- scope: recurring wasm footprint audit for `minimal` with profile `wasm-release` and SQL variant `sql-on`
- compared baseline report path: `docs/audits/reports/2026-03/2026-03-20/wasm-footprint-4.md`
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
| dfx-built `.wasm` | N/A | 1,386,180 | N/A |
| dfx-built deterministic `.wasm.gz` | N/A | 507,623 | N/A |
| dfx-shrunk `.wasm` | N/A | 1,293,185 | N/A |
| dfx-shrunk `.wasm.gz` | N/A | 475,641 | N/A |

## Structural Snapshot (ic-wasm)

| Metric | dfx-built | dfx-shrunk |
| --- | ---: | ---: |
| Function count | 3,331 | 3,331 |
| Callback count | 1 | 1 |
| Data section count | 2 | 2 |
| Data section bytes | 184,940 | 184,940 |
| Exported methods | 7 | 7 |

## Twiggy Top Offenders (Shallow Size)

| Rank | Item | Shallow Bytes | Shallow % |
| ---: | --- | ---: | ---: |
| 1 | data[0] | 184,806 | 14.29% |
| 2 | code[0] | 45,803 | 3.54% |
| 3 | code[1] | 25,350 | 1.96% |
| 4 | code[2] | 17,845 | 1.38% |
| 5 | code[3] | 13,962 | 1.08% |
| 6 | code[5] | 12,393 | 0.96% |
| 7 | code[4] | 12,291 | 0.95% |
| 8 | code[6] | 10,371 | 0.80% |
| 9 | code[8] | 9,431 | 0.73% |
| 10 | code[7] | 8,984 | 0.69% |

## Twiggy Retained Hotspots

| Rank | Item | Retained Bytes | Retained % |
| ---: | --- | ---: | ---: |
| 1 | export "canister_query query" | 282,720 | 21.86% |
| 2 | code[3292] | 282,696 | 21.86% |
| 3 | code[459] | 282,686 | 21.86% |
| 4 | code[1] | 282,202 | 21.82% |
| 5 | data[0] | 184,806 | 14.29% |
| 6 | table[0] | 169,004 | 13.07% |
| 7 | elem[0] | 168,998 | 13.07% |
| 8 | code[0] | 85,433 | 6.61% |
| 9 | code[6] | 49,042 | 3.79% |
| 10 | code[2] | 33,327 | 2.58% |

## Artifacts

- `docs/audits/reports/2026-03/2026-03-24/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.size-report.json`
- `docs/audits/reports/2026-03/2026-03-24/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.size-summary.md`
- `docs/audits/reports/2026-03/2026-03-24/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.twiggy-top.txt`
- `docs/audits/reports/2026-03/2026-03-24/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.twiggy-top.csv`
- `docs/audits/reports/2026-03/2026-03-24/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.twiggy-dominators.txt`
- `docs/audits/reports/2026-03/2026-03-24/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.twiggy-retained.csv`
- `docs/audits/reports/2026-03/2026-03-24/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.twiggy-monos.txt`

## Follow-Up Actions

- owner boundary: `wasm-audit history`; action: preserve size artifacts for baseline reports so trend deltas remain comparable; target report date/run: next `wasm-footprint` run.

## Verification Readout

- `bash scripts/ci/wasm-size-report.sh` -> PASS
- `twiggy top -n 40` -> PASS
- `twiggy top --retained -n 40` -> PASS
- `twiggy dominators -r 160` -> PASS
- `twiggy monos` -> PASS
