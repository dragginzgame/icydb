# Recurring Audit - Wasm Footprint (2026-03-24)

## Report Preamble

- scope: recurring wasm footprint audit for `minimal` with profile `debug` and SQL variant `sql-on`
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
| dfx-built `.wasm` | N/A | 3,351,502 | N/A |
| dfx-built deterministic `.wasm.gz` | N/A | 902,658 | N/A |
| dfx-shrunk `.wasm` | N/A | 2,719,250 | N/A |
| dfx-shrunk `.wasm.gz` | N/A | 821,706 | N/A |

## Structural Snapshot (ic-wasm)

| Metric | dfx-built | dfx-shrunk |
| --- | ---: | ---: |
| Function count | 4,515 | 4,515 |
| Callback count | 1 | 1 |
| Data section count | 2 | 2 |
| Data section bytes | 359,312 | 359,312 |
| Exported methods | 7 | 7 |

## Twiggy Top Offenders (Shallow Size)

| Rank | Item | Shallow Bytes | Shallow % |
| ---: | --- | ---: | ---: |
| 1 | data[0] | 359,182 | 13.21% |
| 2 | code[0] | 18,665 | 0.69% |
| 3 | code[2] | 11,824 | 0.43% |
| 4 | code[3] | 10,639 | 0.39% |
| 5 | code[4] | 10,208 | 0.38% |
| 6 | code[5] | 8,981 | 0.33% |
| 7 | code[7] | 8,880 | 0.33% |
| 8 | code[6] | 8,653 | 0.32% |
| 9 | code[1] | 8,276 | 0.30% |
| 10 | code[9] | 8,152 | 0.30% |

## Twiggy Retained Hotspots

| Rank | Item | Retained Bytes | Retained % |
| ---: | --- | ---: | ---: |
| 1 | export "canister_query query" | 613,300 | 22.55% |
| 2 | code[4417] | 613,276 | 22.55% |
| 3 | code[4424] | 613,266 | 22.55% |
| 4 | code[432] | 613,259 | 22.55% |
| 5 | code[119] | 594,193 | 21.85% |
| 6 | table[0] | 382,232 | 14.06% |
| 7 | elem[0] | 382,226 | 14.06% |
| 8 | data[0] | 359,182 | 13.21% |
| 9 | code[363] | 195,457 | 7.19% |
| 10 | code[7] | 186,298 | 6.85% |

## Artifacts

- `docs/audits/reports/2026-03/2026-03-24/artifacts/wasm-footprint/wasm-footprint.minimal.debug.sql-on.size-report.json`
- `docs/audits/reports/2026-03/2026-03-24/artifacts/wasm-footprint/wasm-footprint.minimal.debug.sql-on.size-summary.md`
- `docs/audits/reports/2026-03/2026-03-24/artifacts/wasm-footprint/wasm-footprint.minimal.debug.sql-on.twiggy-top.txt`
- `docs/audits/reports/2026-03/2026-03-24/artifacts/wasm-footprint/wasm-footprint.minimal.debug.sql-on.twiggy-top.csv`
- `docs/audits/reports/2026-03/2026-03-24/artifacts/wasm-footprint/wasm-footprint.minimal.debug.sql-on.twiggy-dominators.txt`
- `docs/audits/reports/2026-03/2026-03-24/artifacts/wasm-footprint/wasm-footprint.minimal.debug.sql-on.twiggy-retained.csv`
- `docs/audits/reports/2026-03/2026-03-24/artifacts/wasm-footprint/wasm-footprint.minimal.debug.sql-on.twiggy-monos.txt`

## Follow-Up Actions

- owner boundary: `wasm-audit history`; action: preserve size artifacts for baseline reports so trend deltas remain comparable; target report date/run: next `wasm-footprint` run.

## Verification Readout

- `bash scripts/ci/wasm-size-report.sh` -> PASS
- `twiggy top -n 40` -> PASS
- `twiggy top --retained -n 40` -> PASS
- `twiggy dominators -r 160` -> PASS
- `twiggy monos` -> PASS
