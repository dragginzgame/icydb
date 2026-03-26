# Recurring Audit - Wasm Footprint (2026-03-26)

## Report Preamble

- scope: recurring wasm footprint audit for `minimal` with profile `wasm-release` and SQL variant `sql-on`
- compared baseline report path: `docs/audits/reports/2026-03/2026-03-24/wasm-footprint.md`
- code snapshot identifier: `16c600ba`
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
| dfx-built `.wasm` | 1,386,180 | 1,253,076 | -133,104 |
| dfx-built deterministic `.wasm.gz` | 507,623 | 460,468 | -47,155 |
| dfx-shrunk `.wasm` | 1,293,185 | 1,168,588 | -124,597 |
| dfx-shrunk `.wasm.gz` | 475,641 | 437,145 | -38,496 |

## Structural Snapshot (ic-wasm)

| Metric | dfx-built | dfx-shrunk |
| --- | ---: | ---: |
| Function count | 3,064 | 3,064 |
| Callback count | 1 | 1 |
| Data section count | 2 | 2 |
| Data section bytes | 180,320 | 180,320 |
| Exported methods | 7 | 7 |

## Twiggy Top Offenders (Shallow Size)

| Rank | Item | Shallow Bytes | Shallow % |
| ---: | --- | ---: | ---: |
| 1 | data[0] | 180,186 | 15.42% |
| 2 | code[0] | 18,659 | 1.60% |
| 3 | code[1] | 17,848 | 1.53% |
| 4 | code[2] | 15,564 | 1.33% |
| 5 | code[3] | 14,466 | 1.24% |
| 6 | code[5] | 10,358 | 0.89% |
| 7 | code[7] | 9,457 | 0.81% |
| 8 | code[6] | 8,977 | 0.77% |
| 9 | code[9] | 8,584 | 0.73% |
| 10 | code[8] | 8,572 | 0.73% |

## Twiggy Retained Hotspots

| Rank | Item | Retained Bytes | Retained % |
| ---: | --- | ---: | ---: |
| 1 | export "canister_query query" | 219,513 | 18.78% |
| 2 | code[3025] | 219,489 | 18.78% |
| 3 | code[3] | 219,480 | 18.78% |
| 4 | data[0] | 180,186 | 15.42% |
| 5 | table[0] | 167,977 | 14.37% |
| 6 | elem[0] | 167,971 | 14.37% |
| 7 | code[28] | 71,894 | 6.15% |
| 8 | code[0] | 60,288 | 5.16% |
| 9 | code[2] | 48,014 | 4.11% |
| 10 | code[6] | 40,724 | 3.48% |

## Artifacts

- `docs/audits/reports/2026-03/2026-03-26/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.size-report.json`
- `docs/audits/reports/2026-03/2026-03-26/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.size-summary.md`
- `docs/audits/reports/2026-03/2026-03-26/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.twiggy-top.txt`
- `docs/audits/reports/2026-03/2026-03-26/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.twiggy-top.csv`
- `docs/audits/reports/2026-03/2026-03-26/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.twiggy-dominators.txt`
- `docs/audits/reports/2026-03/2026-03-26/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.twiggy-retained.csv`
- `docs/audits/reports/2026-03/2026-03-26/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.twiggy-monos.txt`

## Follow-Up Actions

- No follow-up actions required for this run.

## Verification Readout

- `bash scripts/ci/wasm-size-report.sh` -> PASS
- `twiggy top -n 40` -> PASS
- `twiggy top --retained -n 40` -> PASS
- `twiggy dominators -r 160` -> PASS
- `twiggy monos` -> PASS
