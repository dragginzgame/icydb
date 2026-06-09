# Recurring Audit - Wasm Footprint (2026-06-09)

## Report Preamble

- scope: recurring wasm footprint audit for `one_simple` with profile `wasm-release` and SQL variant `sql-on`
- compared baseline report path: `docs/audits/reports/2026-06/2026-06-08/wasm-footprint.md`
- code snapshot identifier: `83dc6bcad`
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
| icp-built `.wasm` | 2,551,082 | 2,500,666 | -50,416 |
| icp-built deterministic `.wasm.gz` | 819,561 | 805,778 | -13,783 |
| icp-shrunk `.wasm` | 2,376,231 | 2,328,689 | -47,542 |
| icp-shrunk `.wasm.gz` | 779,048 | 764,167 | -14,881 |

## Structural Snapshot (ic-wasm)

| Metric | icp-built | icp-shrunk |
| --- | ---: | ---: |
| Function count | 5,665 | 5,665 |
| Callback count | 1 | 1 |
| Data section count | 3 | 3 |
| Data section bytes | 167,300 | 167,300 |
| Exported methods | 2 | 2 |

## Twiggy Top Offenders (Shallow Size)

| Rank | Item | Shallow Bytes | Shallow % |
| ---: | --- | ---: | ---: |
| 1 | data[0] | 167,034 | 7.17% |
| 2 | code[0] | 33,635 | 1.44% |
| 3 | code[1] | 32,386 | 1.39% |
| 4 | code[2] | 27,593 | 1.18% |
| 5 | code[3] | 20,378 | 0.88% |
| 6 | code[4] | 17,855 | 0.77% |
| 7 | code[5] | 15,294 | 0.66% |
| 8 | code[7] | 14,257 | 0.61% |
| 9 | code[6] | 14,213 | 0.61% |
| 10 | code[8] | 13,053 | 0.56% |

## Twiggy Retained Hotspots

| Rank | Item | Retained Bytes | Retained % |
| ---: | --- | ---: | ---: |
| 1 | export "canister_query query_one_simple_fluent" | 632,244 | 27.15% |
| 2 | code[5644] | 632,202 | 27.15% |
| 3 | code[13] | 632,193 | 27.15% |
| 4 | code[9] | 368,119 | 15.81% |
| 5 | code[390] | 213,550 | 9.17% |
| 6 | code[23] | 212,407 | 9.12% |
| 7 | code[22] | 192,058 | 8.25% |
| 8 | code[30] | 180,116 | 7.73% |
| 9 | table[0] | 172,840 | 7.42% |
| 10 | elem[0] | 172,834 | 7.42% |

## Artifacts

- `docs/audits/reports/2026-06/2026-06-09/artifacts/wasm-footprint/wasm-footprint.one_simple.wasm-release.sql-on.size-report.json`
- `docs/audits/reports/2026-06/2026-06-09/artifacts/wasm-footprint/wasm-footprint.one_simple.wasm-release.sql-on.size-summary.md`
- `docs/audits/reports/2026-06/2026-06-09/artifacts/wasm-footprint/wasm-footprint.one_simple.wasm-release.sql-on.twiggy-top.txt`
- `docs/audits/reports/2026-06/2026-06-09/artifacts/wasm-footprint/wasm-footprint.one_simple.wasm-release.sql-on.twiggy-top.csv`
- `docs/audits/reports/2026-06/2026-06-09/artifacts/wasm-footprint/wasm-footprint.one_simple.wasm-release.sql-on.twiggy-dominators.txt`
- `docs/audits/reports/2026-06/2026-06-09/artifacts/wasm-footprint/wasm-footprint.one_simple.wasm-release.sql-on.twiggy-retained.csv`
- `docs/audits/reports/2026-06/2026-06-09/artifacts/wasm-footprint/wasm-footprint.one_simple.wasm-release.sql-on.twiggy-monos.txt`

## Follow-Up Actions

- No follow-up actions required for this run.

## Verification Readout

- `bash scripts/ci/wasm-size-report.sh` -> PASS
- `twiggy top -n 40` -> PASS
- `twiggy top --retained -n 40` -> PASS
- `twiggy dominators -r 160` -> PASS
- `twiggy monos` -> PASS
