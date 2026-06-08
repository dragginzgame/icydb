# Recurring Audit - Wasm Footprint (2026-06-08)

## Report Preamble

- scope: recurring wasm footprint audit for `one_simple` with profile `wasm-release` and SQL variant `sql-on`
- compared baseline report path: `docs/audits/reports/2026-06/2026-06-06/wasm-footprint.md`
- code snapshot identifier: `16b63b730`
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
| icp-built `.wasm` | 2,623,214 | 2,581,894 | -41,320 |
| icp-built deterministic `.wasm.gz` | 843,777 | 836,224 | -7,553 |
| icp-shrunk `.wasm` | 2,442,434 | 2,405,339 | -37,095 |
| icp-shrunk `.wasm.gz` | 801,233 | 792,823 | -8,410 |

## Structural Snapshot (ic-wasm)

| Metric | icp-built | icp-shrunk |
| --- | ---: | ---: |
| Function count | 5,755 | 5,755 |
| Callback count | 1 | 1 |
| Data section count | 3 | 3 |
| Data section bytes | 181,156 | 181,156 |
| Exported methods | 3 | 3 |

## Twiggy Top Offenders (Shallow Size)

| Rank | Item | Shallow Bytes | Shallow % |
| ---: | --- | ---: | ---: |
| 1 | data[0] | 180,890 | 7.52% |
| 2 | code[0] | 34,152 | 1.42% |
| 3 | code[1] | 31,464 | 1.31% |
| 4 | code[2] | 27,593 | 1.15% |
| 5 | code[3] | 19,951 | 0.83% |
| 6 | code[4] | 17,727 | 0.74% |
| 7 | code[5] | 15,294 | 0.64% |
| 8 | code[7] | 14,370 | 0.60% |
| 9 | code[6] | 14,134 | 0.59% |
| 10 | code[8] | 13,335 | 0.55% |

## Twiggy Retained Hotspots

| Rank | Item | Retained Bytes | Retained % |
| ---: | --- | ---: | ---: |
| 1 | export "canister_query query_one_simple_fluent" | 640,162 | 26.61% |
| 2 | code[5733] | 640,120 | 26.61% |
| 3 | code[13] | 640,111 | 26.61% |
| 4 | code[11] | 368,119 | 15.30% |
| 5 | code[399] | 213,550 | 8.88% |
| 6 | code[26] | 212,407 | 8.83% |
| 7 | code[24] | 192,058 | 7.98% |
| 8 | code[25] | 181,271 | 7.54% |
| 9 | data[0] | 180,890 | 7.52% |
| 10 | code[77] | 169,009 | 7.03% |

## Artifacts

- `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.one_simple.wasm-release.sql-on.size-report.json`
- `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.one_simple.wasm-release.sql-on.size-summary.md`
- `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.one_simple.wasm-release.sql-on.twiggy-top.txt`
- `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.one_simple.wasm-release.sql-on.twiggy-top.csv`
- `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.one_simple.wasm-release.sql-on.twiggy-dominators.txt`
- `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.one_simple.wasm-release.sql-on.twiggy-retained.csv`
- `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.one_simple.wasm-release.sql-on.twiggy-monos.txt`

## Follow-Up Actions

- No follow-up actions required for this run.

## Verification Readout

- `bash scripts/ci/wasm-size-report.sh` -> PASS
- `twiggy top -n 40` -> PASS
- `twiggy top --retained -n 40` -> PASS
- `twiggy dominators -r 160` -> PASS
- `twiggy monos` -> PASS
