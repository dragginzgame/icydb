# Recurring Audit - Wasm Footprint (2026-06-08)

## Report Preamble

- scope: recurring wasm footprint audit for `one_simple` with profile `wasm-release` and SQL variant `sql-on`
- compared baseline report path: `docs/audits/reports/2026-06/2026-06-06/wasm-footprint.md`
- code snapshot identifier: `1043f2d10`
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
| icp-built `.wasm` | 2,623,214 | 2,571,095 | -52,119 |
| icp-built deterministic `.wasm.gz` | 843,777 | 827,291 | -16,486 |
| icp-shrunk `.wasm` | 2,442,434 | 2,394,529 | -47,905 |
| icp-shrunk `.wasm.gz` | 801,233 | 785,550 | -15,683 |

## Structural Snapshot (ic-wasm)

| Metric | icp-built | icp-shrunk |
| --- | ---: | ---: |
| Function count | 5,755 | 5,755 |
| Callback count | 1 | 1 |
| Data section count | 3 | 3 |
| Data section bytes | 181,484 | 181,484 |
| Exported methods | 3 | 3 |

## Twiggy Top Offenders (Shallow Size)

| Rank | Item | Shallow Bytes | Shallow % |
| ---: | --- | ---: | ---: |
| 1 | data[0] | 181,218 | 7.57% |
| 2 | code[0] | 33,635 | 1.40% |
| 3 | code[1] | 32,386 | 1.35% |
| 4 | code[2] | 27,593 | 1.15% |
| 5 | code[3] | 20,378 | 0.85% |
| 6 | code[4] | 17,859 | 0.75% |
| 7 | code[5] | 15,294 | 0.64% |
| 8 | code[7] | 14,258 | 0.60% |
| 9 | code[6] | 14,213 | 0.59% |
| 10 | code[8] | 13,140 | 0.55% |

## Twiggy Retained Hotspots

| Rank | Item | Retained Bytes | Retained % |
| ---: | --- | ---: | ---: |
| 1 | export "canister_query query_one_simple_fluent" | 635,600 | 26.54% |
| 2 | code[5733] | 635,558 | 26.54% |
| 3 | code[13] | 635,549 | 26.54% |
| 4 | code[10] | 368,119 | 15.37% |
| 5 | code[398] | 213,550 | 8.92% |
| 6 | code[25] | 212,407 | 8.87% |
| 7 | code[24] | 192,058 | 8.02% |
| 8 | data[0] | 181,218 | 7.57% |
| 9 | code[31] | 180,117 | 7.52% |
| 10 | code[76] | 169,006 | 7.06% |

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
