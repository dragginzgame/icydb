# Recurring Audit - Wasm Footprint (2026-06-09)

## Report Preamble

- scope: recurring wasm footprint audit for `one_simple` with profile `wasm-release` and SQL variant `sql-on`
- compared baseline report path: `docs/audits/reports/2026-06/2026-06-08/wasm-footprint.md`
- code snapshot identifier: `c95398ed5`
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
| icp-built `.wasm` | 2,551,082 | 2,522,199 | -28,883 |
| icp-built deterministic `.wasm.gz` | 819,561 | 812,932 | -6,629 |
| icp-shrunk `.wasm` | 2,376,231 | 2,348,857 | -27,374 |
| icp-shrunk `.wasm.gz` | 779,048 | 770,377 | -8,671 |

## Structural Snapshot (ic-wasm)

| Metric | icp-built | icp-shrunk |
| --- | ---: | ---: |
| Function count | 5,686 | 5,686 |
| Callback count | 1 | 1 |
| Data section count | 3 | 3 |
| Data section bytes | 175,964 | 175,964 |
| Exported methods | 2 | 2 |

## Twiggy Top Offenders (Shallow Size)

| Rank | Item | Shallow Bytes | Shallow % |
| ---: | --- | ---: | ---: |
| 1 | data[0] | 175,698 | 7.48% |
| 2 | code[0] | 33,635 | 1.43% |
| 3 | code[1] | 32,386 | 1.38% |
| 4 | code[2] | 27,593 | 1.17% |
| 5 | code[3] | 20,378 | 0.87% |
| 6 | code[4] | 17,855 | 0.76% |
| 7 | code[5] | 15,294 | 0.65% |
| 8 | code[7] | 14,258 | 0.61% |
| 9 | code[6] | 14,213 | 0.61% |
| 10 | code[8] | 13,140 | 0.56% |

## Twiggy Retained Hotspots

| Rank | Item | Retained Bytes | Retained % |
| ---: | --- | ---: | ---: |
| 1 | export "canister_query query_one_simple_fluent" | 634,925 | 27.03% |
| 2 | code[5665] | 634,883 | 27.03% |
| 3 | code[13] | 634,874 | 27.03% |
| 4 | code[10] | 368,119 | 15.67% |
| 5 | code[391] | 213,550 | 9.09% |
| 6 | code[24] | 212,407 | 9.04% |
| 7 | code[23] | 192,058 | 8.18% |
| 8 | code[31] | 180,116 | 7.67% |
| 9 | data[0] | 175,698 | 7.48% |
| 10 | table[0] | 174,816 | 7.44% |

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
