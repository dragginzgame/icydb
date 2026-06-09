# Recurring Audit - Wasm Footprint (2026-06-09)

## Report Preamble

- scope: recurring wasm footprint audit for `one_complex` with profile `wasm-release` and SQL variant `sql-on`
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
| icp-built `.wasm` | 2,574,928 | 2,524,387 | -50,541 |
| icp-built deterministic `.wasm.gz` | 827,504 | 814,123 | -13,381 |
| icp-shrunk `.wasm` | 2,398,649 | 2,350,992 | -47,657 |
| icp-shrunk `.wasm.gz` | 786,254 | 771,289 | -14,965 |

## Structural Snapshot (ic-wasm)

| Metric | icp-built | icp-shrunk |
| --- | ---: | ---: |
| Function count | 5,704 | 5,704 |
| Callback count | 1 | 1 |
| Data section count | 3 | 3 |
| Data section bytes | 169,604 | 169,604 |
| Exported methods | 2 | 2 |

## Twiggy Top Offenders (Shallow Size)

| Rank | Item | Shallow Bytes | Shallow % |
| ---: | --- | ---: | ---: |
| 1 | data[0] | 169,338 | 7.20% |
| 2 | code[0] | 33,635 | 1.43% |
| 3 | code[1] | 32,386 | 1.38% |
| 4 | code[2] | 27,593 | 1.17% |
| 5 | code[3] | 20,378 | 0.87% |
| 6 | code[4] | 17,788 | 0.76% |
| 7 | code[5] | 15,294 | 0.65% |
| 8 | code[7] | 14,258 | 0.61% |
| 9 | code[6] | 14,213 | 0.60% |
| 10 | code[8] | 13,055 | 0.56% |

## Twiggy Retained Hotspots

| Rank | Item | Retained Bytes | Retained % |
| ---: | --- | ---: | ---: |
| 1 | export "canister_query query_one_complex_fluent" | 648,818 | 27.60% |
| 2 | code[5683] | 648,775 | 27.60% |
| 3 | code[13] | 648,766 | 27.60% |
| 4 | code[9] | 368,003 | 15.65% |
| 5 | code[393] | 213,550 | 9.08% |
| 6 | code[24] | 212,407 | 9.03% |
| 7 | code[23] | 192,058 | 8.17% |
| 8 | code[31] | 180,116 | 7.66% |
| 9 | table[0] | 172,779 | 7.35% |
| 10 | elem[0] | 172,773 | 7.35% |

## Artifacts

- `docs/audits/reports/2026-06/2026-06-09/artifacts/wasm-footprint/wasm-footprint.one_complex.wasm-release.sql-on.size-report.json`
- `docs/audits/reports/2026-06/2026-06-09/artifacts/wasm-footprint/wasm-footprint.one_complex.wasm-release.sql-on.size-summary.md`
- `docs/audits/reports/2026-06/2026-06-09/artifacts/wasm-footprint/wasm-footprint.one_complex.wasm-release.sql-on.twiggy-top.txt`
- `docs/audits/reports/2026-06/2026-06-09/artifacts/wasm-footprint/wasm-footprint.one_complex.wasm-release.sql-on.twiggy-top.csv`
- `docs/audits/reports/2026-06/2026-06-09/artifacts/wasm-footprint/wasm-footprint.one_complex.wasm-release.sql-on.twiggy-dominators.txt`
- `docs/audits/reports/2026-06/2026-06-09/artifacts/wasm-footprint/wasm-footprint.one_complex.wasm-release.sql-on.twiggy-retained.csv`
- `docs/audits/reports/2026-06/2026-06-09/artifacts/wasm-footprint/wasm-footprint.one_complex.wasm-release.sql-on.twiggy-monos.txt`

## Follow-Up Actions

- No follow-up actions required for this run.

## Verification Readout

- `bash scripts/ci/wasm-size-report.sh` -> PASS
- `twiggy top -n 40` -> PASS
- `twiggy top --retained -n 40` -> PASS
- `twiggy dominators -r 160` -> PASS
- `twiggy monos` -> PASS
