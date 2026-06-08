# Recurring Audit - Wasm Footprint (2026-06-08)

## Report Preamble

- scope: recurring wasm footprint audit for `one_complex` with profile `wasm-release` and SQL variant `sql-on`
- compared baseline report path: `docs/audits/reports/2026-06/2026-06-06/wasm-footprint.md`
- code snapshot identifier: `16abe9f7b`
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
| icp-built `.wasm` | 2,649,374 | 2,574,913 | -74,461 |
| icp-built deterministic `.wasm.gz` | 852,454 | 828,253 | -24,201 |
| icp-shrunk `.wasm` | 2,467,165 | 2,398,629 | -68,536 |
| icp-shrunk `.wasm.gz` | 809,131 | 786,287 | -22,844 |

## Structural Snapshot (ic-wasm)

| Metric | icp-built | icp-shrunk |
| --- | ---: | ---: |
| Function count | 5,743 | 5,743 |
| Callback count | 1 | 1 |
| Data section count | 3 | 3 |
| Data section bytes | 184,796 | 184,796 |
| Exported methods | 2 | 2 |

## Twiggy Top Offenders (Shallow Size)

| Rank | Item | Shallow Bytes | Shallow % |
| ---: | --- | ---: | ---: |
| 1 | data[0] | 184,530 | 7.69% |
| 2 | code[0] | 33,635 | 1.40% |
| 3 | code[1] | 32,386 | 1.35% |
| 4 | code[2] | 27,593 | 1.15% |
| 5 | code[3] | 20,378 | 0.85% |
| 6 | code[4] | 17,788 | 0.74% |
| 7 | code[5] | 15,294 | 0.64% |
| 8 | code[7] | 14,258 | 0.59% |
| 9 | code[6] | 14,213 | 0.59% |
| 10 | code[8] | 13,140 | 0.55% |

## Twiggy Retained Hotspots

| Rank | Item | Retained Bytes | Retained % |
| ---: | --- | ---: | ---: |
| 1 | export "canister_query query_one_complex_fluent" | 651,814 | 27.17% |
| 2 | code[5722] | 651,771 | 27.17% |
| 3 | code[13] | 651,762 | 27.17% |
| 4 | code[10] | 368,003 | 15.34% |
| 5 | code[396] | 213,550 | 8.90% |
| 6 | code[25] | 212,407 | 8.86% |
| 7 | code[24] | 192,058 | 8.01% |
| 8 | data[0] | 184,530 | 7.69% |
| 9 | code[32] | 180,118 | 7.51% |
| 10 | table[0] | 176,929 | 7.38% |

## Artifacts

- `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.one_complex.wasm-release.sql-on.size-report.json`
- `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.one_complex.wasm-release.sql-on.size-summary.md`
- `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.one_complex.wasm-release.sql-on.twiggy-top.txt`
- `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.one_complex.wasm-release.sql-on.twiggy-top.csv`
- `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.one_complex.wasm-release.sql-on.twiggy-dominators.txt`
- `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.one_complex.wasm-release.sql-on.twiggy-retained.csv`
- `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.one_complex.wasm-release.sql-on.twiggy-monos.txt`

## Follow-Up Actions

- No follow-up actions required for this run.

## Verification Readout

- `bash scripts/ci/wasm-size-report.sh` -> PASS
- `twiggy top -n 40` -> PASS
- `twiggy top --retained -n 40` -> PASS
- `twiggy dominators -r 160` -> PASS
- `twiggy monos` -> PASS
