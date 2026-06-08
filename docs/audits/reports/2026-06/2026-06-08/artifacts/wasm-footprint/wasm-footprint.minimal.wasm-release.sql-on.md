# Recurring Audit - Wasm Footprint (2026-06-08)

## Report Preamble

- scope: recurring wasm footprint audit for `minimal` with profile `wasm-release` and SQL variant `sql-on`
- compared baseline report path: `docs/audits/reports/2026-06/2026-06-06/wasm-footprint.md`
- code snapshot identifier: `449d796ac`
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
| icp-built `.wasm` | 370,058 | 362,960 | -7,098 |
| icp-built deterministic `.wasm.gz` | 132,701 | 130,590 | -2,111 |
| icp-shrunk `.wasm` | 339,468 | 333,067 | -6,401 |
| icp-shrunk `.wasm.gz` | 125,278 | 123,367 | -1,911 |

## Structural Snapshot (ic-wasm)

| Metric | icp-built | icp-shrunk |
| --- | ---: | ---: |
| Function count | 1,007 | 1,007 |
| Callback count | 1 | 1 |
| Data section count | 3 | 3 |
| Data section bytes | 39,440 | 39,440 |
| Exported methods | 2 | 2 |

## Twiggy Top Offenders (Shallow Size)

| Rank | Item | Shallow Bytes | Shallow % |
| ---: | --- | ---: | ---: |
| 1 | data[0] | 39,378 | 11.82% |
| 2 | code[0] | 19,258 | 5.78% |
| 3 | code[1] | 16,079 | 4.83% |
| 4 | code[2] | 12,416 | 3.73% |
| 5 | code[3] | 8,588 | 2.58% |
| 6 | code[4] | 6,739 | 2.02% |
| 7 | code[5] | 6,531 | 1.96% |
| 8 | code[6] | 5,390 | 1.62% |
| 9 | code[7] | 4,793 | 1.44% |
| 10 | code[8] | 4,583 | 1.38% |

## Twiggy Retained Hotspots

| Rank | Item | Retained Bytes | Retained % |
| ---: | --- | ---: | ---: |
| 1 | export "canister_query __icydb_metrics" | 118,843 | 35.68% |
| 2 | code[993] | 118,809 | 35.67% |
| 3 | code[0] | 118,800 | 35.67% |
| 4 | table[0] | 64,818 | 19.46% |
| 5 | elem[0] | 64,812 | 19.46% |
| 6 | data[0] | 39,378 | 11.82% |
| 7 | code[1] | 37,104 | 11.14% |
| 8 | code[11] | 18,046 | 5.42% |
| 9 | code[22] | 17,267 | 5.18% |
| 10 | code[2] | 12,721 | 3.82% |

## Artifacts

- `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.size-report.json`
- `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.size-summary.md`
- `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.twiggy-top.txt`
- `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.twiggy-top.csv`
- `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.twiggy-dominators.txt`
- `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.twiggy-retained.csv`
- `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.twiggy-monos.txt`

## Follow-Up Actions

- No follow-up actions required for this run.

## Verification Readout

- `bash scripts/ci/wasm-size-report.sh` -> PASS
- `twiggy top -n 40` -> PASS
- `twiggy top --retained -n 40` -> PASS
- `twiggy dominators -r 160` -> PASS
- `twiggy monos` -> PASS
