# Recurring Audit - Wasm Footprint (2026-06-08)

## Report Preamble

- scope: recurring wasm footprint audit for `ten_simple` with profile `wasm-release` and SQL variant `sql-on`
- compared baseline report path: `docs/audits/reports/2026-06/2026-06-06/wasm-footprint.md`
- code snapshot identifier: `95088970b`
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
| icp-built `.wasm` | 2,650,262 | 2,597,063 | -53,199 |
| icp-built deterministic `.wasm.gz` | 845,981 | 829,362 | -16,619 |
| icp-shrunk `.wasm` | 2,467,798 | 2,418,794 | -49,004 |
| icp-shrunk `.wasm.gz` | 803,888 | 788,240 | -15,648 |

## Structural Snapshot (ic-wasm)

| Metric | icp-built | icp-shrunk |
| --- | ---: | ---: |
| Function count | 5,777 | 5,777 |
| Callback count | 1 | 1 |
| Data section count | 3 | 3 |
| Data section bytes | 183,620 | 183,620 |
| Exported methods | 3 | 3 |

## Twiggy Top Offenders (Shallow Size)

| Rank | Item | Shallow Bytes | Shallow % |
| ---: | --- | ---: | ---: |
| 1 | data[0] | 183,354 | 7.58% |
| 2 | code[0] | 33,635 | 1.39% |
| 3 | code[1] | 32,386 | 1.34% |
| 4 | code[2] | 27,593 | 1.14% |
| 5 | code[3] | 20,378 | 0.84% |
| 6 | code[4] | 17,859 | 0.74% |
| 7 | code[5] | 15,294 | 0.63% |
| 8 | code[7] | 14,258 | 0.59% |
| 9 | code[6] | 14,213 | 0.59% |
| 10 | code[8] | 13,140 | 0.54% |

## Twiggy Retained Hotspots

| Rank | Item | Retained Bytes | Retained % |
| ---: | --- | ---: | ---: |
| 1 | export "canister_query query_ten_simple_fluent" | 635,890 | 26.29% |
| 2 | code[5755] | 635,848 | 26.29% |
| 3 | code[13] | 635,839 | 26.29% |
| 4 | code[10] | 368,119 | 15.22% |
| 5 | code[408] | 213,550 | 8.83% |
| 6 | code[25] | 212,407 | 8.78% |
| 7 | code[24] | 192,058 | 7.94% |
| 8 | data[0] | 183,354 | 7.58% |
| 9 | code[31] | 180,117 | 7.45% |
| 10 | table[0] | 172,585 | 7.14% |

## Artifacts

- `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.ten_simple.wasm-release.sql-on.size-report.json`
- `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.ten_simple.wasm-release.sql-on.size-summary.md`
- `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.ten_simple.wasm-release.sql-on.twiggy-top.txt`
- `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.ten_simple.wasm-release.sql-on.twiggy-top.csv`
- `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.ten_simple.wasm-release.sql-on.twiggy-dominators.txt`
- `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.ten_simple.wasm-release.sql-on.twiggy-retained.csv`
- `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.ten_simple.wasm-release.sql-on.twiggy-monos.txt`

## Follow-Up Actions

- No follow-up actions required for this run.

## Verification Readout

- `bash scripts/ci/wasm-size-report.sh` -> PASS
- `twiggy top -n 40` -> PASS
- `twiggy top --retained -n 40` -> PASS
- `twiggy dominators -r 160` -> PASS
- `twiggy monos` -> PASS
