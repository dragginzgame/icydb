# Recurring Audit - Wasm Footprint (2026-06-08)

## Report Preamble

- scope: recurring wasm footprint audit for `ten_simple` with profile `wasm-release` and SQL variant `sql-on`
- compared baseline report path: `docs/audits/reports/2026-06/2026-06-06/wasm-footprint.md`
- code snapshot identifier: `115184a8a`
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
| icp-built `.wasm` | 2,650,262 | 2,576,302 | -73,960 |
| icp-built deterministic `.wasm.gz` | 845,981 | 821,445 | -24,536 |
| icp-shrunk `.wasm` | 2,467,798 | 2,399,770 | -68,028 |
| icp-shrunk `.wasm.gz` | 803,888 | 781,843 | -22,045 |

## Structural Snapshot (ic-wasm)

| Metric | icp-built | icp-shrunk |
| --- | ---: | ---: |
| Function count | 5,724 | 5,724 |
| Callback count | 1 | 1 |
| Data section count | 3 | 3 |
| Data section bytes | 184,284 | 184,284 |
| Exported methods | 2 | 2 |

## Twiggy Top Offenders (Shallow Size)

| Rank | Item | Shallow Bytes | Shallow % |
| ---: | --- | ---: | ---: |
| 1 | data[0] | 184,018 | 7.67% |
| 2 | code[0] | 33,635 | 1.40% |
| 3 | code[1] | 32,386 | 1.35% |
| 4 | code[2] | 27,593 | 1.15% |
| 5 | code[3] | 20,378 | 0.85% |
| 6 | code[4] | 17,855 | 0.74% |
| 7 | code[5] | 15,294 | 0.64% |
| 8 | code[7] | 14,258 | 0.59% |
| 9 | code[6] | 14,213 | 0.59% |
| 10 | code[8] | 13,140 | 0.55% |

## Twiggy Retained Hotspots

| Rank | Item | Retained Bytes | Retained % |
| ---: | --- | ---: | ---: |
| 1 | export "canister_query query_ten_simple_fluent" | 635,186 | 26.47% |
| 2 | code[5703] | 635,144 | 26.47% |
| 3 | code[13] | 635,135 | 26.47% |
| 4 | code[10] | 368,119 | 15.34% |
| 5 | code[403] | 213,550 | 8.90% |
| 6 | code[24] | 212,407 | 8.85% |
| 7 | table[0] | 198,699 | 8.28% |
| 8 | elem[0] | 198,693 | 8.28% |
| 9 | code[23] | 192,058 | 8.00% |
| 10 | data[0] | 184,018 | 7.67% |

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
