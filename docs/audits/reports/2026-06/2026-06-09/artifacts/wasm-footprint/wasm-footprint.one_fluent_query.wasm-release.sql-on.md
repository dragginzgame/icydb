# Recurring Audit - Wasm Footprint (2026-06-09)

## Report Preamble

- scope: recurring wasm footprint audit for `one_fluent_query` with profile `wasm-release` and SQL variant `sql-on`
- compared baseline report path: `docs/audits/reports/2026-06/2026-06-08/wasm-footprint.md`
- code snapshot identifier: `a43cb9272`
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
| icp-built `.wasm` | 2,531,292 | 2,479,651 | -51,641 |
| icp-built deterministic `.wasm.gz` | 812,703 | 799,690 | -13,013 |
| icp-shrunk `.wasm` | 2,358,229 | 2,309,470 | -48,759 |
| icp-shrunk `.wasm.gz` | 772,595 | 757,102 | -15,493 |

## Structural Snapshot (ic-wasm)

| Metric | icp-built | icp-shrunk |
| --- | ---: | ---: |
| Function count | 5,609 | 5,609 |
| Callback count | 1 | 1 |
| Data section count | 3 | 3 |
| Data section bytes | 165,356 | 165,356 |
| Exported methods | 1 | 1 |

## Twiggy Top Offenders (Shallow Size)

| Rank | Item | Shallow Bytes | Shallow % |
| ---: | --- | ---: | ---: |
| 1 | data[0] | 165,090 | 7.15% |
| 2 | code[0] | 33,635 | 1.46% |
| 3 | code[1] | 32,386 | 1.40% |
| 4 | code[2] | 27,593 | 1.19% |
| 5 | code[3] | 20,378 | 0.88% |
| 6 | code[4] | 17,840 | 0.77% |
| 7 | code[5] | 17,202 | 0.74% |
| 8 | code[8] | 15,377 | 0.67% |
| 9 | code[6] | 15,294 | 0.66% |
| 10 | code[9] | 14,255 | 0.62% |

## Twiggy Retained Hotspots

| Rank | Item | Retained Bytes | Retained % |
| ---: | --- | ---: | ---: |
| 1 | export "canister_query query_one_fluent" | 637,681 | 27.61% |
| 2 | code[5590] | 637,646 | 27.61% |
| 3 | code[8] | 636,203 | 27.55% |
| 4 | code[11] | 368,119 | 15.94% |
| 5 | code[5] | 257,938 | 11.17% |
| 6 | code[385] | 213,550 | 9.25% |
| 7 | code[24] | 212,407 | 9.20% |
| 8 | code[23] | 192,058 | 8.32% |
| 9 | table[0] | 172,558 | 7.47% |
| 10 | elem[0] | 172,552 | 7.47% |

## Artifacts

- `docs/audits/reports/2026-06/2026-06-09/artifacts/wasm-footprint/wasm-footprint.one_fluent_query.wasm-release.sql-on.size-report.json`
- `docs/audits/reports/2026-06/2026-06-09/artifacts/wasm-footprint/wasm-footprint.one_fluent_query.wasm-release.sql-on.size-summary.md`
- `docs/audits/reports/2026-06/2026-06-09/artifacts/wasm-footprint/wasm-footprint.one_fluent_query.wasm-release.sql-on.twiggy-top.txt`
- `docs/audits/reports/2026-06/2026-06-09/artifacts/wasm-footprint/wasm-footprint.one_fluent_query.wasm-release.sql-on.twiggy-top.csv`
- `docs/audits/reports/2026-06/2026-06-09/artifacts/wasm-footprint/wasm-footprint.one_fluent_query.wasm-release.sql-on.twiggy-dominators.txt`
- `docs/audits/reports/2026-06/2026-06-09/artifacts/wasm-footprint/wasm-footprint.one_fluent_query.wasm-release.sql-on.twiggy-retained.csv`
- `docs/audits/reports/2026-06/2026-06-09/artifacts/wasm-footprint/wasm-footprint.one_fluent_query.wasm-release.sql-on.twiggy-monos.txt`

## Follow-Up Actions

- No follow-up actions required for this run.

## Verification Readout

- `bash scripts/ci/wasm-size-report.sh` -> PASS
- `twiggy top -n 40` -> PASS
- `twiggy top --retained -n 40` -> PASS
- `twiggy dominators -r 160` -> PASS
- `twiggy monos` -> PASS
