# Recurring Audit - Wasm Footprint (2026-06-09)

## Report Preamble

- scope: recurring wasm footprint audit for `one_fluent_query` with profile `wasm-release` and SQL variant `sql-on`
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
| icp-built `.wasm` | 2,531,292 | 2,480,993 | -50,299 |
| icp-built deterministic `.wasm.gz` | 812,703 | 799,081 | -13,622 |
| icp-shrunk `.wasm` | 2,358,229 | 2,310,808 | -47,421 |
| icp-shrunk `.wasm.gz` | 772,595 | 757,669 | -14,926 |

## Structural Snapshot (ic-wasm)

| Metric | icp-built | icp-shrunk |
| --- | ---: | ---: |
| Function count | 5,609 | 5,609 |
| Callback count | 1 | 1 |
| Data section count | 3 | 3 |
| Data section bytes | 166,660 | 166,660 |
| Exported methods | 1 | 1 |

## Twiggy Top Offenders (Shallow Size)

| Rank | Item | Shallow Bytes | Shallow % |
| ---: | --- | ---: | ---: |
| 1 | data[0] | 166,394 | 7.20% |
| 2 | code[0] | 33,635 | 1.46% |
| 3 | code[1] | 32,386 | 1.40% |
| 4 | code[2] | 27,593 | 1.19% |
| 5 | code[3] | 20,378 | 0.88% |
| 6 | code[4] | 17,855 | 0.77% |
| 7 | code[5] | 17,202 | 0.74% |
| 8 | code[8] | 15,377 | 0.67% |
| 9 | code[6] | 15,294 | 0.66% |
| 10 | code[9] | 14,255 | 0.62% |

## Twiggy Retained Hotspots

| Rank | Item | Retained Bytes | Retained % |
| ---: | --- | ---: | ---: |
| 1 | export "canister_query query_one_fluent" | 637,715 | 27.60% |
| 2 | code[5590] | 637,680 | 27.60% |
| 3 | code[8] | 636,237 | 27.53% |
| 4 | code[11] | 368,119 | 15.93% |
| 5 | code[5] | 257,938 | 11.16% |
| 6 | code[385] | 213,550 | 9.24% |
| 7 | code[24] | 212,407 | 9.19% |
| 8 | code[23] | 192,058 | 8.31% |
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
