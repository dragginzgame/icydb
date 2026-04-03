# Recurring Audit - Wasm Footprint (2026-04-02)

## Report Preamble

- scope: recurring wasm footprint audit for `minimal` with profile `wasm-release` and SQL variant `sql-on`
- compared baseline report path: `docs/audits/reports/2026-03/2026-03-31/wasm-footprint.md`
- code snapshot identifier: `25a2a119`
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
| dfx-built `.wasm` | 1,207,667 | 1,296,561 | +88,894 |
| dfx-built deterministic `.wasm.gz` | 445,424 | 478,963 | +33,539 |
| dfx-shrunk `.wasm` | 1,125,935 | 1,209,113 | +83,178 |
| dfx-shrunk `.wasm.gz` | 425,708 | 456,535 | +30,827 |

## Structural Snapshot (ic-wasm)

| Metric | dfx-built | dfx-shrunk |
| --- | ---: | ---: |
| Function count | 3,227 | 3,227 |
| Callback count | 1 | 1 |
| Data section count | 2 | 2 |
| Data section bytes | 171,184 | 171,184 |
| Exported methods | 7 | 7 |

## Twiggy Top Offenders (Shallow Size)

| Rank | Item | Shallow Bytes | Shallow % |
| ---: | --- | ---: | ---: |
| 1 | data[0] | 171,034 | 14.15% |
| 2 | code[0] | 23,202 | 1.92% |
| 3 | code[1] | 17,843 | 1.48% |
| 4 | code[2] | 17,331 | 1.43% |
| 5 | code[3] | 14,821 | 1.23% |
| 6 | code[4] | 12,516 | 1.04% |
| 7 | code[6] | 10,167 | 0.84% |
| 8 | code[7] | 9,701 | 0.80% |
| 9 | code[8] | 9,431 | 0.78% |
| 10 | code[9] | 8,809 | 0.73% |

## Twiggy Retained Hotspots

| Rank | Item | Retained Bytes | Retained % |
| ---: | --- | ---: | ---: |
| 1 | export "canister_query query" | 264,532 | 21.88% |
| 2 | code[3188] | 264,508 | 21.88% |
| 3 | code[449] | 264,498 | 21.88% |
| 4 | code[0] | 264,014 | 21.84% |
| 5 | data[0] | 171,034 | 14.15% |
| 6 | table[0] | 146,183 | 12.09% |
| 7 | elem[0] | 146,177 | 12.09% |
| 8 | code[10] | 40,862 | 3.38% |
| 9 | code[28] | 38,864 | 3.21% |
| 10 | code[3] | 35,398 | 2.93% |

## Artifacts

- `docs/audits/reports/2026-04/2026-04-02/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.size-report.json`
- `docs/audits/reports/2026-04/2026-04-02/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.size-summary.md`
- `docs/audits/reports/2026-04/2026-04-02/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.twiggy-top.txt`
- `docs/audits/reports/2026-04/2026-04-02/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.twiggy-top.csv`
- `docs/audits/reports/2026-04/2026-04-02/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.twiggy-dominators.txt`
- `docs/audits/reports/2026-04/2026-04-02/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.twiggy-retained.csv`
- `docs/audits/reports/2026-04/2026-04-02/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.twiggy-monos.txt`

## Follow-Up Actions

- No follow-up actions required for this run.

## Verification Readout

- `bash scripts/ci/wasm-size-report.sh` -> PASS
- `twiggy top -n 40` -> PASS
- `twiggy top --retained -n 40` -> PASS
- `twiggy dominators -r 160` -> PASS
- `twiggy monos` -> PASS
