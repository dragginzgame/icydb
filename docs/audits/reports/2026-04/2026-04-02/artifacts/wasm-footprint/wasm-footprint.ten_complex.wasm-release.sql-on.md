# Recurring Audit - Wasm Footprint (2026-04-02)

## Report Preamble

- scope: recurring wasm footprint audit for `ten_complex` with profile `wasm-release` and SQL variant `sql-on`
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
| dfx-built `.wasm` | 1,363,736 | 1,448,881 | +85,145 |
| dfx-built deterministic `.wasm.gz` | 492,543 | 525,480 | +32,937 |
| dfx-shrunk `.wasm` | 1,272,395 | 1,351,999 | +79,604 |
| dfx-shrunk `.wasm.gz` | 471,219 | 501,274 | +30,055 |

## Structural Snapshot (ic-wasm)

| Metric | dfx-built | dfx-shrunk |
| --- | ---: | ---: |
| Function count | 3,515 | 3,515 |
| Callback count | 1 | 1 |
| Data section count | 2 | 2 |
| Data section bytes | 179,388 | 179,388 |
| Exported methods | 7 | 7 |

## Twiggy Top Offenders (Shallow Size)

| Rank | Item | Shallow Bytes | Shallow % |
| ---: | --- | ---: | ---: |
| 1 | data[0] | 179,238 | 13.26% |
| 2 | code[0] | 23,437 | 1.73% |
| 3 | code[1] | 22,143 | 1.64% |
| 4 | code[2] | 17,844 | 1.32% |
| 5 | code[3] | 17,220 | 1.27% |
| 6 | code[4] | 14,821 | 1.10% |
| 7 | code[5] | 12,517 | 0.93% |
| 8 | code[7] | 11,239 | 0.83% |
| 9 | code[8] | 10,831 | 0.80% |
| 10 | code[9] | 10,167 | 0.75% |

## Twiggy Retained Hotspots

| Rank | Item | Retained Bytes | Retained % |
| ---: | --- | ---: | ---: |
| 1 | table[0] | 277,535 | 20.53% |
| 2 | elem[0] | 277,529 | 20.53% |
| 3 | export "canister_query query" | 249,245 | 18.44% |
| 4 | code[3476] | 249,221 | 18.43% |
| 5 | code[502] | 249,211 | 18.43% |
| 6 | code[0] | 248,727 | 18.40% |
| 7 | data[0] | 179,238 | 13.26% |
| 8 | code[1] | 85,606 | 6.33% |
| 9 | code[13] | 40,862 | 3.02% |
| 10 | code[33] | 37,987 | 2.81% |

## Artifacts

- `docs/audits/reports/2026-04/2026-04-02/artifacts/wasm-footprint/wasm-footprint.ten_complex.wasm-release.sql-on.size-report.json`
- `docs/audits/reports/2026-04/2026-04-02/artifacts/wasm-footprint/wasm-footprint.ten_complex.wasm-release.sql-on.size-summary.md`
- `docs/audits/reports/2026-04/2026-04-02/artifacts/wasm-footprint/wasm-footprint.ten_complex.wasm-release.sql-on.twiggy-top.txt`
- `docs/audits/reports/2026-04/2026-04-02/artifacts/wasm-footprint/wasm-footprint.ten_complex.wasm-release.sql-on.twiggy-top.csv`
- `docs/audits/reports/2026-04/2026-04-02/artifacts/wasm-footprint/wasm-footprint.ten_complex.wasm-release.sql-on.twiggy-dominators.txt`
- `docs/audits/reports/2026-04/2026-04-02/artifacts/wasm-footprint/wasm-footprint.ten_complex.wasm-release.sql-on.twiggy-retained.csv`
- `docs/audits/reports/2026-04/2026-04-02/artifacts/wasm-footprint/wasm-footprint.ten_complex.wasm-release.sql-on.twiggy-monos.txt`

## Follow-Up Actions

- No follow-up actions required for this run.

## Verification Readout

- `bash scripts/ci/wasm-size-report.sh` -> PASS
- `twiggy top -n 40` -> PASS
- `twiggy top --retained -n 40` -> PASS
- `twiggy dominators -r 160` -> PASS
- `twiggy monos` -> PASS
