# Recurring Audit - Wasm Footprint (2026-04-02)

## Report Preamble

- scope: recurring wasm footprint audit for `one_simple` with profile `wasm-release` and SQL variant `sql-on`
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
| dfx-built `.wasm` | 1,344,522 | 1,429,011 | +84,489 |
| dfx-built deterministic `.wasm.gz` | 490,239 | 522,840 | +32,601 |
| dfx-shrunk `.wasm` | 1,254,475 | 1,333,660 | +79,185 |
| dfx-shrunk `.wasm.gz` | 469,274 | 498,823 | +29,549 |

## Structural Snapshot (ic-wasm)

| Metric | dfx-built | dfx-shrunk |
| --- | ---: | ---: |
| Function count | 3,464 | 3,464 |
| Callback count | 1 | 1 |
| Data section count | 2 | 2 |
| Data section bytes | 176,572 | 176,572 |
| Exported methods | 7 | 7 |

## Twiggy Top Offenders (Shallow Size)

| Rank | Item | Shallow Bytes | Shallow % |
| ---: | --- | ---: | ---: |
| 1 | data[0] | 176,422 | 13.23% |
| 2 | code[0] | 23,436 | 1.76% |
| 3 | code[1] | 22,143 | 1.66% |
| 4 | code[2] | 17,844 | 1.34% |
| 5 | code[3] | 17,220 | 1.29% |
| 6 | code[4] | 14,821 | 1.11% |
| 7 | code[5] | 12,517 | 0.94% |
| 8 | code[7] | 11,239 | 0.84% |
| 9 | code[8] | 10,831 | 0.81% |
| 10 | code[9] | 10,167 | 0.76% |

## Twiggy Retained Hotspots

| Rank | Item | Retained Bytes | Retained % |
| ---: | --- | ---: | ---: |
| 1 | table[0] | 263,150 | 19.73% |
| 2 | elem[0] | 263,144 | 19.73% |
| 3 | export "canister_query query" | 249,242 | 18.69% |
| 4 | code[3425] | 249,218 | 18.69% |
| 5 | code[491] | 249,208 | 18.69% |
| 6 | code[0] | 248,724 | 18.65% |
| 7 | data[0] | 176,422 | 13.23% |
| 8 | code[1357] | 85,772 | 6.43% |
| 9 | code[1] | 85,606 | 6.42% |
| 10 | code[13] | 40,862 | 3.06% |

## Artifacts

- `docs/audits/reports/2026-04/2026-04-02/artifacts/wasm-footprint/wasm-footprint.one_simple.wasm-release.sql-on.size-report.json`
- `docs/audits/reports/2026-04/2026-04-02/artifacts/wasm-footprint/wasm-footprint.one_simple.wasm-release.sql-on.size-summary.md`
- `docs/audits/reports/2026-04/2026-04-02/artifacts/wasm-footprint/wasm-footprint.one_simple.wasm-release.sql-on.twiggy-top.txt`
- `docs/audits/reports/2026-04/2026-04-02/artifacts/wasm-footprint/wasm-footprint.one_simple.wasm-release.sql-on.twiggy-top.csv`
- `docs/audits/reports/2026-04/2026-04-02/artifacts/wasm-footprint/wasm-footprint.one_simple.wasm-release.sql-on.twiggy-dominators.txt`
- `docs/audits/reports/2026-04/2026-04-02/artifacts/wasm-footprint/wasm-footprint.one_simple.wasm-release.sql-on.twiggy-retained.csv`
- `docs/audits/reports/2026-04/2026-04-02/artifacts/wasm-footprint/wasm-footprint.one_simple.wasm-release.sql-on.twiggy-monos.txt`

## Follow-Up Actions

- No follow-up actions required for this run.

## Verification Readout

- `bash scripts/ci/wasm-size-report.sh` -> PASS
- `twiggy top -n 40` -> PASS
- `twiggy top --retained -n 40` -> PASS
- `twiggy dominators -r 160` -> PASS
- `twiggy monos` -> PASS
