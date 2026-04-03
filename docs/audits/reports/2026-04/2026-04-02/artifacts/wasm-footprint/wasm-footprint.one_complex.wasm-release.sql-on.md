# Recurring Audit - Wasm Footprint (2026-04-02)

## Report Preamble

- scope: recurring wasm footprint audit for `one_complex` with profile `wasm-release` and SQL variant `sql-on`
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
| dfx-built `.wasm` | 1,346,964 | 1,431,325 | +84,361 |
| dfx-built deterministic `.wasm.gz` | 490,764 | 523,482 | +32,718 |
| dfx-shrunk `.wasm` | 1,256,836 | 1,335,893 | +79,057 |
| dfx-shrunk `.wasm.gz` | 469,842 | 499,410 | +29,568 |

## Structural Snapshot (ic-wasm)

| Metric | dfx-built | dfx-shrunk |
| --- | ---: | ---: |
| Function count | 3,467 | 3,467 |
| Callback count | 1 | 1 |
| Data section count | 2 | 2 |
| Data section bytes | 177,596 | 177,596 |
| Exported methods | 7 | 7 |

## Twiggy Top Offenders (Shallow Size)

| Rank | Item | Shallow Bytes | Shallow % |
| ---: | --- | ---: | ---: |
| 1 | data[0] | 177,446 | 13.28% |
| 2 | code[0] | 23,436 | 1.75% |
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
| 1 | table[0] | 263,224 | 19.70% |
| 2 | elem[0] | 263,218 | 19.70% |
| 3 | export "canister_query query" | 249,242 | 18.66% |
| 4 | code[3428] | 249,218 | 18.66% |
| 5 | code[492] | 249,208 | 18.65% |
| 6 | code[0] | 248,724 | 18.62% |
| 7 | data[0] | 177,446 | 13.28% |
| 8 | code[1358] | 85,773 | 6.42% |
| 9 | code[1] | 85,606 | 6.41% |
| 10 | code[13] | 40,862 | 3.06% |

## Artifacts

- `docs/audits/reports/2026-04/2026-04-02/artifacts/wasm-footprint/wasm-footprint.one_complex.wasm-release.sql-on.size-report.json`
- `docs/audits/reports/2026-04/2026-04-02/artifacts/wasm-footprint/wasm-footprint.one_complex.wasm-release.sql-on.size-summary.md`
- `docs/audits/reports/2026-04/2026-04-02/artifacts/wasm-footprint/wasm-footprint.one_complex.wasm-release.sql-on.twiggy-top.txt`
- `docs/audits/reports/2026-04/2026-04-02/artifacts/wasm-footprint/wasm-footprint.one_complex.wasm-release.sql-on.twiggy-top.csv`
- `docs/audits/reports/2026-04/2026-04-02/artifacts/wasm-footprint/wasm-footprint.one_complex.wasm-release.sql-on.twiggy-dominators.txt`
- `docs/audits/reports/2026-04/2026-04-02/artifacts/wasm-footprint/wasm-footprint.one_complex.wasm-release.sql-on.twiggy-retained.csv`
- `docs/audits/reports/2026-04/2026-04-02/artifacts/wasm-footprint/wasm-footprint.one_complex.wasm-release.sql-on.twiggy-monos.txt`

## Follow-Up Actions

- No follow-up actions required for this run.

## Verification Readout

- `bash scripts/ci/wasm-size-report.sh` -> PASS
- `twiggy top -n 40` -> PASS
- `twiggy top --retained -n 40` -> PASS
- `twiggy dominators -r 160` -> PASS
- `twiggy monos` -> PASS
