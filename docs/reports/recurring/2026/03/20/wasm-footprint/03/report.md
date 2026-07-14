# Recurring Audit - Wasm Footprint (2026-03-20)

## Report Preamble

- scope: recurring wasm footprint audit for `minimal` with profile `wasm-release` and SQL variant `sql-on`
- compared baseline report path: `docs/audits/reports/2026-03/2026-03-20/wasm-footprint.md`
- code snapshot identifier: `d8f08504`
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
| dfx-built `.wasm` | 1,889,792 | 1,889,792 | +0 |
| dfx-built deterministic `.wasm.gz` | 618,982 | 618,982 | +0 |
| dfx-shrunk `.wasm` | 1,769,633 | 1,769,633 | +0 |
| dfx-shrunk `.wasm.gz` | 578,227 | 578,227 | +0 |

## Structural Snapshot (ic-wasm)

| Metric | dfx-built | dfx-shrunk |
| --- | ---: | ---: |
| Function count | 4,199 | 4,199 |
| Callback count | 1 | 1 |
| Data section count | 2 | 2 |
| Data section bytes | 187,508 | 187,508 |
| Exported methods | 7 | 7 |

## Twiggy Top Offenders (Shallow Size)

| Rank | Item | Shallow Bytes | Shallow % |
| ---: | --- | ---: | ---: |
| 1 | data[0] | 187,374 | 10.59% |
| 2 | code[0] | 25,238 | 1.43% |
| 3 | code[1] | 23,871 | 1.35% |
| 4 | code[2] | 21,103 | 1.19% |
| 5 | code[3] | 20,277 | 1.15% |
| 6 | code[4] | 17,848 | 1.01% |
| 7 | code[5] | 16,040 | 0.91% |
| 8 | code[6] | 14,877 | 0.84% |
| 9 | code[7] | 13,965 | 0.79% |
| 10 | code[8] | 13,342 | 0.75% |

## Twiggy Retained Hotspots

| Rank | Item | Retained Bytes | Retained % |
| ---: | --- | ---: | ---: |
| 1 | table[0] | 912,985 | 51.59% |
| 2 | elem[0] | 912,979 | 51.59% |
| 3 | code[0] | 259,215 | 14.65% |
| 4 | code[150] | 228,423 | 12.91% |
| 5 | code[254] | 226,378 | 12.79% |
| 6 | code[3] | 225,219 | 12.73% |
| 7 | data[0] | 187,374 | 10.59% |
| 8 | code[127] | 76,798 | 4.34% |
| 9 | code[412] | 74,444 | 4.21% |
| 10 | code[20] | 73,772 | 4.17% |

## Artifacts

- `docs/audits/reports/2026-03/2026-03-20/artifacts/wasm-footprint/wasm-footprint-3.minimal.wasm-release.sql-on.size-report.json`
- `docs/audits/reports/2026-03/2026-03-20/artifacts/wasm-footprint/wasm-footprint-3.minimal.wasm-release.sql-on.size-summary.md`
- `docs/audits/reports/2026-03/2026-03-20/artifacts/wasm-footprint/wasm-footprint-3.minimal.wasm-release.sql-on.twiggy-top.txt`
- `docs/audits/reports/2026-03/2026-03-20/artifacts/wasm-footprint/wasm-footprint-3.minimal.wasm-release.sql-on.twiggy-top.csv`
- `docs/audits/reports/2026-03/2026-03-20/artifacts/wasm-footprint/wasm-footprint-3.minimal.wasm-release.sql-on.twiggy-dominators.txt`
- `docs/audits/reports/2026-03/2026-03-20/artifacts/wasm-footprint/wasm-footprint-3.minimal.wasm-release.sql-on.twiggy-retained.csv`
- `docs/audits/reports/2026-03/2026-03-20/artifacts/wasm-footprint/wasm-footprint-3.minimal.wasm-release.sql-on.twiggy-monos.txt`

## Follow-Up Actions

- No follow-up actions required for this run.

## Verification Readout

- `bash scripts/ci/wasm-size-report.sh` -> PASS
- `twiggy top -n 40` -> PASS
- `twiggy top --retained -n 40` -> PASS
- `twiggy dominators -r 160` -> PASS
- `twiggy monos` -> PASS
