# Recurring Audit - Wasm Footprint (2026-06-13)

## Report Preamble

- scope: recurring wasm footprint audit for `minimal, minimal_metrics, one_simple, one_sql_query, one_fluent_query, one_complex, ten_simple, ten_complex` with profile `wasm-release` and SQL variant `sql-on`
- compared baseline report path: `docs/audits/reports/2026-06/2026-06-11/wasm-footprint.md`
- code snapshot identifier: `f76650cdd`
- method tag/version: `WASM-1.0`
- comparability status: `comparable`

## Checklist Results

| Requirement | Status | Evidence |
| --- | --- | --- |
| Wasm size artifacts captured | PASS | per-canister size reports + summaries written under `artifacts/wasm-footprint/` |
| Twiggy top breakdown generated | PASS | per-canister top text/csv artifacts written |
| Twiggy dominator breakdown generated | PASS | per-canister dominator text artifacts written |
| Twiggy monomorphization breakdown generated | PASS | per-canister monos artifacts written |
| Baseline delta availability | PASS | baseline size artifacts loaded for all canisters |

PASS=5, PARTIAL=0, FAIL=0

## Per-Canister Size Snapshot

| Canister | Baseline Status | Previous shrunk `.wasm` | Current shrunk `.wasm` | Previous shrunk `.wasm.gz` | Current shrunk `.wasm.gz` | Size Summary |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| `minimal` | PASS | 6516 | 6516 | 4096 | 4095 | `docs/audits/reports/2026-06/2026-06-13/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.size-summary.md` |
| `minimal_metrics` | PASS | 288647 | 288647 | 111774 | 111775 | `docs/audits/reports/2026-06/2026-06-13/artifacts/wasm-footprint/wasm-footprint.minimal_metrics.wasm-release.sql-on.size-summary.md` |
| `one_simple` | PASS | 2113490 | 2113542 | 690406 | 690321 | `docs/audits/reports/2026-06/2026-06-13/artifacts/wasm-footprint/wasm-footprint.one_simple.wasm-release.sql-on.size-summary.md` |
| `one_sql_query` | PASS | 2619276 | 2624578 | 904603 | 907613 | `docs/audits/reports/2026-06/2026-06-13/artifacts/wasm-footprint/wasm-footprint.one_sql_query.wasm-release.sql-on.size-summary.md` |
| `one_fluent_query` | PASS | 2113483 | 2113535 | 690246 | 690154 | `docs/audits/reports/2026-06/2026-06-13/artifacts/wasm-footprint/wasm-footprint.one_fluent_query.wasm-release.sql-on.size-summary.md` |
| `one_complex` | PASS | 2131241 | 2131293 | 696413 | 696354 | `docs/audits/reports/2026-06/2026-06-13/artifacts/wasm-footprint/wasm-footprint.one_complex.wasm-release.sql-on.size-summary.md` |
| `ten_simple` | PASS | 2129989 | 2130041 | 691918 | 691948 | `docs/audits/reports/2026-06/2026-06-13/artifacts/wasm-footprint/wasm-footprint.ten_simple.wasm-release.sql-on.size-summary.md` |
| `ten_complex` | PASS | 2147799 | 2147851 | 698401 | 698415 | `docs/audits/reports/2026-06/2026-06-13/artifacts/wasm-footprint/wasm-footprint.ten_complex.wasm-release.sql-on.size-summary.md` |

## Follow-Up Actions

- No follow-up actions required for this run.

## Verification Readout

- `bash scripts/ci/wasm-audit-report.sh --date 2026-06-13` -> PASS
- per-canister size-report JSON + Twiggy artifacts -> PASS
