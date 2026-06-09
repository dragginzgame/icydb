# Recurring Audit - Wasm Footprint (2026-06-09)

## Report Preamble

- scope: recurring wasm footprint audit for `minimal, minimal_metrics, one_simple, one_sql_query, one_fluent_query, one_complex, ten_simple, ten_complex` with profile `wasm-release` and SQL variant `sql-on`
- compared baseline report path: `docs/audits/reports/2026-06/2026-06-08/wasm-footprint.md`
- code snapshot identifier: `c18430894`
- method tag/version: `WASM-1.0`
- comparability status: `non-comparable (one or more baseline size artifacts are missing or use an incompatible metric schema)`

## Checklist Results

| Requirement | Status | Evidence |
| --- | --- | --- |
| Wasm size artifacts captured | PASS | per-canister size reports + summaries written under `artifacts/wasm-footprint/` |
| Twiggy top breakdown generated | PASS | per-canister top text/csv artifacts written |
| Twiggy dominator breakdown generated | PASS | per-canister dominator text artifacts written |
| Twiggy monomorphization breakdown generated | PASS | per-canister monos artifacts written |
| Baseline delta availability | PARTIAL | one or more prior scoped size artifacts are missing or use an incompatible metric schema |

PASS=4, PARTIAL=1, FAIL=0

## Per-Canister Size Snapshot

| Canister | Baseline Status | Previous shrunk `.wasm` | Current shrunk `.wasm` | Previous shrunk `.wasm.gz` | Current shrunk `.wasm.gz` | Size Summary |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| `minimal` | PASS | 287347 | 6516 | 110600 | 4095 | `docs/audits/reports/2026-06/2026-06-09/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.size-summary.md` |
| `minimal_metrics` | PARTIAL | N/A | 288647 | N/A | 111771 | `docs/audits/reports/2026-06/2026-06-09/artifacts/wasm-footprint/wasm-footprint.minimal_metrics.wasm-release.sql-on.size-summary.md` |
| `one_simple` | PASS | 2376231 | 2292201 | 779048 | 751714 | `docs/audits/reports/2026-06/2026-06-09/artifacts/wasm-footprint/wasm-footprint.one_simple.wasm-release.sql-on.size-summary.md` |
| `one_sql_query` | PASS | 2916794 | 2843063 | 1006633 | 982345 | `docs/audits/reports/2026-06/2026-06-09/artifacts/wasm-footprint/wasm-footprint.one_sql_query.wasm-release.sql-on.size-summary.md` |
| `one_fluent_query` | PASS | 2358229 | 2292322 | 772595 | 751253 | `docs/audits/reports/2026-06/2026-06-09/artifacts/wasm-footprint/wasm-footprint.one_fluent_query.wasm-release.sql-on.size-summary.md` |
| `one_complex` | PASS | 2398649 | 2314311 | 786254 | 759104 | `docs/audits/reports/2026-06/2026-06-09/artifacts/wasm-footprint/wasm-footprint.one_complex.wasm-release.sql-on.size-summary.md` |
| `ten_simple` | PASS | 2399770 | 2315722 | 781843 | 754129 | `docs/audits/reports/2026-06/2026-06-09/artifacts/wasm-footprint/wasm-footprint.ten_simple.wasm-release.sql-on.size-summary.md` |
| `ten_complex` | PASS | 2422108 | 2337752 | 788861 | 761623 | `docs/audits/reports/2026-06/2026-06-09/artifacts/wasm-footprint/wasm-footprint.ten_complex.wasm-release.sql-on.size-summary.md` |

## Follow-Up Actions

- owner boundary: `wasm-audit history`; action: preserve scoped current-schema baseline size artifacts so future consolidated summary runs stay comparable.

## Verification Readout

- `bash scripts/ci/wasm-audit-report.sh --date 2026-06-09` -> PASS
- per-canister size-report JSON + Twiggy artifacts -> PASS
