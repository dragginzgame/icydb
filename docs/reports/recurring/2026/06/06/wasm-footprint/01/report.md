# Recurring Audit - Wasm Footprint (2026-06-06)

## Report Preamble

- scope: recurring wasm footprint audit for `minimal, one_simple, one_sql_query, one_fluent_query, one_complex, ten_simple, ten_complex` with profile `wasm-release` and SQL variant `sql-on`
- compared baseline report path: `docs/audits/reports/2026-04/2026-04-27/wasm-footprint.md`
- code snapshot identifier: `2b97a0d33`
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

| Canister | Baseline Status | Previous shrunk `.wasm` | Current shrunk `.wasm` | Previous shrunk `.wasm.gz` | Current shrunk `.wasm.gz` | Detail Report |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| `minimal` | PARTIAL | N/A | 339,468 | N/A | 125,278 | `docs/audits/reports/2026-06/2026-06-06/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.md` |
| `one_simple` | PARTIAL | N/A | 2,442,434 | N/A | 801,233 | `docs/audits/reports/2026-06/2026-06-06/artifacts/wasm-footprint/wasm-footprint.one_simple.wasm-release.sql-on.md` |
| `one_sql_query` | PARTIAL | N/A | 2,955,294 | N/A | 1,023,008 | `docs/audits/reports/2026-06/2026-06-06/artifacts/wasm-footprint/wasm-footprint.one_sql_query.wasm-release.sql-on.md` |
| `one_fluent_query` | PARTIAL | N/A | 2,389,137 | N/A | 786,522 | `docs/audits/reports/2026-06/2026-06-06/artifacts/wasm-footprint/wasm-footprint.one_fluent_query.wasm-release.sql-on.md` |
| `one_complex` | PARTIAL | N/A | 2,467,165 | N/A | 809,131 | `docs/audits/reports/2026-06/2026-06-06/artifacts/wasm-footprint/wasm-footprint.one_complex.wasm-release.sql-on.md` |
| `ten_simple` | PARTIAL | N/A | 2,467,798 | N/A | 803,888 | `docs/audits/reports/2026-06/2026-06-06/artifacts/wasm-footprint/wasm-footprint.ten_simple.wasm-release.sql-on.md` |
| `ten_complex` | PARTIAL | N/A | 2,492,449 | N/A | 812,186 | `docs/audits/reports/2026-06/2026-06-06/artifacts/wasm-footprint/wasm-footprint.ten_complex.wasm-release.sql-on.md` |

## Generated Endpoint Surface

| Canister | SQL query | SQL DDL | SQL fixtures | Metrics | Metrics reset | Snapshot | Schema | Custom exports |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `minimal` | no | no | no | yes | no | no | no | none |
| `one_simple` | no | no | no | yes | no | no | no | `query_one_simple_fluent` |
| `one_sql_query` | no | no | no | no | no | no | no | `query_one_sql` |
| `one_fluent_query` | no | no | no | no | no | no | no | `query_one_fluent` |
| `one_complex` | no | no | no | yes | no | no | no | `query_one_complex_fluent` |
| `ten_simple` | no | no | no | yes | no | no | no | `query_ten_simple_fluent` |
| `ten_complex` | no | no | no | yes | no | no | no | `query_ten_complex_fluent` |

## Follow-Up Actions

- owner boundary: `wasm-audit history`; action: preserve scoped current-schema baseline size artifacts so future consolidated summary runs stay comparable.

## Verification Readout

- `bash scripts/ci/wasm-audit-report.sh --date 2026-06-06` -> PASS
- per-canister size-report JSON + Twiggy artifacts -> PASS
