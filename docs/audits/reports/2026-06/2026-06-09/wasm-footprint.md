# Recurring Audit - Wasm Footprint (2026-06-09)

## Report Preamble

- scope: recurring wasm footprint audit for `minimal, one_simple, one_sql_query, one_fluent_query, one_complex, ten_simple, ten_complex` with profile `wasm-release` and SQL variant `sql-on`
- compared baseline report path: `docs/audits/reports/2026-06/2026-06-08/wasm-footprint.md`
- code snapshot identifier: `2587d8f03`
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
| `minimal` | PASS | 287,347 | 287,347 | 110,600 | 110,606 | `docs/audits/reports/2026-06/2026-06-09/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.size-summary.md` |
| `one_simple` | PASS | 2,376,231 | 2,325,923 | 779,048 | 763,370 | `docs/audits/reports/2026-06/2026-06-09/artifacts/wasm-footprint/wasm-footprint.one_simple.wasm-release.sql-on.size-summary.md` |
| `one_sql_query` | PASS | 2,916,794 | 2,863,363 | 1,006,633 | 988,609 | `docs/audits/reports/2026-06/2026-06-09/artifacts/wasm-footprint/wasm-footprint.one_sql_query.wasm-release.sql-on.size-summary.md` |
| `one_fluent_query` | PASS | 2,358,229 | 2,307,914 | 772,595 | 756,689 | `docs/audits/reports/2026-06/2026-06-09/artifacts/wasm-footprint/wasm-footprint.one_fluent_query.wasm-release.sql-on.size-summary.md` |
| `one_complex` | PASS | 2,398,649 | 2,348,226 | 786,254 | 770,796 | `docs/audits/reports/2026-06/2026-06-09/artifacts/wasm-footprint/wasm-footprint.one_complex.wasm-release.sql-on.size-summary.md` |
| `ten_simple` | PASS | 2,399,770 | 2,349,444 | 781,843 | 766,682 | `docs/audits/reports/2026-06/2026-06-09/artifacts/wasm-footprint/wasm-footprint.ten_simple.wasm-release.sql-on.size-summary.md` |
| `ten_complex` | PASS | 2,422,108 | 2,371,667 | 788,861 | 774,102 | `docs/audits/reports/2026-06/2026-06-09/artifacts/wasm-footprint/wasm-footprint.ten_complex.wasm-release.sql-on.size-summary.md` |

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

- No follow-up actions required for this run.

## Verification Readout

- `bash scripts/ci/wasm-audit-report.sh --date 2026-06-09` -> PASS
- per-canister size-report JSON + Twiggy artifacts -> PASS
