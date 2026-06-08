# Recurring Audit - Wasm Footprint (2026-06-08)

## Report Preamble

- scope: recurring wasm footprint audit for `minimal, one_simple, one_sql_query, one_fluent_query, one_complex, ten_simple, ten_complex` with profile `wasm-release` and SQL variant `sql-on`
- compared baseline report path: `docs/audits/reports/2026-06/2026-06-06/wasm-footprint.md`
- code snapshot identifier: `75f6d6e9a`
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

| Canister | Baseline Status | Previous shrunk `.wasm` | Current shrunk `.wasm` | Previous shrunk `.wasm.gz` | Current shrunk `.wasm.gz` | Detail Report |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| `minimal` | PASS | 339,468 | 287,329 | 125,278 | 110,588 | `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.md` |
| `one_simple` | PASS | 2,442,434 | 2,376,200 | 801,233 | 778,994 | `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.one_simple.wasm-release.sql-on.md` |
| `one_sql_query` | PASS | 2,955,294 | 2,918,968 | 1,023,008 | 1,007,814 | `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.one_sql_query.wasm-release.sql-on.md` |
| `one_fluent_query` | PASS | 2,389,137 | 2,358,339 | 786,522 | 772,575 | `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.one_fluent_query.wasm-release.sql-on.md` |
| `one_complex` | PASS | 2,467,165 | 2,398,618 | 809,131 | 786,145 | `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.one_complex.wasm-release.sql-on.md` |
| `ten_simple` | PASS | 2,467,798 | 2,399,739 | 803,888 | 781,509 | `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.ten_simple.wasm-release.sql-on.md` |
| `ten_complex` | PASS | 2,492,449 | 2,422,077 | 812,186 | 788,645 | `docs/audits/reports/2026-06/2026-06-08/artifacts/wasm-footprint/wasm-footprint.ten_complex.wasm-release.sql-on.md` |

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

- `bash scripts/ci/wasm-audit-report.sh --date 2026-06-08` -> PASS
- per-canister size-report JSON + Twiggy artifacts -> PASS
