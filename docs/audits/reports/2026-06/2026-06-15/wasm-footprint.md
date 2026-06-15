# Recurring Audit - Wasm Footprint (2026-06-15)

## Report Preamble

- scope: recurring wasm footprint audit for `default_empty, one_entity_fluent_rows, one_entity_fluent_execute, one_entity_sql_query, ten_entity_fluent_rows` with profile `wasm-release` and SQL variant `sql-on`
- compared baseline report path: `docs/audits/reports/2026-06/2026-06-13/wasm-footprint.md`
- code snapshot identifier: `1728183f3`
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
| `default_empty` | PARTIAL | N/A | 6648 | N/A | 4108 | `docs/audits/reports/2026-06/2026-06-15/artifacts/wasm-footprint/wasm-footprint.default_empty.wasm-release.sql-on.size-summary.md` |
| `one_entity_fluent_rows` | PARTIAL | N/A | 1943471 | N/A | 629825 | `docs/audits/reports/2026-06/2026-06-15/artifacts/wasm-footprint/wasm-footprint.one_entity_fluent_rows.wasm-release.sql-on.size-summary.md` |
| `one_entity_fluent_execute` | PARTIAL | N/A | 2118876 | N/A | 695621 | `docs/audits/reports/2026-06/2026-06-15/artifacts/wasm-footprint/wasm-footprint.one_entity_fluent_execute.wasm-release.sql-on.size-summary.md` |
| `one_entity_sql_query` | PARTIAL | N/A | 2662479 | N/A | 921053 | `docs/audits/reports/2026-06/2026-06-15/artifacts/wasm-footprint/wasm-footprint.one_entity_sql_query.wasm-release.sql-on.size-summary.md` |
| `ten_entity_fluent_rows` | PARTIAL | N/A | 1960120 | N/A | 631452 | `docs/audits/reports/2026-06/2026-06-15/artifacts/wasm-footprint/wasm-footprint.ten_entity_fluent_rows.wasm-release.sql-on.size-summary.md` |

## Follow-Up Actions

- owner boundary: `wasm-audit history`; action: preserve scoped current-schema baseline size artifacts so future consolidated summary runs stay comparable.

## Verification Readout

- `bash scripts/ci/wasm-audit-report.sh --date 2026-06-15` -> PASS
- per-canister size-report JSON + Twiggy artifacts -> PASS
