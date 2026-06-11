# Recurring Audit - Wasm Footprint (2026-06-11)

## Report Preamble

- scope: recurring wasm footprint audit for `minimal, one_simple, one_sql_query, one_fluent_query, one_complex, ten_simple, ten_complex` with profile `wasm-release` and SQL variant `sql-on`
- compared baseline report path: `docs/audits/reports/2026-06/2026-06-10/wasm-footprint.md`
- code snapshot identifier: `c637e7ea5`
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
| `minimal` | PASS | 6516 | 6516 | 4095 | 4096 | `docs/audits/reports/2026-06/2026-06-11/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.size-summary.md` |
| `one_simple` | PASS | 2192755 | 2202324 | 716659 | 722117 | `docs/audits/reports/2026-06/2026-06-11/artifacts/wasm-footprint/wasm-footprint.one_simple.wasm-release.sql-on.size-summary.md` |
| `one_sql_query` | PASS | 2729686 | 2745618 | 942293 | 952334 | `docs/audits/reports/2026-06/2026-06-11/artifacts/wasm-footprint/wasm-footprint.one_sql_query.wasm-release.sql-on.size-summary.md` |
| `one_fluent_query` | PASS | 2192748 | 2202445 | 716557 | 722028 | `docs/audits/reports/2026-06/2026-06-11/artifacts/wasm-footprint/wasm-footprint.one_fluent_query.wasm-release.sql-on.size-summary.md` |
| `one_complex` | PASS | 2213588 | 2225610 | 723253 | 729878 | `docs/audits/reports/2026-06/2026-06-11/artifacts/wasm-footprint/wasm-footprint.one_complex.wasm-release.sql-on.size-summary.md` |
| `ten_simple` | PASS | 2213431 | 2226350 | 718778 | 724732 | `docs/audits/reports/2026-06/2026-06-11/artifacts/wasm-footprint/wasm-footprint.ten_simple.wasm-release.sql-on.size-summary.md` |
| `ten_complex` | PASS | 2234324 | 2249568 | 725752 | 732258 | `docs/audits/reports/2026-06/2026-06-11/artifacts/wasm-footprint/wasm-footprint.ten_complex.wasm-release.sql-on.size-summary.md` |

## Follow-Up Actions

- No follow-up actions required for this run.

## Verification Readout

- `bash scripts/ci/wasm-audit-report.sh --date 2026-06-11` -> PASS
- per-canister size-report JSON + Twiggy artifacts -> PASS
