# Recurring Audit - Wasm Footprint (2026-08-26)

## Report Preamble

- scope: recurring wasm footprint audit for `one_entity_typed_query, one_entity_dynamic_query, one_entity_sql_query` with profile `wasm-release` and SQL variant `sql-on`
- audit definition: [`crosscutting-wasm-footprint.md`](../../../../../../../audits/recurring/crosscutting/crosscutting-wasm-footprint.md)
- compared baseline report path: `docs/reports/recurring/2026/08/26/wasm-footprint/01/report.md`
- code snapshot identifier: `b6b5c287d`
- method tag/version: `WASM-3.0`
- comparability status: `comparable`

## Checklist Results

| Requirement | Status | Evidence |
| --- | --- | --- |
| Wasm size artifacts captured | PASS | per-canister size reports + summaries written under `artifacts/` |
| Twiggy top breakdown generated | PASS | per-canister top text artifacts written |
| Twiggy dominator breakdown generated | PASS | per-canister dominator text artifacts written |
| Twiggy monomorphization breakdown generated | PASS | per-canister monos artifacts written |
| Baseline delta availability | PASS | baseline size artifacts loaded for all canisters |

PASS=5, PARTIAL=0, FAIL=0

## Per-Canister Size Snapshot

| Canister | Baseline Status | Previous final `.wasm` | Current final `.wasm` | Previous final `.wasm.gz` | Current final `.wasm.gz` | Size Summary |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| `one_entity_typed_query` | PASS | 2117501 | 2010198 | 812145 | 780618 | `docs/reports/recurring/2026/08/26/wasm-footprint/02/artifacts/wasm-footprint.one_entity_typed_query.wasm-release.sql-on.size-summary.md` |
| `one_entity_dynamic_query` | PASS | 2971900 | 2864615 | 1172327 | 1137805 | `docs/reports/recurring/2026/08/26/wasm-footprint/02/artifacts/wasm-footprint.one_entity_dynamic_query.wasm-release.sql-on.size-summary.md` |
| `one_entity_sql_query` | PASS | 3287248 | 3179832 | 1302888 | 1274179 | `docs/reports/recurring/2026/08/26/wasm-footprint/02/artifacts/wasm-footprint.one_entity_sql_query.wasm-release.sql-on.size-summary.md` |

## Follow-Up Actions

- Close the 0.246 footprint slice as
  `NO-BUILD — SQL PREMIUM IS CURRENTLY JUSTIFIED`; no broad removable owner
  conservatively clears 32 KiB final raw Wasm.
- Preserve repeated SQL parsing as a separately authorized performance and
  attribution-correctness opportunity. Do not add a second AST cache or SQL
  runtime mode.

## SQL Ingress Owner Audit

The typed actor performs a planner-free `Database::get`, so its 1,169,634-byte
directional gap from SQL is not a measure of removable frontend overhead. The
dynamic actor retains a structural planner/executor and narrows the directional
gap to 315,217 bytes, but it also exposes four extra measurement methods and is
not an exact matched control.

Named attribution assigns 49,539 shallow bytes to the SQL parser, 77,132 to
semantic lowering, 492,774 to the SQL executor family and 209,878 to SQL query
planning. The corresponding dynamic executor and plan families retain 351,653
and 166,912 shallow bytes. These signals overlap, but they show that most of
the original typed comparison is the maintained structural query engine. SQL's
remaining increment includes its required grammar/lowering and broader
projection, aggregate, explain and introspection semantics.

One DRY defect remains: entity routing performs a full parse before compiled
command lookup, and cache-miss compilation parses the SQL again. The first
parse is outside current compile/total attribution. The required parser cannot
be removed, however, and its entire named retained subtree is only 37,345
bytes. Parse-once therefore cannot conservatively satisfy this audit's 32 KiB
final-raw build gate even though it may save meaningful request instructions.

Full call-chain, authority, cost-ceiling and instruction evidence is in
[`artifacts/sql-ingress-owner-audit.md`](artifacts/sql-ingress-owner-audit.md).
Machine-readable findings are in [`findings.json`](findings.json).

## Verification Readout

- Clean predecessor production builds for all three actors -> PASS
- `wasm-audit-report.sh --skip-build` artifact capture -> PASS
- Per-canister size-report JSON + Twiggy artifacts -> PASS
- Focused SQL phase-attribution audit -> PASS
