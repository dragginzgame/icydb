# Recurring Audit Summary - 2026-04-13

## Report Preamble

- scope: crosscutting recurring subset run (`canonical-semantic-authority`)
- compared baseline report path: `docs/audits/reports/2026-03/2026-03-24/summary.md`
- code snapshot identifier: `d23cd2cf5`
- method tag/version: `Method V3`
- comparability status: `non-comparable` (standalone subset run, and the current canonical-semantic-authority rerun rebuilt raw-count artifacts without the missing checked-in baseline TSVs)

## Audit Run Order and Results

1. `crosscutting/crosscutting-canonical-semantic-authority` -> `canonical-semantic-authority.md` (Risk: 3.8/10)

## Global Findings

- Canonical semantic authority remains structurally stable (`3.8/10`) with no high-risk drift triggers and no missing canonical typed models across the `8` tracked concept families.
- Owner-count range remains `2..4`, boundary-count range remains `2..3`, and confirmed owner drift count is `0`.
- The main monitored semantic seam is now the bounded `SqlTextFunctionCall -> Expr::FunctionCall` lowering path plus executor-owned text-function evaluation, rather than a new planner/runtime/facade split.
- Recent executor/session test-helper consolidation and the removed demo-only wasm profile shim reduced support-surface noise, but they did not add or remove canonical semantic owners in the DB semantics path.
- The newer unified fluent/public query result surfaces and the direct public SQL payload proof improve edge confidence without introducing a second semantic owner for grouped or SQL results.

## Follow-Up Actions

- No mandatory follow-up actions for this subset run.
- Monitoring-only: keep the bounded scalar text-function slice on the canonical `Expr::FunctionCall` path with executor-owned evaluation, and keep grouped computed projection fail-closed until it is designed explicitly.
- Monitoring-only: keep schema/runtime `IndexKeyItem` parity and diagnostic-only `canonical_text(...)` renderers in the next canonical semantic authority cycle.

## Verification Readout

- `bash scripts/ci/check-layer-authority-invariants.sh` -> PASS
- `cargo check -p icydb-core` -> PASS
- `cargo test -p icydb-core canonicalization_ownership_stays_in_access_and_predicate_layers -- --nocapture` -> PASS
