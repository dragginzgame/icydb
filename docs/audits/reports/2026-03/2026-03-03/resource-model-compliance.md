# Resource Model Compliance Audit - 2026-03-03

Scope: conformance against `docs/contracts/RESOURCE_MODEL.md`.

## Checklist

| Requirement | Status | Evidence |
| ---- | ---- | ---- |
| 1. Class B operators route through budget-accounted execution context | PASS | `ExecutionContext` in `crates/icydb-core/src/db/executor/aggregate/contracts/grouped.rs` |
| 2. Class B DISTINCT insertions are budget-accounted | PASS | `ExecutionContext::admit_distinct_key` path via grouped distinct fold in `crates/icydb-core/src/db/executor/load/grouped_distinct.rs` |
| 3. Zero-key grouped uses implicit-single-group admission | PASS | `record_implicit_single_group` + callsite in `crates/icydb-core/src/db/executor/load/grouped_distinct.rs` |
| 4. Grouped strategy labels do not imply streaming runtime behavior | PASS | grouped route executes materialized in `crates/icydb-core/src/db/executor/route/planner/execution.rs`; strategy is `*Materialized` in route contracts |
| 5. `SUM(DISTINCT)` / `COUNT(DISTINCT)` enforce deterministic caps | PASS | grouped distinct budget checks in `ExecutionContext` and grouped distinct fold runtime |
| 6. No Class C shape reachable through grouped `HAVING + ORDER + LIMIT` | PARTIAL | bounded by grouped hard limits + grouped order policy; no dedicated single test matrix proving this exact composition path |
| 7. Grouped `ORDER BY` policy is bounded (`LIMIT` gate) | PASS | `GroupPlanError::OrderRequiresLimit` enforced in `crates/icydb-core/src/db/query/plan/validate/grouped.rs` |

## Targeted Test Evidence

- `cargo test -p icydb-core grouped_plan_rejects_order_without_limit -- --nocapture` -> PASS

## Counts

- PASS: 6
- PARTIAL: 1
- FAIL: 0

## Follow-Ups

1. Add one explicit grouped `HAVING + ORDER + LIMIT` resource-composition matrix test covering both accepted and rejected shapes.

