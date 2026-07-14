# Resource Model Compliance Audit - 2026-03-05

Scope: conformance against `docs/contracts/RESOURCE_MODEL.md`.

## Checklist

| Requirement | Status | Evidence |
| ---- | ---- | ---- |
| 1. Class B operators route through budget-accounted execution context | PASS | `ExecutionContext` in `crates/icydb-core/src/db/executor/aggregate/contracts/grouped.rs` |
| 2. Class B DISTINCT insertions are budget-accounted | PASS | `ExecutionContext::admit_distinct_key` path via grouped distinct fold in `crates/icydb-core/src/db/executor/load/grouped_distinct.rs` |
| 3. Zero-key grouped uses implicit-single-group admission | PASS | `record_implicit_single_group` + grouped distinct callsites |
| 4. Grouped strategy labels do not imply streaming runtime behavior | PASS | grouped route executes materialized pathways and explicit grouped strategy contracts in route planner |
| 5. `SUM(DISTINCT)` / `COUNT(DISTINCT)` enforce deterministic caps | PASS | grouped distinct budget checks in `ExecutionContext` and grouped fold runtime |
| 6. No Class C shape reachable through grouped `HAVING + ORDER + LIMIT` | PASS | explicit coverage present: `grouped_plan_having_order_limit_composition_enforces_bounded_policy` in `crates/icydb-core/src/db/query/plan/tests/group.rs` |
| 7. Grouped `ORDER BY` policy is bounded (`LIMIT` gate) | PASS | `GroupPlanError::OrderRequiresLimit` in `crates/icydb-core/src/db/query/plan/validate/grouped/policy.rs` |

## Targeted Test Evidence

- `cargo test -p icydb-core grouped_plan_rejects_order_without_limit -- --nocapture` -> BLOCKED in that environment by a local test-execution issue

## Counts

- PASS: 7
- PARTIAL: 0
- FAIL: 0

## Follow-Ups

1. Re-run grouped resource-contract tests in a stable local environment to reconfirm runtime evidence.
