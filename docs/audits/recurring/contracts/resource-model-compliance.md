# RECURRING AUDIT — Resource Model Compliance

## Purpose

Verify executor/planner behavior remains compliant with
`docs/contracts/RESOURCE_MODEL.md`.

This is a contract-compliance audit.
It is not a feature-design proposal.

---

## Scope

Check resource-model conformance for:

- planner proof vs runtime cap separation
- grouped budgeting and cardinality limits
- DISTINCT state accounting boundaries
- global DISTINCT field aggregate routing
- materialized non-grouped DISTINCT helper boundaries
- scalar scan-budget route gating
- grouped ordering/pagination policy guardrails
- grouped strategy labeling vs runtime behavior
- continuation/cursor interactions that affect boundedness
- runtime budget enforcement behavior under exhaustion
- budget propagation across planner/route/executor boundaries
- resource-class routing and classification coverage
- runtime observability counters that must remain diagnostic-only

---

## Required Checklist

For each run, explicitly mark `PASS` / `PARTIAL` / `FAIL` with concrete
evidence paths.

### Policy Compliance

1. All Class B operators route through budget-accounted execution context.
2. All Class B DISTINCT insertions are admitted through budget-accounted boundaries.
3. Zero-key grouped uses implicit-single-group admission.
4. Ordered-group strategy labels do not imply streaming runtime behavior.
5. `SUM(DISTINCT)` and `COUNT(DISTINCT)` enforce caps deterministically.
6. No Class C shape is reachable through grouped `HAVING + ORDER + LIMIT`.
7. Grouped `ORDER BY` policy is bounded (for example, explicit `LIMIT` gate).
8. Class B operators are never routed through unbudgeted execution paths.
9. Class C shapes are rejected before execution routing.
10. Planner boundedness proof remains separate from runtime caps.
11. Global DISTINCT field aggregates route through grouped Class B accounting.
12. Non-grouped materialized DISTINCT helpers remain effective-window bounded
    and do not become grouped Class B authorities.

### Runtime Enforcement

13. Distinct/group budget exhaustion fails closed (no unbounded fallback path).
14. Aggregation budget exhaustion propagates deterministic resource-class error.
15. Budget propagation is preserved across planner -> route -> executor ->
    grouped execution boundaries.
16. Runtime usage is monotonic and cannot exceed configured caps.
17. Resource-model bounded operators do not emit unbounded intermediate state.
18. Scalar scan-budget hints are shape-gated and disabled when continuation,
    order, or filter conditions violate the scan-budget contract.

### Budget Lifecycle and Coverage

19. Per-query resource budgets reset correctly between independent executions.
20. All grouped operators are explicitly classified under the resource model.
21. Grouped continuation signatures include budget-relevant shape so cursor
    reuse cannot cross incompatible grouped limits.
22. Runtime metrics counters such as `rows_scanned` and `rows_aggregated` stay
    diagnostic-only and do not affect planner, route, or executor behavior.

---

## Output Contract

Write one dated result file for each run:

- `docs/audits/reports/YYYY-MM/YYYY-MM-DD/resource-model-compliance*.md`

Result must include:

- run metadata + comparability note
  - compared baseline report path (daily baseline rule: first run of day
    compares to latest prior comparable report or `N/A`; same-day reruns
    compare to that day's `resource-model-compliance.md` baseline)
  - method tag/version
  - comparability status (`comparable` or `non-comparable` with reason)
- checklist tables grouped by:
  - `Policy Compliance`
  - `Runtime Enforcement`
  - `Budget Lifecycle and Coverage`
- each checklist row must include:
  - requirement
  - status (`PASS`/`PARTIAL`/`FAIL`)
  - evidence path(s)
  - short drift/regression risk note
- short pass/partial/fail counts
- explicit follow-up actions for each `PARTIAL`/`FAIL`
- verification readout (`PASS`/`FAIL`/`BLOCKED`)

Do not overwrite prior dated results.

## Baseline Verification Commands

Start with:

- `cargo test -p icydb-core db::executor::aggregate::contracts::grouped::tests::budget -- --nocapture`
- `cargo test -p icydb-core db::executor::group::tests::grouped_budget_observability_projects_budget_and_limits -- --nocapture`
- `cargo test -p icydb-core db::query::plan::tests::group::grouped_plan_rejects_validation_shape_matrix -- --nocapture`
- `cargo test -p icydb-core db::query::plan::tests::group::grouped_plan_having_order_limit_composition_enforces_bounded_policy -- --nocapture`
- `cargo test -p icydb-core db::query::plan::tests::group::grouped_plan_accepts_global_distinct_field_without_group_keys_matrix -- --nocapture`
- `cargo test -p icydb-core db::executor::aggregate::materialized_distinct::tests::insert_materialized_distinct_value_dedups_repeated_values -- --nocapture`
- `cargo test -p icydb-core db::executor::tests::aggregate_core::aggregate_core_sum_distinct_uses_grouped_global_distinct_path -- --nocapture`
- `cargo test -p icydb-core db::executor::tests::aggregate_core::aggregate_core_avg_distinct_uses_grouped_global_distinct_path -- --nocapture`
- `cargo test -p icydb-core db::executor::tests::aggregate_core::aggregate_core_grouped_scalar_distinct_policy_violation_fails_without_scan -- --nocapture`
- `cargo test -p icydb-core db::executor::planning::route::tests::route_matrix_load_unique_secondary_order_limit_one_uses_bounded_scan_budget_hint -- --nocapture`
- `cargo test -p icydb-core db::executor::planning::route::tests::route_matrix_load_non_pk_order_disables_scan_budget_hint -- --nocapture`
- `cargo test -p icydb-core db::executor::planning::route::tests::route_grouped_runtime_revalidation_flags_match_baseline -- --nocapture`
- `cargo test -p icydb-core db::executor::planning::route::tests::route_feature_budget_shape_kinds_stay_within_soft_delta -- --nocapture`
- `cargo test -p icydb-core db::query::fingerprint::shape_signature::tests::signature_changes_when_grouped_limits_change -- --nocapture`
- `cargo test -p icydb-core db::executor::tests::metrics::grouped_load_emits_rows_aggregated_metrics -- --nocapture`

Add targeted commands for any newly introduced Class B operator, scalar
scan-budget route, grouped continuation shape, or resource observability
surface.
