# Resource Model Compliance Audit - 2026-05-14

## Run Metadata + Comparability Note

- scope: resource-model contract compliance for grouped budgeting, DISTINCT
  accounting, scan-budget route gating, continuation signatures, and
  diagnostic-only resource observability
- compared baseline report path:
  `docs/audits/reports/2026-03/2026-03-12/resource-model-compliance.md`
- code snapshot identifier: `499a8478a` plus local uncommitted audit/design and
  schema-reconcile split changes
- method tag/version: `Method V4`
- comparability status: `non-comparable`
- non-comparable reason: Method V4 expands the run from three smoke checks to
  the full current resource-model contract surface.

## Method Changes

- Added planner-proof vs runtime-cap separation to the recurring checklist.
- Added global DISTINCT field aggregate routing and non-grouped materialized
  DISTINCT helper boundary checks.
- Added scalar scan-budget route gating checks.
- Added grouped continuation signature and diagnostic metrics checks.
- Added explicit baseline verification commands to prevent future runs from
  collapsing to partial smoke coverage.

## Policy Compliance

| Requirement | Status | Evidence | Drift / Regression Risk |
| ----------- | ------ | -------- | ----------------------- |
| All Class B operators route through budget-accounted execution context. | PASS | `crates/icydb-core/src/db/executor/aggregate/runtime/grouped_fold/mod.rs`; `cargo test -p icydb-core db::executor::aggregate::contracts::grouped::tests::budget -- --nocapture` | Low |
| All Class B DISTINCT insertions are admitted through budget-accounted boundaries. | PASS | `crates/icydb-core/src/db/executor/aggregate/contracts/grouped/context.rs`; grouped budget tests | Low |
| Zero-key grouped uses implicit-single-group admission. | PASS | `crates/icydb-core/src/db/executor/aggregate/runtime/grouped_fold/distinct.rs`; `grouped_plan_accepts_global_distinct_field_without_group_keys_matrix` | Low |
| Ordered-group strategy labels do not imply streaming runtime behavior. | PASS | `docs/contracts/RESOURCE_MODEL.md`; `route_grouped_runtime_revalidation_flags_match_baseline` | Low |
| `SUM(DISTINCT)` and `COUNT(DISTINCT)` enforce caps deterministically. | PASS | grouped budget tests; `aggregate_core_sum_distinct_uses_grouped_global_distinct_path`; `aggregate_core_avg_distinct_uses_grouped_global_distinct_path` | Low |
| No Class C shape is reachable through grouped `HAVING + ORDER + LIMIT`. | PASS | `grouped_plan_having_order_limit_composition_enforces_bounded_policy` | Low |
| Grouped `ORDER BY` policy is bounded by explicit admission gates. | PASS | `grouped_plan_rejects_validation_shape_matrix` | Low |
| Class B operators are never routed through unbudgeted execution paths. | PASS | grouped fold context creation and DISTINCT admission tests | Low |
| Class C shapes are rejected before execution routing. | PASS | `grouped_plan_rejects_validation_shape_matrix`; `aggregate_core_grouped_scalar_distinct_policy_violation_fails_without_scan` | Low |
| Planner boundedness proof remains separate from runtime caps. | PASS | `docs/contracts/RESOURCE_MODEL.md`; grouped validation matrix and grouped budget runtime tests cover both boundaries separately | Low |
| Global DISTINCT field aggregates route through grouped Class B accounting. | PASS | `aggregate_core_sum_distinct_uses_grouped_global_distinct_path`; `aggregate_core_avg_distinct_uses_grouped_global_distinct_path` | Low |
| Non-grouped materialized DISTINCT helpers remain effective-window bounded and do not become grouped Class B authorities. | PASS | `insert_materialized_distinct_value_dedups_repeated_values`; `docs/contracts/RESOURCE_MODEL.md` | Low |

## Runtime Enforcement

| Requirement | Status | Evidence | Drift / Regression Risk |
| ----------- | ------ | -------- | ----------------------- |
| Distinct/group budget exhaustion fails closed. | PASS | grouped budget tests for max groups, estimated bytes, per-group distinct, and total distinct | Low |
| Aggregation budget exhaustion propagates deterministic resource-class error. | PASS | grouped budget tests assert typed `GroupError` resource names and limits | Low |
| Budget propagation is preserved across planner -> route -> executor -> grouped execution boundaries. | PASS | `execute_group_fold_stage` builds `ExecutionContext` from route grouped execution; grouped observability test passes | Low |
| Runtime usage is monotonic and cannot exceed configured caps. | PASS | `grouped_execution_budget_counters_remain_consistent_for_distinct_grouped_fold`; grouped hard-limit tests | Low |
| Resource-model bounded operators do not emit unbounded intermediate state. | PASS | grouped validation policy plus runtime hard-limit tests | Low |
| Scalar scan-budget hints are shape-gated and disabled when unsafe. | PASS | `route_matrix_load_unique_secondary_order_limit_one_uses_bounded_scan_budget_hint`; `route_matrix_load_non_pk_order_disables_scan_budget_hint` | Low |

## Budget Lifecycle and Coverage

| Requirement | Status | Evidence | Drift / Regression Risk |
| ----------- | ------ | -------- | ----------------------- |
| Per-query resource budgets reset correctly between independent executions. | PASS | grouped fold creates a fresh `ExecutionContext` per route execution; grouped budget tests construct independent contexts | Low |
| All grouped operators are explicitly classified under the resource model. | PASS | `docs/contracts/RESOURCE_MODEL.md`; `route_feature_budget_shape_kinds_stay_within_soft_delta`; grouped validation matrix | Low-Medium |
| Grouped continuation signatures include budget-relevant shape. | PASS | `signature_changes_when_grouped_limits_change` | Low |
| Runtime metrics counters remain diagnostic-only. | PASS | `grouped_load_emits_rows_aggregated_metrics`; `docs/contracts/RESOURCE_MODEL.md` | Low |

## Counts

- PASS: 22
- PARTIAL: 0
- FAIL: 0

## Overall Resource Compliance Risk Index

**2/10**

The core resource contract is healthy. The main risk was not current runtime
behavior; it was audit drift. The recurring definition now has concrete
baseline commands for the current contract surface.

## Verification Readout

- `cargo test -p icydb-core db::executor::aggregate::contracts::grouped::tests::budget -- --nocapture` -> PASS
- `cargo test -p icydb-core db::executor::group::tests::grouped_budget_observability_projects_budget_and_limits -- --nocapture` -> PASS
- `cargo test -p icydb-core db::query::plan::tests::group::grouped_plan_rejects_validation_shape_matrix -- --nocapture` -> PASS
- `cargo test -p icydb-core db::query::plan::tests::group::grouped_plan_having_order_limit_composition_enforces_bounded_policy -- --nocapture` -> PASS
- `cargo test -p icydb-core db::query::plan::tests::group::grouped_plan_accepts_global_distinct_field_without_group_keys_matrix -- --nocapture` -> PASS
- `cargo test -p icydb-core db::executor::aggregate::materialized_distinct::tests::insert_materialized_distinct_value_dedups_repeated_values -- --nocapture` -> PASS
- `cargo test -p icydb-core db::executor::tests::aggregate_core::aggregate_core_sum_distinct_uses_grouped_global_distinct_path -- --nocapture` -> PASS
- `cargo test -p icydb-core db::executor::tests::aggregate_core::aggregate_core_avg_distinct_uses_grouped_global_distinct_path -- --nocapture` -> PASS
- `cargo test -p icydb-core db::executor::tests::aggregate_core::aggregate_core_grouped_scalar_distinct_policy_violation_fails_without_scan -- --nocapture` -> PASS
- `cargo test -p icydb-core db::executor::planning::route::tests::route_matrix_load_unique_secondary_order_limit_one_uses_bounded_scan_budget_hint -- --nocapture` -> PASS
- `cargo test -p icydb-core db::executor::planning::route::tests::route_matrix_load_non_pk_order_disables_scan_budget_hint -- --nocapture` -> PASS
- `cargo test -p icydb-core db::executor::planning::route::tests::route_grouped_runtime_revalidation_flags_match_baseline -- --nocapture` -> PASS
- `cargo test -p icydb-core db::executor::planning::route::tests::route_feature_budget_shape_kinds_stay_within_soft_delta -- --nocapture` -> PASS
- `cargo test -p icydb-core db::query::fingerprint::shape_signature::tests::signature_changes_when_grouped_limits_change -- --nocapture` -> PASS
- `cargo test -p icydb-core db::executor::tests::metrics::grouped_load_emits_rows_aggregated_metrics -- --nocapture` -> PASS

## Follow-Up Actions

No runtime follow-up is required. Keep the expanded Method V4 baseline for the
next comparable run, and add one targeted command whenever a new Class B
operator, scan-budget route, grouped continuation shape, or resource
observability surface is introduced.
