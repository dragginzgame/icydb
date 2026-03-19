//! Module: executor::route::capability
//! Responsibility: derive route capability snapshots from executable plans.
//! Does not own: fast-path execution dispatch or post-access kernel behavior.
//! Boundary: capability and eligibility helpers for route planning.

use crate::{
    db::{
        direction::Direction,
        executor::{
            aggregate::{
                AggregateExecutionPolicyInputs, derive_aggregate_execution_policy_for_model,
            },
            pipeline::contracts::LoadExecutor,
            route::{
                bounded_probe_hint_is_safe, pk_order_stream_fast_path_shape_supported_for_model,
                secondary_order_contract_active,
            },
        },
        query::{
            builder::AggregateExpr,
            plan::{AccessPlannedQuery, secondary_order_contract_is_deterministic},
        },
    },
    model::entity::EntityModel,
    traits::{EntityKind, EntitySchema, EntityValue},
};

use crate::db::executor::route::{ExecutionRoutePlan, RouteCapabilities};

/// Derive budget-safety flags for one plan at the route capability boundary.
pub(in crate::db::executor) fn derive_budget_safety_flags_for_model(
    model: &EntityModel,
    plan: &AccessPlannedQuery,
) -> (bool, bool, bool) {
    let logical = plan.scalar_plan();
    let has_residual_filter = logical.predicate.is_some();
    let access_order_satisfied_by_path =
        crate::db::executor::route::access_order_satisfied_by_route_contract_for_model(model, plan);
    let has_order = logical
        .order
        .as_ref()
        .is_some_and(|order| !order.fields.is_empty());
    let requires_post_access_sort = has_order && !access_order_satisfied_by_path;

    (
        has_residual_filter,
        access_order_satisfied_by_path,
        requires_post_access_sort,
    )
}

/// Derive budget-safety flags for one plan at the route capability boundary.
pub(in crate::db::executor) fn derive_budget_safety_flags<E>(
    plan: &AccessPlannedQuery,
) -> (bool, bool, bool)
where
    E: EntitySchema,
{
    derive_budget_safety_flags_for_model(E::MODEL, plan)
}

/// Return whether one plan shape is safe for direct streaming execution.
pub(in crate::db::executor) fn stream_order_contract_safe_for_model(
    model: &EntityModel,
    plan: &AccessPlannedQuery,
) -> bool {
    let (has_residual_filter, _, requires_post_access_sort) =
        derive_budget_safety_flags_for_model(model, plan);

    plan.scalar_plan().mode.is_load() && !has_residual_filter && !requires_post_access_sort
}

/// Return true when bounded physical fetch hints are valid for this direction.
pub(in crate::db::executor::route) const fn direction_allows_physical_fetch_hint(
    direction: Direction,
    desc_physical_reverse_supported: bool,
) -> bool {
    !matches!(direction, Direction::Desc) || desc_physical_reverse_supported
}

impl ExecutionRoutePlan {
    // Return the effective physical fetch hint for fallback stream resolution.
    // DESC fallback must disable bounded hints when reverse traversal is unavailable.
    pub(in crate::db::executor) const fn fallback_physical_fetch_hint(
        &self,
        direction: Direction,
    ) -> Option<usize> {
        if direction_allows_physical_fetch_hint(direction, self.desc_physical_reverse_supported()) {
            self.scan_hints.physical_fetch_hint
        } else {
            None
        }
    }
}

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    /// Derive one canonical execution capability snapshot for a plan + direction.
    pub(in crate::db::executor::route) fn derive_execution_capabilities(
        plan: &AccessPlannedQuery,
        direction: Direction,
        aggregate_expr: Option<&AggregateExpr>,
    ) -> RouteCapabilities {
        derive_execution_capabilities_for_model(E::MODEL, plan, direction, aggregate_expr)
    }
}

pub(in crate::db::executor::route) fn derive_execution_capabilities_for_model(
    model: &EntityModel,
    plan: &AccessPlannedQuery,
    direction: Direction,
    aggregate_expr: Option<&AggregateExpr>,
) -> RouteCapabilities {
    let access_class = plan.access_strategy().class();
    let (has_residual_filter, _, requires_post_access_sort) =
        derive_budget_safety_flags_for_model(model, plan);
    let aggregate_execution_policy = derive_aggregate_execution_policy_for_model(
        model,
        plan,
        direction,
        aggregate_expr,
        AggregateExecutionPolicyInputs::new(has_residual_filter, requires_post_access_sort),
    );
    let field_min_eligibility = aggregate_execution_policy.field_min_fast_path();
    let field_max_eligibility = aggregate_execution_policy.field_max_fast_path();

    RouteCapabilities {
        stream_order_contract_safe: stream_order_contract_safe_for_model(model, plan),
        pk_order_fast_path_eligible: pk_order_stream_fast_path_shape_supported_for_model(
            model, plan,
        ),
        desc_physical_reverse_supported: desc_physical_reverse_traversal_supported(plan, direction),
        count_pushdown_shape_supported: aggregate_execution_policy.count_pushdown_shape_supported(),
        count_pushdown_existing_rows_shape_supported: count_pushdown_existing_rows_shape_supported(
            &access_class,
        ),
        index_range_limit_pushdown_shape_supported:
            index_range_limit_pushdown_shape_supported_for_model(model, plan),
        composite_aggregate_fast_path_eligible: aggregate_execution_policy
            .composite_aggregate_fast_path_eligible(),
        bounded_probe_hint_safe: bounded_probe_hint_is_safe(plan),
        field_min_fast_path_eligible: field_min_eligibility.eligible,
        field_max_fast_path_eligible: field_max_eligibility.eligible,
        field_min_fast_path_ineligibility_reason: field_min_eligibility.ineligibility_reason,
        field_max_fast_path_ineligibility_reason: field_max_eligibility.ineligibility_reason,
    }
}

fn desc_physical_reverse_traversal_supported(
    plan: &AccessPlannedQuery,
    direction: Direction,
) -> bool {
    matches!(direction, Direction::Desc) && access_supports_reverse_traversal(plan)
}

fn access_supports_reverse_traversal(plan: &AccessPlannedQuery) -> bool {
    let access_strategy = plan.access_strategy();

    access_strategy.class().reverse_supported()
}

const fn count_pushdown_existing_rows_shape_supported(
    access_class: &crate::db::access::AccessRouteClass,
) -> bool {
    access_class.single_path() && (access_class.prefix_scan() || access_class.range_scan())
}

fn index_range_limit_pushdown_shape_supported_for_model(
    model: &EntityModel,
    plan: &AccessPlannedQuery,
) -> bool {
    let order = plan.scalar_plan().order.as_ref();
    let order_contract_eligible = order.is_none_or(|_| {
        secondary_order_contract_is_deterministic(model, plan.scalar_plan())
            && secondary_order_contract_active(
                plan.planner_route_profile(model)
                    .logical_pushdown_eligibility(),
            )
    });
    let access_class = plan.access_strategy().class();

    order_contract_eligible
        && access_class.index_range_limit_pushdown_shape_supported_for_order(
            order.map(|order| order.fields.as_slice()),
            model.primary_key.name,
        )
}
