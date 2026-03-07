//! Module: executor::route::capability
//! Responsibility: derive route capability snapshots from executable plans.
//! Does not own: fast-path execution dispatch or post-access kernel behavior.
//! Boundary: capability and eligibility helpers for route planning.

use crate::{
    db::{
        access::AccessPlan,
        direction::Direction,
        executor::{
            aggregate::capability::{
                AggregateExecutionPolicyInputs, derive_aggregate_execution_policy,
            },
            load::LoadExecutor,
            route::{access_order_satisfied_by_route_contract, secondary_order_contract_active},
        },
        query::{builder::AggregateExpr, plan::AccessPlannedQuery},
    },
    traits::{EntityKind, EntitySchema, EntityValue},
};

use crate::db::executor::route::{ExecutionRoutePlan, RouteCapabilities};

/// Derive budget-safety flags for one plan at the route capability boundary.
pub(in crate::db::executor) fn derive_budget_safety_flags<E, K>(
    plan: &AccessPlannedQuery<K>,
) -> (bool, bool, bool)
where
    E: EntitySchema<Key = K>,
{
    let logical = plan.scalar_plan();
    let has_residual_filter = logical.predicate.is_some();
    let access_order_satisfied_by_path = access_order_satisfied_by_path::<E, K>(plan);
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

/// Return whether one plan shape is safe for direct streaming execution.
pub(in crate::db::executor) fn streaming_access_shape_safe<E, K>(
    plan: &AccessPlannedQuery<K>,
) -> bool
where
    E: EntitySchema<Key = K>,
{
    if !plan.scalar_plan().mode.is_load() {
        return false;
    }

    let (has_residual_filter, _, requires_post_access_sort) =
        derive_budget_safety_flags::<E, K>(plan);
    if has_residual_filter {
        return false;
    }
    if requires_post_access_sort {
        return false;
    }

    true
}

fn access_order_satisfied_by_path<E, K>(plan: &AccessPlannedQuery<K>) -> bool
where
    E: EntitySchema<Key = K>,
{
    access_order_satisfied_by_route_contract::<E, K>(plan)
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
    /// Derive one canonical route capability snapshot for a plan + direction.
    pub(in crate::db::executor::route) fn derive_route_capabilities(
        plan: &AccessPlannedQuery<E::Key>,
        direction: Direction,
        aggregate_expr: Option<&AggregateExpr>,
    ) -> RouteCapabilities {
        let access_class = plan.access_strategy().class();
        let (has_residual_filter, _, requires_post_access_sort) =
            derive_budget_safety_flags::<E, _>(plan);
        let aggregate_execution_policy = derive_aggregate_execution_policy::<E>(
            plan,
            direction,
            aggregate_expr,
            AggregateExecutionPolicyInputs::new(has_residual_filter, requires_post_access_sort),
        );
        let field_min_eligibility = aggregate_execution_policy.field_min_fast_path();
        let field_max_eligibility = aggregate_execution_policy.field_max_fast_path();

        RouteCapabilities {
            streaming_access_shape_safe: streaming_access_shape_safe::<E, _>(plan),
            pk_order_fast_path_eligible: Self::pk_order_stream_fast_path_shape_supported(plan),
            desc_physical_reverse_supported: Self::is_desc_physical_reverse_traversal_supported(
                &plan.access,
                direction,
            ),
            count_pushdown_access_shape_supported: aggregate_execution_policy
                .count_pushdown_access_shape_supported(),
            count_pushdown_existing_rows_shape_supported:
                Self::count_pushdown_existing_rows_shape_supported(access_class),
            index_range_limit_pushdown_shape_eligible:
                Self::is_index_range_limit_pushdown_shape_eligible(plan),
            composite_aggregate_fast_path_eligible: aggregate_execution_policy
                .composite_aggregate_fast_path_eligible(),
            bounded_probe_hint_safe: Self::bounded_probe_hint_is_safe(plan),
            field_min_fast_path_eligible: field_min_eligibility.eligible,
            field_max_fast_path_eligible: field_max_eligibility.eligible,
            field_min_fast_path_ineligibility_reason: field_min_eligibility.ineligibility_reason,
            field_max_fast_path_ineligibility_reason: field_max_eligibility.ineligibility_reason,
        }
    }

    /// Return whether DESC physical reverse traversal is supported for this access shape.
    pub(super) fn is_desc_physical_reverse_traversal_supported(
        access: &AccessPlan<E::Key>,
        direction: Direction,
    ) -> bool {
        if !matches!(direction, Direction::Desc) {
            return false;
        }

        Self::access_supports_reverse_traversal(access)
    }

    fn access_supports_reverse_traversal(access: &AccessPlan<E::Key>) -> bool {
        let access_strategy = access.resolve_strategy();

        access_strategy.class().reverse_supported()
    }

    // Route-owned gate for COUNT streaming paths that must preserve stale-key
    // safety through `ExistingRows` fold mode on secondary index traversal.
    const fn count_pushdown_existing_rows_shape_supported(
        access_class: crate::db::access::AccessRouteClass,
    ) -> bool {
        access_class.single_path() && (access_class.prefix_scan() || access_class.range_scan())
    }

    // Route-owned shape gate for index-range limited pushdown eligibility.
    pub(super) fn is_index_range_limit_pushdown_shape_eligible(
        plan: &AccessPlannedQuery<E::Key>,
    ) -> bool {
        let order = plan.scalar_plan().order.as_ref();
        if order.is_some() {
            let logical_pushdown_eligibility = plan
                .planner_route_profile(E::MODEL)
                .logical_pushdown_eligibility();
            if !secondary_order_contract_active(logical_pushdown_eligibility) {
                return false;
            }
        }

        let access_class = plan.access_strategy().class();
        access_class.index_range_limit_pushdown_shape_eligible_for_order(
            order.map(|order| order.fields.as_slice()),
            E::MODEL.primary_key.name,
        )
    }
}
