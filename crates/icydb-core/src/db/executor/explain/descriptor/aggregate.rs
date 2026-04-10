//! Module: db::executor::explain::descriptor::aggregate
//! Responsibility: module-local ownership and contracts for db::executor::explain::descriptor::aggregate.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::db::{
    executor::{
        ExecutionPreparation,
        preparation::slot_map_for_model_plan,
        route::{AggregateRouteShape, build_execution_route_plan_for_aggregate_spec},
    },
    query::{
        explain::{ExplainAccessPath as ExplainAccessRoute, ExplainExecutionDescriptor},
        plan::AccessPlannedQuery,
    },
    sql::lowering::PreparedSqlScalarAggregateStrategy,
};

use crate::db::executor::explain::descriptor::shared::{
    aggregate_covering_projection_for_terminal, explain_aggregate_ordering_source,
    explain_execution_mode, explain_node_properties_for_route,
};

// Assemble one canonical scalar aggregate execution descriptor through one
// planner-owned aggregate route-shape boundary.
#[inline(never)]
pub(in crate::db) fn assemble_aggregate_terminal_execution_descriptor(
    plan: &AccessPlannedQuery,
    aggregate: AggregateRouteShape<'_>,
) -> ExplainExecutionDescriptor {
    let aggregation = aggregate.kind();

    assemble_aggregate_terminal_execution_descriptor_from_shape(
        plan,
        aggregate,
        aggregation,
        aggregate.target_field(),
    )
}

// Assemble one canonical typed SQL scalar aggregate execution descriptor from
// one already-prepared SQL scalar strategy so EXPLAIN does not rediscover
// aggregate kind or projected-field shape from raw SQL terminal variants.
#[inline(never)]
pub(in crate::db) fn assemble_prepared_sql_scalar_aggregate_execution_descriptor(
    plan: &AccessPlannedQuery,
    strategy: &PreparedSqlScalarAggregateStrategy,
    aggregate: AggregateRouteShape<'_>,
) -> ExplainExecutionDescriptor {
    assemble_aggregate_terminal_execution_descriptor_from_shape(
        plan,
        aggregate,
        strategy.aggregate_kind(),
        strategy.projected_field(),
    )
}

fn assemble_aggregate_terminal_execution_descriptor_from_shape(
    plan: &AccessPlannedQuery,
    aggregate: AggregateRouteShape<'_>,
    aggregation: crate::db::query::plan::AggregateKind,
    projected_field: Option<&str>,
) -> ExplainExecutionDescriptor {
    // Phase 1: derive one aggregate route plan using precomputed execution preparation.
    let execution_preparation =
        ExecutionPreparation::from_plan(plan, slot_map_for_model_plan(plan));
    let route_plan =
        build_execution_route_plan_for_aggregate_spec(plan, aggregate, &execution_preparation);
    let route_shape = route_plan.shape();

    // Phase 2: project route-owned ordering + execution semantics into explain fields.
    let ordering_source = explain_aggregate_ordering_source(&route_plan, route_shape);
    let execution_mode = explain_execution_mode(route_shape);
    let covering_projection =
        aggregate_covering_projection_for_terminal(plan, aggregation, &execution_preparation);
    let node_properties = explain_node_properties_for_route(
        &route_plan,
        aggregation,
        projected_field,
        covering_projection,
    );

    // Phase 3: emit one stable descriptor payload consumed by explain surfaces.
    // Aggregate routes intentionally stay off the removed secondary-read label
    // surface. COUNT/EXISTS/extrema use their own planner-visible route
    // contracts, so aggregate EXPLAIN should not reintroduce load-side
    // correctness vocabulary here.
    ExplainExecutionDescriptor {
        access_strategy: ExplainAccessRoute::from_access_plan(&plan.access),
        // Covering flag reflects index-only aggregate fast-path eligibility for
        // scalar aggregate terminals.
        covering_projection,
        aggregation,
        execution_mode,
        ordering_source,
        limit: route_plan.continuation().limit(),
        cursor: route_plan.continuation().capabilities().applied(),
        node_properties,
    }
}
