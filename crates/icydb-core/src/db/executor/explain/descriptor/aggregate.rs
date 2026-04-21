//! Module: db::executor::explain::descriptor::aggregate
//! Responsibility: assemble stable EXPLAIN descriptor payloads for scalar
//! aggregate terminals from planner-owned aggregate route shapes.
//! Does not own: aggregate route selection policy or final explain rendering formats.
//! Boundary: projects aggregate execution contracts into descriptor fields consumed by explain surfaces.

use crate::db::{
    executor::{
        ExecutionPreparation,
        planning::preparation::slot_map_for_model_plan,
        route::{
            AggregateRouteShape, ExecutionRoutePlan, build_execution_route_plan_for_aggregate_spec,
        },
    },
    query::{
        explain::{ExplainExecutionDescriptor, explain_access_plan},
        plan::{AccessPlannedQuery, AggregateKind},
    },
};

use crate::db::executor::explain::descriptor::shared::{
    aggregate_covering_projection_for_terminal, explain_aggregate_ordering_source,
    explain_execution_mode, explain_node_properties_for_route,
};

///
/// AggregateExplainPreparation
///
/// AggregateExplainPreparation bundles the route-plan and covering-projection
/// facts derived once for aggregate EXPLAIN assembly so the final descriptor
/// projection does not rebuild aggregate execution routing details inline.
///

struct AggregateExplainPreparation {
    route_plan: ExecutionRoutePlan,
    covering_projection: bool,
}

impl AggregateExplainPreparation {
    // Build the aggregate EXPLAIN preparation bundle once from the logical
    // plan and aggregate route shape so route selection and covering
    // projection stay aligned for all aggregate descriptor projections.
    fn from_shape(
        plan: &AccessPlannedQuery,
        aggregate: AggregateRouteShape<'_>,
        aggregation: AggregateKind,
    ) -> Self {
        let execution_preparation =
            ExecutionPreparation::from_plan(plan, slot_map_for_model_plan(plan));
        let route_plan =
            build_execution_route_plan_for_aggregate_spec(plan, aggregate, &execution_preparation);
        let covering_projection =
            aggregate_covering_projection_for_terminal(plan, aggregation, &execution_preparation);

        Self {
            route_plan,
            covering_projection,
        }
    }
}

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

// Assemble one canonical scalar aggregate execution descriptor from one
// aggregate shape plus preselected aggregation semantics.
#[inline(never)]
pub(in crate::db) fn assemble_scalar_aggregate_execution_descriptor_with_projection(
    plan: &AccessPlannedQuery,
    aggregate: AggregateRouteShape<'_>,
    aggregation: AggregateKind,
    projected_field: Option<&str>,
) -> ExplainExecutionDescriptor {
    assemble_aggregate_terminal_execution_descriptor_from_shape(
        plan,
        aggregate,
        aggregation,
        projected_field,
    )
}

fn assemble_aggregate_terminal_execution_descriptor_from_shape(
    plan: &AccessPlannedQuery,
    aggregate: AggregateRouteShape<'_>,
    aggregation: AggregateKind,
    projected_field: Option<&str>,
) -> ExplainExecutionDescriptor {
    // Phase 1: derive one aggregate route plan using precomputed execution preparation.
    let explain_preparation = AggregateExplainPreparation::from_shape(plan, aggregate, aggregation);

    // Phase 2: project route-owned ordering + execution semantics into explain fields.
    let ordering_source = explain_aggregate_ordering_source(&explain_preparation.route_plan);
    let execution_mode = explain_execution_mode(&explain_preparation.route_plan);
    let node_properties = explain_node_properties_for_route(
        &explain_preparation.route_plan,
        aggregation,
        projected_field,
        explain_preparation.covering_projection,
    );

    // Phase 3: emit one stable descriptor payload consumed by explain surfaces.
    // Aggregate routes intentionally stay off the removed secondary-read label
    // surface. COUNT/EXISTS/extrema use their own planner-visible route
    // contracts, so aggregate EXPLAIN should not reintroduce load-side
    // correctness vocabulary here.
    ExplainExecutionDescriptor {
        access_strategy: explain_access_plan(&plan.access),
        // Covering flag reflects index-only aggregate fast-path eligibility for
        // scalar aggregate terminals.
        covering_projection: explain_preparation.covering_projection,
        aggregation,
        execution_mode,
        ordering_source,
        limit: explain_preparation.route_plan.continuation().limit(),
        cursor: explain_preparation.route_plan.continuation().applied(),
        node_properties,
    }
}
