//! Module: db::executor::explain::descriptor::aggregate
//! Responsibility: module-local ownership and contracts for db::executor::explain::descriptor::aggregate.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::db::{
    executor::{
        ExecutionPreparation,
        preparation::slot_map_for_model_plan,
        route::{AggregateSeekSpec, build_execution_route_plan_for_aggregate_spec_with_model},
    },
    query::{
        builder::AggregateExpr,
        explain::{
            ExplainAccessPath as ExplainAccessRoute, ExplainExecutionDescriptor,
            ExplainExecutionOrderingSource,
        },
        plan::AccessPlannedQuery,
    },
};
use std::str;

use crate::db::executor::explain::descriptor::shared::{
    aggregate_covering_projection_for_terminal, explain_execution_mode,
    explain_node_properties_for_route,
};

// Assemble one canonical scalar aggregate execution descriptor through one
// model-owned authority path.
#[inline(never)]
pub(in crate::db) fn assemble_aggregate_terminal_execution_descriptor_with_model(
    model: &'static crate::model::entity::EntityModel,
    plan: &AccessPlannedQuery,
    aggregate: AggregateExpr,
) -> ExplainExecutionDescriptor {
    let aggregation = aggregate.kind();
    let projected_field = aggregate.target_field().map(str::to_string);

    // Phase 1: derive one aggregate route plan using precomputed execution preparation.
    let execution_preparation =
        ExecutionPreparation::from_plan(model, plan, slot_map_for_model_plan(model, plan));
    let route_plan = build_execution_route_plan_for_aggregate_spec_with_model(
        model,
        plan,
        aggregate,
        &execution_preparation,
    );
    let route_shape = route_plan.shape();

    // Phase 2: project route-owned ordering + execution semantics into explain fields.
    let ordering_source = match route_plan.aggregate_seek_spec() {
        Some(AggregateSeekSpec::First { fetch }) => {
            ExplainExecutionOrderingSource::IndexSeekFirst { fetch }
        }
        Some(AggregateSeekSpec::Last { fetch }) => {
            ExplainExecutionOrderingSource::IndexSeekLast { fetch }
        }
        None if route_shape.is_materialized() => ExplainExecutionOrderingSource::Materialized,
        None => ExplainExecutionOrderingSource::AccessOrder,
    };
    let execution_mode = explain_execution_mode(route_shape);
    let covering_projection =
        aggregate_covering_projection_for_terminal(plan, aggregation, &execution_preparation);
    let node_properties = explain_node_properties_for_route(
        &route_plan,
        aggregation,
        projected_field.as_deref(),
        covering_projection,
    );

    // Phase 3: emit one stable descriptor payload consumed by explain surfaces.
    // Aggregate routes intentionally do not inherit the secondary-read
    // authority labels here. Aggregate COUNT/EXISTS/extrema shortcuts still
    // need their own missing-row sensitivity classification, so aggregate
    // EXPLAIN must stay authority-unclassified until that model exists.
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
