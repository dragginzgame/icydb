//! Module: db::executor::aggregate::runtime::grouped_fold::engine_init
//! Responsibility: module-local ownership and contracts for db::executor::aggregate::runtime::grouped_fold::engine_init.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::executor::{
        aggregate::{
            ExecutionContext, GroupedAggregateEngine,
            runtime::grouped_distinct::global_distinct_field_target_and_kind,
        },
        pipeline::contracts::GroupedRouteStage,
        route::aggregate_materialized_fold_direction,
    },
    error::InternalError,
    value::Value,
};

// Build grouped aggregate engines for canonical grouped terminal projection layout.
#[expect(clippy::type_complexity)]
pub(super) fn build_grouped_engines(
    route: &GroupedRouteStage,
    grouped_execution_context: &ExecutionContext,
) -> Result<(Vec<Box<dyn GroupedAggregateEngine>>, Vec<Vec<Value>>), InternalError> {
    if global_distinct_field_target_and_kind(route.grouped_distinct_execution_strategy()).is_some()
    {
        return Ok((Vec::new(), Vec::new()));
    }

    let grouped_engines = route
        .projection_layout()
        .aggregate_positions()
        .iter()
        .enumerate()
        .map(|(aggregate_index, projection_index)| {
            let aggregate_spec = route
                .grouped_aggregate_execution_specs()
                .get(aggregate_index)
                .ok_or_else(|| {
                    GroupedRouteStage::aggregate_index_out_of_bounds_for_projection_layout(
                        *projection_index,
                        aggregate_index,
                    )
                })?;

            Ok::<Box<dyn GroupedAggregateEngine>, InternalError>(Box::new(
                grouped_execution_context.create_grouped_state_with_target(
                    aggregate_spec.kind(),
                    aggregate_materialized_fold_direction(aggregate_spec.kind()),
                    aggregate_spec.distinct(),
                    aggregate_spec.target_field().cloned(),
                )?,
            )
                as Box<dyn GroupedAggregateEngine>)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let short_circuit_keys = vec![Vec::<Value>::new(); grouped_engines.len()];

    Ok((grouped_engines, short_circuit_keys))
}
