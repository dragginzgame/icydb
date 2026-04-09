//! Module: db::executor::aggregate::runtime::grouped_fold::engine_init
//! Responsibility: module-local ownership and contracts for db::executor::aggregate::runtime::grouped_fold::engine_init.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::executor::{
        aggregate::{
            ExecutionContext,
            runtime::{
                grouped_distinct::global_distinct_field_target_and_kind,
                grouped_fold::bundle::{GroupedAggregateBundle, GroupedAggregateBundleSpec},
            },
        },
        pipeline::contracts::GroupedRouteStage,
        route::aggregate_materialized_fold_direction,
    },
    error::InternalError,
};

// Build the shared grouped aggregate bundle for canonical grouped terminal projection layout.
pub(super) fn build_grouped_bundle(
    route: &GroupedRouteStage,
    grouped_execution_context: &ExecutionContext,
) -> Result<GroupedAggregateBundle, InternalError> {
    if global_distinct_field_target_and_kind(route.grouped_distinct_execution_strategy()).is_some()
    {
        return Ok(GroupedAggregateBundle::new(Vec::new()));
    }

    let grouped_specs = route
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

            Ok::<GroupedAggregateBundleSpec, InternalError>(GroupedAggregateBundleSpec::new(
                aggregate_spec.kind(),
                aggregate_materialized_fold_direction(aggregate_spec.kind()),
                aggregate_spec.distinct(),
                aggregate_spec.target_field().cloned(),
                grouped_execution_context
                    .config()
                    .max_distinct_values_per_group(),
            ))
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(GroupedAggregateBundle::new(grouped_specs))
}
