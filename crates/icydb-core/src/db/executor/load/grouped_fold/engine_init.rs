use crate::{
    db::{
        executor::{
            aggregate::{AggregateEngine, ExecutionContext},
            load::{GroupedRouteStageProjection, LoadExecutor, invariant},
            route::aggregate_materialized_fold_direction,
        },
        query::plan::GroupedDistinctExecutionStrategy,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
    value::Value,
};

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    // Build grouped aggregate engines for canonical grouped terminal projection layout.
    #[expect(clippy::type_complexity)]
    pub(super) fn build_grouped_engines<R>(
        route: &R,
        grouped_execution_context: &ExecutionContext,
    ) -> Result<(Vec<AggregateEngine<E>>, Vec<Vec<Value>>), InternalError>
    where
        R: GroupedRouteStageProjection<E>,
    {
        if matches!(
            route.grouped_distinct_execution_strategy(),
            GroupedDistinctExecutionStrategy::GlobalDistinctFieldCount { .. }
                | GroupedDistinctExecutionStrategy::GlobalDistinctFieldSum { .. }
        ) {
            return Ok((Vec::new(), Vec::new()));
        }

        let grouped_engines = route
            .projection_layout()
            .aggregate_positions()
            .iter()
            .enumerate()
            .map(|(aggregate_index, projection_index)| {
                let aggregate_expr = route
                    .grouped_aggregate_exprs()
                    .get(aggregate_index)
                    .ok_or_else(|| {
                        invariant(format!(
                            "grouped aggregate index out of bounds for projection layout: projection_index={projection_index}, aggregate_index={aggregate_index}"
                        ))
                    })?;
                if aggregate_expr.target_field().is_some() {
                    return Err(invariant(format!(
                        "grouped field-target aggregate reached executor after planning: {:?}",
                        aggregate_expr.kind()
                    )));
                }

                Ok(grouped_execution_context.create_grouped_engine::<E>(
                    aggregate_expr.kind(),
                    aggregate_materialized_fold_direction(aggregate_expr.kind()),
                    aggregate_expr.is_distinct(),
                ))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let short_circuit_keys = vec![Vec::<Value>::new(); grouped_engines.len()];

        Ok((grouped_engines, short_circuit_keys))
    }
}
