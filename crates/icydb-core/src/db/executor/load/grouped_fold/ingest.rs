use std::cmp::Ordering;

use crate::{
    db::{
        Context,
        contracts::canonical_value_compare,
        data::DataKey,
        executor::{
            aggregate::{AggregateEngine, ExecutionContext, FoldControl},
            group::CanonicalKey,
            load::{GroupedRouteStageProjection, GroupedStreamStage, LoadExecutor},
        },
        predicate::MissingRowPolicy,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
    value::Value,
};

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    // Ingest grouped source rows into aggregate reducers while preserving budget contracts.
    pub(super) fn ingest_grouped_rows_into_engines<R>(
        route: &R,
        stream: &mut GroupedStreamStage<'_, E>,
        grouped_execution_context: &mut ExecutionContext,
        grouped_engines: &mut [AggregateEngine<E>],
        short_circuit_keys: &mut [Vec<Value>],
        max_groups_bound: usize,
    ) -> Result<(usize, usize), InternalError>
    where
        R: GroupedRouteStageProjection<E>,
    {
        let mut scanned_rows = 0usize;
        let mut filtered_rows = 0usize;
        let (ctx, execution_preparation, resolved) = stream.parts_mut();
        let compiled_predicate = execution_preparation.compiled_predicate();

        while let Some(key) = resolved.key_stream_mut().next_key()? {
            let row = match route.plan().scalar_plan().consistency {
                MissingRowPolicy::Error => ctx.read_strict(&key),
                MissingRowPolicy::Ignore => ctx.read(&key),
            };
            let row = match row {
                Ok(row) => row,
                Err(err) if err.is_not_found() => continue,
                Err(err) => return Err(err),
            };
            scanned_rows = scanned_rows.saturating_add(1);
            let (id, entity) = Context::<E>::deserialize_row((key, row))?;
            if let Some(compiled_predicate) = compiled_predicate
                && !compiled_predicate.eval(&entity)
            {
                continue;
            }
            filtered_rows = filtered_rows.saturating_add(1);

            let group_values = route
                .group_fields()
                .iter()
                .map(|field| {
                    entity.get_value_by_index(field.index()).ok_or_else(|| {
                        crate::db::executor::load::invariant(format!(
                            "grouped field slot missing on entity: index={}",
                            field.index()
                        ))
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            let group_key = Value::List(group_values)
                .canonical_key()
                .map_err(crate::db::executor::group::KeyCanonicalError::into_internal_error)?;
            let canonical_group_value = group_key.canonical_value().clone();
            let data_key = DataKey::try_new::<E>(id.key())?;

            for (index, engine) in grouped_engines.iter_mut().enumerate() {
                if short_circuit_keys[index].iter().any(|done| {
                    canonical_value_compare(done, &canonical_group_value) == Ordering::Equal
                }) {
                    continue;
                }

                let fold_control = engine
                    .ingest_grouped(group_key.clone(), &data_key, grouped_execution_context)
                    .map_err(Self::map_group_error)?;
                if matches!(fold_control, FoldControl::Break) {
                    short_circuit_keys[index].push(canonical_group_value.clone());
                    debug_assert!(
                        short_circuit_keys[index].len() <= max_groups_bound,
                        "grouped short-circuit key tracking must stay bounded by max_groups",
                    );
                }
            }
        }

        Ok((scanned_rows, filtered_rows))
    }
}
