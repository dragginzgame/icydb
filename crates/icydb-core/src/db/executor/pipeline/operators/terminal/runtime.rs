use crate::{
    db::{
        Context,
        cursor::CursorBoundary,
        data::DataKey,
        executor::{
            ExecutionKernel, KeyStreamLoopControl, LoadExecutor, OrderedKeyStream,
            traversal::row_read_consistency_for_plan,
        },
        query::plan::AccessPlannedQuery,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
    types::Id,
};

impl ExecutionKernel {
    // Return whether load execution can safely use the row-collector short path
    // without changing cursor/pagination/filter semantics.
    pub(in crate::db::executor::pipeline::operators::terminal) const fn load_row_collector_short_path_eligible<
        K,
    >(
        plan: &AccessPlannedQuery<K>,
        cursor_boundary: Option<&CursorBoundary>,
    ) -> bool {
        let logical = plan.scalar_plan();
        logical.mode.is_load()
            && cursor_boundary.is_none()
            && logical.predicate.is_none()
            && logical.order.is_none()
            && logical.page.is_none()
    }

    // Run one row-collector stream over the already decorated
    // key stream. Rows are fetched only for keys that survive upstream stream
    // decorators and staged as canonical `(Id<E>, E)` outputs.
    #[expect(clippy::type_complexity)]
    pub(in crate::db::executor::pipeline::operators::terminal) fn run_row_collector_stream<E>(
        ctx: &Context<'_, E>,
        plan: &AccessPlannedQuery<E::Key>,
        key_stream: &mut dyn OrderedKeyStream,
    ) -> Result<(Vec<(Id<E>, E)>, usize), InternalError>
    where
        E: EntityKind + EntityValue,
    {
        // Phase 1: initialize row staging and read-consistency policy.
        let mut rows: Vec<(Id<E>, E)> = Vec::new();
        let mut keys_scanned = 0usize;
        let consistency = row_read_consistency_for_plan(plan);
        let mut pre_key = || KeyStreamLoopControl::Emit;
        let mut on_key =
            |data_key: DataKey, entity: Option<E>| -> Result<KeyStreamLoopControl, InternalError> {
                let Some(entity) = entity else {
                    return Ok(KeyStreamLoopControl::Emit);
                };
                keys_scanned = keys_scanned.saturating_add(1);
                rows.push((Id::from_key(data_key.try_key::<E>()?), entity));

                Ok(KeyStreamLoopControl::Emit)
            };

        // Phase 2: materialize rows from keys and append staged outputs.
        LoadExecutor::<E>::drive_field_entity_stream(
            ctx,
            consistency,
            key_stream,
            &mut pre_key,
            &mut on_key,
        )?;

        Ok((rows, keys_scanned))
    }
}
