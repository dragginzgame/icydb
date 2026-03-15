use crate::{
    db::{
        Context,
        cursor::CursorBoundary,
        executor::{
            ExecutionKernel, LoadExecutor, OrderedKeyStream,
            pipeline::operators::reducer::{
                KernelReducer, ReducerControl, StreamInputMode, StreamItem,
            },
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

    // Run one row-only reducer for load collection over the already decorated
    // key stream. Rows are fetched only for keys that survive upstream stream
    // decorators and are staged before ephemeral row-item delivery.
    #[expect(clippy::type_complexity)]
    pub(in crate::db::executor::pipeline::operators::terminal) fn run_row_stream_reducer<E, R>(
        ctx: &Context<'_, E>,
        plan: &AccessPlannedQuery<E::Key>,
        key_stream: &mut dyn OrderedKeyStream,
        mut reducer: R,
    ) -> Result<(Vec<(Id<E>, E)>, usize), InternalError>
    where
        E: EntityKind + EntityValue,
        R: KernelReducer<E>,
    {
        // Phase 1: enforce reducer input-mode contract and initialize row staging.
        if !matches!(R::INPUT_MODE, StreamInputMode::RowOnly) {
            return Err(crate::db::error::query_executor_invariant(
                "row-stream reducer runner requires row-only reducer input mode",
            ));
        }

        let mut rows: Vec<(Id<E>, E)> = Vec::new();
        let mut keys_scanned = 0usize;
        let consistency = row_read_consistency_for_plan(plan);

        // Phase 2: materialize rows from keys and feed ephemeral row borrows to reducer.
        while let Some(data_key) = key_stream.next_key()? {
            let Some(entity) =
                LoadExecutor::<E>::read_entity_for_field_extrema(ctx, consistency, &data_key)?
            else {
                continue;
            };
            keys_scanned = keys_scanned.saturating_add(1);
            rows.push((Id::from_key(data_key.try_key::<E>()?), entity));

            // Ephemeral staging contract: pass a borrow scoped to this call only.
            let Some((_, staged_entity)) = rows.last() else {
                return Err(crate::db::error::query_executor_invariant(
                    "row-stream reducer staging unexpectedly missing last row",
                ));
            };
            match reducer.on_item(StreamItem::Row(staged_entity))? {
                ReducerControl::Continue => {}
                ReducerControl::StopEarly => break,
            }
        }

        let _ = reducer.finish()?;

        Ok((rows, keys_scanned))
    }
}
