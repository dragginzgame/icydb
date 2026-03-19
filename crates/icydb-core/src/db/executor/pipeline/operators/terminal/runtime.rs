use crate::{
    db::{
        cursor::CursorBoundary,
        executor::{
            ExecutionKernel, OrderedKeyStream,
            terminal::page::{KernelRow, ScalarRowRuntimeHandle},
            traversal::row_read_consistency_for_plan,
        },
        query::plan::AccessPlannedQuery,
    },
    error::InternalError,
    model::entity::EntityModel,
};

impl ExecutionKernel {
    // Return whether load execution can safely use the row-collector short path
    // without changing cursor/pagination/filter semantics.
    pub(in crate::db::executor::pipeline::operators::terminal) const fn load_row_collector_short_path_eligible(
        plan: &AccessPlannedQuery,
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
    // key stream and stage structural kernel rows only.
    pub(in crate::db::executor::pipeline::operators::terminal) fn run_row_collector_stream(
        plan: &AccessPlannedQuery,
        key_stream: &mut dyn OrderedKeyStream,
        row_runtime: &mut ScalarRowRuntimeHandle<'_>,
    ) -> Result<(Vec<KernelRow>, usize), InternalError> {
        // Phase 1: initialize row staging and read-consistency policy.
        let mut rows = Vec::new();
        let mut keys_scanned = 0usize;
        let consistency = row_read_consistency_for_plan(plan);

        // Phase 2: materialize rows from keys and append staged structural outputs.
        while let Some(key) = key_stream.next_key()? {
            keys_scanned = keys_scanned.saturating_add(1);
            let Some(row) = row_runtime.read_kernel_row(consistency, &key, false, None)? else {
                continue;
            };
            rows.push(row);
        }

        Ok((rows, keys_scanned))
    }

    // Materialize one cursorless short-path load through the structural row runtime.
    pub(in crate::db::executor) fn try_materialize_load_via_row_collector(
        plan: &AccessPlannedQuery,
        model: &'static EntityModel,
        cursor_boundary: Option<&CursorBoundary>,
        key_stream: &mut dyn OrderedKeyStream,
        row_runtime: &mut ScalarRowRuntimeHandle<'_>,
    ) -> Result<
        Option<(
            crate::db::executor::pipeline::contracts::StructuralCursorPage,
            usize,
            usize,
        )>,
        InternalError,
    > {
        if !Self::load_row_collector_short_path_eligible(plan, cursor_boundary) {
            return Ok(None);
        }

        let (rows, keys_scanned) = Self::run_row_collector_stream(plan, key_stream, row_runtime)?;
        crate::db::executor::projection::validate_projection_over_slot_rows(
            model,
            &plan.projection_spec(model),
            rows.len(),
            &mut |row_index, slot| rows[row_index].slot(slot),
        )?;
        let post_access_rows = rows.len();
        let data_rows = rows.into_iter().map(KernelRow::into_data_row).collect();
        let page =
            crate::db::executor::pipeline::contracts::StructuralCursorPage::new(data_rows, None);

        Ok(Some((page, keys_scanned, post_access_rows)))
    }
}
