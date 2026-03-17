//! Module: executor::pipeline::operators::terminal
//! Responsibility: terminal load row-collector materialization seam.
//! Does not own: aggregate fold reducers or access-path planning/routing policy.
//! Boundary: owns cursorless load row-collector short-path execution mechanics.

mod collector;
mod runtime;

use crate::{
    db::{
        Context,
        cursor::CursorBoundary,
        executor::{
            ExecutionKernel, OrderedKeyStream,
            pipeline::{
                contracts::CursorPage, operators::terminal::collector::RowCollectorReducer,
            },
            projection::validate_projection_over_slot_rows,
        },
        query::plan::AccessPlannedQuery,
        response::EntityResponse,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};

impl ExecutionKernel {
    // Attempt one row-collector load materialization short path.
    // This path is intentionally narrow (cursorless, unpaged, no post-access
    // phases) to preserve exact behavior while proving row-only reducer wiring.
    pub(in crate::db::executor) fn try_materialize_load_via_row_collector<E>(
        ctx: &Context<'_, E>,
        plan: &AccessPlannedQuery<E::Key>,
        cursor_boundary: Option<&CursorBoundary>,
        key_stream: &mut dyn OrderedKeyStream,
    ) -> Result<Option<(CursorPage<E>, usize, usize)>, InternalError>
    where
        E: EntityKind + EntityValue,
    {
        if !Self::load_row_collector_short_path_eligible(plan, cursor_boundary) {
            return Ok(None);
        }

        let (rows, keys_scanned) =
            Self::run_row_stream_reducer(ctx, plan, key_stream, RowCollectorReducer)?;
        validate_projection_over_slot_rows(
            E::MODEL,
            &plan.projection_spec(E::MODEL),
            rows.len(),
            &mut |row_index, slot| rows[row_index].1.get_value_by_index(slot),
        )?;
        let page = CursorPage {
            items: EntityResponse::from_rows(rows),
            next_cursor: None,
        };
        let post_access_rows = page.items.len();

        Ok(Some((page, keys_scanned, post_access_rows)))
    }
}
