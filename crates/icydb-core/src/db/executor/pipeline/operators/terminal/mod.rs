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
            ExecutionKernel, LoadExecutor, OrderedKeyStream,
            pipeline::{
                contracts::CursorPage, operators::terminal::collector::RowCollectorReducer,
            },
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
        let projected_rows =
            LoadExecutor::<E>::project_materialized_rows_if_needed(plan, rows.as_slice())?;
        LoadExecutor::<E>::validate_projection_alignment(
            rows.as_slice(),
            projected_rows.as_deref(),
        )?;
        let page = CursorPage {
            items: EntityResponse::from_rows(rows),
            next_cursor: None,
        };
        let post_access_rows = page.items.len();

        Ok(Some((page, keys_scanned, post_access_rows)))
    }
}
