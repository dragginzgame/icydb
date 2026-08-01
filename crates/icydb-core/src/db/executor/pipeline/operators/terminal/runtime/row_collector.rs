//! Module: executor::pipeline::operators::terminal::runtime::row_collector
//! Responsibility: cursorless row-collector execution.
//! Does not own: planner route selection or outer response shaping.
//! Boundary: consumes a prepared materialization request.

use crate::{
    db::executor::{
        ExecutionKernel,
        pipeline::contracts::{RowCollectorMaterializationRequest, ScalarPageMaterialization},
        terminal::page::{
            ScalarRowRuntimeHandle, execute_kernel_row_scan, resolve_cursorless_short_path_plan,
        },
    },
    error::InternalError,
};

impl ExecutionKernel {
    pub(in crate::db::executor) fn try_materialize_load_via_row_collector<'a>(
        request: RowCollectorMaterializationRequest<'a>,
        row_runtime: &mut ScalarRowRuntimeHandle<'a>,
    ) -> Result<Option<ScalarPageMaterialization>, InternalError> {
        let RowCollectorMaterializationRequest {
            plan,
            scan_budget_hint,
            load_order_route_mode,
            continuation,
            cursor_boundary,
            capabilities,
            consistency,
            key_stream,
        } = request;

        let Some(short_path_plan) =
            resolve_cursorless_short_path_plan(plan, cursor_boundary, capabilities)?
        else {
            return Ok(None);
        };

        continuation.validate_load_scan_budget_hint(scan_budget_hint, load_order_route_mode)?;

        let (rows, rows_scanned) = execute_kernel_row_scan(short_path_plan.scan_request(
            key_stream,
            scan_budget_hint,
            consistency,
            row_runtime,
        ))?;
        let rows = rows.into_plain_rows()?;
        let (payload, post_access_rows) = short_path_plan.materialize_rows(rows)?;

        Ok(Some(ScalarPageMaterialization {
            payload,
            rows_scanned,
            post_access_rows,
        }))
    }
}
