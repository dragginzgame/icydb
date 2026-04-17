//! Module: db::executor::pipeline::operators::terminal::runtime
//! Responsibility: cursorless row-collector materialization over structural
//! row and retained-slot-row payloads.
//! Does not own: planner route selection or outer-session projection shaping.
//! Boundary: exposes the shared row-collector short path while keeping payload
//! assembly inside executor-owned structural contracts.

use crate::{
    db::executor::{
        ExecutionKernel,
        pipeline::contracts::{MaterializedExecutionPayload, RowCollectorMaterializationRequest},
        terminal::page::{
            ScalarRowRuntimeHandle, execute_kernel_row_scan, resolve_cursorless_short_path_plan,
        },
        traversal::row_read_consistency_for_plan,
    },
    error::InternalError,
};

impl ExecutionKernel {
    // Materialize one cursorless short-path load through the structural row
    // runtime under the same continuation and bounded-scan contract as the
    // canonical scalar page kernel.
    pub(in crate::db::executor) fn try_materialize_load_via_row_collector<'a>(
        request: RowCollectorMaterializationRequest<'a>,
        row_runtime: &mut ScalarRowRuntimeHandle<'a>,
    ) -> Result<Option<(MaterializedExecutionPayload, usize, usize)>, InternalError> {
        // Phase 1: destructure the request once so the short path cannot drift
        // from the kernel-owned scan contract.
        let RowCollectorMaterializationRequest {
            plan,
            scan_budget_hint,
            load_order_route_contract,
            continuation,
            cursor_boundary,
            capabilities,
            key_stream,
        } = request;

        let Some(short_path_plan) =
            resolve_cursorless_short_path_plan(plan, cursor_boundary, capabilities)?
        else {
            return Ok(None);
        };

        // Phase 2: validate the shared continuation/budget contract once
        // before the short path builds its canonical scan request.
        continuation.validate_load_scan_budget_hint(scan_budget_hint, load_order_route_contract)?;

        // Phase 3: derive the shared scan contract from plan-owned
        // consistency only, then let the resolved short-path plan build the
        // exact kernel request it wants to run.
        let consistency = row_read_consistency_for_plan(plan);
        let (rows, keys_scanned) = execute_kernel_row_scan(short_path_plan.scan_request(
            key_stream,
            scan_budget_hint,
            consistency,
            row_runtime,
        ))?;

        // Phase 4: the short-path plan owns post-access shaping and final
        // payload selection from here onward.
        let (payload, post_access_rows) = short_path_plan.materialize_rows(plan, rows)?;
        Ok(Some((payload, keys_scanned, post_access_rows)))
    }
}
