//! Module: db::executor::pipeline::operators::terminal::runtime
//! Responsibility: cursorless row-collector materialization over structural
//! row and retained-slot-row payloads.
//! Does not own: planner route selection or outer-session projection shaping.
//! Boundary: exposes the shared row-collector short path while keeping payload
//! assembly inside executor-owned structural contracts.

use crate::{
    db::{
        executor::{
            ExecutionKernel, OrderedKeyStream, ScalarContinuationContext,
            pipeline::contracts::{
                MaterializedExecutionPayload, RowCollectorMaterializationRequest,
            },
            route::LoadOrderRouteContract,
            terminal::page::{
                KernelRow, KernelRowScanRequest, KernelRowScanStrategy, ScalarRowRuntimeHandle,
                execute_kernel_row_scan, resolve_cursorless_short_path_plan,
            },
            traversal::row_read_consistency_for_plan,
        },
        query::plan::AccessPlannedQuery,
    },
    error::InternalError,
};

impl ExecutionKernel {
    // Run one row-collector stream over the already decorated key stream and
    // stage structural kernel rows only.
    pub(in crate::db::executor::pipeline::operators::terminal) fn run_row_collector_stream(
        request: RowCollectorStreamRequest<'_, '_>,
    ) -> Result<(Vec<KernelRow>, usize), InternalError> {
        let RowCollectorStreamRequest {
            plan,
            scan_budget_hint,
            load_order_route_contract,
            continuation,
            row_keep_cap,
            scan_strategy,
            key_stream,
            row_runtime,
        } = request;

        // Phase 1: derive the shared row scan contract from plan-owned
        // consistency only. Scan strategy is already resolved by the short
        // path owner before this runtime boundary executes.
        let consistency = row_read_consistency_for_plan(plan);
        let _ = continuation;
        let _ = load_order_route_contract;

        // Phase 2: reuse the canonical structural row scan boundary and only
        // add the retained-slot keep cap needed by cursorless materialization.
        execute_kernel_row_scan(KernelRowScanRequest {
            key_stream,
            scan_budget_hint,
            consistency,
            scan_strategy,
            row_keep_cap,
            row_runtime,
        })
    }

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

        continuation.validate_load_scan_budget_hint(scan_budget_hint, load_order_route_contract)?;

        let (mut rows, keys_scanned) = Self::run_row_collector_stream(RowCollectorStreamRequest {
            plan,
            scan_budget_hint,
            load_order_route_contract,
            continuation,
            row_keep_cap: short_path_plan.row_keep_cap(),
            scan_strategy: short_path_plan.scan_strategy(),
            key_stream,
            row_runtime,
        })?;

        short_path_plan.apply_post_access(plan, &mut rows)?;

        let post_access_rows = rows.len();
        let payload = short_path_plan.finalize_payload(rows)?;

        Ok(Some((payload, keys_scanned, post_access_rows)))
    }
}

///
/// RowCollectorStreamRequest
///
/// RowCollectorStreamRequest keeps the structural row-collector scan contract
/// explicit while avoiding another wide helper signature in the terminal
/// runtime. The slot-only payload mode belongs to the same boundary as the
/// scan budget, continuation contract, and decorated key stream.
///

pub(in crate::db::executor::pipeline::operators::terminal) struct RowCollectorStreamRequest<'a, 'r>
{
    plan: &'a AccessPlannedQuery,
    scan_budget_hint: Option<usize>,
    load_order_route_contract: LoadOrderRouteContract,
    continuation: &'a ScalarContinuationContext,
    row_keep_cap: Option<usize>,
    scan_strategy: KernelRowScanStrategy<'a>,
    key_stream: &'a mut dyn OrderedKeyStream,
    row_runtime: &'r mut ScalarRowRuntimeHandle<'a>,
}
