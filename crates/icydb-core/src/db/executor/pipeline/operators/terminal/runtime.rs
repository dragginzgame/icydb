//! Module: db::executor::pipeline::operators::terminal::runtime
//! Responsibility: module-local ownership and contracts for db::executor::pipeline::operators::terminal::runtime.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

#[cfg(feature = "sql")]
use crate::db::data::DataKey;
#[cfg(feature = "sql")]
use crate::value::{StorageKey, Value};

#[cfg(feature = "sql")]
type CoveringSlotRows = (Vec<Vec<Option<Value>>>, usize);
#[cfg(feature = "sql")]
type CoveringProjectedRows = (Vec<Vec<Value>>, usize);
#[cfg(feature = "sql")]
type CoveringProjectedTextRows = (Vec<Vec<String>>, usize);
#[cfg(feature = "sql")]
type CoveringComponentSlotGroup = (usize, Vec<usize>);
#[cfg(feature = "sql")]
type DecodedCoveringComponentRows = Vec<(DataKey, Vec<Value>)>;
#[cfg(feature = "sql")]
type RenderedCoveringComponentRows = Vec<(DataKey, Vec<String>)>;
#[cfg(feature = "sql")]
type CoveringSlotRowPairs = Vec<(DataKey, Vec<Option<Value>>)>;
#[cfg(feature = "sql")]
type CoveringProjectedRowPairs = Vec<(DataKey, Vec<Value>)>;
#[cfg(feature = "sql")]
type CoveringProjectedTextRowPairs = Vec<(DataKey, Vec<String>)>;

#[cfg(feature = "sql")]
const SQL_COVERING_BOOL_PAYLOAD_LEN: usize = 1;
#[cfg(feature = "sql")]
const SQL_COVERING_U64_PAYLOAD_LEN: usize = 8;
#[cfg(feature = "sql")]
const SQL_COVERING_ULID_PAYLOAD_LEN: usize = 16;
#[cfg(feature = "sql")]
const SQL_COVERING_TEXT_ESCAPE_PREFIX: u8 = 0x00;
#[cfg(feature = "sql")]
const SQL_COVERING_TEXT_TERMINATOR: u8 = 0x00;
#[cfg(feature = "sql")]
const SQL_COVERING_TEXT_ESCAPED_ZERO: u8 = 0xFF;
#[cfg(feature = "sql")]
const SQL_COVERING_I64_SIGN_BIT_BIAS: u64 = 1u64 << 63;

///
/// SqlCoveringMaterializationContext
///
/// Shared immutable inputs for SQL-only covering slot-row materialization.
/// Keeps the outer short-path helper below the argument-count lint while
/// preserving one explicit runtime-owned contract surface.
///

#[cfg(feature = "sql")]
struct SqlCoveringMaterializationContext<'a> {
    plan: &'a AccessPlannedQuery,
    model: &'static EntityModel,
    store: StoreHandle,
    covering_component_scan: Option<CoveringComponentScanState<'a>>,
    load_terminal_fast_path: Option<&'a LoadTerminalFastPathContract>,
    scan_budget_hint: Option<usize>,
    predicate_slots: Option<&'a PredicateProgram>,
    prefer_rendered_projection_rows: bool,
}

///
/// SqlDirectProjectedFieldSource
///
/// SQL-only direct projected value source derived from the existing
/// planner-owned covering contract. This keeps the projected-row short path
/// explicit without broadening the covering route semantics themselves.
///

#[cfg(feature = "sql")]
enum SqlDirectProjectedFieldSource {
    PrimaryKey,
    Constant(Value),
    IndexComponent { decoded_component_index: usize },
}

///
/// SqlRouteCoveringSlotLayout
///
/// Planner-derived slot layout and component-slot grouping for the
/// route-owned SQL covering-read materializer.
///

#[cfg(feature = "sql")]
struct SqlRouteCoveringSlotLayout {
    primary_key_slot: usize,
    slot_count: usize,
    constant_slots: Vec<(usize, Value)>,
    component_slots: Vec<CoveringComponentSlotGroup>,
}

use crate::{
    db::{
        cursor::CursorBoundary,
        executor::{
            BudgetedOrderedKeyStream, CoveringMembershipRows, CoveringProjectionComponentRows,
            ExecutionKernel, OrderedKeyStream, ScalarContinuationBindings,
            SingleComponentCoveringProjectionOutcome, SingleComponentCoveringScanRequest,
            collect_single_component_covering_projection_from_lowered_specs,
            collect_single_component_covering_projection_values_from_lowered_specs,
            covering_projection_scan_direction, decode_covering_projection_pairs,
            exact_output_key_count_hint, key_stream_budget_is_redundant,
            map_covering_membership_pairs, map_covering_projection_pairs,
            pipeline::contracts::{
                CoveringComponentScanState, DirectCoveringScanMaterializationRequest,
                RowCollectorMaterializationRequest,
            },
            projection::direct_projection_field_slots,
            read_row_presence_with_consistency_from_store, reorder_covering_projection_pairs,
            resolve_covering_memberships_from_lowered_specs,
            resolve_covering_projection_components_from_lowered_specs,
            route::{
                LoadOrderRouteContract, LoadTerminalFastPathContract,
                access_order_satisfied_by_route_contract_for_model,
            },
            terminal::page::{KernelRow, KernelRowPayloadMode, ScalarRowRuntimeHandle},
            traversal::row_read_consistency_for_plan,
        },
        predicate::PredicateProgram,
        query::plan::{
            AccessPlannedQuery, CoveringProjectionOrder, CoveringReadExecutionPlan,
            CoveringReadFieldSource, constant_covering_projection_value_from_access,
            expr::projection_references_only_fields,
        },
        registry::StoreHandle,
    },
    error::InternalError,
    model::entity::EntityModel,
};

impl ExecutionKernel {
    // Materialize one direct covering-scan short path before generic
    // key-stream resolution when the same cursorless SQL covering contract can
    // already produce the final structural page directly.
    pub(in crate::db::executor) fn try_materialize_load_via_direct_covering_scan(
        request: DirectCoveringScanMaterializationRequest<'_>,
        model: &'static EntityModel,
        store: StoreHandle,
        covering_component_scan: Option<CoveringComponentScanState<'_>>,
    ) -> Result<
        Option<(
            crate::db::executor::pipeline::contracts::StructuralCursorPage,
            usize,
            usize,
        )>,
        InternalError,
    > {
        #[cfg(feature = "sql")]
        {
            let DirectCoveringScanMaterializationRequest {
                plan,
                scan_budget_hint,
                cursor_boundary,
                load_terminal_fast_path,
                predicate_slots,
                validate_projection,
                retain_slot_rows,
                prefer_rendered_projection_rows,
            } = request;

            let sql_covering_context = SqlCoveringMaterializationContext {
                plan,
                model,
                store,
                covering_component_scan,
                load_terminal_fast_path,
                scan_budget_hint,
                predicate_slots,
                prefer_rendered_projection_rows,
            };

            return try_materialize_cursorless_sql_covering_scan_without_key_stream(
                sql_covering_context,
                cursor_boundary,
                retain_slot_rows,
                validate_projection,
            );
        }

        #[allow(unreachable_code)]
        {
            let _ = (request, model, store, covering_component_scan);

            Ok(None)
        }
    }

    // Return whether load execution can safely use the row-collector short path
    // without changing cursor/pagination/filter semantics.
    pub(in crate::db::executor::pipeline::operators::terminal) fn load_row_collector_short_path_eligible(
        plan: &AccessPlannedQuery,
        model: &'static EntityModel,
        cursor_boundary: Option<&CursorBoundary>,
        retain_slot_rows: bool,
    ) -> bool {
        let logical = plan.scalar_plan();
        let generic_short_path = logical.mode.is_load()
            && cursor_boundary.is_none()
            && logical.predicate.is_none()
            && logical.order.is_none()
            && logical.page.is_none();

        let sql_projection_short_path = logical.mode.is_load()
            && retain_slot_rows
            && cursor_boundary.is_none()
            && !logical.distinct
            && (logical.order.is_none()
                || access_order_satisfied_by_route_contract_for_model(model, plan));

        generic_short_path || sql_projection_short_path
    }

    // Run one row-collector stream over the already decorated
    // key stream and stage structural kernel rows only.
    pub(in crate::db::executor::pipeline::operators::terminal) fn run_row_collector_stream(
        request: RowCollectorStreamRequest<'_, '_>,
    ) -> Result<(Vec<KernelRow>, usize), InternalError> {
        let RowCollectorStreamRequest {
            plan,
            scan_budget_hint,
            load_order_route_contract,
            continuation,
            row_keep_cap,
            payload_mode,
            key_stream,
            row_runtime,
            predicate_slots,
            slot_only_required_slots,
        } = request;

        // Phase 1: initialize row staging and read-consistency policy.
        let staged_capacity = exact_output_key_count_hint(key_stream, scan_budget_hint)
            .map_or_else(
                || row_keep_cap.unwrap_or(0),
                |hint| row_keep_cap.map_or(hint, |cap| usize::min(hint, cap)),
            );
        let mut rows = Vec::with_capacity(staged_capacity);
        let mut keys_scanned = 0usize;
        let consistency = row_read_consistency_for_plan(plan);
        let predicate_preapplied = plan.has_residual_predicate();
        let _ = continuation;
        let _ = load_order_route_contract;

        // Phase 2: materialize rows from keys and append staged structural outputs.
        if let Some(scan_budget) = scan_budget_hint
            && !key_stream_budget_is_redundant(key_stream, scan_budget)
        {
            let mut budgeted = BudgetedOrderedKeyStream::new(key_stream, scan_budget);
            while let Some(key) = budgeted.next_key()? {
                keys_scanned = keys_scanned.saturating_add(1);
                let Some(row) = row_runtime.read_kernel_row(
                    consistency,
                    &key,
                    payload_mode,
                    predicate_preapplied,
                    predicate_slots,
                    slot_only_required_slots,
                )?
                else {
                    continue;
                };
                rows.push(row);
                if row_keep_cap.is_some_and(|cap| rows.len() >= cap) {
                    break;
                }
            }
        } else {
            while let Some(key) = key_stream.next_key()? {
                keys_scanned = keys_scanned.saturating_add(1);
                let Some(row) = row_runtime.read_kernel_row(
                    consistency,
                    &key,
                    payload_mode,
                    predicate_preapplied,
                    predicate_slots,
                    slot_only_required_slots,
                )?
                else {
                    continue;
                };
                rows.push(row);
                if row_keep_cap.is_some_and(|cap| rows.len() >= cap) {
                    break;
                }
            }
        }

        Ok((rows, keys_scanned))
    }

    // Materialize one cursorless short-path load through the structural row
    // runtime under the same continuation and bounded-scan contract as the
    // canonical scalar page kernel.
    pub(in crate::db::executor) fn try_materialize_load_via_row_collector<'a>(
        request: RowCollectorMaterializationRequest<'a>,
        model: &'static EntityModel,
        row_runtime: &mut ScalarRowRuntimeHandle<'a>,
        store: StoreHandle,
        covering_component_scan: Option<CoveringComponentScanState<'a>>,
    ) -> Result<
        Option<(
            crate::db::executor::pipeline::contracts::StructuralCursorPage,
            usize,
            usize,
        )>,
        InternalError,
    > {
        // Phase 1: destructure the one short-path request envelope once so the
        // terminal helper cannot drift from the kernel-owned scan contract.
        let RowCollectorMaterializationRequest {
            plan,
            scan_budget_hint,
            load_order_route_contract,
            continuation,
            cursor_boundary,
            load_terminal_fast_path,
            predicate_slots,
            validate_projection,
            retain_slot_rows,
            slot_only_required_slots,
            prefer_rendered_projection_rows,
            key_stream,
        } = request;

        if !Self::load_row_collector_short_path_eligible(
            plan,
            model,
            cursor_boundary,
            retain_slot_rows,
        ) {
            return Ok(None);
        }

        continuation.validate_load_scan_budget_hint(scan_budget_hint, load_order_route_contract)?;

        #[cfg(feature = "sql")]
        let sql_covering_context = SqlCoveringMaterializationContext {
            plan,
            model,
            store,
            covering_component_scan,
            load_terminal_fast_path,
            scan_budget_hint,
            predicate_slots,
            prefer_rendered_projection_rows,
        };

        #[cfg(feature = "sql")]
        if retain_slot_rows
            && let Some(sql_page) = try_materialize_cursorless_sql_short_path(
                sql_covering_context,
                key_stream,
                validate_projection,
            )?
        {
            return Ok(Some(sql_page));
        }

        let payload_mode = if retain_slot_rows && cursor_boundary.is_none() {
            KernelRowPayloadMode::SlotsOnly
        } else {
            KernelRowPayloadMode::FullRow
        };
        let row_keep_cap =
            cursorless_row_collector_keep_cap(plan, cursor_boundary, retain_slot_rows);
        let (mut rows, keys_scanned) = Self::run_row_collector_stream(RowCollectorStreamRequest {
            plan,
            scan_budget_hint,
            load_order_route_contract,
            continuation,
            row_keep_cap,
            payload_mode,
            key_stream,
            row_runtime,
            predicate_slots,
            slot_only_required_slots,
        })?;
        if retain_slot_rows && !cursorless_sql_page_window_is_redundant(plan, rows.len()) {
            apply_cursorless_sql_page_window(plan, &mut rows);
        }
        if validate_projection {
            crate::db::executor::projection::validate_projection_over_slot_rows(
                model,
                &plan.projection_spec(model),
                rows.len(),
                &mut |row_index, slot| rows[row_index].slot(slot),
            )?;
        }
        let post_access_rows = rows.len();
        let page = finalize_cursorless_row_collector_page(rows, retain_slot_rows)?;

        Ok(Some((page, keys_scanned, post_access_rows)))
    }
}

#[cfg(feature = "sql")]
// Attempt the SQL-only cursorless short path before falling back to the shared
// row-collector kernel. This keeps the already-projected and retained-slot-row
// lanes under one explicit terminal-owned contract.
fn try_materialize_cursorless_sql_short_path(
    context: SqlCoveringMaterializationContext<'_>,
    key_stream: &mut dyn OrderedKeyStream,
    validate_projection: bool,
) -> Result<
    Option<(
        crate::db::executor::pipeline::contracts::StructuralCursorPage,
        usize,
        usize,
    )>,
    InternalError,
> {
    if let Some(page) =
        try_materialize_cursorless_sql_covering_scan_short_path(&context, validate_projection)?
    {
        return Ok(Some(page));
    }

    try_materialize_cursorless_sql_key_stream_short_path(&context, key_stream, validate_projection)
}

#[cfg(feature = "sql")]
// Attempt the cursorless SQL covering-scan lane before any generic ordered
// key stream is resolved. This is restricted to the same short-path cohort the
// terminal already proves it can materialize from route-owned covering scans.
fn try_materialize_cursorless_sql_covering_scan_without_key_stream(
    context: SqlCoveringMaterializationContext<'_>,
    cursor_boundary: Option<&CursorBoundary>,
    retain_slot_rows: bool,
    validate_projection: bool,
) -> Result<
    Option<(
        crate::db::executor::pipeline::contracts::StructuralCursorPage,
        usize,
        usize,
    )>,
    InternalError,
> {
    if !ExecutionKernel::load_row_collector_short_path_eligible(
        context.plan,
        context.model,
        cursor_boundary,
        retain_slot_rows,
    ) {
        return Ok(None);
    }

    try_materialize_cursorless_sql_covering_scan_short_path(&context, validate_projection)
}

#[cfg(feature = "sql")]
// Attempt one cursorless SQL covering-scan short path that consumes only the
// route-owned covering component scan contract and does not need a generic
// ordered key stream.
fn try_materialize_cursorless_sql_covering_scan_short_path(
    context: &SqlCoveringMaterializationContext<'_>,
    validate_projection: bool,
) -> Result<
    Option<(
        crate::db::executor::pipeline::contracts::StructuralCursorPage,
        usize,
        usize,
    )>,
    InternalError,
> {
    if context.prefer_rendered_projection_rows
        && let Some((mut projected_rows, keys_scanned)) =
            try_materialize_sql_route_covering_projected_text_rows(context)?
    {
        if !cursorless_sql_page_window_is_redundant(context.plan, projected_rows.len()) {
            apply_cursorless_sql_page_window(context.plan, &mut projected_rows);
        }
        let post_access_rows = projected_rows.len();
        let page = crate::db::executor::pipeline::contracts::StructuralCursorPage::new_with_rendered_projected_rows(
            projected_rows,
            post_access_rows,
            None,
        );

        return Ok(Some((page, keys_scanned, post_access_rows)));
    }

    if let Some((mut projected_rows, keys_scanned)) =
        try_materialize_sql_route_covering_projected_rows(context)?
    {
        if !cursorless_sql_page_window_is_redundant(context.plan, projected_rows.len()) {
            apply_cursorless_sql_page_window(context.plan, &mut projected_rows);
        }
        let post_access_rows = projected_rows.len();
        let page =
            crate::db::executor::pipeline::contracts::StructuralCursorPage::new_with_projected_rows(
                projected_rows,
                post_access_rows,
                None,
            );

        return Ok(Some((page, keys_scanned, post_access_rows)));
    }

    if let Some((mut slot_rows, keys_scanned)) =
        try_materialize_sql_route_covering_slot_rows(context)?
    {
        if !cursorless_sql_page_window_is_redundant(context.plan, slot_rows.len()) {
            apply_cursorless_sql_page_window(context.plan, &mut slot_rows);
        }
        if validate_projection {
            crate::db::executor::projection::validate_projection_over_slot_rows(
                context.model,
                &context.plan.projection_spec(context.model),
                slot_rows.len(),
                &mut |row_index, slot| slot_rows[row_index].get(slot).cloned().flatten(),
            )?;
        }
        let post_access_rows = slot_rows.len();
        let page =
            crate::db::executor::pipeline::contracts::StructuralCursorPage::new_with_slot_rows(
                slot_rows,
                post_access_rows,
                None,
            );

        return Ok(Some((page, keys_scanned, post_access_rows)));
    }

    Ok(None)
}

#[cfg(feature = "sql")]
// Attempt the remaining cursorless SQL short paths that still need the
// already-resolved ordered key stream.
fn try_materialize_cursorless_sql_key_stream_short_path(
    context: &SqlCoveringMaterializationContext<'_>,
    key_stream: &mut dyn OrderedKeyStream,
    validate_projection: bool,
) -> Result<
    Option<(
        crate::db::executor::pipeline::contracts::StructuralCursorPage,
        usize,
        usize,
    )>,
    InternalError,
> {
    if context.prefer_rendered_projection_rows
        && let Some((mut projected_rows, keys_scanned)) =
            try_materialize_sql_projected_text_rows(context, key_stream)?
    {
        if !cursorless_sql_page_window_is_redundant(context.plan, projected_rows.len()) {
            apply_cursorless_sql_page_window(context.plan, &mut projected_rows);
        }
        let post_access_rows = projected_rows.len();
        let page = crate::db::executor::pipeline::contracts::StructuralCursorPage::new_with_rendered_projected_rows(
            projected_rows,
            post_access_rows,
            None,
        );

        return Ok(Some((page, keys_scanned, post_access_rows)));
    }

    if let Some((mut projected_rows, keys_scanned)) =
        try_materialize_sql_projected_rows(context, key_stream)?
    {
        if !cursorless_sql_page_window_is_redundant(context.plan, projected_rows.len()) {
            apply_cursorless_sql_page_window(context.plan, &mut projected_rows);
        }
        let post_access_rows = projected_rows.len();
        let page =
            crate::db::executor::pipeline::contracts::StructuralCursorPage::new_with_projected_rows(
                projected_rows,
                post_access_rows,
                None,
            );

        return Ok(Some((page, keys_scanned, post_access_rows)));
    }

    if let Some((mut slot_rows, keys_scanned)) =
        try_materialize_sql_covering_slot_rows(context, key_stream)?
    {
        if !cursorless_sql_page_window_is_redundant(context.plan, slot_rows.len()) {
            apply_cursorless_sql_page_window(context.plan, &mut slot_rows);
        }
        if validate_projection {
            crate::db::executor::projection::validate_projection_over_slot_rows(
                context.model,
                &context.plan.projection_spec(context.model),
                slot_rows.len(),
                &mut |row_index, slot| slot_rows[row_index].get(slot).cloned().flatten(),
            )?;
        }
        let post_access_rows = slot_rows.len();
        let page =
            crate::db::executor::pipeline::contracts::StructuralCursorPage::new_with_slot_rows(
                slot_rows,
                post_access_rows,
                None,
            );

        return Ok(Some((page, keys_scanned, post_access_rows)));
    }

    Ok(None)
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
    continuation: ScalarContinuationBindings<'a>,
    row_keep_cap: Option<usize>,
    payload_mode: KernelRowPayloadMode,
    key_stream: &'a mut dyn OrderedKeyStream,
    row_runtime: &'r mut ScalarRowRuntimeHandle<'a>,
    predicate_slots: Option<&'a PredicateProgram>,
    slot_only_required_slots: Option<&'a [usize]>,
}

// Return the number of kept rows the cursorless structural SQL short path
// must materialize before later pagination becomes redundant.
fn cursorless_row_collector_keep_cap(
    plan: &AccessPlannedQuery,
    cursor_boundary: Option<&CursorBoundary>,
    retain_slot_rows: bool,
) -> Option<usize> {
    if !retain_slot_rows || cursor_boundary.is_some() {
        return None;
    }

    let page = plan.scalar_plan().page.as_ref()?;
    let limit = page.limit?;
    let offset = usize::try_from(page.offset).unwrap_or(usize::MAX);
    let limit = usize::try_from(limit).unwrap_or(usize::MAX);

    Some(offset.saturating_add(limit))
}

// Return whether the cursorless SQL short path already staged the final page.
fn cursorless_sql_page_window_is_redundant(plan: &AccessPlannedQuery, row_count: usize) -> bool {
    let Some(page) = plan.scalar_plan().page.as_ref() else {
        return true;
    };

    if page.offset != 0 {
        return false;
    }

    page.limit
        .is_none_or(|limit| row_count <= usize::try_from(limit).unwrap_or(usize::MAX))
}

// Finalize one cursorless structural page after short-path row collection.
fn finalize_cursorless_row_collector_page(
    rows: Vec<KernelRow>,
    retain_slot_rows: bool,
) -> Result<crate::db::executor::pipeline::contracts::StructuralCursorPage, InternalError> {
    #[cfg(feature = "sql")]
    {
        if retain_slot_rows {
            let row_count = rows.len();
            let slot_rows = rows.into_iter().map(KernelRow::into_slots).collect();

            return Ok(
                crate::db::executor::pipeline::contracts::StructuralCursorPage::new_with_slot_rows(
                    slot_rows, row_count, None,
                ),
            );
        }
    }

    let _ = retain_slot_rows;
    let data_rows = rows
        .into_iter()
        .map(KernelRow::into_data_row)
        .collect::<Result<Vec<_>, _>>()?;

    Ok(crate::db::executor::pipeline::contracts::StructuralCursorPage::new(data_rows, None))
}

// Apply the SQL-only cursorless LIMIT/OFFSET window directly on the collected
// row set when the route already guarantees final order and the outer surface
// does not retain scalar continuation state.
fn apply_cursorless_sql_page_window<T>(plan: &AccessPlannedQuery, rows: &mut Vec<T>) {
    let Some(page) = plan.scalar_plan().page.as_ref() else {
        return;
    };

    let total = rows.len();
    let start = usize::try_from(page.offset)
        .unwrap_or(usize::MAX)
        .min(total);
    let end = match page.limit {
        Some(limit) => start
            .saturating_add(usize::try_from(limit).unwrap_or(usize::MAX))
            .min(total),
        None => total,
    };

    rows.drain(..start);
    rows.truncate(end.saturating_sub(start));
}

#[cfg(feature = "sql")]
// Attempt one SQL-only direct projected-row materialization path when the
// route-owned covering contract already determines every output value and the
// query owes no residual predicate evaluation.
fn try_materialize_sql_projected_text_rows(
    context: &SqlCoveringMaterializationContext<'_>,
    key_stream: &mut dyn OrderedKeyStream,
) -> Result<Option<CoveringProjectedTextRows>, InternalError> {
    if context.plan.has_residual_predicate() {
        return Ok(None);
    }

    if let Some(projected_rows) = try_materialize_sql_route_covering_projected_text_rows(context)? {
        return Ok(Some(projected_rows));
    }

    try_materialize_sql_route_constant_projected_text_rows(context, key_stream)
}

#[cfg(feature = "sql")]
// Attempt one SQL-only direct projected-row materialization path when the
// route-owned covering contract already determines every output value and the
// query owes no residual predicate evaluation.
fn try_materialize_sql_projected_rows(
    context: &SqlCoveringMaterializationContext<'_>,
    key_stream: &mut dyn OrderedKeyStream,
) -> Result<Option<CoveringProjectedRows>, InternalError> {
    if context.plan.has_residual_predicate() {
        return Ok(None);
    }

    if let Some(projected_rows) = try_materialize_sql_route_covering_projected_rows(context)? {
        return Ok(Some(projected_rows));
    }

    try_materialize_sql_route_constant_projected_rows(context, key_stream)
}

#[cfg(feature = "sql")]
// Attempt one route-owned covering-read projected-row materialization path
// that renders final SQL text cells directly from the planner-owned covering
// contract.
fn try_materialize_sql_route_covering_projected_text_rows(
    context: &SqlCoveringMaterializationContext<'_>,
) -> Result<Option<CoveringProjectedTextRows>, InternalError> {
    let Some(LoadTerminalFastPathContract::CoveringRead(covering)) =
        context.load_terminal_fast_path
    else {
        return Ok(None);
    };
    let Some(scan_state) = context.covering_component_scan else {
        return Ok(None);
    };
    let projection = context.plan.projection_spec(context.model);
    let Some(projection_field_slots) = direct_projection_field_slots(context.model, &projection)
    else {
        return Ok(None);
    };

    if let Some(projected_rows) = try_materialize_sql_route_single_component_projected_text_rows(
        context,
        covering,
        projection_field_slots.as_slice(),
    )? {
        return Ok(Some(projected_rows));
    }

    if let Some(projected_rows) = try_materialize_sql_route_constant_covering_projected_text_rows(
        context,
        covering,
        projection_field_slots.as_slice(),
    )? {
        return Ok(Some(projected_rows));
    }

    let Some(layout) = sql_route_covering_slot_layout(context.model, covering, None)? else {
        return Ok(None);
    };
    let Some(projected_field_sources) = sql_route_direct_projected_field_sources(
        covering,
        projection_field_slots.as_slice(),
        layout.component_slots.as_slice(),
    )?
    else {
        return Ok(None);
    };
    let Some((rendered_rows, keys_scanned)) = sql_route_covering_component_text_rows(
        context.plan,
        context.store,
        scan_state,
        covering,
        context.scan_budget_hint,
        layout.component_slots.as_slice(),
    )?
    else {
        return Ok(None);
    };

    // Phase 1: materialize already-rendered SQL rows directly from the
    // planner-owned covering contract instead of staging `Value` rows.
    let mut rows = sql_route_covering_projected_text_rows_from_rendered(
        &projected_field_sources,
        rendered_rows,
    )?;

    // Phase 2: preserve the existing covering order contract exactly as the
    // value-row path does.
    reorder_covering_projection_pairs(covering.order_contract, rows.as_mut_slice());

    Ok(Some((
        rows.into_iter().map(|(_, row)| row).collect::<Vec<_>>(),
        keys_scanned,
    )))
}

#[cfg(feature = "sql")]
// Attempt one SQL-only index-covered slot-row materialization path that can
// derive every referenced value from one decoded covering component, bound
// index-prefix constants, and the authoritative primary key on each data key.
fn try_materialize_sql_covering_slot_rows(
    context: &SqlCoveringMaterializationContext<'_>,
    key_stream: &mut dyn OrderedKeyStream,
) -> Result<Option<CoveringSlotRows>, InternalError> {
    // Phase 1: first try the explicit route-owned covering-read contract when
    // it can be satisfied by one index component plus any PK/constant fields.
    if let Some(rows) = try_materialize_sql_route_covering_slot_rows(context)? {
        return Ok(Some(rows));
    }

    // Phase 2: then try the constant-covering path that rebuilds rows from
    // already-resolved keys without re-entering index storage.
    if let Some((slot_template, primary_key_slot)) =
        sql_constant_covering_slot_row_template_from_route_contract(
            context.model,
            context.load_terminal_fast_path,
            context.predicate_slots,
        )
        .or_else(|| {
            sql_constant_covering_slot_row_template(
                context.plan,
                context.model,
                context.predicate_slots,
            )
        })
    {
        let consistency = row_read_consistency_for_plan(context.plan);
        let row_check_required =
            sql_route_covering_row_check_required(context.load_terminal_fast_path);
        let mut rows = Vec::with_capacity(
            exact_output_key_count_hint(key_stream, context.scan_budget_hint).unwrap_or(0),
        );
        let mut keys_scanned = 0usize;

        if let Some(scan_budget) = context.scan_budget_hint
            && !key_stream_budget_is_redundant(key_stream, scan_budget)
        {
            let mut budgeted = BudgetedOrderedKeyStream::new(key_stream, scan_budget);
            while let Some(key) = budgeted.next_key()? {
                keys_scanned = keys_scanned.saturating_add(1);
                if row_check_required
                    && !read_row_presence_with_consistency_from_store(
                        context.store,
                        &key,
                        consistency,
                    )?
                {
                    continue;
                }

                let mut row = slot_template.clone();
                row[primary_key_slot] = Some(key.storage_key().as_primary_key_value());

                if let Some(predicate_program) = context.predicate_slots
                    && !predicate_program.eval_with_slot_value_ref_reader(&mut |slot| {
                        row.get(slot).and_then(Option::as_ref)
                    })
                {
                    continue;
                }

                rows.push(row);
            }
        } else {
            while let Some(key) = key_stream.next_key()? {
                keys_scanned = keys_scanned.saturating_add(1);
                if row_check_required
                    && !read_row_presence_with_consistency_from_store(
                        context.store,
                        &key,
                        consistency,
                    )?
                {
                    continue;
                }

                let mut row = slot_template.clone();
                row[primary_key_slot] = Some(key.storage_key().as_primary_key_value());

                if let Some(predicate_program) = context.predicate_slots
                    && !predicate_program.eval_with_slot_value_ref_reader(&mut |slot| {
                        row.get(slot).and_then(Option::as_ref)
                    })
                {
                    continue;
                }

                rows.push(row);
            }
        }

        return Ok(Some((rows, keys_scanned)));
    }

    Ok(None)
}

#[cfg(feature = "sql")]
// Attempt one route-owned covering-read projected-row materialization path
// when the SQL projection is a direct unique field list and the route already
// proves every referenced source.
fn try_materialize_sql_route_covering_projected_rows(
    context: &SqlCoveringMaterializationContext<'_>,
) -> Result<Option<CoveringProjectedRows>, InternalError> {
    let Some(LoadTerminalFastPathContract::CoveringRead(covering)) =
        context.load_terminal_fast_path
    else {
        return Ok(None);
    };
    let Some(scan_state) = context.covering_component_scan else {
        return Ok(None);
    };
    let projection = context.plan.projection_spec(context.model);
    let Some(projection_field_slots) = direct_projection_field_slots(context.model, &projection)
    else {
        return Ok(None);
    };

    if let Some(projected_rows) = try_materialize_sql_route_single_component_projected_rows(
        context,
        covering,
        projection_field_slots.as_slice(),
    )? {
        return Ok(Some(projected_rows));
    }

    if let Some(projected_rows) = try_materialize_sql_route_constant_covering_projected_rows(
        context,
        covering,
        projection_field_slots.as_slice(),
    )? {
        return Ok(Some(projected_rows));
    }

    let Some(layout) = sql_route_covering_slot_layout(context.model, covering, None)? else {
        return Ok(None);
    };
    let Some(projected_field_sources) = sql_route_direct_projected_field_sources(
        covering,
        projection_field_slots.as_slice(),
        layout.component_slots.as_slice(),
    )?
    else {
        return Ok(None);
    };
    let Some((decoded_rows, keys_scanned)) = sql_route_covering_component_rows(
        context.plan,
        context.store,
        scan_state,
        covering,
        context.scan_budget_hint,
        layout.component_slots.as_slice(),
    )?
    else {
        return Ok(None);
    };

    // Phase 1: materialize already-projected SQL rows directly from the
    // planner-owned covering contract instead of staging full slot rows.
    let mut rows =
        sql_route_covering_projected_rows_from_decoded(&projected_field_sources, decoded_rows)?;

    // Phase 2: preserve the existing covering order contract exactly as the
    // slot-row path does.
    reorder_covering_projection_pairs(covering.order_contract, rows.as_mut_slice());

    Ok(Some((
        rows.into_iter().map(|(_, row)| row).collect::<Vec<_>>(),
        keys_scanned,
    )))
}

#[cfg(feature = "sql")]
// Attempt one route-owned constant-plus-primary-key projected-text path while
// keeping membership-level witness handling below the shared covering kernel.
fn try_materialize_sql_route_constant_covering_projected_text_rows(
    context: &SqlCoveringMaterializationContext<'_>,
    covering: &CoveringReadExecutionPlan,
    projection_field_slots: &[(String, usize)],
) -> Result<Option<CoveringProjectedTextRows>, InternalError> {
    if !covering.existing_row_mode.uses_storage_existence_witness() {
        return Ok(None);
    }

    let Some(projected_field_sources) =
        sql_route_constant_projected_field_sources(covering, projection_field_slots)
    else {
        return Ok(None);
    };
    let Some((raw_pairs, keys_scanned)) = sql_route_covering_membership_rows(
        context.plan,
        context.store,
        context.covering_component_scan,
        covering,
        context.scan_budget_hint,
    )?
    else {
        return Ok(None);
    };

    // Phase 1: let the shared covering membership boundary apply the
    // membership-level witness before any constant-plus-primary-key SQL
    // projection is materialized.
    let consistency = row_read_consistency_for_plan(context.plan);
    let mut rows = map_covering_membership_pairs(
        raw_pairs,
        context.store,
        consistency,
        covering.existing_row_mode,
        |data_key| {
            sql_project_text_row_from_constant_covering_sources(
                projected_field_sources.as_slice(),
                &data_key.storage_key(),
            )
        },
    )?;

    // Phase 2: preserve the planner-owned logical order exactly as the
    // decoded-component covering paths do.
    reorder_covering_projection_pairs(covering.order_contract, rows.as_mut_slice());

    Ok(Some((
        rows.into_iter().map(|(_, row)| row).collect::<Vec<_>>(),
        keys_scanned,
    )))
}

#[cfg(feature = "sql")]
// Attempt one route-owned constant-plus-primary-key projected-row path while
// keeping membership-level witness handling below the shared covering kernel.
fn try_materialize_sql_route_constant_covering_projected_rows(
    context: &SqlCoveringMaterializationContext<'_>,
    covering: &CoveringReadExecutionPlan,
    projection_field_slots: &[(String, usize)],
) -> Result<Option<CoveringProjectedRows>, InternalError> {
    if !covering.existing_row_mode.uses_storage_existence_witness() {
        return Ok(None);
    }

    let Some(projected_field_sources) =
        sql_route_constant_projected_field_sources(covering, projection_field_slots)
    else {
        return Ok(None);
    };
    let Some((raw_pairs, keys_scanned)) = sql_route_covering_membership_rows(
        context.plan,
        context.store,
        context.covering_component_scan,
        covering,
        context.scan_budget_hint,
    )?
    else {
        return Ok(None);
    };

    // Phase 1: let the shared covering membership boundary apply the
    // membership-level witness before any constant-plus-primary-key SQL
    // projection is materialized.
    let consistency = row_read_consistency_for_plan(context.plan);
    let mut rows = map_covering_membership_pairs(
        raw_pairs,
        context.store,
        consistency,
        covering.existing_row_mode,
        |data_key| {
            sql_project_row_from_constant_covering_sources(
                projected_field_sources.as_slice(),
                &data_key.storage_key(),
            )
        },
    )?;

    // Phase 2: preserve the planner-owned logical order exactly as the
    // decoded-component covering paths do.
    reorder_covering_projection_pairs(covering.order_contract, rows.as_mut_slice());

    Ok(Some((
        rows.into_iter().map(|(_, row)| row).collect::<Vec<_>>(),
        keys_scanned,
    )))
}

#[cfg(feature = "sql")]
// Attempt one route-owned direct projected-row path when every projected value
// comes from the authoritative primary key or a bound access constant.
fn try_materialize_sql_route_constant_projected_text_rows(
    context: &SqlCoveringMaterializationContext<'_>,
    key_stream: &mut dyn OrderedKeyStream,
) -> Result<Option<CoveringProjectedTextRows>, InternalError> {
    let Some(LoadTerminalFastPathContract::CoveringRead(covering)) =
        context.load_terminal_fast_path
    else {
        return Ok(None);
    };
    if !sql_route_covering_allows_constant_short_path(context.load_terminal_fast_path) {
        return Ok(None);
    }
    let projection = context.plan.projection_spec(context.model);
    let Some(projection_field_slots) = direct_projection_field_slots(context.model, &projection)
    else {
        return Ok(None);
    };
    let Some(projected_field_sources) =
        sql_route_constant_projected_field_sources(covering, projection_field_slots.as_slice())
    else {
        return Ok(None);
    };

    let consistency = row_read_consistency_for_plan(context.plan);
    let row_check_required = sql_route_covering_row_check_required(context.load_terminal_fast_path);
    let mut rows = Vec::with_capacity(
        exact_output_key_count_hint(key_stream, context.scan_budget_hint).unwrap_or(0),
    );
    let mut keys_scanned = 0usize;

    if let Some(scan_budget) = context.scan_budget_hint
        && !key_stream_budget_is_redundant(key_stream, scan_budget)
    {
        let mut budgeted = BudgetedOrderedKeyStream::new(key_stream, scan_budget);
        while let Some(key) = budgeted.next_key()? {
            keys_scanned = keys_scanned.saturating_add(1);
            if row_check_required
                && !read_row_presence_with_consistency_from_store(context.store, &key, consistency)?
            {
                continue;
            }

            rows.push(sql_project_text_row_from_constant_covering_sources(
                projected_field_sources.as_slice(),
                &key.storage_key(),
            )?);
        }
    } else {
        while let Some(key) = key_stream.next_key()? {
            keys_scanned = keys_scanned.saturating_add(1);
            if row_check_required
                && !read_row_presence_with_consistency_from_store(context.store, &key, consistency)?
            {
                continue;
            }

            rows.push(sql_project_text_row_from_constant_covering_sources(
                projected_field_sources.as_slice(),
                &key.storage_key(),
            )?);
        }
    }

    Ok(Some((rows, keys_scanned)))
}

#[cfg(feature = "sql")]
// Attempt one route-owned direct projected-row path when every projected value
// comes from the authoritative primary key or a bound access constant.
fn try_materialize_sql_route_constant_projected_rows(
    context: &SqlCoveringMaterializationContext<'_>,
    key_stream: &mut dyn OrderedKeyStream,
) -> Result<Option<CoveringProjectedRows>, InternalError> {
    let Some(LoadTerminalFastPathContract::CoveringRead(covering)) =
        context.load_terminal_fast_path
    else {
        return Ok(None);
    };
    if !sql_route_covering_allows_constant_short_path(context.load_terminal_fast_path) {
        return Ok(None);
    }
    let projection = context.plan.projection_spec(context.model);
    let Some(projection_field_slots) = direct_projection_field_slots(context.model, &projection)
    else {
        return Ok(None);
    };
    let Some(projected_field_sources) =
        sql_route_constant_projected_field_sources(covering, projection_field_slots.as_slice())
    else {
        return Ok(None);
    };

    let consistency = row_read_consistency_for_plan(context.plan);
    let row_check_required = sql_route_covering_row_check_required(context.load_terminal_fast_path);
    let mut rows = Vec::with_capacity(
        exact_output_key_count_hint(key_stream, context.scan_budget_hint).unwrap_or(0),
    );
    let mut keys_scanned = 0usize;

    if let Some(scan_budget) = context.scan_budget_hint
        && !key_stream_budget_is_redundant(key_stream, scan_budget)
    {
        let mut budgeted = BudgetedOrderedKeyStream::new(key_stream, scan_budget);
        while let Some(key) = budgeted.next_key()? {
            keys_scanned = keys_scanned.saturating_add(1);
            if row_check_required
                && !read_row_presence_with_consistency_from_store(context.store, &key, consistency)?
            {
                continue;
            }

            rows.push(sql_project_row_from_constant_covering_sources(
                projected_field_sources.as_slice(),
                &key.storage_key(),
            )?);
        }
    } else {
        while let Some(key) = key_stream.next_key()? {
            keys_scanned = keys_scanned.saturating_add(1);
            if row_check_required
                && !read_row_presence_with_consistency_from_store(context.store, &key, consistency)?
            {
                continue;
            }

            rows.push(sql_project_row_from_constant_covering_sources(
                projected_field_sources.as_slice(),
                &key.storage_key(),
            )?);
        }
    }

    Ok(Some((rows, keys_scanned)))
}

#[cfg(feature = "sql")]
// Attempt one traversal-order direct projected-row path for single-component
// secondary covering reads.
fn try_materialize_sql_route_single_component_projected_text_rows(
    context: &SqlCoveringMaterializationContext<'_>,
    covering: &CoveringReadExecutionPlan,
    projection_field_slots: &[(String, usize)],
) -> Result<Option<CoveringProjectedTextRows>, InternalError> {
    let Some(scan_state) = context.covering_component_scan else {
        return Ok(None);
    };
    let Some((component_index, projected_field_sources)) =
        sql_route_single_component_projected_field_sources(covering, projection_field_slots)
    else {
        return Ok(None);
    };

    let consistency = row_read_consistency_for_plan(context.plan);
    let projected_rows = collect_single_component_covering_projection_from_lowered_specs(
        SingleComponentCoveringScanRequest {
            store: context.store,
            entity_tag: scan_state.entity_tag,
            index_prefix_specs: scan_state.index_prefix_specs,
            index_range_specs: scan_state.index_range_specs,
            direction: covering_projection_scan_direction(covering.order_contract),
            limit: covering_component_scan_budget_hint(
                covering.order_contract,
                context.scan_budget_hint,
            )
            .unwrap_or(usize::MAX),
            component_index,
            consistency,
            existing_row_mode: covering.existing_row_mode,
        },
        |storage_key, component| {
            let Some(rendered_component) = render_sql_covering_component_text(component)? else {
                return Ok(None);
            };

            Ok(Some(sql_project_text_row_from_single_covering_component(
                projected_field_sources.as_slice(),
                storage_key,
                rendered_component.as_str(),
            )?))
        },
    )?;
    let SingleComponentCoveringProjectionOutcome::Supported(projected_rows) = projected_rows else {
        return Ok(None);
    };
    let keys_scanned = projected_rows.len();

    Ok(Some((projected_rows, keys_scanned)))
}

#[cfg(feature = "sql")]
// Attempt one traversal-order direct projected-row path for single-component
// secondary covering reads.
fn try_materialize_sql_route_single_component_projected_rows(
    context: &SqlCoveringMaterializationContext<'_>,
    covering: &CoveringReadExecutionPlan,
    projection_field_slots: &[(String, usize)],
) -> Result<Option<CoveringProjectedRows>, InternalError> {
    let Some(scan_state) = context.covering_component_scan else {
        return Ok(None);
    };
    let Some((component_index, projected_field_sources)) =
        sql_route_single_component_projected_field_sources(covering, projection_field_slots)
    else {
        return Ok(None);
    };

    let consistency = row_read_consistency_for_plan(context.plan);
    let projected_rows = collect_single_component_covering_projection_values_from_lowered_specs(
        SingleComponentCoveringScanRequest {
            store: context.store,
            entity_tag: scan_state.entity_tag,
            index_prefix_specs: scan_state.index_prefix_specs,
            index_range_specs: scan_state.index_range_specs,
            direction: covering_projection_scan_direction(covering.order_contract),
            limit: covering_component_scan_budget_hint(
                covering.order_contract,
                context.scan_budget_hint,
            )
            .unwrap_or(usize::MAX),
            component_index,
            consistency,
            existing_row_mode: covering.existing_row_mode,
        },
        |storage_key, decoded_component| {
            sql_project_row_from_single_covering_component(
                projected_field_sources.as_slice(),
                storage_key,
                decoded_component,
            )
        },
    )?;
    let SingleComponentCoveringProjectionOutcome::Supported(projected_rows) = projected_rows else {
        return Ok(None);
    };
    let keys_scanned = projected_rows.len();

    Ok(Some((projected_rows, keys_scanned)))
}

#[cfg(feature = "sql")]
// Return whether one route-owned covering contract still requires an explicit
// row-presence check before SQL slot-row emission.
const fn sql_route_covering_row_check_required(
    load_terminal_fast_path: Option<&LoadTerminalFastPathContract>,
) -> bool {
    match load_terminal_fast_path {
        Some(LoadTerminalFastPathContract::CoveringRead(covering)) => {
            covering.existing_row_mode.requires_row_presence_check()
        }
        None => false,
    }
}

#[cfg(feature = "sql")]
// Return whether a constant-plus-primary-key SQL short path may trust the
// current covering route contract. These short paths only see the ordered key
// stream itself, so they cannot honor membership-level storage witnesses.
const fn sql_route_covering_allows_constant_short_path(
    load_terminal_fast_path: Option<&LoadTerminalFastPathContract>,
) -> bool {
    match load_terminal_fast_path {
        Some(LoadTerminalFastPathContract::CoveringRead(covering)) => {
            !covering.existing_row_mode.uses_storage_existence_witness()
        }
        None => true,
    }
}

#[cfg(feature = "sql")]
// Attempt one route-owned covering-read slot-row materialization path when
// the explicit route contract can satisfy every projected field from index
// components, bound constants, and the primary key alone.
fn try_materialize_sql_route_covering_slot_rows(
    context: &SqlCoveringMaterializationContext<'_>,
) -> Result<Option<CoveringSlotRows>, InternalError> {
    let Some(LoadTerminalFastPathContract::CoveringRead(covering)) =
        context.load_terminal_fast_path
    else {
        return Ok(None);
    };
    let Some(scan_state) = context.covering_component_scan else {
        return Ok(None);
    };
    let Some(layout) =
        sql_route_covering_slot_layout(context.model, covering, context.predicate_slots)?
    else {
        return Ok(None);
    };
    let Some((decoded_rows, keys_scanned)) = sql_route_covering_component_rows(
        context.plan,
        context.store,
        scan_state,
        covering,
        context.scan_budget_hint,
        layout.component_slots.as_slice(),
    )?
    else {
        return Ok(None);
    };

    // Phase 1: materialize slot rows from decoded covering components.
    let mut rows =
        sql_route_covering_slot_rows_from_decoded(&layout, context.predicate_slots, decoded_rows);

    // Phase 2: restore the required output order when the covering contract is
    // primary-key ordered rather than traversal ordered.
    reorder_covering_projection_pairs(covering.order_contract, rows.as_mut_slice());

    Ok(Some((
        rows.into_iter().map(|(_, row)| row).collect::<Vec<_>>(),
        keys_scanned,
    )))
}

#[cfg(feature = "sql")]
// Derive one route-owned slot layout plus component-slot grouping for the
// SQL covering-read fast path and reject any residual predicate that reaches
// beyond the fail-closed covered slot set.
fn sql_route_covering_slot_layout(
    model: &'static EntityModel,
    covering: &CoveringReadExecutionPlan,
    predicate_slots: Option<&PredicateProgram>,
) -> Result<Option<SqlRouteCoveringSlotLayout>, InternalError> {
    let primary_key_slot = model
        .fields
        .iter()
        .position(|field| field.name == model.primary_key.name)
        .ok_or_else(|| {
            InternalError::query_executor_invariant(
                "covering-read SQL short path requires a primary-key slot",
            )
        })?;
    let mut constant_slots = Vec::new();
    let mut covered_slots = vec![false; model.fields.len()];
    let mut component_slots = Vec::<CoveringComponentSlotGroup>::new();

    // Phase 1: project one sparse slot layout from the planner-owned contract and
    // group decoded component fields by their index component position.
    covered_slots[primary_key_slot] = true;
    for field in &covering.fields {
        match &field.source {
            CoveringReadFieldSource::PrimaryKey => {
                covered_slots[field.field_slot.index] = true;
            }
            CoveringReadFieldSource::Constant(value) => {
                constant_slots.push((field.field_slot.index, value.clone()));
                covered_slots[field.field_slot.index] = true;
            }
            CoveringReadFieldSource::IndexComponent { component_index } => {
                if let Some((_, slots)) = component_slots
                    .iter_mut()
                    .find(|(group_index, _)| group_index == component_index)
                {
                    slots.push(field.field_slot.index);
                } else {
                    component_slots.push((*component_index, vec![field.field_slot.index]));
                }
                covered_slots[field.field_slot.index] = true;
            }
        }
    }

    // Phase 2: reject empty component plans and predicates that would still
    // require row materialization outside the covered slot set.
    if component_slots.is_empty() {
        return Ok(None);
    }
    if predicate_slots.is_some_and(|predicate| !predicate.references_only_slots(&covered_slots)) {
        return Ok(None);
    }

    Ok(Some(SqlRouteCoveringSlotLayout {
        primary_key_slot,
        slot_count: model.fields.len(),
        constant_slots,
        component_slots,
    }))
}

#[cfg(feature = "sql")]
// Resolve one direct projected-row source layout from the planner-owned
// covering contract plus the canonical direct field projection order.
fn sql_route_direct_projected_field_sources(
    covering: &CoveringReadExecutionPlan,
    projection_field_slots: &[(String, usize)],
    component_slots: &[CoveringComponentSlotGroup],
) -> Result<Option<Vec<SqlDirectProjectedFieldSource>>, InternalError> {
    let mut projected_field_sources = Vec::with_capacity(projection_field_slots.len());

    for (_, field_slot) in projection_field_slots {
        let Some(covering_field) = covering
            .fields
            .iter()
            .find(|field| field.field_slot.index == *field_slot)
        else {
            return Ok(None);
        };

        let projected_source = match &covering_field.source {
            CoveringReadFieldSource::PrimaryKey => SqlDirectProjectedFieldSource::PrimaryKey,
            CoveringReadFieldSource::Constant(value) => {
                SqlDirectProjectedFieldSource::Constant(value.clone())
            }
            CoveringReadFieldSource::IndexComponent { component_index } => {
                let Some(decoded_component_index) = component_slots
                    .iter()
                    .position(|(group_index, _)| group_index == component_index)
                else {
                    return Err(InternalError::query_executor_invariant(
                        "covering-read SQL projected-row path requires one decoded component source per projected field",
                    ));
                };

                SqlDirectProjectedFieldSource::IndexComponent {
                    decoded_component_index,
                }
            }
        };

        projected_field_sources.push(projected_source);
    }

    Ok(Some(projected_field_sources))
}

#[cfg(feature = "sql")]
// Resolve one traversal-order single-component projected-row source layout.
fn sql_route_single_component_projected_field_sources(
    covering: &CoveringReadExecutionPlan,
    projection_field_slots: &[(String, usize)],
) -> Option<(usize, Vec<SqlDirectProjectedFieldSource>)> {
    if !matches!(
        covering.order_contract,
        CoveringProjectionOrder::IndexOrder(_)
    ) {
        return None;
    }

    let mut shared_component_index = None;
    let mut projected_field_sources = Vec::with_capacity(projection_field_slots.len());

    for (_, field_slot) in projection_field_slots {
        let covering_field = covering
            .fields
            .iter()
            .find(|field| field.field_slot.index == *field_slot)?;

        let projected_source = match &covering_field.source {
            CoveringReadFieldSource::PrimaryKey => SqlDirectProjectedFieldSource::PrimaryKey,
            CoveringReadFieldSource::Constant(value) => {
                SqlDirectProjectedFieldSource::Constant(value.clone())
            }
            CoveringReadFieldSource::IndexComponent { component_index } => {
                match shared_component_index {
                    Some(existing) if existing != *component_index => return None,
                    Some(_) => {}
                    None => shared_component_index = Some(*component_index),
                }

                SqlDirectProjectedFieldSource::IndexComponent {
                    decoded_component_index: 0,
                }
            }
        };

        projected_field_sources.push(projected_source);
    }

    Some((shared_component_index?, projected_field_sources))
}

#[cfg(feature = "sql")]
// Resolve one direct projected-row layout when every projected value comes
// from the authoritative primary key or a bound constant only.
fn sql_route_constant_projected_field_sources(
    covering: &CoveringReadExecutionPlan,
    projection_field_slots: &[(String, usize)],
) -> Option<Vec<SqlDirectProjectedFieldSource>> {
    let mut projected_field_sources = Vec::with_capacity(projection_field_slots.len());

    for (_, field_slot) in projection_field_slots {
        let covering_field = covering
            .fields
            .iter()
            .find(|field| field.field_slot.index == *field_slot)?;

        let projected_source = match &covering_field.source {
            CoveringReadFieldSource::PrimaryKey => SqlDirectProjectedFieldSource::PrimaryKey,
            CoveringReadFieldSource::Constant(value) => {
                SqlDirectProjectedFieldSource::Constant(value.clone())
            }
            CoveringReadFieldSource::IndexComponent { .. } => return None,
        };

        projected_field_sources.push(projected_source);
    }

    Some(projected_field_sources)
}

#[cfg(feature = "sql")]
// Scan one route-owned covering membership stream under the existing-row
// contract while preserving the planner-owned traversal order and per-entry
// existence witness.
fn sql_route_covering_membership_rows(
    plan: &AccessPlannedQuery,
    store: StoreHandle,
    scan_state: Option<CoveringComponentScanState<'_>>,
    covering: &CoveringReadExecutionPlan,
    scan_budget_hint: Option<usize>,
) -> Result<Option<(CoveringMembershipRows, usize)>, InternalError> {
    let Some(scan_state) = scan_state else {
        return Ok(None);
    };
    let scan_direction = covering_projection_scan_direction(covering.order_contract);
    let effective_scan_budget_hint =
        covering_component_scan_budget_hint(covering.order_contract, scan_budget_hint);
    let raw_pairs = resolve_covering_memberships_from_lowered_specs(
        scan_state.entity_tag,
        scan_state.index_prefix_specs,
        scan_state.index_range_specs,
        scan_direction,
        effective_scan_budget_hint.unwrap_or(usize::MAX),
        |_| Ok(store),
    )?;
    let keys_scanned = raw_pairs.len();
    let _ = plan;

    Ok(Some((raw_pairs, keys_scanned)))
}

#[cfg(feature = "sql")]
// Scan and decode one route-owned covering component stream under the
// existing-row contract while preserving the planner-owned traversal order.
fn sql_route_covering_component_text_rows(
    plan: &AccessPlannedQuery,
    store: StoreHandle,
    scan_state: CoveringComponentScanState<'_>,
    covering: &CoveringReadExecutionPlan,
    scan_budget_hint: Option<usize>,
    component_slots: &[CoveringComponentSlotGroup],
) -> Result<Option<(RenderedCoveringComponentRows, usize)>, InternalError> {
    let scan_direction = covering_projection_scan_direction(covering.order_contract);
    let effective_scan_budget_hint =
        covering_component_scan_budget_hint(covering.order_contract, scan_budget_hint);
    let component_indices = component_slots
        .iter()
        .map(|(component_index, _)| *component_index)
        .collect::<Vec<_>>();

    let raw_pairs: CoveringProjectionComponentRows =
        resolve_covering_projection_components_from_lowered_specs(
            scan_state.entity_tag,
            scan_state.index_prefix_specs,
            scan_state.index_range_specs,
            scan_direction,
            effective_scan_budget_hint.unwrap_or(usize::MAX),
            component_indices.as_slice(),
            |_| Ok(store),
        )?;
    let keys_scanned = raw_pairs.len();
    let consistency = row_read_consistency_for_plan(plan);
    let rendered_rows = map_covering_projection_pairs(
        raw_pairs,
        store,
        consistency,
        covering.existing_row_mode,
        render_sql_covering_projection_components,
    )?;

    Ok(rendered_rows.map(|rows| (rows, keys_scanned)))
}

#[cfg(feature = "sql")]
// Scan and decode one route-owned covering component stream under the
// existing-row contract while preserving the planner-owned traversal order.
fn sql_route_covering_component_rows(
    plan: &AccessPlannedQuery,
    store: StoreHandle,
    scan_state: CoveringComponentScanState<'_>,
    covering: &CoveringReadExecutionPlan,
    scan_budget_hint: Option<usize>,
    component_slots: &[CoveringComponentSlotGroup],
) -> Result<Option<(DecodedCoveringComponentRows, usize)>, InternalError> {
    let scan_direction = covering_projection_scan_direction(covering.order_contract);
    let effective_scan_budget_hint =
        covering_component_scan_budget_hint(covering.order_contract, scan_budget_hint);
    let component_indices = component_slots
        .iter()
        .map(|(component_index, _)| *component_index)
        .collect::<Vec<_>>();

    let raw_pairs: CoveringProjectionComponentRows =
        resolve_covering_projection_components_from_lowered_specs(
            scan_state.entity_tag,
            scan_state.index_prefix_specs,
            scan_state.index_range_specs,
            scan_direction,
            effective_scan_budget_hint.unwrap_or(usize::MAX),
            component_indices.as_slice(),
            |_| Ok(store),
        )?;
    let keys_scanned = raw_pairs.len();
    let consistency = row_read_consistency_for_plan(plan);
    let decoded_rows = decode_covering_projection_pairs(
        raw_pairs,
        store,
        consistency,
        covering.existing_row_mode,
        |decoded| {
            if decoded.len() != component_slots.len() {
                return Err(InternalError::query_executor_invariant(
                    "covering-read SQL short path component scan returned mismatched component count",
                ));
            }

            Ok(decoded)
        },
    )?;

    Ok(decoded_rows.map(|rows| (rows, keys_scanned)))
}

#[cfg(feature = "sql")]
// Materialize final slot rows from one decoded covering component stream while
// preserving residual predicate evaluation over the projected slot layout.
fn sql_route_covering_slot_rows_from_decoded(
    layout: &SqlRouteCoveringSlotLayout,
    predicate_slots: Option<&PredicateProgram>,
    decoded_rows: DecodedCoveringComponentRows,
) -> CoveringSlotRowPairs {
    let mut rows = Vec::with_capacity(decoded_rows.len());

    for (data_key, component_values) in decoded_rows {
        // Phase 1: build one fresh sparse slot row from the immutable covering
        // layout instead of cloning a full slot template per decoded key.
        let mut row = vec![None; layout.slot_count];
        row[layout.primary_key_slot] = Some(data_key.storage_key().as_primary_key_value());
        for (slot, value) in &layout.constant_slots {
            row[*slot] = Some(value.clone());
        }

        // Phase 2: fill decoded component values, cloning only when one
        // component fans out to more than one projected slot.
        for ((_, slots), component_value) in layout
            .component_slots
            .iter()
            .zip(component_values.into_iter())
        {
            let Some((last_slot, prefix_slots)) = slots.split_last() else {
                continue;
            };

            for slot in prefix_slots {
                row[*slot] = Some(component_value.clone());
            }
            row[*last_slot] = Some(component_value);
        }

        // Phase 3: preserve the residual predicate contract before staging the
        // row into the SQL projection output.
        if let Some(predicate_program) = predicate_slots
            && !predicate_program
                .eval_with_slot_value_ref_reader(&mut |slot| row.get(slot).and_then(Option::as_ref))
        {
            continue;
        }

        rows.push((data_key, row));
    }

    rows
}

#[cfg(feature = "sql")]
// Materialize final projected SQL rows from one decoded covering component
// stream when the SQL projection is already a direct unique field list.
fn sql_route_covering_projected_rows_from_decoded(
    projected_field_sources: &[SqlDirectProjectedFieldSource],
    decoded_rows: DecodedCoveringComponentRows,
) -> Result<CoveringProjectedRowPairs, InternalError> {
    let mut rows = Vec::with_capacity(decoded_rows.len());

    for (data_key, component_values) in decoded_rows {
        let mut projected_row = Vec::with_capacity(projected_field_sources.len());

        for projected_source in projected_field_sources {
            let value = match projected_source {
                SqlDirectProjectedFieldSource::PrimaryKey => {
                    data_key.storage_key().as_primary_key_value()
                }
                SqlDirectProjectedFieldSource::Constant(value) => value.clone(),
                SqlDirectProjectedFieldSource::IndexComponent {
                    decoded_component_index,
                } => component_values
                    .get(*decoded_component_index)
                    .cloned()
                    .ok_or_else(|| {
                        InternalError::query_executor_invariant(
                            "covering-read SQL projected-row path decoded fewer components than the direct projection contract requires",
                        )
                    })?,
            };
            projected_row.push(value);
        }

        rows.push((data_key, projected_row));
    }

    Ok(rows)
}

#[cfg(feature = "sql")]
// Materialize final projected SQL rows directly from one rendered covering
// component stream when the SQL projection is already a direct unique field
// list.
fn sql_route_covering_projected_text_rows_from_rendered(
    projected_field_sources: &[SqlDirectProjectedFieldSource],
    rendered_rows: RenderedCoveringComponentRows,
) -> Result<CoveringProjectedTextRowPairs, InternalError> {
    let mut rows = Vec::with_capacity(rendered_rows.len());

    for (data_key, component_values) in rendered_rows {
        let mut projected_row = Vec::with_capacity(projected_field_sources.len());

        for projected_source in projected_field_sources {
            let value = match projected_source {
                SqlDirectProjectedFieldSource::PrimaryKey => {
                    render_sql_primary_key_text(&data_key.storage_key())
                }
                SqlDirectProjectedFieldSource::Constant(value) => {
                    render_sql_direct_constant_text(value)?
                }
                SqlDirectProjectedFieldSource::IndexComponent {
                    decoded_component_index,
                } => component_values
                    .get(*decoded_component_index)
                    .cloned()
                    .ok_or_else(|| {
                        InternalError::query_executor_invariant(
                            "covering-read SQL projected-row text path rendered fewer components than the direct projection contract requires",
                        )
                    })?,
            };
            projected_row.push(value);
        }

        rows.push((data_key, projected_row));
    }

    Ok(rows)
}

#[cfg(feature = "sql")]
// Project one SQL row directly from the authoritative primary key plus any
// bound covering constants.
fn sql_project_row_from_constant_covering_sources(
    projected_field_sources: &[SqlDirectProjectedFieldSource],
    storage_key: &StorageKey,
) -> Result<Vec<Value>, InternalError> {
    let mut projected_row = Vec::with_capacity(projected_field_sources.len());

    for projected_source in projected_field_sources {
        let value = match projected_source {
            SqlDirectProjectedFieldSource::PrimaryKey => storage_key.as_primary_key_value(),
            SqlDirectProjectedFieldSource::Constant(value) => value.clone(),
            SqlDirectProjectedFieldSource::IndexComponent { .. } => {
                return Err(InternalError::query_executor_invariant(
                    "constant projected-row path must not reference decoded index components",
                ));
            }
        };
        projected_row.push(value);
    }

    Ok(projected_row)
}

#[cfg(feature = "sql")]
// Project one SQL row directly into rendered text from the authoritative
// primary key plus any bound covering constants.
fn sql_project_text_row_from_constant_covering_sources(
    projected_field_sources: &[SqlDirectProjectedFieldSource],
    storage_key: &StorageKey,
) -> Result<Vec<String>, InternalError> {
    let mut projected_row = Vec::with_capacity(projected_field_sources.len());

    for projected_source in projected_field_sources {
        let value = match projected_source {
            SqlDirectProjectedFieldSource::PrimaryKey => render_sql_primary_key_text(storage_key),
            SqlDirectProjectedFieldSource::Constant(value) => {
                render_sql_direct_constant_text(value)?
            }
            SqlDirectProjectedFieldSource::IndexComponent { .. } => {
                return Err(InternalError::query_executor_invariant(
                    "constant projected-row text path must not reference decoded index components",
                ));
            }
        };
        projected_row.push(value);
    }

    Ok(projected_row)
}

#[cfg(feature = "sql")]
// Project one SQL row directly from a single decoded covering component plus
// the authoritative primary key.
fn sql_project_row_from_single_covering_component(
    projected_field_sources: &[SqlDirectProjectedFieldSource],
    storage_key: StorageKey,
    decoded_component: &Value,
) -> Result<Vec<Value>, InternalError> {
    let mut projected_row = Vec::with_capacity(projected_field_sources.len());

    for projected_source in projected_field_sources {
        let value = match projected_source {
            SqlDirectProjectedFieldSource::PrimaryKey => storage_key.as_primary_key_value(),
            SqlDirectProjectedFieldSource::Constant(value) => value.clone(),
            SqlDirectProjectedFieldSource::IndexComponent {
                decoded_component_index,
            } => {
                if *decoded_component_index != 0 {
                    return Err(InternalError::query_executor_invariant(
                        "single-component projected-row path must only reference decoded component zero",
                    ));
                }

                decoded_component.clone()
            }
        };
        projected_row.push(value);
    }

    Ok(projected_row)
}

#[cfg(feature = "sql")]
// Project one SQL row directly into rendered text from a single covering
// component plus the authoritative primary key.
fn sql_project_text_row_from_single_covering_component(
    projected_field_sources: &[SqlDirectProjectedFieldSource],
    storage_key: StorageKey,
    rendered_component: &str,
) -> Result<Vec<String>, InternalError> {
    let mut projected_row = Vec::with_capacity(projected_field_sources.len());

    for projected_source in projected_field_sources {
        let value = match projected_source {
            SqlDirectProjectedFieldSource::PrimaryKey => render_sql_primary_key_text(&storage_key),
            SqlDirectProjectedFieldSource::Constant(value) => {
                render_sql_direct_constant_text(value)?
            }
            SqlDirectProjectedFieldSource::IndexComponent {
                decoded_component_index,
            } => {
                if *decoded_component_index != 0 {
                    return Err(InternalError::query_executor_invariant(
                        "single-component projected-row text path must only reference decoded component zero",
                    ));
                }

                rendered_component.to_string()
            }
        };
        projected_row.push(value);
    }

    Ok(projected_row)
}

#[cfg(feature = "sql")]
// Resolve one safe bounded component-scan hint for the SQL covering-read path.
// Index-order projections may stop early at the bounded fetch limit, but any
// route that still owes primary-key reordering must consume the full component
// stream before it can safely apply the logical page window.
const fn covering_component_scan_budget_hint(
    order_contract: CoveringProjectionOrder,
    scan_budget_hint: Option<usize>,
) -> Option<usize> {
    match order_contract {
        CoveringProjectionOrder::IndexOrder(_) => scan_budget_hint,
        CoveringProjectionOrder::PrimaryKeyOrder(_) => None,
    }
}

#[cfg(feature = "sql")]
// Build one slot-row template directly from the route-owned scalar
// covering-read contract when every projected value comes from a bound
// constant or the primary key.
fn sql_constant_covering_slot_row_template_from_route_contract(
    model: &'static EntityModel,
    load_terminal_fast_path: Option<&LoadTerminalFastPathContract>,
    predicate_slots: Option<&PredicateProgram>,
) -> Option<(Vec<Option<Value>>, usize)> {
    let LoadTerminalFastPathContract::CoveringRead(covering) = load_terminal_fast_path?;
    if !sql_route_covering_allows_constant_short_path(load_terminal_fast_path) {
        return None;
    }
    let primary_key_slot = model
        .fields
        .iter()
        .position(|field| field.name == model.primary_key.name)?;
    let mut slot_template = vec![None; model.fields.len()];
    let mut covered_slots = vec![false; model.fields.len()];
    covered_slots[primary_key_slot] = true;

    // Phase 1: project one canonical slot template directly from the route
    // contract. Any index-component field still falls back to the existing
    // local helper because this short path only reconstructs constants plus
    // primary-key values today.
    for field in &covering.fields {
        match &field.source {
            CoveringReadFieldSource::PrimaryKey => {
                covered_slots[field.field_slot.index] = true;
            }
            CoveringReadFieldSource::Constant(value) => {
                slot_template[field.field_slot.index] = Some(value.clone());
                covered_slots[field.field_slot.index] = true;
            }
            CoveringReadFieldSource::IndexComponent { .. } => return None,
        }
    }

    // Phase 2: keep the existing predicate-slot safety rule before the row
    // collector stops reading persisted rows.
    if predicate_slots.is_some_and(|predicate| !predicate.references_only_slots(&covered_slots)) {
        return None;
    }

    Some((slot_template, primary_key_slot))
}

#[cfg(feature = "sql")]
// Build one slot-row template when projection and predicate semantics stay
// fully within bound access-prefix fields plus the primary key.
fn sql_constant_covering_slot_row_template(
    plan: &AccessPlannedQuery,
    model: &'static EntityModel,
    predicate_slots: Option<&PredicateProgram>,
) -> Option<(Vec<Option<Value>>, usize)> {
    let projection = plan.projection_spec(model);
    let primary_key_slot = model
        .fields
        .iter()
        .position(|field| field.name == model.primary_key.name)?;
    let mut slot_template = vec![None; model.fields.len()];
    let mut covered_slots = vec![false; model.fields.len()];
    let mut covered_fields = vec![model.primary_key.name];
    covered_slots[primary_key_slot] = true;

    // Phase 1: recover every equality-bound index-prefix component once.
    for (slot, field) in model.fields.iter().enumerate() {
        if slot == primary_key_slot {
            continue;
        }

        if let Some(value) =
            constant_covering_projection_value_from_access(&plan.access, field.name)
        {
            slot_template[slot] = Some(value);
            covered_slots[slot] = true;
            covered_fields.push(field.name);
        }
    }

    // Phase 2: require both projection and residual predicate to stay within
    // the covered slot set before we stop reading persisted rows.
    if !projection_references_only_fields(&projection, covered_fields.as_slice()) {
        return None;
    }
    if plan.has_residual_predicate()
        && !predicate_slots.is_some_and(|predicate| predicate.references_only_slots(&covered_slots))
    {
        return None;
    }

    Some((slot_template, primary_key_slot))
}

#[cfg(feature = "sql")]
fn render_sql_covering_projection_components(
    components: Vec<Vec<u8>>,
) -> Result<Option<Vec<String>>, InternalError> {
    let mut rendered = Vec::with_capacity(components.len());
    for component in components {
        let Some(value) = render_sql_covering_component_text(component.as_slice())? else {
            return Ok(None);
        };
        rendered.push(value);
    }

    Ok(Some(rendered))
}

#[cfg(feature = "sql")]
fn render_sql_covering_component_text(component: &[u8]) -> Result<Option<String>, InternalError> {
    let Some((&tag, payload)) = component.split_first() else {
        return Err(InternalError::bytes_covering_component_payload_empty());
    };

    if tag == crate::value::ValueTag::Bool.to_u8() {
        let Some(value) = payload.first() else {
            return Err(InternalError::bytes_covering_bool_payload_truncated());
        };
        if payload.len() != SQL_COVERING_BOOL_PAYLOAD_LEN {
            return Err(InternalError::bytes_covering_component_payload_invalid_length("bool"));
        }

        return match *value {
            0 => Ok(Some(false.to_string())),
            1 => Ok(Some(true.to_string())),
            _ => Err(InternalError::bytes_covering_bool_payload_invalid_value()),
        };
    }
    if tag == crate::value::ValueTag::Int.to_u8() {
        if payload.len() != SQL_COVERING_U64_PAYLOAD_LEN {
            return Err(InternalError::bytes_covering_component_payload_invalid_length("int"));
        }

        let mut bytes = [0u8; SQL_COVERING_U64_PAYLOAD_LEN];
        bytes.copy_from_slice(payload);
        let biased = u64::from_be_bytes(bytes);
        let unsigned = biased ^ SQL_COVERING_I64_SIGN_BIT_BIAS;
        let value = i64::from_be_bytes(unsigned.to_be_bytes());

        return Ok(Some(value.to_string()));
    }
    if tag == crate::value::ValueTag::Uint.to_u8() {
        if payload.len() != SQL_COVERING_U64_PAYLOAD_LEN {
            return Err(InternalError::bytes_covering_component_payload_invalid_length("uint"));
        }

        let mut bytes = [0u8; SQL_COVERING_U64_PAYLOAD_LEN];
        bytes.copy_from_slice(payload);

        return Ok(Some(u64::from_be_bytes(bytes).to_string()));
    }
    if tag == crate::value::ValueTag::Text.to_u8() {
        let mut bytes = Vec::new();
        let mut i = 0usize;

        while i < payload.len() {
            let byte = payload[i];
            if byte != SQL_COVERING_TEXT_ESCAPE_PREFIX {
                bytes.push(byte);
                i = i.saturating_add(1);
                continue;
            }

            let Some(next) = payload.get(i.saturating_add(1)).copied() else {
                return Err(InternalError::bytes_covering_text_payload_invalid_terminator());
            };
            match next {
                SQL_COVERING_TEXT_TERMINATOR => {
                    i = i.saturating_add(2);
                    if i != payload.len() {
                        return Err(InternalError::bytes_covering_text_payload_trailing_bytes());
                    }

                    let text = String::from_utf8(bytes)
                        .map_err(|_| InternalError::bytes_covering_text_payload_invalid_utf8())?;

                    return Ok(Some(text));
                }
                SQL_COVERING_TEXT_ESCAPED_ZERO => {
                    bytes.push(0);
                    i = i.saturating_add(2);
                }
                _ => {
                    return Err(InternalError::bytes_covering_text_payload_invalid_escape_byte());
                }
            }
        }

        return Err(InternalError::bytes_covering_text_payload_missing_terminator());
    }
    if tag == crate::value::ValueTag::Ulid.to_u8() {
        if payload.len() != SQL_COVERING_ULID_PAYLOAD_LEN {
            return Err(InternalError::bytes_covering_component_payload_invalid_length("ulid"));
        }

        let mut bytes = [0u8; SQL_COVERING_ULID_PAYLOAD_LEN];
        bytes.copy_from_slice(payload);

        return Ok(Some(crate::types::Ulid::from_bytes(bytes).to_string()));
    }
    if tag == crate::value::ValueTag::Unit.to_u8() {
        return Ok(Some("()".to_string()));
    }

    Ok(None)
}

#[cfg(feature = "sql")]
fn render_sql_primary_key_text(storage_key: &StorageKey) -> String {
    match storage_key {
        StorageKey::Account(value) => value.to_string(),
        StorageKey::Int(value) => value.to_string(),
        StorageKey::Principal(value) => value.to_string(),
        StorageKey::Subaccount(value) => value.to_string(),
        StorageKey::Timestamp(value) => value.as_millis().to_string(),
        StorageKey::Uint(value) => value.to_string(),
        StorageKey::Ulid(value) => value.to_string(),
        StorageKey::Unit => "()".to_string(),
    }
}

#[cfg(feature = "sql")]
fn render_sql_direct_constant_text(value: &Value) -> Result<String, InternalError> {
    match value {
        Value::Account(value) => Ok(value.to_string()),
        Value::Bool(value) => Ok(value.to_string()),
        Value::Int(value) => Ok(value.to_string()),
        Value::Principal(value) => Ok(value.to_string()),
        Value::Subaccount(value) => Ok(value.to_string()),
        Value::Text(value) => Ok(value.clone()),
        Value::Timestamp(value) => Ok(value.as_millis().to_string()),
        Value::Uint(value) => Ok(value.to_string()),
        Value::Ulid(value) => Ok(value.to_string()),
        Value::Unit => Ok("()".to_string()),
        _ => Err(InternalError::query_executor_invariant(
            "rendered SQL direct projected-row path requires one canonically renderable constant value",
        )),
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::db::{
        direction::Direction,
        executor::route::LoadTerminalFastPathContract,
        query::plan::{
            CoveringExistingRowMode, CoveringProjectionOrder, CoveringReadExecutionPlan,
        },
    };

    #[test]
    fn covering_component_scan_budget_hint_disables_bounded_fetch_for_pk_reorder() {
        assert_eq!(
            super::covering_component_scan_budget_hint(
                CoveringProjectionOrder::PrimaryKeyOrder(Direction::Asc),
                Some(3),
            ),
            None,
            "component scans that still owe primary-key reordering must ignore bounded fetch hints",
        );
        assert_eq!(
            super::covering_component_scan_budget_hint(
                CoveringProjectionOrder::PrimaryKeyOrder(Direction::Desc),
                Some(4),
            ),
            None,
            "descending primary-key reorder must also consume the full component stream",
        );
        assert_eq!(
            super::covering_component_scan_budget_hint(
                CoveringProjectionOrder::IndexOrder(Direction::Asc),
                Some(5),
            ),
            Some(5),
            "index-order covering scans may preserve their bounded fetch hint",
        );
    }

    #[test]
    fn constant_covering_short_paths_reject_storage_existence_witness() {
        let row_check_covering =
            LoadTerminalFastPathContract::CoveringRead(CoveringReadExecutionPlan {
                fields: Vec::new(),
                order_contract: CoveringProjectionOrder::IndexOrder(Direction::Asc),
                prefix_len: 0,
                existing_row_mode: CoveringExistingRowMode::RequiresRowPresenceCheck,
            });
        let storage_witness_covering =
            LoadTerminalFastPathContract::CoveringRead(CoveringReadExecutionPlan {
                fields: Vec::new(),
                order_contract: CoveringProjectionOrder::IndexOrder(Direction::Asc),
                prefix_len: 0,
                existing_row_mode: CoveringExistingRowMode::StorageExistenceWitness,
            });

        assert!(
            super::sql_route_covering_allows_constant_short_path(Some(&row_check_covering)),
            "constant-plus-primary-key SQL short paths may still run under row_check_required because they perform their own authoritative row presence checks",
        );
        assert!(
            !super::sql_route_covering_allows_constant_short_path(Some(&storage_witness_covering)),
            "constant-plus-primary-key SQL short paths must fail closed under storage_existence_witness because the ordered key stream does not carry membership-level missing-row witnesses",
        );
    }
}
