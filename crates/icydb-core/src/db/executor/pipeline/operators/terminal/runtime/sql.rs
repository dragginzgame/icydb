//! Module: db::executor::pipeline::operators::terminal::runtime
//! Responsibility: module-local ownership and contracts for db::executor::pipeline::operators::terminal::runtime.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

#[cfg(feature = "sql")]
use crate::db::data::DataKey;
#[cfg(feature = "sql")]
use crate::value::Value;

#[cfg(feature = "sql")]
type CoveringSlotRows = (Vec<crate::db::executor::RetainedSlotRow>, usize);
#[cfg(feature = "sql")]
type CoveringComponentSlotGroup = (usize, Vec<usize>);
#[cfg(feature = "sql")]
type DecodedCoveringComponentRows = Vec<(DataKey, Vec<Value>)>;
#[cfg(feature = "sql")]
type CoveringSlotRowPairs = Vec<(DataKey, crate::db::executor::RetainedSlotRow)>;

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
    store: StoreHandle,
    covering_component_scan: Option<CoveringComponentScanState<'a>>,
    load_terminal_fast_path: Option<&'a LoadTerminalFastPathContract>,
    prepared_projection_shape: Option<&'a PreparedProjectionShape>,
    prepared_projection_validation:
        Option<&'a crate::db::executor::projection::PreparedSlotProjectionValidation>,
    prepared_sql_projection: Option<&'a PreparedSqlExecutionProjection>,
    projection_materialization: ProjectionMaterializationMode,
    fuse_immediate_sql_terminal: bool,
    scan_budget_hint: Option<usize>,
    predicate_slots: Option<&'a PredicateProgram>,
}

#[cfg(feature = "sql")]
// Constant-covering key scans choose their row-presence policy once so the
// hot per-key loop does not branch on both `row_check_required` and missing-
// row consistency for every scanned key.
#[derive(Clone, Copy)]
enum SqlStoreRowPresenceMode {
    Unchecked,
    IgnoreMissing,
    RequireExisting,
}

#[cfg(feature = "sql")]
const fn sql_store_row_presence_mode(
    row_check_required: bool,
    consistency: crate::db::predicate::MissingRowPolicy,
) -> SqlStoreRowPresenceMode {
    if !row_check_required {
        return SqlStoreRowPresenceMode::Unchecked;
    }

    match consistency {
        crate::db::predicate::MissingRowPolicy::Ignore => SqlStoreRowPresenceMode::IgnoreMissing,
        crate::db::predicate::MissingRowPolicy::Error => SqlStoreRowPresenceMode::RequireExisting,
    }
}

#[cfg(feature = "sql")]
fn scan_constant_covering_keys<NextKeyFn, OnKeyFn>(
    store: StoreHandle,
    row_presence_mode: SqlStoreRowPresenceMode,
    next_key: &mut NextKeyFn,
    on_kept_key: &mut OnKeyFn,
) -> Result<usize, InternalError>
where
    NextKeyFn: FnMut() -> Result<Option<DataKey>, InternalError>,
    OnKeyFn: FnMut(DataKey) -> Result<(), InternalError>,
{
    let mut keys_scanned = 0usize;

    match row_presence_mode {
        SqlStoreRowPresenceMode::Unchecked => {
            while let Some(key) = next_key()? {
                keys_scanned = keys_scanned.saturating_add(1);
                on_kept_key(key)?;
            }
        }
        SqlStoreRowPresenceMode::IgnoreMissing => {
            while let Some(key) = next_key()? {
                keys_scanned = keys_scanned.saturating_add(1);
                if !read_row_presence_ignoring_missing_from_store(store, &key)? {
                    continue;
                }

                on_kept_key(key)?;
            }
        }
        SqlStoreRowPresenceMode::RequireExisting => {
            while let Some(key) = next_key()? {
                keys_scanned = keys_scanned.saturating_add(1);
                if !read_row_presence_requiring_existing_from_store(store, &key)? {
                    continue;
                }

                on_kept_key(key)?;
            }
        }
    }

    Ok(keys_scanned)
}

///
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
    component_indices: Vec<usize>,
    slot_sources: Vec<Option<SqlRouteCoveringSlotSource>>,
}

///
/// SqlRouteCoveringSlotLayoutBuilder
///
/// Mutable builder for one route-owned covering slot layout.
/// It keeps field-source lowering and final admissibility checks separate so
/// the prepared slot-layout helper does not own all phases in one function.
///

#[cfg(feature = "sql")]
struct SqlRouteCoveringSlotLayoutBuilder {
    primary_key_slot: usize,
    slot_count: usize,
    covered_slots: Vec<bool>,
    constant_slots: Vec<(usize, Value)>,
    component_slots: Vec<CoveringComponentSlotGroup>,
    component_indices: Vec<usize>,
    slot_sources: Vec<Option<SqlRouteCoveringSlotSource>>,
}

#[cfg(feature = "sql")]
impl SqlRouteCoveringSlotLayoutBuilder {
    fn finish(self) -> SqlRouteCoveringSlotLayout {
        SqlRouteCoveringSlotLayout {
            primary_key_slot: self.primary_key_slot,
            slot_count: self.slot_count,
            constant_slots: self.constant_slots,
            component_slots: self.component_slots,
            component_indices: self.component_indices,
            slot_sources: self.slot_sources,
        }
    }
}

///
/// SqlRouteCoveringSlotSource
///
/// SqlRouteCoveringSlotSource records how one route-owned covering slot can
/// be read directly from PK, constant, or decoded component storage without
/// first materializing a retained slot row.
///

#[cfg(feature = "sql")]
#[derive(Clone, Copy)]
enum SqlRouteCoveringSlotSource {
    PrimaryKey,
    Constant(usize),
    DecodedComponent(usize),
}

///
/// SqlConstantCoveringSlotTemplate
///
/// SqlConstantCoveringSlotTemplate keeps the constant-only covering slot-row
/// shape in sparse form until one row survives residual predicate checks.
/// This lets the constant covering short path avoid cloning one full dense
/// slot template for every key before the row is even admitted.
///

#[cfg(feature = "sql")]
struct SqlConstantCoveringSlotTemplate {
    slot_count: usize,
    primary_key_slot: usize,
    constant_slots: Vec<(usize, Value)>,
    slot_to_constant_index: Vec<Option<usize>>,
}

///
/// SqlConstantCoveringSlotTemplateBuilder
///
/// Mutable builder for one constant-covering slot template.
/// It keeps shared slot-template bootstrap and constant-slot registration in
/// one place so the route-derived and access-derived template helpers do not
/// duplicate the same state management.
///

#[cfg(feature = "sql")]
struct SqlConstantCoveringSlotTemplateBuilder {
    slot_count: usize,
    primary_key_slot: usize,
    covered_slots: Vec<bool>,
    constant_slots: Vec<(usize, Value)>,
    slot_to_constant_index: Vec<Option<usize>>,
}

#[cfg(feature = "sql")]
impl SqlConstantCoveringSlotTemplateBuilder {
    fn finish(self) -> SqlConstantCoveringSlotTemplate {
        SqlConstantCoveringSlotTemplate {
            slot_count: self.slot_count,
            primary_key_slot: self.primary_key_slot,
            constant_slots: self.constant_slots,
            slot_to_constant_index: self.slot_to_constant_index,
        }
    }
}

///
/// PreparedSqlExecutionProjection
///
/// PreparedSqlExecutionProjection freezes the SQL-only projection metadata
/// that is invariant for one execution attempt.
/// The terminal runtime borrows this prepared state directly so cursorless SQL
/// short paths do not rebuild slot templates or validation
/// metadata inside the hot execution loop.
///

#[cfg(feature = "sql")]
pub(in crate::db::executor) struct PreparedSqlExecutionProjection {
    route_covering_slot_layout: Option<SqlRouteCoveringSlotLayout>,
    constant_covering_slot_template: Option<SqlConstantCoveringSlotTemplate>,
}

impl PreparedSqlExecutionProjection {
    #[must_use]
    const fn route_covering_slot_layout(&self) -> Option<&SqlRouteCoveringSlotLayout> {
        self.route_covering_slot_layout.as_ref()
    }

    #[must_use]
    const fn constant_covering_slot_template(&self) -> Option<&SqlConstantCoveringSlotTemplate> {
        self.constant_covering_slot_template.as_ref()
    }
}

use crate::{
    db::{
        cursor::CursorBoundary,
        executor::{
            BudgetedOrderedKeyStream, CoveringProjectionComponentRows, ExecutionKernel,
            OrderedKeyStream, ScalarContinuationBindings, covering_projection_scan_direction,
            decode_covering_projection_pairs, exact_output_key_count_hint,
            key_stream_budget_is_redundant,
            pipeline::contracts::{
                CoveringComponentScanState, DirectCoveringScanMaterializationRequest,
                MaterializedExecutionPayload, ProjectionMaterializationMode,
                RowCollectorMaterializationRequest,
            },
            projection::{
                PreparedProjectionShape, project_sql_projection_slot_rows_for_dispatch,
                render_sql_projection_slot_rows_for_dispatch,
            },
            read_row_presence_ignoring_missing_from_store,
            read_row_presence_requiring_existing_from_store, reorder_covering_projection_pairs,
            resolve_covering_projection_components_from_lowered_specs,
            route::{
                LoadOrderRouteContract, LoadTerminalFastPathContract,
                access_order_satisfied_by_route_contract,
            },
            terminal::{
                RowLayout,
                page::{
                    KernelRow, KernelRowPayloadMode, KernelRowScanRequest, ScalarRowRuntimeHandle,
                    execute_kernel_row_scan,
                },
            },
            traversal::row_read_consistency_for_plan,
        },
        predicate::PredicateProgram,
        query::plan::{
            AccessPlannedQuery, CoveringProjectionOrder, CoveringReadExecutionPlan,
            CoveringReadFieldSource, constant_covering_projection_value_from_access,
        },
        registry::StoreHandle,
    },
    error::InternalError,
};

impl ExecutionKernel {
    // Materialize one direct covering-scan short path before generic
    // key-stream resolution when the same cursorless SQL covering contract can
    // already produce the final structural page directly.
    #[cfg(feature = "sql")]
    pub(in crate::db::executor) fn try_materialize_load_via_direct_covering_scan(
        request: DirectCoveringScanMaterializationRequest<'_>,
        store: StoreHandle,
        covering_component_scan: Option<CoveringComponentScanState<'_>>,
    ) -> Result<Option<(MaterializedExecutionPayload, usize, usize)>, InternalError> {
        let DirectCoveringScanMaterializationRequest {
            plan,
            scan_budget_hint,
            cursor_boundary,
            load_terminal_fast_path,
            predicate_slots,
            validate_projection,
            retain_slot_rows,
            prepared_projection_shape,
            prepared_projection_validation,
            prepared_sql_projection,
            projection_materialization,
            fuse_immediate_sql_terminal,
        } = request;

        let sql_covering_context = SqlCoveringMaterializationContext {
            plan,
            store,
            covering_component_scan,
            load_terminal_fast_path,
            prepared_projection_shape,
            prepared_projection_validation,
            prepared_sql_projection,
            projection_materialization,
            fuse_immediate_sql_terminal,
            scan_budget_hint,
            predicate_slots,
        };

        try_materialize_cursorless_sql_covering_scan_without_key_stream(
            &sql_covering_context,
            cursor_boundary,
            retain_slot_rows,
            validate_projection,
        )
    }

    // Materialize one direct covering-scan short path before generic
    // key-stream resolution when SQL covering materialization is compiled out.
    #[cfg(not(feature = "sql"))]
    pub(in crate::db::executor) fn try_materialize_load_via_direct_covering_scan(
        request: DirectCoveringScanMaterializationRequest<'_>,
        store: StoreHandle,
        covering_component_scan: Option<CoveringComponentScanState<'_>>,
    ) -> Result<Option<(MaterializedExecutionPayload, usize, usize)>, InternalError> {
        let _ = (request, store, covering_component_scan);

        Ok(None)
    }

    // Return whether load execution can safely use the row-collector short path
    // without changing cursor/pagination/filter semantics.
    pub(in crate::db::executor::pipeline::operators::terminal) fn load_row_collector_short_path_eligible(
        plan: &AccessPlannedQuery,
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
            && (logical.order.is_none() || access_order_satisfied_by_route_contract(plan));

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
            retained_slot_layout,
        } = request;

        // Phase 1: derive the shared row scan contract from plan-owned
        // consistency and residual-predicate state.
        let consistency = row_read_consistency_for_plan(plan);
        let predicate_preapplied = plan.has_residual_predicate();
        let _ = continuation;
        let _ = load_order_route_contract;

        // Phase 2: reuse the canonical structural row scan boundary from the
        // generic scalar-page runtime and only add the SQL-specific keep-cap.
        execute_kernel_row_scan(KernelRowScanRequest {
            key_stream,
            scan_budget_hint,
            consistency,
            payload_mode,
            predicate_slots,
            predicate_preapplied,
            retained_slot_layout,
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
        store: StoreHandle,
        covering_component_scan: Option<CoveringComponentScanState<'a>>,
    ) -> Result<Option<(MaterializedExecutionPayload, usize, usize)>, InternalError> {
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
            retained_slot_layout,
            prepared_projection_shape,
            prepared_projection_validation,
            prepared_sql_projection,
            projection_materialization,
            fuse_immediate_sql_terminal,
            key_stream,
        } = request;

        if !Self::load_row_collector_short_path_eligible(plan, cursor_boundary, retain_slot_rows) {
            return Ok(None);
        }

        continuation.validate_load_scan_budget_hint(scan_budget_hint, load_order_route_contract)?;

        #[cfg(feature = "sql")]
        let sql_covering_context = SqlCoveringMaterializationContext {
            plan,
            store,
            covering_component_scan,
            load_terminal_fast_path,
            prepared_projection_shape,
            prepared_projection_validation,
            prepared_sql_projection,
            projection_materialization,
            fuse_immediate_sql_terminal,
            scan_budget_hint,
            predicate_slots,
        };

        #[cfg(feature = "sql")]
        if retain_slot_rows
            && let Some(sql_page) = try_materialize_cursorless_sql_short_path(
                &sql_covering_context,
                key_stream,
                validate_projection,
            )?
        {
            return Ok(Some(sql_page));
        }

        let payload_mode = select_cursorless_row_collector_payload_mode(
            retain_slot_rows,
            cursor_boundary,
            retained_slot_layout,
        );
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
            retained_slot_layout,
        })?;
        apply_cursorless_row_collector_post_access(
            plan,
            validate_projection,
            prepared_projection_validation,
            retain_slot_rows,
            &mut rows,
        )?;
        let post_access_rows = rows.len();
        let finalize_mode =
            select_cursorless_row_collector_finalize_mode(&sql_covering_context, retain_slot_rows)?;
        let payload = finalize_cursorless_row_collector_payload(rows, finalize_mode)?;

        Ok(Some((payload, keys_scanned, post_access_rows)))
    }
}

#[cfg(feature = "sql")]
// Attempt the SQL-only cursorless short path before falling back to the shared
// row-collector kernel. This keeps the already-projected and retained-slot-row
// lanes under one explicit terminal-owned contract.
fn try_materialize_cursorless_sql_short_path(
    context: &SqlCoveringMaterializationContext<'_>,
    key_stream: &mut dyn OrderedKeyStream,
    validate_projection: bool,
) -> Result<Option<(MaterializedExecutionPayload, usize, usize)>, InternalError> {
    if let Some(page) =
        try_materialize_cursorless_sql_covering_scan_short_path(context, validate_projection)?
    {
        return Ok(Some(page));
    }

    try_materialize_cursorless_sql_key_stream_short_path(context, key_stream, validate_projection)
}

#[cfg(feature = "sql")]
// Attempt the cursorless SQL covering-scan lane before any generic ordered
// key stream is resolved. This is restricted to the same short-path cohort the
// terminal already proves it can materialize from route-owned covering scans.
fn try_materialize_cursorless_sql_covering_scan_without_key_stream(
    context: &SqlCoveringMaterializationContext<'_>,
    cursor_boundary: Option<&CursorBoundary>,
    retain_slot_rows: bool,
    validate_projection: bool,
) -> Result<Option<(MaterializedExecutionPayload, usize, usize)>, InternalError> {
    if !ExecutionKernel::load_row_collector_short_path_eligible(
        context.plan,
        cursor_boundary,
        retain_slot_rows,
    ) {
        return Ok(None);
    }

    try_materialize_cursorless_sql_covering_scan_short_path(context, validate_projection)
}

#[cfg(feature = "sql")]
// Attempt one cursorless SQL covering-scan short path that consumes only the
// route-owned covering component scan contract and does not need a generic
// ordered key stream.
fn try_materialize_cursorless_sql_covering_scan_short_path(
    context: &SqlCoveringMaterializationContext<'_>,
    validate_projection: bool,
) -> Result<Option<(MaterializedExecutionPayload, usize, usize)>, InternalError> {
    if let Some((slot_rows, keys_scanned)) = try_materialize_sql_route_covering_slot_rows(context)?
    {
        let page = finalize_cursorless_sql_slot_row_page(
            context,
            context.plan,
            context.prepared_projection_validation,
            validate_projection,
            slot_rows,
            keys_scanned,
        )?;

        return Ok(Some(page));
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
) -> Result<Option<(MaterializedExecutionPayload, usize, usize)>, InternalError> {
    if let Some((slot_rows, keys_scanned)) =
        try_materialize_sql_covering_slot_rows(context, key_stream)?
    {
        let page = finalize_cursorless_sql_slot_row_page(
            context,
            context.plan,
            context.prepared_projection_validation,
            validate_projection,
            slot_rows,
            keys_scanned,
        )?;

        return Ok(Some(page));
    }

    Ok(None)
}

#[cfg(feature = "sql")]
// Finalize one cursorless SQL slot-row page after either covering-scan or
// key-stream materialization so pagination, projection validation, and page
// packaging stay on one canonical boundary.
fn finalize_cursorless_sql_slot_row_page(
    context: &SqlCoveringMaterializationContext<'_>,
    plan: &AccessPlannedQuery,
    prepared_projection_validation: Option<
        &crate::db::executor::projection::PreparedSlotProjectionValidation,
    >,
    validate_projection: bool,
    mut slot_rows: Vec<crate::db::executor::RetainedSlotRow>,
    keys_scanned: usize,
) -> Result<(MaterializedExecutionPayload, usize, usize), InternalError> {
    if !cursorless_sql_page_window_is_redundant(plan, slot_rows.len()) {
        apply_cursorless_sql_page_window(plan, &mut slot_rows);
    }

    if validate_projection {
        let prepared_projection_validation =
            required_prepared_projection_validation(prepared_projection_validation)?;
        for row in &slot_rows {
            crate::db::executor::projection::validate_prepared_projection_row(
                prepared_projection_validation,
                &mut |slot| row.slot_ref(slot),
            )?;
        }
    }

    let post_access_rows = slot_rows.len();
    let finalize_mode = select_cursorless_row_collector_finalize_mode(context, true)?;
    let payload = finalize_cursorless_slot_row_payload(slot_rows, finalize_mode)?;

    Ok((payload, keys_scanned, post_access_rows))
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
    retained_slot_layout: Option<&'a crate::db::executor::RetainedSlotLayout>,
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

// Select one row payload mode before cursorless row collection so the scan
// loop does not branch on data-vs-slot materialization per row.
const fn select_cursorless_row_collector_payload_mode(
    retain_slot_rows: bool,
    cursor_boundary: Option<&CursorBoundary>,
    retained_slot_layout: Option<&crate::db::executor::RetainedSlotLayout>,
) -> KernelRowPayloadMode {
    if retain_slot_rows && cursor_boundary.is_none() {
        KernelRowPayloadMode::SlotsOnly
    } else if retained_slot_layout.is_some() {
        KernelRowPayloadMode::FullRowRetained
    } else {
        KernelRowPayloadMode::DataRowOnly
    }
}

// Apply the remaining cursorless SQL post-access work after the kernel scan:
// optional page window and optional slot-row projection validation.
fn apply_cursorless_row_collector_post_access(
    plan: &AccessPlannedQuery,
    validate_projection: bool,
    prepared_projection_validation: Option<
        &crate::db::executor::projection::PreparedSlotProjectionValidation,
    >,
    retain_slot_rows: bool,
    rows: &mut Vec<KernelRow>,
) -> Result<(), InternalError> {
    if retain_slot_rows && !cursorless_sql_page_window_is_redundant(plan, rows.len()) {
        apply_cursorless_sql_page_window(plan, rows);
    }

    if validate_projection {
        let prepared_projection_validation =
            required_prepared_projection_validation(prepared_projection_validation)?;
        for row in rows {
            crate::db::executor::projection::validate_prepared_projection_row(
                prepared_projection_validation,
                &mut |slot| row.slot_ref(slot),
            )?;
        }
    }

    Ok(())
}

// Cursorless row-collector finalization still has two families:
// structural page output and fused immediate SQL terminal output.
// Select that family once before converting kernel rows.
#[cfg(feature = "sql")]
enum CursorlessRowCollectorFinalizeMode<'a> {
    StructuralDataRows,
    StructuralSlotRows,
    SqlProjected {
        prepared_projection_shape: &'a PreparedProjectionShape,
    },
    SqlRendered {
        prepared_projection_shape: &'a PreparedProjectionShape,
    },
}

#[cfg(feature = "sql")]
fn select_cursorless_row_collector_finalize_mode<'a>(
    context: &'a SqlCoveringMaterializationContext<'a>,
    retain_slot_rows: bool,
) -> Result<CursorlessRowCollectorFinalizeMode<'a>, InternalError> {
    if context.fuse_immediate_sql_terminal && retain_slot_rows {
        let prepared_projection_shape =
            required_prepared_projection_shape(context.prepared_projection_shape)?;

        return match context.projection_materialization {
            ProjectionMaterializationMode::SqlImmediateMaterialization => {
                Ok(CursorlessRowCollectorFinalizeMode::SqlProjected {
                    prepared_projection_shape,
                })
            }
            ProjectionMaterializationMode::SqlImmediateRenderedDispatch => {
                Ok(CursorlessRowCollectorFinalizeMode::SqlRendered {
                    prepared_projection_shape,
                })
            }
            ProjectionMaterializationMode::SharedValidation => {
                Ok(CursorlessRowCollectorFinalizeMode::StructuralSlotRows)
            }
        };
    }

    if retain_slot_rows {
        return Ok(CursorlessRowCollectorFinalizeMode::StructuralSlotRows);
    }

    Ok(CursorlessRowCollectorFinalizeMode::StructuralDataRows)
}

#[cfg(feature = "sql")]
fn finalize_cursorless_row_collector_payload(
    rows: Vec<KernelRow>,
    finalize_mode: CursorlessRowCollectorFinalizeMode<'_>,
) -> Result<MaterializedExecutionPayload, InternalError> {
    match finalize_mode {
        CursorlessRowCollectorFinalizeMode::StructuralDataRows => {
            Ok(MaterializedExecutionPayload::StructuralPage(
                crate::db::executor::pipeline::contracts::StructuralCursorPage::new(
                    collect_cursorless_data_rows(rows)?,
                    None,
                ),
            ))
        }
        mode @ (CursorlessRowCollectorFinalizeMode::StructuralSlotRows
        | CursorlessRowCollectorFinalizeMode::SqlProjected { .. }
        | CursorlessRowCollectorFinalizeMode::SqlRendered { .. }) => {
            finalize_cursorless_slot_row_payload(collect_cursorless_slot_rows(rows)?, mode)
        }
    }
}

#[cfg(feature = "sql")]
fn finalize_cursorless_slot_row_payload(
    slot_rows: Vec<crate::db::executor::RetainedSlotRow>,
    finalize_mode: CursorlessRowCollectorFinalizeMode<'_>,
) -> Result<MaterializedExecutionPayload, InternalError> {
    match finalize_mode {
        CursorlessRowCollectorFinalizeMode::StructuralSlotRows => {
            Ok(MaterializedExecutionPayload::StructuralPage(
                crate::db::executor::pipeline::contracts::StructuralCursorPage::new_with_slot_rows(
                    slot_rows, None,
                ),
            ))
        }
        CursorlessRowCollectorFinalizeMode::SqlProjected {
            prepared_projection_shape,
        } => Ok(MaterializedExecutionPayload::SqlProjectedRows(
            project_sql_projection_slot_rows_for_dispatch(prepared_projection_shape, slot_rows)?,
        )),
        CursorlessRowCollectorFinalizeMode::SqlRendered {
            prepared_projection_shape,
        } => Ok(MaterializedExecutionPayload::SqlRenderedRows(
            render_sql_projection_slot_rows_for_dispatch(prepared_projection_shape, slot_rows)?,
        )),
        CursorlessRowCollectorFinalizeMode::StructuralDataRows => {
            Err(InternalError::query_executor_invariant(
                "slot-row cursorless finalization requires one slot-row payload mode",
            ))
        }
    }
}

#[cfg(feature = "sql")]
fn collect_cursorless_slot_rows(
    rows: Vec<KernelRow>,
) -> Result<Vec<crate::db::executor::RetainedSlotRow>, InternalError> {
    rows.into_iter()
        .map(KernelRow::into_retained_slot_row)
        .collect()
}

#[cfg(feature = "sql")]
fn collect_cursorless_data_rows(
    rows: Vec<KernelRow>,
) -> Result<Vec<crate::db::data::DataRow>, InternalError> {
    rows.into_iter().map(KernelRow::into_data_row).collect()
}

#[cfg(feature = "sql")]
fn required_prepared_projection_shape(
    prepared_projection_shape: Option<&PreparedProjectionShape>,
) -> Result<&PreparedProjectionShape, InternalError> {
    prepared_projection_shape.ok_or_else(|| {
        InternalError::query_executor_invariant(
            "fused cursorless SQL terminal requires prepared projection shape",
        )
    })
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
    if start == 0 {
        rows.truncate(end);
        return;
    }

    let mut kept = 0usize;
    for read_index in start..end {
        if kept != read_index {
            rows.swap(kept, read_index);
        }
        kept = kept.saturating_add(1);
    }

    rows.truncate(kept);
}

#[cfg(feature = "sql")]
// Require the prepared projection-validation bundle whenever one cursorless
// slot-row path still asks the shared executor validator to run.
fn required_prepared_projection_validation(
    prepared_projection_validation: Option<
        &crate::db::executor::projection::PreparedSlotProjectionValidation,
    >,
) -> Result<&crate::db::executor::projection::PreparedSlotProjectionValidation, InternalError> {
    prepared_projection_validation.ok_or_else(|| {
        InternalError::query_executor_invariant(
            "slot-row projection validation requires prepared projection state",
        )
    })
}

#[cfg(feature = "sql")]
// Return whether one covering-projection pair stream must be reordered to
// restore the planner-owned logical output order.
const fn sql_covering_projection_pairs_require_reorder(
    order_contract: CoveringProjectionOrder,
) -> bool {
    matches!(order_contract, CoveringProjectionOrder::PrimaryKeyOrder(_))
}

#[cfg(feature = "sql")]
// Strip the authoritative data keys once one covering slot-row pair stream is
// already in the required final output order.
fn strip_covering_slot_row_pairs(
    rows: CoveringSlotRowPairs,
) -> Vec<crate::db::executor::RetainedSlotRow> {
    rows.into_iter().map(|(_, row)| row).collect()
}

#[cfg(feature = "sql")]
/// Build one prepared SQL projection bundle from plan-owned and route-owned
/// metadata before execution begins.
pub(in crate::db::executor) fn prepare_sql_execution_projection(
    row_layout: RowLayout,
    plan: &AccessPlannedQuery,
    compiled_predicate: Option<&PredicateProgram>,
    projection_materialization: ProjectionMaterializationMode,
    load_terminal_fast_path: Option<&LoadTerminalFastPathContract>,
) -> Result<Option<PreparedSqlExecutionProjection>, InternalError> {
    if !projection_materialization.retain_slot_rows() {
        return Ok(None);
    }

    // Phase 1: prepare the one route-owned covering slot layout used by the
    // structural slot-row short path when the planner proved covering access.
    let route_covering_slot_layout =
        if let Some(covering) = sql_route_covering_contract(load_terminal_fast_path) {
            sql_route_covering_slot_layout(row_layout, covering, compiled_predicate)?
        } else {
            None
        };

    // Phase 2: prepare the sparse constant-covering slot template used by the
    // key-stream short path when output rows can be reconstructed from PK and
    // bound constants only.
    let constant_covering_slot_template =
        sql_constant_covering_slot_row_template_from_route_contract(
            row_layout,
            load_terminal_fast_path,
            compiled_predicate,
        )
        .or_else(|| sql_constant_covering_slot_row_template(plan, row_layout, compiled_predicate));

    Ok(Some(PreparedSqlExecutionProjection {
        route_covering_slot_layout,
        constant_covering_slot_template,
    }))
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
    if let Some(slot_template) = context
        .prepared_sql_projection
        .and_then(PreparedSqlExecutionProjection::constant_covering_slot_template)
    {
        let row_presence_mode = sql_store_row_presence_mode(
            sql_route_covering_row_check_required(context.load_terminal_fast_path),
            row_read_consistency_for_plan(context.plan),
        );
        let mut rows = Vec::with_capacity(
            exact_output_key_count_hint(key_stream, context.scan_budget_hint).unwrap_or(0),
        );
        let keys_scanned = if let Some(scan_budget) = context.scan_budget_hint
            && !key_stream_budget_is_redundant(key_stream, scan_budget)
        {
            let mut budgeted = BudgetedOrderedKeyStream::new(key_stream, scan_budget);

            scan_constant_covering_slot_rows(
                context.store,
                row_presence_mode,
                slot_template,
                context.predicate_slots,
                &mut || budgeted.next_key(),
                &mut rows,
            )?
        } else {
            scan_constant_covering_slot_rows(
                context.store,
                row_presence_mode,
                slot_template,
                context.predicate_slots,
                &mut || key_stream.next_key(),
                &mut rows,
            )?
        };

        return Ok(Some((rows, keys_scanned)));
    }

    Ok(None)
}

#[cfg(feature = "sql")]
// Materialize constant-covering retained rows from one caller-owned key source
// so the budgeted and unbudgeted short paths share the same row-presence,
// predicate, and retained-row construction logic.
fn scan_constant_covering_slot_rows<NextKeyFn>(
    store: StoreHandle,
    row_presence_mode: SqlStoreRowPresenceMode,
    slot_template: &SqlConstantCoveringSlotTemplate,
    predicate_slots: Option<&PredicateProgram>,
    next_key: &mut NextKeyFn,
    rows: &mut Vec<crate::db::executor::RetainedSlotRow>,
) -> Result<usize, InternalError>
where
    NextKeyFn: FnMut() -> Result<Option<DataKey>, InternalError>,
{
    scan_constant_covering_keys(store, row_presence_mode, next_key, &mut |key| {
        let primary_key_value = key.storage_key().as_primary_key_value();

        if let Some(predicate_program) = predicate_slots
            && !predicate_program.eval_with_slot_value_ref_reader(&mut |slot| {
                sql_constant_covering_slot_ref(slot_template, &primary_key_value, slot)
            })
        {
            return Ok(());
        }

        rows.push(sql_constant_covering_retained_row(
            slot_template,
            primary_key_value,
        ));

        Ok(())
    })
}

#[cfg(feature = "sql")]
const fn sql_route_covering_contract(
    load_terminal_fast_path: Option<&LoadTerminalFastPathContract>,
) -> Option<&CoveringReadExecutionPlan> {
    match load_terminal_fast_path {
        Some(LoadTerminalFastPathContract::CoveringRead(covering)) => Some(covering),
        None => None,
    }
}

#[cfg(feature = "sql")]
// Return whether one route-owned covering contract still requires an explicit
// row-presence check before SQL slot-row emission.
const fn sql_route_covering_row_check_required(
    load_terminal_fast_path: Option<&LoadTerminalFastPathContract>,
) -> bool {
    match sql_route_covering_contract(load_terminal_fast_path) {
        Some(covering) => covering.existing_row_mode.requires_row_presence_check(),
        None => false,
    }
}

#[cfg(feature = "sql")]
// Attempt one route-owned covering-read slot-row materialization path when
// the explicit route contract can satisfy every projected field from index
// components, bound constants, and the primary key alone.
fn try_materialize_sql_route_covering_slot_rows(
    context: &SqlCoveringMaterializationContext<'_>,
) -> Result<Option<CoveringSlotRows>, InternalError> {
    let Some(prepared_sql_projection) = context.prepared_sql_projection else {
        return Ok(None);
    };
    let Some(covering) = sql_route_covering_contract(context.load_terminal_fast_path) else {
        return Ok(None);
    };
    let Some(scan_state) = context.covering_component_scan else {
        return Ok(None);
    };
    let Some(layout) = prepared_sql_projection.route_covering_slot_layout() else {
        return Ok(None);
    };
    let Some((decoded_rows, keys_scanned)) = sql_route_covering_component_rows(
        context.plan,
        context.store,
        scan_state,
        covering,
        context.scan_budget_hint,
        layout,
    )?
    else {
        return Ok(None);
    };

    // Phase 1: materialize slot rows from decoded covering components.
    let mut rows =
        sql_route_covering_slot_rows_from_decoded(layout, context.predicate_slots, decoded_rows);

    // Phase 2: restore the required output order when the covering contract is
    // primary-key ordered rather than traversal ordered.
    if sql_covering_projection_pairs_require_reorder(covering.order_contract) {
        reorder_covering_projection_pairs(covering.order_contract, rows.as_mut_slice());
    }

    Ok(Some((strip_covering_slot_row_pairs(rows), keys_scanned)))
}

#[cfg(feature = "sql")]
// Derive one route-owned slot layout plus component-slot grouping for the
// SQL covering-read fast path and reject any residual predicate that reaches
// beyond the fail-closed covered slot set.
#[expect(
    clippy::unnecessary_wraps,
    reason = "shared SQL runtime setup keeps one fallible boundary even when this helper currently returns only success or None"
)]
fn sql_route_covering_slot_layout(
    row_layout: RowLayout,
    covering: &CoveringReadExecutionPlan,
    predicate_slots: Option<&PredicateProgram>,
) -> Result<Option<SqlRouteCoveringSlotLayout>, InternalError> {
    let mut layout = sql_route_covering_slot_layout_seed(row_layout);
    sql_extend_route_covering_slot_layout(&mut layout, covering);

    // Phase 2: reject empty component plans and predicates that would still
    // require row materialization outside the covered slot set.
    if layout.component_slots.is_empty() {
        return Ok(None);
    }
    if !sql_predicate_slots_admitted(layout.covered_slots.as_slice(), predicate_slots) {
        return Ok(None);
    }

    Ok(Some(layout.finish()))
}

#[cfg(feature = "sql")]
// Seed one route-covering slot-layout builder with the always-covered primary
// key slot so later field-source lowering can stay focused on non-PK sources.
fn sql_route_covering_slot_layout_seed(row_layout: RowLayout) -> SqlRouteCoveringSlotLayoutBuilder {
    let primary_key_slot = row_layout.primary_key_slot();
    let mut covered_slots = vec![false; row_layout.field_count()];
    let mut slot_sources = vec![None; row_layout.field_count()];
    covered_slots[primary_key_slot] = true;
    slot_sources[primary_key_slot] = Some(SqlRouteCoveringSlotSource::PrimaryKey);

    SqlRouteCoveringSlotLayoutBuilder {
        primary_key_slot,
        slot_count: row_layout.field_count(),
        covered_slots,
        constant_slots: Vec::new(),
        component_slots: Vec::new(),
        component_indices: Vec::new(),
        slot_sources,
    }
}

#[cfg(feature = "sql")]
// Extend one route-covering slot-layout builder from the planner-owned field
// contract so source lowering stays on one explicit phase boundary.
fn sql_extend_route_covering_slot_layout(
    layout: &mut SqlRouteCoveringSlotLayoutBuilder,
    covering: &CoveringReadExecutionPlan,
) {
    for field in &covering.fields {
        match &field.source {
            CoveringReadFieldSource::PrimaryKey => {
                layout.covered_slots[field.field_slot.index] = true;
                layout.slot_sources[field.field_slot.index] =
                    Some(SqlRouteCoveringSlotSource::PrimaryKey);
            }
            CoveringReadFieldSource::Constant(value) => {
                layout
                    .constant_slots
                    .push((field.field_slot.index, value.clone()));
                layout.covered_slots[field.field_slot.index] = true;
                layout.slot_sources[field.field_slot.index] =
                    Some(SqlRouteCoveringSlotSource::Constant(
                        layout.constant_slots.len().saturating_sub(1),
                    ));
            }
            CoveringReadFieldSource::IndexComponent { component_index } => {
                let decoded_component_index =
                    sql_route_covering_component_slot_group_index(layout, *component_index);
                layout.covered_slots[field.field_slot.index] = true;
                layout.slot_sources[field.field_slot.index] = Some(
                    SqlRouteCoveringSlotSource::DecodedComponent(decoded_component_index),
                );
                layout.component_slots[decoded_component_index]
                    .1
                    .push(field.field_slot.index);
            }
        }
    }
}

#[cfg(feature = "sql")]
// Resolve one decoded-component slot group, creating it on first use so
// planner field lowering can treat repeated component references uniformly.
fn sql_route_covering_component_slot_group_index(
    layout: &mut SqlRouteCoveringSlotLayoutBuilder,
    component_index: usize,
) -> usize {
    if let Some((decoded_component_index, _)) = layout
        .component_slots
        .iter()
        .enumerate()
        .find(|(_, (group_component_index, _))| *group_component_index == component_index)
    {
        return decoded_component_index;
    }

    layout.component_slots.push((component_index, Vec::new()));
    layout.component_indices.push(component_index);

    layout.component_slots.len().saturating_sub(1)
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
    route_covering_slot_layout: &SqlRouteCoveringSlotLayout,
) -> Result<Option<(DecodedCoveringComponentRows, usize)>, InternalError> {
    let scan_direction = covering_projection_scan_direction(covering.order_contract);
    let effective_scan_budget_hint =
        covering_component_scan_budget_hint(covering.order_contract, scan_budget_hint);
    let raw_pairs: CoveringProjectionComponentRows =
        resolve_covering_projection_components_from_lowered_specs(
            scan_state.entity_tag,
            scan_state.index_prefix_specs,
            scan_state.index_range_specs,
            scan_direction,
            effective_scan_budget_hint.unwrap_or(usize::MAX),
            route_covering_slot_layout.component_indices.as_slice(),
            |_| Ok(store),
        )?;
    let keys_scanned = raw_pairs.len();
    let consistency = row_read_consistency_for_plan(plan);
    let decoded_rows = decode_covering_projection_pairs(
        raw_pairs,
        store,
        consistency,
        covering.existing_row_mode,
        |decoded| sql_validate_route_covering_component_values(route_covering_slot_layout, decoded),
    )?;

    Ok(decoded_rows.map(|rows| (rows, keys_scanned)))
}

#[cfg(feature = "sql")]
// Validate one decoded covering-component vector against the prepared route
// layout before later row shaping assumes a one-to-one component mapping.
fn sql_validate_route_covering_component_values(
    layout: &SqlRouteCoveringSlotLayout,
    decoded: Vec<Value>,
) -> Result<Vec<Value>, InternalError> {
    if decoded.len() != layout.component_slots.len() {
        return Err(InternalError::query_executor_invariant(
            "covering-read SQL short path component scan returned mismatched component count",
        ));
    }

    Ok(decoded)
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
        let primary_key_value = data_key.storage_key().as_primary_key_value();

        // Phase 1: preserve the residual predicate contract directly over the
        // decoded covering sources so rejected rows do not allocate one full
        // retained-slot image first.
        if let Some(predicate_program) = predicate_slots
            && !predicate_program.eval_with_slot_value_ref_reader(&mut |slot| {
                sql_route_covering_decoded_slot_ref(
                    layout,
                    &primary_key_value,
                    component_values.as_slice(),
                    slot,
                )
            })
        {
            continue;
        }

        // Phase 2: only surviving rows pay the retained-slot materialization cost.
        rows.push(sql_route_covering_slot_row_pair(
            layout,
            data_key,
            primary_key_value,
            component_values,
        ));
    }

    rows
}

#[cfg(feature = "sql")]
// Build one `(data_key, retained_row)` pair from already-admitted covering
// component values so the outer materializer loop stays focused on filtering.
fn sql_route_covering_slot_row_pair(
    layout: &SqlRouteCoveringSlotLayout,
    data_key: DataKey,
    primary_key_value: Value,
    component_values: Vec<Value>,
) -> (DataKey, crate::db::executor::RetainedSlotRow) {
    (
        data_key,
        sql_route_covering_retained_row(layout, primary_key_value, component_values),
    )
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
    row_layout: RowLayout,
    load_terminal_fast_path: Option<&LoadTerminalFastPathContract>,
    predicate_slots: Option<&PredicateProgram>,
) -> Option<SqlConstantCoveringSlotTemplate> {
    let covering = sql_route_covering_contract(load_terminal_fast_path)?;
    let mut template = sql_constant_covering_slot_template_seed(row_layout);

    // Phase 1: project one canonical slot template directly from the route
    // contract. Any index-component field still falls back to the existing
    // local helper because this short path only reconstructs constants plus
    // primary-key values today.
    for field in &covering.fields {
        match &field.source {
            CoveringReadFieldSource::PrimaryKey => {
                template.covered_slots[field.field_slot.index] = true;
            }
            CoveringReadFieldSource::Constant(value) => {
                sql_constant_covering_template_add_constant(
                    &mut template,
                    field.field_slot.index,
                    value.clone(),
                );
            }
            CoveringReadFieldSource::IndexComponent { .. } => return None,
        }
    }

    // Phase 2: keep the existing predicate-slot safety rule before the row
    // collector stops reading persisted rows.
    if !sql_constant_covering_predicate_slots_admitted(
        template.covered_slots.as_slice(),
        predicate_slots,
    ) {
        return None;
    }

    Some(template.finish())
}

#[cfg(feature = "sql")]
// Build one slot-row template when projection and predicate semantics stay
// fully within bound access-prefix fields plus the primary key.
fn sql_constant_covering_slot_row_template(
    plan: &AccessPlannedQuery,
    row_layout: RowLayout,
    predicate_slots: Option<&PredicateProgram>,
) -> Option<SqlConstantCoveringSlotTemplate> {
    let mut template = sql_constant_covering_slot_template_seed(row_layout);

    // Phase 1: recover every equality-bound index-prefix component once.
    for slot in 0..template.slot_count {
        if slot == template.primary_key_slot {
            continue;
        }

        let field_name = row_layout.field_name(slot)?;

        if let Some(value) =
            constant_covering_projection_value_from_access(&plan.access, field_name)
        {
            sql_constant_covering_template_add_constant(&mut template, slot, value);
        }
    }

    // Phase 2: require both projection and residual predicate to stay within
    // the covered slot set before we stop reading persisted rows.
    if !sql_constant_covering_plan_admitted(
        plan,
        template.covered_slots.as_slice(),
        predicate_slots,
    ) {
        return None;
    }

    Some(template.finish())
}

#[cfg(feature = "sql")]
// Return whether one residual predicate stays fully within the currently
// covered slot set for a constant-covering template candidate.
fn sql_constant_covering_predicate_slots_admitted(
    covered_slots: &[bool],
    predicate_slots: Option<&PredicateProgram>,
) -> bool {
    sql_predicate_slots_admitted(covered_slots, predicate_slots)
}

#[cfg(feature = "sql")]
// Return whether one residual predicate stays fully within the currently
// covered slot set for any SQL short-path candidate.
fn sql_predicate_slots_admitted(
    covered_slots: &[bool],
    predicate_slots: Option<&PredicateProgram>,
) -> bool {
    predicate_slots.is_none_or(|predicate| predicate.references_only_slots(covered_slots))
}

#[cfg(feature = "sql")]
// Return whether one constant-covering access-derived template may skip row
// materialization while preserving both projection and residual predicate semantics.
fn sql_constant_covering_plan_admitted(
    plan: &AccessPlannedQuery,
    covered_slots: &[bool],
    predicate_slots: Option<&PredicateProgram>,
) -> bool {
    if plan
        .projection_referenced_slots()
        .iter()
        .any(|slot| !covered_slots.get(*slot).copied().unwrap_or(false))
    {
        return false;
    }

    if plan.has_residual_predicate()
        && !sql_constant_covering_predicate_slots_admitted(covered_slots, predicate_slots)
    {
        return false;
    }

    true
}

#[cfg(feature = "sql")]
// Seed one constant-covering slot-template builder with the always-covered
// primary key so later constant registration can stay focused on non-PK slots.
fn sql_constant_covering_slot_template_seed(
    row_layout: RowLayout,
) -> SqlConstantCoveringSlotTemplateBuilder {
    let primary_key_slot = row_layout.primary_key_slot();
    let slot_count = row_layout.field_count();
    let mut covered_slots = vec![false; slot_count];
    covered_slots[primary_key_slot] = true;

    SqlConstantCoveringSlotTemplateBuilder {
        slot_count,
        primary_key_slot,
        covered_slots,
        constant_slots: Vec::new(),
        slot_to_constant_index: vec![None; slot_count],
    }
}

#[cfg(feature = "sql")]
// Register one constant-projected slot on the shared template builder so both
// constant-covering prep paths reuse the same slot bookkeeping.
fn sql_constant_covering_template_add_constant(
    template: &mut SqlConstantCoveringSlotTemplateBuilder,
    slot: usize,
    value: Value,
) {
    template.constant_slots.push((slot, value));
    template.slot_to_constant_index[slot] = Some(template.constant_slots.len().saturating_sub(1));
    template.covered_slots[slot] = true;
}

#[cfg(feature = "sql")]
fn sql_constant_covering_slot_ref<'a>(
    template: &'a SqlConstantCoveringSlotTemplate,
    primary_key_value: &'a Value,
    slot: usize,
) -> Option<&'a Value> {
    if slot == template.primary_key_slot {
        return Some(primary_key_value);
    }

    let constant_index = template
        .slot_to_constant_index
        .get(slot)
        .copied()
        .flatten()?;
    template
        .constant_slots
        .get(constant_index)
        .map(|(_, value)| value)
}

#[cfg(feature = "sql")]
fn sql_constant_covering_retained_row(
    template: &SqlConstantCoveringSlotTemplate,
    primary_key_value: Value,
) -> crate::db::executor::RetainedSlotRow {
    let mut entries = Vec::with_capacity(template.constant_slots.len().saturating_add(1));
    entries.push((template.primary_key_slot, primary_key_value));
    entries.extend(
        template
            .constant_slots
            .iter()
            .map(|(slot, value)| (*slot, value.clone())),
    );

    crate::db::executor::RetainedSlotRow::from_sparse_entries(template.slot_count, entries)
}

#[cfg(feature = "sql")]
fn sql_route_covering_decoded_slot_ref<'a>(
    layout: &'a SqlRouteCoveringSlotLayout,
    primary_key_value: &'a Value,
    component_values: &'a [Value],
    slot: usize,
) -> Option<&'a Value> {
    match layout.slot_sources.get(slot).and_then(Option::as_ref)? {
        SqlRouteCoveringSlotSource::PrimaryKey => Some(primary_key_value),
        SqlRouteCoveringSlotSource::Constant(constant_index) => layout
            .constant_slots
            .get(*constant_index)
            .map(|(_, value)| value),
        SqlRouteCoveringSlotSource::DecodedComponent(decoded_component_index) => {
            component_values.get(*decoded_component_index)
        }
    }
}

#[cfg(feature = "sql")]
fn sql_route_covering_retained_row(
    layout: &SqlRouteCoveringSlotLayout,
    primary_key_value: Value,
    component_values: Vec<Value>,
) -> crate::db::executor::RetainedSlotRow {
    let mut retained_slots = vec![None; layout.slot_count];
    retained_slots[layout.primary_key_slot] = Some(primary_key_value);
    for (slot, value) in &layout.constant_slots {
        retained_slots[*slot] = Some(value.clone());
    }

    for ((_, slots), component_value) in layout.component_slots.iter().zip(component_values) {
        let Some((last_slot, prefix_slots)) = slots.split_last() else {
            continue;
        };

        for slot in prefix_slots {
            retained_slots[*slot] = Some(component_value.clone());
        }
        retained_slots[*last_slot] = Some(component_value);
    }

    crate::db::executor::RetainedSlotRow::from_dense_slots(retained_slots)
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::db::{direction::Direction, query::plan::CoveringProjectionOrder};

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
}
