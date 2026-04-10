//! Module: db::executor::pipeline::operators::terminal::runtime
//! Responsibility: module-local ownership and contracts for db::executor::pipeline::operators::terminal::runtime.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

#[cfg(feature = "sql")]
use crate::db::data::DataKey;
#[cfg(feature = "sql")]
use crate::value::{StorageKey, Value};

#[cfg(feature = "sql")]
type CoveringSlotRows = (Vec<crate::db::executor::RetainedSlotRow>, usize);
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
type CoveringSlotRowPairs = Vec<(DataKey, crate::db::executor::RetainedSlotRow)>;
#[cfg(feature = "sql")]
type CoveringProjectedRowPairs = Vec<(DataKey, Vec<Value>)>;
#[cfg(feature = "sql")]
type CoveringProjectedTextRowPairs = Vec<(DataKey, Vec<String>)>;
#[cfg(feature = "sql")]
type CoveringProjectedPairs<Row> = Vec<(DataKey, Row)>;
#[cfg(feature = "sql")]
type DirectProjectionSlots = Vec<usize>;
#[cfg(feature = "sql")]
type SqlDirectProjectedSourceLayout = (Vec<SqlDirectProjectedFieldSource>, Vec<Value>);

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
    store: StoreHandle,
    covering_component_scan: Option<CoveringComponentScanState<'a>>,
    load_terminal_fast_path: Option<&'a LoadTerminalFastPathContract>,
    prepared_projection_validation:
        Option<&'a crate::db::executor::projection::PreparedSlotProjectionValidation>,
    prepared_sql_projection: Option<&'a PreparedSqlExecutionProjection>,
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
    Constant { constant_index: usize },
    IndexComponent { decoded_component_index: usize },
}

///
/// SqlConstantProjectedFieldSource
///
/// SqlConstantProjectedFieldSource keeps the constant-only direct projection
/// path explicit once the covering contract already proved no decoded index
/// components are needed for output materialization.
///

#[cfg(feature = "sql")]
enum SqlConstantProjectedFieldSource {
    PrimaryKey,
    Constant { constant_index: usize },
}

#[cfg(feature = "sql")]
// Compact runtime op table for projected SQL row materialization.
enum PreparedSqlProjectedValueOp {
    PrimaryKey,
    Constant(usize),
    Decoded(usize),
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
    component_indices: Vec<usize>,
    slot_sources: Vec<Option<SqlRouteCoveringSlotSource>>,
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
/// PreparedSqlDirectProjectedSourceLayout
///
/// PreparedSqlDirectProjectedSourceLayout freezes one direct SQL projected-row
/// source table plus its constant payloads before execution begins.
/// The cursorless covering fast path then borrows this layout directly instead
/// of rebuilding or validating it during execution.
///

#[cfg(feature = "sql")]
struct PreparedSqlDirectProjectedSourceLayout {
    projected_field_ops: Vec<PreparedSqlProjectedValueOp>,
    constant_values: Vec<Value>,
}

impl PreparedSqlDirectProjectedSourceLayout {
    #[must_use]
    fn projected_field_ops(&self) -> &[PreparedSqlProjectedValueOp] {
        &self.projected_field_ops
    }

    #[must_use]
    fn constant_values(&self) -> &[Value] {
        &self.constant_values
    }
}

///
/// PreparedSqlSingleComponentProjectedSourceLayout
///
/// PreparedSqlSingleComponentProjectedSourceLayout freezes one traversal-order
/// single-component SQL projected-row source table together with the shared
/// decoded component index it requires.
/// Execution borrows this prepared layout directly so the hot path no longer
/// reconstructs single-component source metadata from the covering contract.
///

#[cfg(feature = "sql")]
struct PreparedSqlSingleComponentProjectedSourceLayout {
    component_index: usize,
    projected_field_ops: Vec<PreparedSqlProjectedValueOp>,
    constant_values: Vec<Value>,
}

impl PreparedSqlSingleComponentProjectedSourceLayout {
    #[must_use]
    const fn component_index(&self) -> usize {
        self.component_index
    }

    #[must_use]
    fn projected_field_ops(&self) -> &[PreparedSqlProjectedValueOp] {
        &self.projected_field_ops
    }

    #[must_use]
    fn constant_values(&self) -> &[Value] {
        &self.constant_values
    }
}

///
/// PreparedSqlConstantProjectedSourceLayout
///
/// PreparedSqlConstantProjectedSourceLayout freezes one constant-plus-primary-
/// key projected-row source table plus its constant payloads before execution.
/// That lets constant-only SQL short paths run without rebuilding or
/// revalidating self-authored source metadata on each execution.
///

#[cfg(feature = "sql")]
struct PreparedSqlConstantProjectedSourceLayout {
    projected_field_ops: Vec<PreparedSqlProjectedValueOp>,
    constant_values: Vec<Value>,
}

impl PreparedSqlConstantProjectedSourceLayout {
    #[must_use]
    fn projected_field_ops(&self) -> &[PreparedSqlProjectedValueOp] {
        &self.projected_field_ops
    }

    #[must_use]
    fn constant_values(&self) -> &[Value] {
        &self.constant_values
    }
}

#[cfg(feature = "sql")]
///
/// PreparedSqlProjectedSourceLayout
///
/// PreparedSqlProjectedSourceLayout freezes exactly one SQL projected-row
/// source family for one execution attempt.
/// SQL execution only ever consumes one projected-row family, so this tagged
/// shape avoids carrying mutually exclusive direct, single-component, and
/// constant layouts side by side.
///
enum PreparedSqlProjectedSourceLayout {
    Direct(PreparedSqlDirectProjectedSourceLayout),
    SingleComponent(PreparedSqlSingleComponentProjectedSourceLayout),
    Constant(PreparedSqlConstantProjectedSourceLayout),
}

impl PreparedSqlProjectedSourceLayout {
    #[must_use]
    const fn as_direct(&self) -> Option<&PreparedSqlDirectProjectedSourceLayout> {
        match self {
            Self::Direct(layout) => Some(layout),
            Self::SingleComponent(_) | Self::Constant(_) => None,
        }
    }

    #[must_use]
    const fn as_single_component(
        &self,
    ) -> Option<&PreparedSqlSingleComponentProjectedSourceLayout> {
        match self {
            Self::SingleComponent(layout) => Some(layout),
            Self::Direct(_) | Self::Constant(_) => None,
        }
    }

    #[must_use]
    const fn as_constant(&self) -> Option<&PreparedSqlConstantProjectedSourceLayout> {
        match self {
            Self::Constant(layout) => Some(layout),
            Self::Direct(_) | Self::SingleComponent(_) => None,
        }
    }

    #[must_use]
    const fn is_constant(&self) -> bool {
        matches!(self, Self::Constant(_))
    }

    #[must_use]
    const fn is_single_component(&self) -> bool {
        matches!(self, Self::SingleComponent(_))
    }
}

///
/// PreparedSqlExecutionProjection
///
/// PreparedSqlExecutionProjection freezes the SQL-only projection metadata
/// that is invariant for one execution attempt.
/// The terminal runtime borrows this prepared state directly so cursorless SQL
/// short paths do not rebuild source layouts, slot templates, or validation
/// metadata inside the hot execution loop.
///

#[cfg(feature = "sql")]
pub(in crate::db::executor) struct PreparedSqlExecutionProjection {
    direct_projection_slots: Option<DirectProjectionSlots>,
    route_covering_slot_layout: Option<SqlRouteCoveringSlotLayout>,
    route_projected_source_layout: Option<PreparedSqlProjectedSourceLayout>,
    constant_covering_slot_template: Option<SqlConstantCoveringSlotTemplate>,
}

impl PreparedSqlExecutionProjection {
    #[must_use]
    pub(in crate::db::executor) fn direct_projection_slots(&self) -> Option<&[usize]> {
        self.direct_projection_slots.as_deref()
    }

    #[must_use]
    const fn route_covering_slot_layout(&self) -> Option<&SqlRouteCoveringSlotLayout> {
        self.route_covering_slot_layout.as_ref()
    }

    #[must_use]
    fn route_direct_projected_source_layout(
        &self,
    ) -> Option<&PreparedSqlDirectProjectedSourceLayout> {
        self.route_projected_source_layout
            .as_ref()
            .and_then(PreparedSqlProjectedSourceLayout::as_direct)
    }

    #[must_use]
    fn route_single_component_projected_source_layout(
        &self,
    ) -> Option<&PreparedSqlSingleComponentProjectedSourceLayout> {
        self.route_projected_source_layout
            .as_ref()
            .and_then(PreparedSqlProjectedSourceLayout::as_single_component)
    }

    #[must_use]
    fn route_constant_projected_source_layout(
        &self,
    ) -> Option<&PreparedSqlConstantProjectedSourceLayout> {
        self.route_projected_source_layout
            .as_ref()
            .and_then(PreparedSqlProjectedSourceLayout::as_constant)
    }

    #[must_use]
    const fn constant_covering_slot_template(&self) -> Option<&SqlConstantCoveringSlotTemplate> {
        self.constant_covering_slot_template.as_ref()
    }

    #[must_use]
    fn uses_constant_projected_only_path(&self) -> bool {
        self.route_projected_source_layout
            .as_ref()
            .is_some_and(PreparedSqlProjectedSourceLayout::is_constant)
            && self.route_covering_slot_layout.is_none()
    }

    #[must_use]
    fn uses_single_component_projected_route_path(&self) -> bool {
        self.route_projected_source_layout
            .as_ref()
            .is_some_and(PreparedSqlProjectedSourceLayout::is_single_component)
    }
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
                ProjectionMaterializationMode, RowCollectorMaterializationRequest,
            },
            read_row_presence_with_consistency_from_store, reorder_covering_projection_pairs,
            resolve_covering_memberships_from_lowered_specs,
            resolve_covering_projection_components_from_lowered_specs,
            route::{
                LoadOrderRouteContract, LoadTerminalFastPathContract,
                access_order_satisfied_by_route_contract,
            },
            terminal::{
                RowLayout,
                page::{KernelRow, KernelRowPayloadMode, ScalarRowRuntimeHandle},
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
    ) -> Result<
        Option<(
            crate::db::executor::pipeline::contracts::StructuralCursorPage,
            usize,
            usize,
        )>,
        InternalError,
    > {
        let DirectCoveringScanMaterializationRequest {
            plan,
            scan_budget_hint,
            cursor_boundary,
            load_terminal_fast_path,
            predicate_slots,
            validate_projection,
            retain_slot_rows,
            prepared_projection_validation,
            prepared_sql_projection,
            prefer_rendered_projection_rows,
        } = request;

        let sql_covering_context = SqlCoveringMaterializationContext {
            plan,
            store,
            covering_component_scan,
            load_terminal_fast_path,
            prepared_projection_validation,
            prepared_sql_projection,
            scan_budget_hint,
            predicate_slots,
            prefer_rendered_projection_rows,
        };

        try_materialize_cursorless_sql_covering_scan_without_key_stream(
            sql_covering_context,
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
    ) -> Result<
        Option<(
            crate::db::executor::pipeline::contracts::StructuralCursorPage,
            usize,
            usize,
        )>,
        InternalError,
    > {
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
            prepared_projection_validation,
            prepared_sql_projection,
            prefer_rendered_projection_rows,
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
            prepared_projection_validation,
            prepared_sql_projection,
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
            let prepared_projection_validation =
                required_prepared_projection_validation(prepared_projection_validation)?;
            for row in &rows {
                crate::db::executor::projection::validate_prepared_projection_row(
                    prepared_projection_validation,
                    &mut |slot| row.slot_ref(slot),
                )?;
            }
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
#[expect(
    clippy::too_many_lines,
    reason = "shared cursorless SQL short-path dispatch still keeps projected and slot-row families aligned under one boundary"
)]
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
    let direct_projection_slots = context
        .prepared_sql_projection
        .and_then(PreparedSqlExecutionProjection::direct_projection_slots);
    let prepared_sql_projection = context.prepared_sql_projection;

    if context.prefer_rendered_projection_rows
        && let Some(projection_field_slots) = direct_projection_slots
        && let Some((mut projected_rows, keys_scanned)) = if prepared_sql_projection
            .is_some_and(PreparedSqlExecutionProjection::uses_constant_projected_only_path)
        {
            let Some(LoadTerminalFastPathContract::CoveringRead(covering)) =
                context.load_terminal_fast_path
            else {
                return Ok(None);
            };
            try_materialize_sql_route_constant_covering_projected_text_rows(
                context,
                covering,
                projection_field_slots,
            )?
        } else if prepared_sql_projection
            .is_some_and(PreparedSqlExecutionProjection::uses_single_component_projected_route_path)
        {
            let Some(LoadTerminalFastPathContract::CoveringRead(covering)) =
                context.load_terminal_fast_path
            else {
                return Ok(None);
            };
            try_materialize_sql_route_single_component_projected_text_rows(
                context,
                covering,
                projection_field_slots,
            )?
        } else {
            try_materialize_sql_route_covering_projected_text_rows(context, projection_field_slots)?
        }
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

    let Some(projection_field_slots) = direct_projection_slots else {
        return Ok(None);
    };

    if let Some((mut projected_rows, keys_scanned)) = if prepared_sql_projection
        .is_some_and(PreparedSqlExecutionProjection::uses_constant_projected_only_path)
    {
        let Some(LoadTerminalFastPathContract::CoveringRead(covering)) =
            context.load_terminal_fast_path
        else {
            return Ok(None);
        };
        try_materialize_sql_route_constant_covering_projected_rows(
            context,
            covering,
            projection_field_slots,
        )?
    } else if prepared_sql_projection
        .is_some_and(PreparedSqlExecutionProjection::uses_single_component_projected_route_path)
    {
        let Some(LoadTerminalFastPathContract::CoveringRead(covering)) =
            context.load_terminal_fast_path
        else {
            return Ok(None);
        };
        try_materialize_sql_route_single_component_projected_rows(
            context,
            covering,
            projection_field_slots,
        )?
    } else {
        try_materialize_sql_route_covering_projected_rows(context, projection_field_slots)?
    } {
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
            let prepared_projection_validation =
                required_prepared_projection_validation(context.prepared_projection_validation)?;
            for row in &slot_rows {
                crate::db::executor::projection::validate_prepared_projection_row(
                    prepared_projection_validation,
                    &mut |slot| row.slot_ref(slot),
                )?;
            }
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
    let direct_projection_slots = context
        .prepared_sql_projection
        .and_then(PreparedSqlExecutionProjection::direct_projection_slots);

    if context.prefer_rendered_projection_rows
        && let Some((mut projected_rows, keys_scanned)) =
            try_materialize_sql_projected_text_rows(context, key_stream, direct_projection_slots)?
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
        try_materialize_sql_projected_rows(context, key_stream, direct_projection_slots)?
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
            let prepared_projection_validation =
                required_prepared_projection_validation(context.prepared_projection_validation)?;
            for row in &slot_rows {
                crate::db::executor::projection::validate_prepared_projection_row(
                    prepared_projection_validation,
                    &mut |slot| row.slot_ref(slot),
                )?;
            }
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
            let slot_rows = rows
                .into_iter()
                .map(KernelRow::into_retained_slot_row)
                .collect();

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
// Strip the authoritative data keys once one covering projected-row pair
// stream is already in the required final output order.
fn strip_covering_projected_row_pairs(rows: CoveringProjectedRowPairs) -> Vec<Vec<Value>> {
    rows.into_iter().map(|(_, row)| row).collect()
}

#[cfg(feature = "sql")]
// Strip the authoritative data keys once one covering projected-text pair
// stream is already in the required final output order.
fn strip_covering_projected_text_row_pairs(
    rows: CoveringProjectedTextRowPairs,
) -> Vec<Vec<String>> {
    rows.into_iter().map(|(_, row)| row).collect()
}

#[cfg(feature = "sql")]
/// Build one prepared SQL projection bundle from plan-owned and route-owned
/// metadata before execution begins.
#[expect(
    clippy::too_many_lines,
    reason = "shared SQL projection preparation still owns the generic non-path-specific layout gating in one boundary"
)]
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

    let direct_projection_slots = plan.frozen_direct_projection_slots().map(<[usize]>::to_vec);
    let projection_field_slots = direct_projection_slots.as_deref();

    // Phase 1: choose the one projected-row source family execution can
    // actually consume, and skip decoded covering layout prep entirely for
    // constant-only, non-residual SQL projections.
    let route_projected_source_layout = if plan.has_residual_predicate() {
        None
    } else {
        match (load_terminal_fast_path, projection_field_slots) {
            (
                Some(LoadTerminalFastPathContract::CoveringRead(covering)),
                Some(projection_field_slots),
            ) => {
                if let Some((projected_field_sources, constant_values)) =
                    sql_route_constant_projected_field_sources(covering, projection_field_slots)
                {
                    Some(PreparedSqlProjectedSourceLayout::Constant(
                        PreparedSqlConstantProjectedSourceLayout {
                            projected_field_ops: sql_constant_projected_field_ops(
                                projected_field_sources.as_slice(),
                            ),
                            constant_values,
                        },
                    ))
                } else {
                    None
                }
            }
            _ => None,
        }
    };
    let route_covering_slot_layout = if route_projected_source_layout
        .as_ref()
        .is_some_and(PreparedSqlProjectedSourceLayout::is_constant)
    {
        None
    } else {
        match load_terminal_fast_path {
            Some(LoadTerminalFastPathContract::CoveringRead(covering)) => {
                sql_route_covering_slot_layout(row_layout, covering, compiled_predicate)?
            }
            None => None,
        }
    };
    let route_projected_source_layout = match (
        route_projected_source_layout,
        load_terminal_fast_path,
        projection_field_slots,
        route_covering_slot_layout.as_ref(),
    ) {
        (Some(layout), _, _, _) => Some(layout),
        (
            None,
            Some(LoadTerminalFastPathContract::CoveringRead(covering)),
            Some(projection_field_slots),
            Some(route_covering_slot_layout),
        ) => {
            if let Some((component_index, projected_field_sources, constant_values)) =
                sql_route_single_component_projected_field_sources(
                    covering,
                    projection_field_slots,
                    route_covering_slot_layout,
                )
            {
                Some(PreparedSqlProjectedSourceLayout::SingleComponent(
                    PreparedSqlSingleComponentProjectedSourceLayout {
                        component_index,
                        projected_field_ops: sql_direct_projected_field_ops(
                            projected_field_sources.as_slice(),
                        ),
                        constant_values,
                    },
                ))
            } else {
                sql_route_direct_projected_field_sources(
                    covering,
                    projection_field_slots,
                    route_covering_slot_layout,
                )?
                .map(|(projected_field_sources, constant_values)| {
                    PreparedSqlProjectedSourceLayout::Direct(
                        PreparedSqlDirectProjectedSourceLayout {
                            projected_field_ops: sql_direct_projected_field_ops(
                                projected_field_sources.as_slice(),
                            ),
                            constant_values,
                        },
                    )
                })
            }
        }
        _ => None,
    };
    debug_assert!(
        sql_validate_prepared_projected_source_layout(route_projected_source_layout.as_ref())
            .is_ok(),
        "prepared SQL projected-source layout must remain internally consistent"
    );

    // Phase 2: prepare the slot-row fallback only when the projected-row
    // family does not already prove a constant-only, predicate-free path.
    let constant_covering_slot_template = if route_projected_source_layout
        .as_ref()
        .is_some_and(PreparedSqlProjectedSourceLayout::is_constant)
        && !plan.has_residual_predicate()
    {
        None
    } else {
        sql_constant_covering_slot_row_template_from_route_contract(
            row_layout,
            load_terminal_fast_path,
            compiled_predicate,
        )
        .or_else(|| sql_constant_covering_slot_row_template(plan, row_layout, compiled_predicate))
    };

    Ok(Some(PreparedSqlExecutionProjection {
        direct_projection_slots,
        route_covering_slot_layout,
        route_projected_source_layout,
        constant_covering_slot_template,
    }))
}

#[cfg(feature = "sql")]
fn sql_validate_prepared_projected_source_layout(
    route_projected_source_layout: Option<&PreparedSqlProjectedSourceLayout>,
) -> Result<(), InternalError> {
    if let Some(layout) = route_projected_source_layout {
        match layout {
            PreparedSqlProjectedSourceLayout::Direct(layout) => {
                sql_validate_direct_projected_field_ops(
                    layout.projected_field_ops(),
                    layout.constant_values(),
                )?;
            }
            PreparedSqlProjectedSourceLayout::SingleComponent(layout) => {
                sql_validate_direct_projected_field_ops(
                    layout.projected_field_ops(),
                    layout.constant_values(),
                )?;
            }
            PreparedSqlProjectedSourceLayout::Constant(layout) => {
                sql_validate_constant_projected_field_ops(
                    layout.projected_field_ops(),
                    layout.constant_values(),
                )?;
            }
        }
    }

    Ok(())
}

#[cfg(feature = "sql")]
// Attempt one SQL-only direct projected-output materialization path through
// one shared control-flow skeleton so text/value lanes stay aligned on
// residual-predicate gating, route-covering preference, and constant fallback.
fn try_materialize_sql_projected_output<Rows, RouteFn, ConstantFn>(
    context: &SqlCoveringMaterializationContext<'_>,
    key_stream: &mut dyn OrderedKeyStream,
    projection_field_slots: Option<&[usize]>,
    try_route_covering: RouteFn,
    try_route_constant: ConstantFn,
) -> Result<Option<Rows>, InternalError>
where
    RouteFn: FnOnce(
        &SqlCoveringMaterializationContext<'_>,
        &[usize],
    ) -> Result<Option<Rows>, InternalError>,
    ConstantFn: FnOnce(
        &SqlCoveringMaterializationContext<'_>,
        &mut dyn OrderedKeyStream,
        &[usize],
    ) -> Result<Option<Rows>, InternalError>,
{
    if context.plan.has_residual_predicate() {
        return Ok(None);
    }

    let Some(projection_field_slots) = projection_field_slots else {
        return Ok(None);
    };

    if context
        .prepared_sql_projection
        .is_some_and(PreparedSqlExecutionProjection::uses_constant_projected_only_path)
    {
        return try_route_constant(context, key_stream, projection_field_slots);
    }

    if let Some(projected_rows) = try_route_covering(context, projection_field_slots)? {
        return Ok(Some(projected_rows));
    }

    try_route_constant(context, key_stream, projection_field_slots)
}

#[cfg(feature = "sql")]
// Attempt one SQL-only direct projected-row materialization path when the
// route-owned covering contract already determines every output value and the
// query owes no residual predicate evaluation.
fn try_materialize_sql_projected_text_rows(
    context: &SqlCoveringMaterializationContext<'_>,
    key_stream: &mut dyn OrderedKeyStream,
    projection_field_slots: Option<&[usize]>,
) -> Result<Option<CoveringProjectedTextRows>, InternalError> {
    try_materialize_sql_projected_output(
        context,
        key_stream,
        projection_field_slots,
        try_materialize_sql_route_covering_projected_text_rows,
        try_materialize_sql_route_constant_projected_text_rows,
    )
}

#[cfg(feature = "sql")]
// Attempt one SQL-only direct projected-row materialization path when the
// route-owned covering contract already determines every output value and the
// query owes no residual predicate evaluation.
fn try_materialize_sql_projected_rows(
    context: &SqlCoveringMaterializationContext<'_>,
    key_stream: &mut dyn OrderedKeyStream,
    projection_field_slots: Option<&[usize]>,
) -> Result<Option<CoveringProjectedRows>, InternalError> {
    try_materialize_sql_projected_output(
        context,
        key_stream,
        projection_field_slots,
        try_materialize_sql_route_covering_projected_rows,
        try_materialize_sql_route_constant_projected_rows,
    )
}

#[cfg(feature = "sql")]
// Attempt one route-owned covering-read projected-row materialization path
// that renders final SQL text cells directly from the planner-owned covering
// contract.
fn try_materialize_sql_route_covering_projected_text_rows(
    context: &SqlCoveringMaterializationContext<'_>,
    projection_field_slots: &[usize],
) -> Result<Option<CoveringProjectedTextRows>, InternalError> {
    let Some(prepared_sql_projection) = context.prepared_sql_projection else {
        return Ok(None);
    };
    let Some(LoadTerminalFastPathContract::CoveringRead(covering)) =
        context.load_terminal_fast_path
    else {
        return Ok(None);
    };
    let Some(scan_state) = context.covering_component_scan else {
        return Ok(None);
    };
    if let Some(projected_rows) = try_materialize_sql_route_single_component_projected_text_rows(
        context,
        covering,
        projection_field_slots,
    )? {
        return Ok(Some(projected_rows));
    }

    if let Some(projected_rows) = try_materialize_sql_route_constant_covering_projected_text_rows(
        context,
        covering,
        projection_field_slots,
    )? {
        return Ok(Some(projected_rows));
    }

    let Some(layout) = prepared_sql_projection.route_covering_slot_layout() else {
        return Ok(None);
    };
    let Some(projected_source_layout) =
        prepared_sql_projection.route_direct_projected_source_layout()
    else {
        return Ok(None);
    };
    let Some((rendered_rows, keys_scanned)) = sql_route_covering_component_text_rows(
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

    // Phase 1: materialize already-rendered SQL rows directly from the
    // planner-owned covering contract instead of staging `Value` rows.
    let mut rows = sql_route_covering_projected_text_rows_from_rendered(
        projected_source_layout.projected_field_ops(),
        projected_source_layout.constant_values(),
        rendered_rows,
    )?;

    // Phase 2: preserve the existing covering order contract exactly as the
    // value-row path does.
    if sql_covering_projection_pairs_require_reorder(covering.order_contract) {
        reorder_covering_projection_pairs(covering.order_contract, rows.as_mut_slice());
    }

    Ok(Some((
        strip_covering_projected_text_row_pairs(rows),
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
    if let Some(slot_template) = context
        .prepared_sql_projection
        .and_then(PreparedSqlExecutionProjection::constant_covering_slot_template)
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

                let primary_key_value = key.storage_key().as_primary_key_value();

                if let Some(predicate_program) = context.predicate_slots
                    && !predicate_program.eval_with_slot_value_ref_reader(&mut |slot| {
                        sql_constant_covering_slot_ref(slot_template, &primary_key_value, slot)
                    })
                {
                    continue;
                }

                rows.push(sql_constant_covering_retained_row(
                    slot_template,
                    primary_key_value,
                ));
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

                let primary_key_value = key.storage_key().as_primary_key_value();

                if let Some(predicate_program) = context.predicate_slots
                    && !predicate_program.eval_with_slot_value_ref_reader(&mut |slot| {
                        sql_constant_covering_slot_ref(slot_template, &primary_key_value, slot)
                    })
                {
                    continue;
                }

                rows.push(sql_constant_covering_retained_row(
                    slot_template,
                    primary_key_value,
                ));
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
    projection_field_slots: &[usize],
) -> Result<Option<CoveringProjectedRows>, InternalError> {
    let Some(prepared_sql_projection) = context.prepared_sql_projection else {
        return Ok(None);
    };
    let Some(LoadTerminalFastPathContract::CoveringRead(covering)) =
        context.load_terminal_fast_path
    else {
        return Ok(None);
    };
    let Some(scan_state) = context.covering_component_scan else {
        return Ok(None);
    };
    if let Some(projected_rows) = try_materialize_sql_route_single_component_projected_rows(
        context,
        covering,
        projection_field_slots,
    )? {
        return Ok(Some(projected_rows));
    }

    if let Some(projected_rows) = try_materialize_sql_route_constant_covering_projected_rows(
        context,
        covering,
        projection_field_slots,
    )? {
        return Ok(Some(projected_rows));
    }

    let Some(layout) = prepared_sql_projection.route_covering_slot_layout() else {
        return Ok(None);
    };
    let Some(projected_source_layout) =
        prepared_sql_projection.route_direct_projected_source_layout()
    else {
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

    // Phase 1: materialize already-projected SQL rows directly from the
    // planner-owned covering contract instead of staging full slot rows.
    let mut rows = sql_route_covering_projected_rows_from_decoded(
        projected_source_layout.projected_field_ops(),
        projected_source_layout.constant_values(),
        decoded_rows,
    )?;

    // Phase 2: preserve the existing covering order contract exactly as the
    // slot-row path does.
    if sql_covering_projection_pairs_require_reorder(covering.order_contract) {
        reorder_covering_projection_pairs(covering.order_contract, rows.as_mut_slice());
    }

    Ok(Some((
        strip_covering_projected_row_pairs(rows),
        keys_scanned,
    )))
}

#[cfg(feature = "sql")]
// Attempt one route-owned constant-plus-primary-key projected-output path
// while keeping the membership witness and row-order contract under one
// shared emission boundary.
fn try_materialize_sql_route_constant_covering_projected_output<Row, ProjectRowFn>(
    context: &SqlCoveringMaterializationContext<'_>,
    covering: &CoveringReadExecutionPlan,
    _projection_field_slots: &[usize],
    project_row: ProjectRowFn,
) -> Result<Option<(CoveringProjectedPairs<Row>, usize)>, InternalError>
where
    ProjectRowFn:
        Fn(&[PreparedSqlProjectedValueOp], &[Value], &StorageKey) -> Result<Row, InternalError>,
{
    let Some(prepared_sql_projection) = context.prepared_sql_projection else {
        return Ok(None);
    };
    if !covering.existing_row_mode.uses_storage_existence_witness() {
        return Ok(None);
    }

    let Some(projected_source_layout) =
        prepared_sql_projection.route_constant_projected_source_layout()
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
            project_row(
                projected_source_layout.projected_field_ops(),
                projected_source_layout.constant_values(),
                &data_key.storage_key(),
            )
        },
    )?;

    // Phase 2: preserve the planner-owned logical order exactly as the
    // decoded-component covering paths do.
    if sql_covering_projection_pairs_require_reorder(covering.order_contract) {
        reorder_covering_projection_pairs(covering.order_contract, rows.as_mut_slice());
    }

    Ok(Some((rows, keys_scanned)))
}

#[cfg(feature = "sql")]
// Attempt one route-owned constant-plus-primary-key projected-text path while
// keeping membership-level witness handling below the shared covering kernel.
fn try_materialize_sql_route_constant_covering_projected_text_rows(
    context: &SqlCoveringMaterializationContext<'_>,
    covering: &CoveringReadExecutionPlan,
    projection_field_slots: &[usize],
) -> Result<Option<CoveringProjectedTextRows>, InternalError> {
    let Some((rows, keys_scanned)) = try_materialize_sql_route_constant_covering_projected_output(
        context,
        covering,
        projection_field_slots,
        sql_project_text_row_from_constant_covering_sources,
    )?
    else {
        return Ok(None);
    };

    Ok(Some((
        strip_covering_projected_text_row_pairs(rows),
        keys_scanned,
    )))
}

#[cfg(feature = "sql")]
// Attempt one route-owned constant-plus-primary-key projected-row path while
// keeping membership-level witness handling below the shared covering kernel.
fn try_materialize_sql_route_constant_covering_projected_rows(
    context: &SqlCoveringMaterializationContext<'_>,
    covering: &CoveringReadExecutionPlan,
    projection_field_slots: &[usize],
) -> Result<Option<CoveringProjectedRows>, InternalError> {
    let Some((rows, keys_scanned)) = try_materialize_sql_route_constant_covering_projected_output(
        context,
        covering,
        projection_field_slots,
        |projected_field_ops, constant_values, storage_key| {
            Ok(sql_project_row_from_constant_covering_sources(
                projected_field_ops,
                constant_values,
                storage_key,
            ))
        },
    )?
    else {
        return Ok(None);
    };

    Ok(Some((
        strip_covering_projected_row_pairs(rows),
        keys_scanned,
    )))
}

#[cfg(feature = "sql")]
// Attempt one route-owned direct projected-row path when every projected value
// comes from the authoritative primary key or a bound access constant.
fn try_materialize_sql_route_constant_projected_text_rows(
    context: &SqlCoveringMaterializationContext<'_>,
    key_stream: &mut dyn OrderedKeyStream,
    _projection_field_slots: &[usize],
) -> Result<Option<CoveringProjectedTextRows>, InternalError> {
    let Some(prepared_sql_projection) = context.prepared_sql_projection else {
        return Ok(None);
    };
    let Some(LoadTerminalFastPathContract::CoveringRead(_covering)) =
        context.load_terminal_fast_path
    else {
        return Ok(None);
    };
    if !sql_route_covering_allows_constant_short_path(context.load_terminal_fast_path) {
        return Ok(None);
    }
    let Some(projected_source_layout) =
        prepared_sql_projection.route_constant_projected_source_layout()
    else {
        return Ok(None);
    };
    let projected_field_ops = projected_source_layout.projected_field_ops();
    let constant_values = projected_source_layout.constant_values();

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
                projected_field_ops,
                constant_values,
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
                projected_field_ops,
                constant_values,
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
    _projection_field_slots: &[usize],
) -> Result<Option<CoveringProjectedRows>, InternalError> {
    let Some(prepared_sql_projection) = context.prepared_sql_projection else {
        return Ok(None);
    };
    let Some(LoadTerminalFastPathContract::CoveringRead(_covering)) =
        context.load_terminal_fast_path
    else {
        return Ok(None);
    };
    if !sql_route_covering_allows_constant_short_path(context.load_terminal_fast_path) {
        return Ok(None);
    }
    let Some(projected_source_layout) =
        prepared_sql_projection.route_constant_projected_source_layout()
    else {
        return Ok(None);
    };
    let projected_field_ops = projected_source_layout.projected_field_ops();
    let constant_values = projected_source_layout.constant_values();

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
                projected_field_ops,
                constant_values,
                &key.storage_key(),
            ));
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
                projected_field_ops,
                constant_values,
                &key.storage_key(),
            ));
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
    _projection_field_slots: &[usize],
) -> Result<Option<CoveringProjectedTextRows>, InternalError> {
    let Some(prepared_sql_projection) = context.prepared_sql_projection else {
        return Ok(None);
    };
    let Some(scan_state) = context.covering_component_scan else {
        return Ok(None);
    };
    let Some(projected_source_layout) =
        prepared_sql_projection.route_single_component_projected_source_layout()
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
            component_index: projected_source_layout.component_index(),
            consistency,
            existing_row_mode: covering.existing_row_mode,
        },
        |storage_key, component| {
            let Some(rendered_component) = render_sql_covering_component_text(component)? else {
                return Ok(None);
            };

            Ok(Some(sql_project_text_row_from_single_covering_component(
                projected_source_layout.projected_field_ops(),
                projected_source_layout.constant_values(),
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
    _projection_field_slots: &[usize],
) -> Result<Option<CoveringProjectedRows>, InternalError> {
    let Some(prepared_sql_projection) = context.prepared_sql_projection else {
        return Ok(None);
    };
    let Some(scan_state) = context.covering_component_scan else {
        return Ok(None);
    };
    let Some(projected_source_layout) =
        prepared_sql_projection.route_single_component_projected_source_layout()
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
            component_index: projected_source_layout.component_index(),
            consistency,
            existing_row_mode: covering.existing_row_mode,
        },
        |storage_key, decoded_component| {
            sql_project_row_from_single_covering_component(
                projected_source_layout.projected_field_ops(),
                projected_source_layout.constant_values(),
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
    let Some(prepared_sql_projection) = context.prepared_sql_projection else {
        return Ok(None);
    };
    let Some(LoadTerminalFastPathContract::CoveringRead(covering)) =
        context.load_terminal_fast_path
    else {
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
    let primary_key_slot = row_layout.primary_key_slot();
    let mut constant_slots = Vec::new();
    let mut covered_slots = vec![false; row_layout.field_count()];
    let mut component_slots = Vec::<CoveringComponentSlotGroup>::new();
    let mut component_indices = Vec::new();
    let mut slot_sources = vec![None; row_layout.field_count()];

    // Phase 1: project one dense slot layout from the planner-owned contract and
    // group decoded component fields by their index component position.
    covered_slots[primary_key_slot] = true;
    slot_sources[primary_key_slot] = Some(SqlRouteCoveringSlotSource::PrimaryKey);
    for field in &covering.fields {
        match &field.source {
            CoveringReadFieldSource::PrimaryKey => {
                covered_slots[field.field_slot.index] = true;
                slot_sources[field.field_slot.index] = Some(SqlRouteCoveringSlotSource::PrimaryKey);
            }
            CoveringReadFieldSource::Constant(value) => {
                constant_slots.push((field.field_slot.index, value.clone()));
                covered_slots[field.field_slot.index] = true;
                slot_sources[field.field_slot.index] = Some(SqlRouteCoveringSlotSource::Constant(
                    constant_slots.len().saturating_sub(1),
                ));
            }
            CoveringReadFieldSource::IndexComponent { component_index } => {
                let decoded_component_index = if let Some((decoded_component_index, (_, slots))) =
                    component_slots.iter_mut().enumerate().find(
                        |(_, (group_component_index, _))| {
                            *group_component_index == *component_index
                        },
                    ) {
                    slots.push(field.field_slot.index);
                    decoded_component_index
                } else {
                    component_slots.push((*component_index, vec![field.field_slot.index]));
                    component_indices.push(*component_index);
                    component_slots.len().saturating_sub(1)
                };
                covered_slots[field.field_slot.index] = true;
                slot_sources[field.field_slot.index] = Some(
                    SqlRouteCoveringSlotSource::DecodedComponent(decoded_component_index),
                );
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
        slot_count: row_layout.field_count(),
        constant_slots,
        component_slots,
        component_indices,
        slot_sources,
    }))
}

#[cfg(feature = "sql")]
// Resolve one direct projected-row source layout from the planner-owned
// covering contract plus the canonical direct field projection order.
fn sql_route_direct_projected_field_sources(
    covering: &CoveringReadExecutionPlan,
    projection_field_slots: &[usize],
    route_covering_slot_layout: &SqlRouteCoveringSlotLayout,
) -> Result<Option<SqlDirectProjectedSourceLayout>, InternalError> {
    let mut projected_field_sources = Vec::with_capacity(projection_field_slots.len());
    let mut constant_values = Vec::new();

    for field_slot in projection_field_slots {
        let Some(slot_source) = route_covering_slot_layout
            .slot_sources
            .get(*field_slot)
            .and_then(Option::as_ref)
        else {
            return Ok(None);
        };

        let projected_source = match slot_source {
            SqlRouteCoveringSlotSource::PrimaryKey => SqlDirectProjectedFieldSource::PrimaryKey,
            SqlRouteCoveringSlotSource::Constant(constant_slot_index) => {
                let Some((_, value)) = route_covering_slot_layout
                    .constant_slots
                    .get(*constant_slot_index)
                else {
                    return Err(InternalError::query_executor_invariant(
                        "covering-read SQL projected-row path referenced a missing constant slot source",
                    ));
                };
                constant_values.push(value.clone());

                SqlDirectProjectedFieldSource::Constant {
                    constant_index: constant_values.len().saturating_sub(1),
                }
            }
            SqlRouteCoveringSlotSource::DecodedComponent(decoded_component_index) => {
                SqlDirectProjectedFieldSource::IndexComponent {
                    decoded_component_index: *decoded_component_index,
                }
            }
        };

        projected_field_sources.push(projected_source);
    }

    let _ = covering;

    Ok(Some((projected_field_sources, constant_values)))
}

#[cfg(feature = "sql")]
fn sql_direct_projected_field_ops(
    projected_field_sources: &[SqlDirectProjectedFieldSource],
) -> Vec<PreparedSqlProjectedValueOp> {
    projected_field_sources
        .iter()
        .map(|projected_source| match projected_source {
            SqlDirectProjectedFieldSource::PrimaryKey => PreparedSqlProjectedValueOp::PrimaryKey,
            SqlDirectProjectedFieldSource::Constant { constant_index } => {
                PreparedSqlProjectedValueOp::Constant(*constant_index)
            }
            SqlDirectProjectedFieldSource::IndexComponent {
                decoded_component_index,
            } => PreparedSqlProjectedValueOp::Decoded(*decoded_component_index),
        })
        .collect()
}

#[cfg(feature = "sql")]
fn sql_validate_direct_projected_field_ops(
    projected_field_ops: &[PreparedSqlProjectedValueOp],
    constant_values: &[Value],
) -> Result<(), InternalError> {
    if projected_field_ops.iter().any(|projected_op| {
        matches!(
            projected_op,
            PreparedSqlProjectedValueOp::Constant(constant_index)
                if *constant_index >= constant_values.len()
        )
    }) {
        return Err(InternalError::query_executor_invariant(
            "covering-read SQL projected-row path referenced a missing direct constant",
        ));
    }

    Ok(())
}

#[cfg(feature = "sql")]
// Resolve one traversal-order single-component projected-row source layout.
fn sql_route_single_component_projected_field_sources(
    covering: &CoveringReadExecutionPlan,
    projection_field_slots: &[usize],
    route_covering_slot_layout: &SqlRouteCoveringSlotLayout,
) -> Option<(usize, Vec<SqlDirectProjectedFieldSource>, Vec<Value>)> {
    if !matches!(
        covering.order_contract,
        CoveringProjectionOrder::IndexOrder(_)
    ) {
        return None;
    }

    let mut shared_component_index = None;
    let mut projected_field_sources = Vec::with_capacity(projection_field_slots.len());
    let mut constant_values = Vec::new();

    for field_slot in projection_field_slots {
        let slot_source = route_covering_slot_layout
            .slot_sources
            .get(*field_slot)
            .and_then(Option::as_ref)?;

        let projected_source = match slot_source {
            SqlRouteCoveringSlotSource::PrimaryKey => SqlDirectProjectedFieldSource::PrimaryKey,
            SqlRouteCoveringSlotSource::Constant(constant_slot_index) => {
                let (_, value) = route_covering_slot_layout
                    .constant_slots
                    .get(*constant_slot_index)?;
                constant_values.push(value.clone());

                SqlDirectProjectedFieldSource::Constant {
                    constant_index: constant_values.len().saturating_sub(1),
                }
            }
            SqlRouteCoveringSlotSource::DecodedComponent(decoded_component_index) => {
                let (component_index, _) = route_covering_slot_layout
                    .component_slots
                    .get(*decoded_component_index)?;
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

    Some((
        shared_component_index?,
        projected_field_sources,
        constant_values,
    ))
}

#[cfg(feature = "sql")]
// Resolve one direct projected-row layout when every projected value comes
// from the authoritative primary key or a bound constant only.
fn sql_route_constant_projected_field_sources(
    covering: &CoveringReadExecutionPlan,
    projection_field_slots: &[usize],
) -> Option<(Vec<SqlConstantProjectedFieldSource>, Vec<Value>)> {
    let mut projected_field_sources = Vec::with_capacity(projection_field_slots.len());
    let mut constant_values = Vec::new();

    for field_slot in projection_field_slots {
        let covering_field = covering
            .fields
            .iter()
            .find(|field| field.field_slot.index == *field_slot)?;

        let projected_source = match &covering_field.source {
            CoveringReadFieldSource::PrimaryKey => SqlConstantProjectedFieldSource::PrimaryKey,
            CoveringReadFieldSource::Constant(value) => {
                constant_values.push(value.clone());

                SqlConstantProjectedFieldSource::Constant {
                    constant_index: constant_values.len().saturating_sub(1),
                }
            }
            CoveringReadFieldSource::IndexComponent { .. } => return None,
        };

        projected_field_sources.push(projected_source);
    }
    let _ = covering;

    Some((projected_field_sources, constant_values))
}

#[cfg(feature = "sql")]
fn sql_constant_projected_field_ops(
    projected_field_sources: &[SqlConstantProjectedFieldSource],
) -> Vec<PreparedSqlProjectedValueOp> {
    projected_field_sources
        .iter()
        .map(|projected_source| match projected_source {
            SqlConstantProjectedFieldSource::PrimaryKey => PreparedSqlProjectedValueOp::PrimaryKey,
            SqlConstantProjectedFieldSource::Constant { constant_index } => {
                PreparedSqlProjectedValueOp::Constant(*constant_index)
            }
        })
        .collect()
}

#[cfg(feature = "sql")]
fn sql_validate_constant_projected_field_ops(
    projected_field_ops: &[PreparedSqlProjectedValueOp],
    constant_values: &[Value],
) -> Result<(), InternalError> {
    if projected_field_ops.iter().any(|projected_op| {
        matches!(
            projected_op,
            PreparedSqlProjectedValueOp::Constant(constant_index)
                if *constant_index >= constant_values.len()
        )
    }) {
        return Err(InternalError::query_executor_invariant(
            "constant projected-row path referenced a missing direct constant",
        ));
    }

    Ok(())
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
    route_covering_slot_layout: &SqlRouteCoveringSlotLayout,
) -> Result<Option<(RenderedCoveringComponentRows, usize)>, InternalError> {
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
        |decoded| {
            if decoded.len() != route_covering_slot_layout.component_slots.len() {
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
        let retained_row =
            sql_route_covering_retained_row(layout, primary_key_value, component_values);
        rows.push((data_key, retained_row));
    }

    rows
}

#[cfg(feature = "sql")]
// Materialize final projected SQL rows from one decoded covering component
// stream when the SQL projection is already a direct unique field list.
fn sql_route_covering_projected_rows_from_decoded(
    projected_field_ops: &[PreparedSqlProjectedValueOp],
    constant_values: &[Value],
    decoded_rows: DecodedCoveringComponentRows,
) -> Result<CoveringProjectedRowPairs, InternalError> {
    let mut rows = Vec::with_capacity(decoded_rows.len());

    for (data_key, component_values) in decoded_rows {
        let mut projected_row = Vec::with_capacity(projected_field_ops.len());

        for projected_op in projected_field_ops {
            let value = match projected_op {
                PreparedSqlProjectedValueOp::PrimaryKey => {
                    data_key.storage_key().as_primary_key_value()
                }
                PreparedSqlProjectedValueOp::Constant(constant_index) => {
                    constant_values[*constant_index].clone()
                }
                PreparedSqlProjectedValueOp::Decoded(decoded_component_index) => component_values
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
    projected_field_ops: &[PreparedSqlProjectedValueOp],
    constant_values: &[Value],
    rendered_rows: RenderedCoveringComponentRows,
) -> Result<CoveringProjectedTextRowPairs, InternalError> {
    let mut rows = Vec::with_capacity(rendered_rows.len());

    for (data_key, component_values) in rendered_rows {
        let mut projected_row = Vec::with_capacity(projected_field_ops.len());

        for projected_op in projected_field_ops {
            let value = match projected_op {
                PreparedSqlProjectedValueOp::PrimaryKey => {
                    render_sql_primary_key_text(&data_key.storage_key())
                }
                PreparedSqlProjectedValueOp::Constant(constant_index) => {
                    let value = &constant_values[*constant_index];
                    render_sql_direct_constant_text(value)?
                }
                PreparedSqlProjectedValueOp::Decoded(decoded_component_index) => component_values
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
    projected_field_ops: &[PreparedSqlProjectedValueOp],
    constant_values: &[Value],
    storage_key: &StorageKey,
) -> Vec<Value> {
    let mut projected_row = Vec::with_capacity(projected_field_ops.len());

    for projected_op in projected_field_ops {
        let value = match projected_op {
            PreparedSqlProjectedValueOp::PrimaryKey => storage_key.as_primary_key_value(),
            PreparedSqlProjectedValueOp::Constant(constant_index) => {
                constant_values[*constant_index].clone()
            }
            PreparedSqlProjectedValueOp::Decoded(_) => unreachable!(
                "constant covering projected-row path must not reference decoded components"
            ),
        };
        projected_row.push(value);
    }

    projected_row
}

// Project one SQL row directly into rendered text from the authoritative
// primary key plus any bound covering constants.
fn sql_project_text_row_from_constant_covering_sources(
    projected_field_ops: &[PreparedSqlProjectedValueOp],
    constant_values: &[Value],
    storage_key: &StorageKey,
) -> Result<Vec<String>, InternalError> {
    let mut projected_row = Vec::with_capacity(projected_field_ops.len());

    for projected_op in projected_field_ops {
        let value = match projected_op {
            PreparedSqlProjectedValueOp::PrimaryKey => render_sql_primary_key_text(storage_key),
            PreparedSqlProjectedValueOp::Constant(constant_index) => {
                let value = &constant_values[*constant_index];
                render_sql_direct_constant_text(value)?
            }
            PreparedSqlProjectedValueOp::Decoded(_) => unreachable!(
                "constant covering projected-row text path must not reference decoded components"
            ),
        };
        projected_row.push(value);
    }

    Ok(projected_row)
}

// Project one SQL row directly from a single decoded covering component plus
// the authoritative primary key.
fn sql_project_row_from_single_covering_component(
    projected_field_ops: &[PreparedSqlProjectedValueOp],
    constant_values: &[Value],
    storage_key: StorageKey,
    decoded_component: &Value,
) -> Result<Vec<Value>, InternalError> {
    let mut projected_row = Vec::with_capacity(projected_field_ops.len());

    for projected_op in projected_field_ops {
        let value = match projected_op {
            PreparedSqlProjectedValueOp::PrimaryKey => storage_key.as_primary_key_value(),
            PreparedSqlProjectedValueOp::Constant(constant_index) => {
                constant_values[*constant_index].clone()
            }
            PreparedSqlProjectedValueOp::Decoded(decoded_component_index) => {
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
    projected_field_ops: &[PreparedSqlProjectedValueOp],
    constant_values: &[Value],
    storage_key: StorageKey,
    rendered_component: &str,
) -> Result<Vec<String>, InternalError> {
    let mut projected_row = Vec::with_capacity(projected_field_ops.len());

    for projected_op in projected_field_ops {
        let value = match projected_op {
            PreparedSqlProjectedValueOp::PrimaryKey => render_sql_primary_key_text(&storage_key),
            PreparedSqlProjectedValueOp::Constant(constant_index) => {
                let value = &constant_values[*constant_index];
                render_sql_direct_constant_text(value)?
            }
            PreparedSqlProjectedValueOp::Decoded(decoded_component_index) => {
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
    row_layout: RowLayout,
    load_terminal_fast_path: Option<&LoadTerminalFastPathContract>,
    predicate_slots: Option<&PredicateProgram>,
) -> Option<SqlConstantCoveringSlotTemplate> {
    let LoadTerminalFastPathContract::CoveringRead(covering) = load_terminal_fast_path?;
    if !sql_route_covering_allows_constant_short_path(load_terminal_fast_path) {
        return None;
    }
    let primary_key_slot = row_layout.primary_key_slot();
    let mut covered_slots = vec![false; row_layout.field_count()];
    let mut constant_slots = Vec::new();
    let mut slot_to_constant_index = vec![None; row_layout.field_count()];
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
                constant_slots.push((field.field_slot.index, value.clone()));
                slot_to_constant_index[field.field_slot.index] =
                    Some(constant_slots.len().saturating_sub(1));
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

    Some(SqlConstantCoveringSlotTemplate {
        slot_count: row_layout.field_count(),
        primary_key_slot,
        constant_slots,
        slot_to_constant_index,
    })
}

#[cfg(feature = "sql")]
// Build one slot-row template when projection and predicate semantics stay
// fully within bound access-prefix fields plus the primary key.
fn sql_constant_covering_slot_row_template(
    plan: &AccessPlannedQuery,
    row_layout: RowLayout,
    predicate_slots: Option<&PredicateProgram>,
) -> Option<SqlConstantCoveringSlotTemplate> {
    let primary_key_slot = row_layout.primary_key_slot();
    let mut covered_slots = vec![false; row_layout.field_count()];
    let mut constant_slots = Vec::new();
    let mut slot_to_constant_index = vec![None; row_layout.field_count()];
    covered_slots[primary_key_slot] = true;

    // Phase 1: recover every equality-bound index-prefix component once.
    for (slot, covered) in covered_slots
        .iter_mut()
        .enumerate()
        .take(row_layout.field_count())
    {
        if slot == primary_key_slot {
            continue;
        }

        let field_name = row_layout.field_name(slot)?;

        if let Some(value) =
            constant_covering_projection_value_from_access(&plan.access, field_name)
        {
            constant_slots.push((slot, value));
            slot_to_constant_index[slot] = Some(constant_slots.len().saturating_sub(1));
            *covered = true;
        }
    }

    // Phase 2: require both projection and residual predicate to stay within
    // the covered slot set before we stop reading persisted rows.
    if plan
        .projection_referenced_slots()
        .iter()
        .any(|slot| !covered_slots.get(*slot).copied().unwrap_or(false))
    {
        return None;
    }
    if plan.has_residual_predicate()
        && !predicate_slots.is_some_and(|predicate| predicate.references_only_slots(&covered_slots))
    {
        return None;
    }

    Some(SqlConstantCoveringSlotTemplate {
        slot_count: row_layout.field_count(),
        primary_key_slot,
        constant_slots,
        slot_to_constant_index,
    })
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
