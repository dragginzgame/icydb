//! Module: db::executor::pipeline::contracts::inputs
//! Responsibility: module-local ownership and contracts for db::executor::pipeline::contracts::inputs.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

#[cfg(feature = "sql")]
use crate::value::Value;

#[cfg(feature = "sql")]
type StructuralSqlProjectionRows = (
    Option<Vec<RetainedSlotRow>>,
    Option<Vec<Vec<Value>>>,
    Option<Vec<Vec<String>>>,
    Vec<DataRow>,
);

use crate::{
    db::{
        cursor::CursorBoundary,
        data::{DataKey, DataRow},
        direction::Direction,
        executor::pipeline::contracts::{FastPathKeyResult, execution::ErasedRuntimeBindings},
        executor::{
            AccessStreamBindings, ExecutionKernel, ExecutionPreparation, ExecutorError,
            OrderedKeyStream, OrderedKeyStreamBox, ScalarContinuationBindings,
            mark_projection_referenced_slots, mark_structural_order_slots,
            pipeline::operators::PreparedSqlExecutionProjection,
            route::access_order_satisfied_by_route_contract_for_model,
            route::{LoadOrderRouteContract, LoadTerminalFastPathContract},
            terminal::{
                RetainedSlotRow, RowDecoder, RowLayout,
                page::{
                    KernelPageMaterializationRequest, KernelRowPayloadMode, ScalarRowRuntimeHandle,
                    ScalarRowRuntimeVTable, materialize_key_stream_into_structural_page,
                },
            },
            traversal::row_read_consistency_for_plan,
        },
        index::predicate::IndexPredicateExecution,
        predicate::{MissingRowPolicy, PredicateProgram},
        query::plan::AccessPlannedQuery,
        registry::StoreHandle,
    },
    error::InternalError,
    model::entity::EntityModel,
};
use std::marker::PhantomData;

type StructuralRowCollectorPayload = (StructuralCursorPage, usize, usize);

///
/// PreparedExecutionProjection
///
/// PreparedExecutionProjection is the executor-owned fixed projection state
/// recovered once before execution begins. It freezes retained-slot layout and
/// SQL short-path projection metadata so the hot execution path does not
/// rebuild projection shape from the logical plan.
///

pub(in crate::db::executor) struct PreparedExecutionProjection {
    slot_only_required_slots: Option<Vec<usize>>,
    #[cfg(feature = "sql")]
    sql: Option<PreparedSqlExecutionProjection>,
}

impl PreparedExecutionProjection {
    /// Build one executor-owned prepared projection bundle from one validated
    /// plan, compiled predicate, and optional route-owned covering contract.
    pub(in crate::db::executor) fn compile(
        model: &'static EntityModel,
        plan: &AccessPlannedQuery,
        compiled_predicate: Option<&PredicateProgram>,
        projection_materialization: ProjectionMaterializationMode,
        load_terminal_fast_path: Option<&LoadTerminalFastPathContract>,
    ) -> Result<Self, InternalError> {
        let slot_only_required_slots = compile_slot_only_required_slots(
            model,
            plan,
            compiled_predicate,
            projection_materialization,
        )?;

        #[cfg(feature = "sql")]
        let sql = crate::db::executor::pipeline::operators::prepare_sql_execution_projection(
            model,
            plan,
            compiled_predicate,
            projection_materialization,
            load_terminal_fast_path,
        )?;

        Ok(Self {
            slot_only_required_slots,
            #[cfg(feature = "sql")]
            sql,
        })
    }

    #[must_use]
    pub(in crate::db::executor) fn slot_only_required_slots(&self) -> Option<&[usize]> {
        self.slot_only_required_slots.as_deref()
    }

    #[cfg(feature = "sql")]
    #[must_use]
    pub(in crate::db::executor) const fn sql(&self) -> Option<&PreparedSqlExecutionProjection> {
        self.sql.as_ref()
    }
}

///
/// ScalarRowRuntimeState
///
/// ScalarRowRuntimeState is the structural row-production descriptor recovered
/// once at the scalar execution boundary.
/// It owns store-read authority plus precomputed structural decode metadata so
/// shared scalar loops can materialize `KernelRow` values without rebuilding
/// typed row-runtime state during execution.
///

#[derive(Clone, Debug)]
struct ScalarRowRuntimeState {
    store: StoreHandle,
    row_layout: RowLayout,
    row_decoder: RowDecoder,
}

impl ScalarRowRuntimeState {
    // Build one structural scalar row-runtime descriptor from resolved
    // boundary inputs.
    const fn new(store: StoreHandle, model: &'static EntityModel) -> Self {
        Self {
            store,
            row_layout: RowLayout::from_model(model),
            row_decoder: RowDecoder::structural(),
        }
    }

    // Read one raw row through the structural store handle while preserving
    // the scalar missing-row consistency contract.
    fn read_row(
        &self,
        consistency: MissingRowPolicy,
        key: &DataKey,
    ) -> Result<Option<crate::db::data::RawRow>, InternalError> {
        let raw_key = key.to_raw()?;
        let row = self.store.with_data(|store| store.get(&raw_key));

        match consistency {
            MissingRowPolicy::Error => row
                .map(Some)
                .ok_or_else(|| InternalError::from(ExecutorError::missing_row(key))),
            MissingRowPolicy::Ignore => Ok(row),
        }
    }
}

///
/// CoveringComponentScanState
///
/// Adapter-owned lowered index scan state for SQL covering-read component
/// materialization.
/// This keeps the entity tag plus lowered prefix/range bounds available to the
/// SQL immediate-materialization lane without reopening typed plan ownership.
///

#[derive(Clone, Copy)]
pub(in crate::db::executor) struct CoveringComponentScanState<'a> {
    pub(in crate::db::executor) entity_tag: crate::types::EntityTag,
    pub(in crate::db::executor) index_prefix_specs:
        &'a [crate::db::executor::LoweredIndexPrefixSpec],
    pub(in crate::db::executor) index_range_specs:
        &'a [crate::db::executor::LoweredIndexRangeSpec],
}

// Read one scalar kernel row through the typed store boundary, then decode the
// persisted row structurally before any predicate or page logic runs.
unsafe fn structural_scalar_read_kernel_row(
    state: *mut (),
    consistency: MissingRowPolicy,
    key: &DataKey,
    payload_mode: KernelRowPayloadMode,
    predicate_preapplied: bool,
    predicate_slots: Option<&PredicateProgram>,
    required_slots: Option<&[usize]>,
) -> Result<Option<crate::db::executor::terminal::page::KernelRow>, InternalError> {
    let state = unsafe { &mut *state.cast::<ScalarRowRuntimeState>() };
    let Some(row) = state.read_row(consistency, key)? else {
        return Ok(None);
    };
    let kernel_row = match payload_mode {
        KernelRowPayloadMode::FullRow => {
            let data_row = (key.clone(), row);
            state.row_decoder.decode(&state.row_layout, data_row)?
        }
        KernelRowPayloadMode::SlotsOnly => {
            let slots = RowDecoder::decode_retained_slots(
                &state.row_layout,
                key.storage_key(),
                &row,
                required_slots.unwrap_or(&[]),
            )?;

            crate::db::executor::terminal::page::KernelRow::new_slot_only(slots)
        }
    };
    if predicate_preapplied
        && let Some(predicate_program) = predicate_slots
        && !predicate_program.eval_with_slot_value_ref_reader(&mut |slot| kernel_row.slot_ref(slot))
    {
        return Ok(None);
    }

    Ok(Some(kernel_row))
}

// Keep borrowed runtime-state handles on the same erased callback boundary
// without reclaiming adapter-owned state on handle drop.
const unsafe fn structural_scalar_borrowed_drop_state(_state: *mut ()) {}

// Build the erased row-runtime vtable for borrowed adapter-owned state.
const fn borrowed_scalar_row_runtime_vtable() -> ScalarRowRuntimeVTable {
    ScalarRowRuntimeVTable {
        read_kernel_row: structural_scalar_read_kernel_row,
        drop_state: structural_scalar_borrowed_drop_state,
    }
}

///
/// StructuralCursorPage
///
/// StructuralCursorPage is the shared scalar page payload emitted by the
/// monomorphic scalar runtime before typed response reconstruction.
/// It preserves post-access row order and the next-page cursor while keeping
/// final entity decode at the outer typed boundary only.
///

pub(in crate::db::executor) struct StructuralCursorPage {
    data_rows: Vec<DataRow>,
    row_count: usize,
    #[cfg(feature = "sql")]
    slot_rows: Option<Vec<RetainedSlotRow>>,
    #[cfg(feature = "sql")]
    projected_rows: Option<Vec<Vec<Value>>>,
    #[cfg(feature = "sql")]
    rendered_projected_rows: Option<Vec<Vec<String>>>,
    next_cursor: Option<crate::db::executor::pipeline::contracts::PageCursor>,
}

impl StructuralCursorPage {
    /// Build one structural scalar page from canonical data rows plus cursor state.
    #[must_use]
    pub(in crate::db::executor) const fn new(
        data_rows: Vec<DataRow>,
        next_cursor: Option<crate::db::executor::pipeline::contracts::PageCursor>,
    ) -> Self {
        Self {
            row_count: data_rows.len(),
            data_rows,
            #[cfg(feature = "sql")]
            slot_rows: None,
            #[cfg(feature = "sql")]
            projected_rows: None,
            #[cfg(feature = "sql")]
            rendered_projected_rows: None,
            next_cursor,
        }
    }

    /// Build one structural scalar page while retaining already-decoded slot
    /// rows for SQL-only projection materialization.
    #[cfg(feature = "sql")]
    #[must_use]
    pub(in crate::db::executor) const fn new_with_slot_rows(
        slot_rows: Vec<RetainedSlotRow>,
        row_count: usize,
        next_cursor: Option<crate::db::executor::pipeline::contracts::PageCursor>,
    ) -> Self {
        Self {
            data_rows: Vec::new(),
            row_count,
            slot_rows: Some(slot_rows),
            projected_rows: None,
            rendered_projected_rows: None,
            next_cursor,
        }
    }

    /// Build one structural scalar page from already-projected SQL value rows.
    #[cfg(feature = "sql")]
    #[must_use]
    pub(in crate::db::executor) const fn new_with_projected_rows(
        projected_rows: Vec<Vec<Value>>,
        row_count: usize,
        next_cursor: Option<crate::db::executor::pipeline::contracts::PageCursor>,
    ) -> Self {
        Self {
            data_rows: Vec::new(),
            row_count,
            slot_rows: None,
            projected_rows: Some(projected_rows),
            rendered_projected_rows: None,
            next_cursor,
        }
    }

    /// Build one structural scalar page from already-rendered SQL text rows.
    #[cfg(feature = "sql")]
    #[must_use]
    pub(in crate::db::executor) const fn new_with_rendered_projected_rows(
        rendered_projected_rows: Vec<Vec<String>>,
        row_count: usize,
        next_cursor: Option<crate::db::executor::pipeline::contracts::PageCursor>,
    ) -> Self {
        Self {
            data_rows: Vec::new(),
            row_count,
            slot_rows: None,
            projected_rows: None,
            rendered_projected_rows: Some(rendered_projected_rows),
            next_cursor,
        }
    }

    /// Return the number of structural rows carried by this page.
    #[must_use]
    pub(in crate::db::executor) const fn row_count(&self) -> usize {
        self.row_count
    }

    /// Borrow structural scalar rows without forcing typed response assembly.
    #[must_use]
    pub(in crate::db::executor) fn data_rows(&self) -> &[DataRow] {
        &self.data_rows
    }

    /// Consume one structural scalar page into SQL-retained slot rows or canonical data rows.
    #[cfg(feature = "sql")]
    #[must_use]
    pub(in crate::db::executor) fn into_sql_parts(self) -> StructuralSqlProjectionRows {
        (
            self.slot_rows,
            self.projected_rows,
            self.rendered_projected_rows,
            self.data_rows,
        )
    }

    /// Consume one structural scalar page into rows plus cursor state.
    #[must_use]
    pub(in crate::db::executor) fn into_parts(
        self,
    ) -> (
        Vec<DataRow>,
        Option<crate::db::executor::pipeline::contracts::PageCursor>,
    ) {
        (self.data_rows, self.next_cursor)
    }
}

///
/// CursorEmissionMode
///
/// Cursor emission contract for structural page materialization.
/// Shared scalar execution uses this to keep no-cursor SQL projection lanes
/// explicit instead of inferring cursor assembly from unrelated bool flags.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) enum CursorEmissionMode {
    Emit,
    Suppress,
}

impl CursorEmissionMode {
    /// Return whether structural page materialization should assemble an
    /// outward continuation cursor.
    #[must_use]
    pub(in crate::db::executor) const fn enabled(self) -> bool {
        matches!(self, Self::Emit)
    }
}

///
/// ProjectionMaterializationMode
///
/// ProjectionMaterializationMode keeps projection-retention behavior explicit
/// at the structural execution boundary instead of scattering multiple
/// interdependent bool flags across kernel/runtime contracts.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) enum ProjectionMaterializationMode {
    SharedValidation,
    SqlImmediateMaterialization,
    SqlImmediateRenderedDispatch,
}

impl ProjectionMaterializationMode {
    /// Return whether this execution attempt still requires the shared
    /// projection-validation pass before surface-owned materialization.
    #[must_use]
    pub(in crate::db::executor) const fn validate_projection(self) -> bool {
        matches!(self, Self::SharedValidation)
    }

    /// Return whether this execution attempt should retain decoded slot rows
    /// for an immediate SQL projection materialization step.
    #[must_use]
    pub(in crate::db::executor) const fn retain_slot_rows(self) -> bool {
        matches!(
            self,
            Self::SqlImmediateMaterialization | Self::SqlImmediateRenderedDispatch
        )
    }

    /// Return whether this execution attempt should prefer already-rendered
    /// SQL projection rows over `Value` materialization when a terminal short
    /// path can prove them directly.
    #[must_use]
    pub(in crate::db::executor) const fn prefer_rendered_projection_rows(self) -> bool {
        matches!(self, Self::SqlImmediateRenderedDispatch)
    }

    /// Return whether this execution attempt should assemble one outward
    /// continuation cursor from the materialized structural page.
    #[must_use]
    pub(in crate::db::executor) const fn emit_cursor(self) -> bool {
        matches!(self, Self::SharedValidation)
    }
}

///
/// RuntimePageMaterializationRequest
///
/// Generic-free page materialization envelope consumed through the executor
/// runtime adapter boundary.
///

pub(in crate::db::executor) struct RuntimePageMaterializationRequest<'a> {
    pub(in crate::db::executor) plan: &'a AccessPlannedQuery,
    pub(in crate::db::executor) predicate_slots: Option<&'a PredicateProgram>,
    pub(in crate::db::executor) key_stream: &'a mut dyn OrderedKeyStream,
    pub(in crate::db::executor) scan_budget_hint: Option<usize>,
    pub(in crate::db::executor) load_order_route_contract: LoadOrderRouteContract,
    pub(in crate::db::executor) validate_projection: bool,
    pub(in crate::db::executor) retain_slot_rows: bool,
    pub(in crate::db::executor) slot_only_required_slots: Option<&'a [usize]>,
    #[cfg(feature = "sql")]
    pub(in crate::db::executor) prepared_sql_projection: Option<&'a PreparedSqlExecutionProjection>,
    pub(in crate::db::executor) cursor_emission: CursorEmissionMode,
    pub(in crate::db::executor) consistency: MissingRowPolicy,
    pub(in crate::db::executor) continuation: ScalarContinuationBindings<'a>,
}

///
/// RowCollectorMaterializationRequest
///
/// Structural short-path materialization envelope for the cursorless
/// row-collector lane.
/// This now carries the route-owned scalar terminal fast-path contract so the
/// terminal runtime can consume planner-selected covering-read metadata
/// without rediscovering it ad hoc.
///

pub(in crate::db::executor) struct RowCollectorMaterializationRequest<'a> {
    pub(in crate::db::executor) plan: &'a AccessPlannedQuery,
    pub(in crate::db::executor) scan_budget_hint: Option<usize>,
    pub(in crate::db::executor) load_order_route_contract: LoadOrderRouteContract,
    pub(in crate::db::executor) continuation: ScalarContinuationBindings<'a>,
    pub(in crate::db::executor) cursor_boundary: Option<&'a CursorBoundary>,
    pub(in crate::db::executor) load_terminal_fast_path: Option<&'a LoadTerminalFastPathContract>,
    pub(in crate::db::executor) predicate_slots: Option<&'a PredicateProgram>,
    pub(in crate::db::executor) validate_projection: bool,
    pub(in crate::db::executor) retain_slot_rows: bool,
    pub(in crate::db::executor) slot_only_required_slots: Option<&'a [usize]>,
    #[cfg(feature = "sql")]
    pub(in crate::db::executor) prepared_sql_projection: Option<&'a PreparedSqlExecutionProjection>,
    pub(in crate::db::executor) prefer_rendered_projection_rows: bool,
    pub(in crate::db::executor) key_stream: &'a mut dyn OrderedKeyStream,
}

///
/// DirectCoveringScanMaterializationRequest
///
/// Structural pre-key-stream covering-scan materialization envelope.
/// This keeps the kernel-owned early covering attempt on the same runtime
/// boundary as the later row-collector path without requiring a placeholder
/// ordered key stream.
///

pub(in crate::db::executor) struct DirectCoveringScanMaterializationRequest<'a> {
    pub(in crate::db::executor) plan: &'a AccessPlannedQuery,
    pub(in crate::db::executor) scan_budget_hint: Option<usize>,
    pub(in crate::db::executor) cursor_boundary: Option<&'a CursorBoundary>,
    pub(in crate::db::executor) load_terminal_fast_path: Option<&'a LoadTerminalFastPathContract>,
    pub(in crate::db::executor) predicate_slots: Option<&'a PredicateProgram>,
    pub(in crate::db::executor) validate_projection: bool,
    pub(in crate::db::executor) retain_slot_rows: bool,
    #[cfg(feature = "sql")]
    pub(in crate::db::executor) prepared_sql_projection: Option<&'a PreparedSqlExecutionProjection>,
    pub(in crate::db::executor) prefer_rendered_projection_rows: bool,
}

///
/// ExecutionRuntime
///
/// Executor-bound runtime adapter resolved once at the typed boundary.
/// All typed access-path and context authority must flow through this trait so
/// shared execution code can remain monomorphic over plan shape.
///

pub(in crate::db::executor) trait ExecutionRuntime {
    /// Resolve one primary-key fast path when the route is already verified.
    fn try_execute_pk_order_stream(
        &self,
        plan: &AccessPlannedQuery,
        direction: Direction,
        physical_fetch_hint: Option<usize>,
    ) -> Result<Option<FastPathKeyResult>, InternalError>;

    /// Resolve one verified secondary-prefix fast path.
    fn try_execute_secondary_index_order_stream(
        &self,
        plan: &AccessPlannedQuery,
        index_prefix_spec: Option<&crate::db::executor::LoweredIndexPrefixSpec>,
        direction: Direction,
        physical_fetch_hint: Option<usize>,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<Option<FastPathKeyResult>, InternalError>;

    /// Resolve one verified index-range limit-pushdown fast path.
    fn try_execute_index_range_limit_pushdown_stream(
        &self,
        plan: &AccessPlannedQuery,
        index_range_spec: Option<&crate::db::executor::LoweredIndexRangeSpec>,
        continuation: crate::db::executor::AccessScanContinuationInput<'_>,
        fetch: usize,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<Option<FastPathKeyResult>, InternalError>;

    /// Resolve the canonical fallback routed key stream for this execution attempt.
    fn resolve_fallback_execution_key_stream(
        &self,
        bindings: AccessStreamBindings<'_>,
        physical_fetch_hint: Option<usize>,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
        preserve_leaf_index_order: bool,
    ) -> Result<OrderedKeyStreamBox, InternalError>;

    /// Attempt the cursorless direct covering-scan short path before generic
    /// key-stream resolution when the same terminal-owned covering contract
    /// can already materialize the final structural page without a key stream.
    fn try_materialize_load_via_direct_covering_scan<'a>(
        &'a self,
        request: DirectCoveringScanMaterializationRequest<'a>,
    ) -> Result<Option<StructuralRowCollectorPayload>, InternalError>;

    /// Attempt the cursorless row-collector short path and erase the typed page result.
    fn try_materialize_load_via_row_collector<'a>(
        &'a self,
        request: RowCollectorMaterializationRequest<'a>,
    ) -> Result<Option<StructuralRowCollectorPayload>, InternalError>;

    /// Materialize one ordered key stream into one structural scalar page payload.
    fn materialize_key_stream_into_structural_page(
        &self,
        request: RuntimePageMaterializationRequest<'_>,
    ) -> Result<StructuralRowCollectorPayload, InternalError>;
}

///
/// ExecutionRuntimeAdapter
///
/// Typed runtime adapter that captures recovered context plus structural
/// runtime helpers once at the execution boundary and exposes one monomorphic
/// runtime trait surface to shared executor code.
///

///
/// ExecutionRuntimeAdapterCore
///
/// Generic-free runtime-adapter payload shared by typed execution-runtime
/// wrappers so structural row-runtime state stays monomorphic after the typed
/// boundary computes access-specific inputs.
///

struct ExecutionRuntimeAdapterCore<'a> {
    runtime: ErasedRuntimeBindings,
    access: &'a crate::db::access::AccessPlan<crate::value::Value>,
    model: &'static EntityModel,
    scalar_row_runtime: Option<ScalarRowRuntimeState>,
    covering_component_scan: Option<CoveringComponentScanState<'a>>,
}

impl ExecutionRuntimeAdapterCore<'_> {
    const fn new<'a>(
        access: &'a crate::db::access::AccessPlan<crate::value::Value>,
        runtime: ErasedRuntimeBindings,
        model: &'static EntityModel,
        scalar_row_runtime: Option<ScalarRowRuntimeState>,
        covering_component_scan: Option<CoveringComponentScanState<'a>>,
    ) -> ExecutionRuntimeAdapterCore<'a> {
        ExecutionRuntimeAdapterCore {
            runtime,
            access,
            model,
            scalar_row_runtime,
            covering_component_scan,
        }
    }

    // Require the scalar materialization runtime when the caller enters one
    // scalar-only row materialization path through the shared runtime trait.
    fn scalar_row_runtime(&self) -> Result<&ScalarRowRuntimeState, InternalError> {
        self.scalar_row_runtime.as_ref().ok_or_else(|| {
            InternalError::query_executor_invariant(
                "scalar row runtime is required for scalar materialization paths",
            )
        })
    }

    fn try_execute_pk_order_stream(
        &self,
        plan: &AccessPlannedQuery,
        direction: Direction,
        physical_fetch_hint: Option<usize>,
    ) -> Result<Option<FastPathKeyResult>, InternalError> {
        self.runtime.pk_order(plan, direction, physical_fetch_hint)
    }

    fn try_execute_secondary_index_order_stream(
        &self,
        plan: &AccessPlannedQuery,
        index_prefix_spec: Option<&crate::db::executor::LoweredIndexPrefixSpec>,
        direction: Direction,
        physical_fetch_hint: Option<usize>,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<Option<FastPathKeyResult>, InternalError> {
        self.runtime.secondary_index_order(
            plan,
            index_prefix_spec,
            direction,
            physical_fetch_hint,
            index_predicate_execution,
        )
    }

    fn try_execute_index_range_limit_pushdown_stream(
        &self,
        plan: &AccessPlannedQuery,
        index_range_spec: Option<&crate::db::executor::LoweredIndexRangeSpec>,
        continuation: crate::db::executor::AccessScanContinuationInput<'_>,
        fetch: usize,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<Option<FastPathKeyResult>, InternalError> {
        self.runtime.index_range_limit_pushdown(
            plan,
            index_range_spec,
            continuation,
            fetch,
            index_predicate_execution,
        )
    }

    fn resolve_fallback_execution_key_stream(
        &self,
        bindings: AccessStreamBindings<'_>,
        physical_fetch_hint: Option<usize>,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
        preserve_leaf_index_order: bool,
    ) -> Result<OrderedKeyStreamBox, InternalError> {
        self.runtime.fallback_execution_keys(
            self.access,
            bindings,
            physical_fetch_hint,
            index_predicate_execution,
            preserve_leaf_index_order,
        )
    }
}

pub(in crate::db::executor) struct ExecutionRuntimeAdapter<'ctx, 'a> {
    core: ExecutionRuntimeAdapterCore<'a>,
    marker: PhantomData<(&'a (), &'ctx ())>,
}

impl<'a> ExecutionRuntimeAdapter<'_, 'a> {
    /// Build one structural runtime adapter from structural runtime authority plus access plan.
    pub(in crate::db::executor) const fn from_runtime_parts(
        access: &'a crate::db::access::AccessPlan<crate::value::Value>,
        runtime: crate::db::executor::stream::access::TraversalRuntime,
        store: StoreHandle,
        model: &'static EntityModel,
    ) -> Self {
        Self {
            core: ExecutionRuntimeAdapterCore::new(
                access,
                ErasedRuntimeBindings::from_runtime(runtime),
                model,
                Some(ScalarRowRuntimeState::new(store, model)),
                None,
            ),
            marker: PhantomData,
        }
    }

    /// Build one structural runtime adapter for scalar execution paths that
    /// may consume route-owned covering-read component scans.
    pub(in crate::db::executor) const fn from_scalar_runtime_parts(
        access: &'a crate::db::access::AccessPlan<crate::value::Value>,
        runtime: crate::db::executor::stream::access::TraversalRuntime,
        store: StoreHandle,
        model: &'static EntityModel,
        covering_component_scan: CoveringComponentScanState<'a>,
    ) -> Self {
        Self {
            core: ExecutionRuntimeAdapterCore::new(
                access,
                ErasedRuntimeBindings::from_runtime(runtime),
                model,
                Some(ScalarRowRuntimeState::new(store, model)),
                Some(covering_component_scan),
            ),
            marker: PhantomData,
        }
    }

    /// Build one stream-only runtime adapter for key-stream resolution paths
    /// that never materialize scalar rows.
    pub(in crate::db::executor) const fn from_stream_runtime_parts(
        access: &'a crate::db::access::AccessPlan<crate::value::Value>,
        runtime: crate::db::executor::stream::access::TraversalRuntime,
        model: &'static EntityModel,
    ) -> Self {
        Self {
            core: ExecutionRuntimeAdapterCore::new(
                access,
                ErasedRuntimeBindings::from_runtime(runtime),
                model,
                None,
                None,
            ),
            marker: PhantomData,
        }
    }
}

impl ExecutionRuntime for ExecutionRuntimeAdapter<'_, '_> {
    fn try_execute_pk_order_stream(
        &self,
        plan: &AccessPlannedQuery,
        direction: Direction,
        physical_fetch_hint: Option<usize>,
    ) -> Result<Option<FastPathKeyResult>, InternalError> {
        self.core
            .try_execute_pk_order_stream(plan, direction, physical_fetch_hint)
    }

    fn try_execute_secondary_index_order_stream(
        &self,
        plan: &AccessPlannedQuery,
        index_prefix_spec: Option<&crate::db::executor::LoweredIndexPrefixSpec>,
        direction: Direction,
        physical_fetch_hint: Option<usize>,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<Option<FastPathKeyResult>, InternalError> {
        self.core.try_execute_secondary_index_order_stream(
            plan,
            index_prefix_spec,
            direction,
            physical_fetch_hint,
            index_predicate_execution,
        )
    }

    fn try_execute_index_range_limit_pushdown_stream(
        &self,
        plan: &AccessPlannedQuery,
        index_range_spec: Option<&crate::db::executor::LoweredIndexRangeSpec>,
        continuation: crate::db::executor::AccessScanContinuationInput<'_>,
        fetch: usize,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<Option<FastPathKeyResult>, InternalError> {
        self.core.try_execute_index_range_limit_pushdown_stream(
            plan,
            index_range_spec,
            continuation,
            fetch,
            index_predicate_execution,
        )
    }

    fn resolve_fallback_execution_key_stream(
        &self,
        bindings: AccessStreamBindings<'_>,
        physical_fetch_hint: Option<usize>,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
        preserve_leaf_index_order: bool,
    ) -> Result<OrderedKeyStreamBox, InternalError> {
        self.core.resolve_fallback_execution_key_stream(
            bindings,
            physical_fetch_hint,
            index_predicate_execution,
            preserve_leaf_index_order,
        )
    }

    fn try_materialize_load_via_direct_covering_scan<'a>(
        &'a self,
        request: DirectCoveringScanMaterializationRequest<'a>,
    ) -> Result<Option<StructuralRowCollectorPayload>, InternalError> {
        ExecutionKernel::try_materialize_load_via_direct_covering_scan(
            request,
            self.core.model,
            self.core.scalar_row_runtime()?.store,
            self.core.covering_component_scan,
        )
    }

    fn try_materialize_load_via_row_collector<'a>(
        &'a self,
        request: RowCollectorMaterializationRequest<'a>,
    ) -> Result<Option<StructuralRowCollectorPayload>, InternalError> {
        // Reuse the adapter-owned structural row-runtime state for the whole
        // query instead of cloning and boxing the same read-only runtime
        // descriptor before every materialization call.
        let scalar_row_runtime = self.core.scalar_row_runtime()?;
        let mut row_runtime = ScalarRowRuntimeHandle::from_borrowed(
            scalar_row_runtime,
            borrowed_scalar_row_runtime_vtable(),
        );

        ExecutionKernel::try_materialize_load_via_row_collector(
            request,
            self.core.model,
            &mut row_runtime,
            scalar_row_runtime.store,
            self.core.covering_component_scan,
        )
    }

    fn materialize_key_stream_into_structural_page(
        &self,
        request: RuntimePageMaterializationRequest<'_>,
    ) -> Result<StructuralRowCollectorPayload, InternalError> {
        // Reuse the adapter-owned structural row-runtime state for the whole
        // query instead of cloning and boxing the same read-only runtime
        // descriptor before every materialization call.
        let scalar_row_runtime = self.core.scalar_row_runtime()?;
        let mut row_runtime = ScalarRowRuntimeHandle::from_borrowed(
            scalar_row_runtime,
            borrowed_scalar_row_runtime_vtable(),
        );

        materialize_key_stream_into_structural_page(
            KernelPageMaterializationRequest {
                model: self.core.model,
                plan: request.plan,
                predicate_slots: request.predicate_slots,
                key_stream: request.key_stream,
                scan_budget_hint: request.scan_budget_hint,
                load_order_route_contract: request.load_order_route_contract,
                validate_projection: request.validate_projection,
                retain_slot_rows: request.retain_slot_rows,
                slot_only_required_slots: request.slot_only_required_slots,
                #[cfg(feature = "sql")]
                prepared_sql_projection: request.prepared_sql_projection,
                cursor_emission: request.cursor_emission,
                consistency: request.consistency,
                continuation: request.continuation,
            },
            &mut row_runtime,
        )
    }
}

///
/// ExecutionInputs
///
/// Shared immutable execution inputs for one load execution attempt.
/// Keeps shared execution code monomorphic by carrying plan shape, runtime
/// bindings, and the pre-resolved runtime adapter instead of typed entity params.
///

pub(in crate::db::executor) struct ExecutionInputs<'a> {
    runtime: &'a dyn ExecutionRuntime,
    plan: &'a AccessPlannedQuery,
    stream_bindings: AccessStreamBindings<'a>,
    execution_preparation: &'a ExecutionPreparation,
    projection_materialization: ProjectionMaterializationMode,
    prepared_projection: PreparedExecutionProjection,
    emit_cursor: bool,
}

///
/// ExecutionInputPreparation
///
/// ExecutionInputPreparation keeps the remaining prepare-time execution
/// invariants together so `ExecutionInputs::new(...)` does not keep widening as
/// more fixed-cost metadata moves out of the hot path.
/// The bundle owns only pre-execution policy inputs, not runtime row streams.
///

pub(in crate::db::executor) struct ExecutionInputPreparation<'a> {
    pub(in crate::db::executor) model: &'static EntityModel,
    pub(in crate::db::executor) load_terminal_fast_path: Option<&'a LoadTerminalFastPathContract>,
    pub(in crate::db::executor) emit_cursor: bool,
}

impl<'a> ExecutionInputs<'a> {
    /// Construct one scalar execution-input projection payload.
    pub(in crate::db::executor) fn new(
        runtime: &'a dyn ExecutionRuntime,
        plan: &'a AccessPlannedQuery,
        stream_bindings: AccessStreamBindings<'a>,
        execution_preparation: &'a ExecutionPreparation,
        projection_materialization: ProjectionMaterializationMode,
        preparation: ExecutionInputPreparation<'a>,
    ) -> Result<Self, InternalError> {
        let ExecutionInputPreparation {
            model,
            load_terminal_fast_path,
            emit_cursor,
        } = preparation;
        let prepared_projection = PreparedExecutionProjection::compile(
            model,
            plan,
            execution_preparation.compiled_predicate(),
            projection_materialization,
            load_terminal_fast_path,
        )?;

        Ok(Self {
            runtime,
            plan,
            stream_bindings,
            execution_preparation,
            projection_materialization,
            prepared_projection,
            emit_cursor,
        })
    }

    /// Construct one scalar execution-input payload from already-prepared
    /// execution and projection state.
    pub(in crate::db::executor) fn new_prepared(
        runtime: &'a dyn ExecutionRuntime,
        plan: &'a AccessPlannedQuery,
        stream_bindings: AccessStreamBindings<'a>,
        execution_preparation: &'a ExecutionPreparation,
        projection_materialization: ProjectionMaterializationMode,
        prepared_projection: PreparedExecutionProjection,
        emit_cursor: bool,
    ) -> Self {
        Self {
            runtime,
            plan,
            stream_bindings,
            execution_preparation,
            projection_materialization,
            prepared_projection,
            emit_cursor,
        }
    }

    /// Borrow the resolved runtime adapter for this execution attempt.
    #[must_use]
    pub(in crate::db::executor) const fn runtime(&self) -> &dyn ExecutionRuntime {
        self.runtime
    }

    /// Borrow logical access plan payload for this execution attempt.
    #[must_use]
    pub(in crate::db::executor) const fn plan(&self) -> &AccessPlannedQuery {
        self.plan
    }

    /// Borrow lowered access stream bindings for this execution attempt.
    #[must_use]
    pub(in crate::db::executor) const fn stream_bindings(&self) -> &AccessStreamBindings<'_> {
        &self.stream_bindings
    }

    /// Borrow precomputed execution-preparation payloads.
    #[must_use]
    pub(in crate::db::executor) const fn execution_preparation(&self) -> &ExecutionPreparation {
        self.execution_preparation
    }

    /// Return whether this execution attempt still requires the shared
    /// projection-validation pass before surface-owned materialization.
    #[must_use]
    pub(in crate::db::executor) const fn validate_projection(&self) -> bool {
        self.projection_materialization.validate_projection()
    }

    /// Return whether this execution attempt should retain decoded slot rows
    /// for an immediate SQL projection materialization step.
    #[must_use]
    pub(in crate::db::executor) const fn retain_slot_rows(&self) -> bool {
        self.projection_materialization.retain_slot_rows()
    }

    /// Borrow the precomputed retained-slot layout for cursorless SQL
    /// materialization when this execution shape keeps slot rows.
    #[must_use]
    pub(in crate::db::executor) fn slot_only_required_slots(&self) -> Option<&[usize]> {
        self.prepared_projection.slot_only_required_slots()
    }

    /// Borrow one precomputed SQL projection bundle for cursorless SQL short
    /// paths when this execution shape retains slot rows.
    #[cfg(feature = "sql")]
    #[must_use]
    pub(in crate::db::executor) const fn prepared_sql_projection(
        &self,
    ) -> Option<&PreparedSqlExecutionProjection> {
        self.prepared_projection.sql()
    }

    /// Return whether this execution attempt should prefer already-rendered
    /// SQL projection rows over `Value` materialization when a terminal short
    /// path can prove them directly.
    #[must_use]
    pub(in crate::db::executor) const fn prefer_rendered_projection_rows(&self) -> bool {
        self.projection_materialization
            .prefer_rendered_projection_rows()
    }

    /// Return whether this execution attempt should assemble one outward
    /// continuation cursor from the materialized structural page.
    #[must_use]
    pub(in crate::db::executor) const fn emit_cursor(&self) -> bool {
        self.emit_cursor
    }

    /// Return row-read missing-row policy for this execution attempt.
    #[must_use]
    pub(in crate::db::executor) const fn consistency(&self) -> MissingRowPolicy {
        row_read_consistency_for_plan(self.plan)
    }
}

// Compile the canonical retained-slot layout once per execution shape so
// cursorless SQL row collectors do not rebuild projection/predicate/order
// reachability ad hoc at each materialization boundary.
fn compile_slot_only_required_slots(
    model: &'static EntityModel,
    plan: &AccessPlannedQuery,
    compiled_predicate: Option<&PredicateProgram>,
    projection_materialization: ProjectionMaterializationMode,
) -> Result<Option<Vec<usize>>, InternalError> {
    if !projection_materialization.retain_slot_rows() {
        return Ok(None);
    }

    let mut required_slots = vec![false; model.fields().len()];

    // Phase 1: projection materialization always owns one stable slot set for
    // retained-slot SQL rows, even when final projection happens later.
    mark_projection_referenced_slots(model, &plan.projection_spec(model), &mut required_slots)?;

    // Phase 2: residual predicate filtering still runs on retained slot rows
    // before the outer SQL materializer consumes them.
    if plan.has_residual_predicate()
        && let Some(predicate_program) = compiled_predicate
    {
        predicate_program.mark_referenced_slots(&mut required_slots);
    }

    // Phase 3: post-access in-memory ordering only needs extra slots when the
    // chosen route does not already satisfy the visible order contract.
    if let Some(order) = plan.scalar_plan().order.as_ref()
        && !access_order_satisfied_by_route_contract_for_model(model, plan)
    {
        mark_structural_order_slots(model, order, &mut required_slots)?;
    }

    Ok(Some(
        required_slots
            .into_iter()
            .enumerate()
            .filter_map(|(slot, required)| required.then_some(slot))
            .collect(),
    ))
}
