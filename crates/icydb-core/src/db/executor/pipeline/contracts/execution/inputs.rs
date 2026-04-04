//! Module: db::executor::pipeline::contracts::inputs
//! Responsibility: module-local ownership and contracts for db::executor::pipeline::contracts::inputs.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

#[cfg(feature = "sql")]
use crate::value::Value;

#[cfg(feature = "sql")]
type StructuralSqlProjectionRows = (Option<Vec<Vec<Option<Value>>>>, Vec<DataRow>);

use crate::{
    db::{
        cursor::CursorBoundary,
        data::{DataKey, DataRow},
        direction::Direction,
        executor::pipeline::contracts::{FastPathKeyResult, execution::ErasedRuntimeBindings},
        executor::{
            AccessStreamBindings, ExecutionKernel, ExecutionPreparation, ExecutorError,
            OrderedKeyStream, OrderedKeyStreamBox, ScalarContinuationBindings,
            terminal::{
                RowDecoder, RowLayout,
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

// Read one scalar kernel row through the typed store boundary, then decode the
// persisted row structurally before any predicate or page logic runs.
unsafe fn structural_scalar_read_kernel_row(
    state: *mut (),
    consistency: MissingRowPolicy,
    key: &DataKey,
    payload_mode: KernelRowPayloadMode,
    predicate_preapplied: bool,
    predicate_slots: Option<&PredicateProgram>,
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
            let slots =
                state
                    .row_decoder
                    .decode_slots(&state.row_layout, key.storage_key(), &row)?;

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
    slot_rows: Option<Vec<Vec<Option<Value>>>>,
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
            next_cursor,
        }
    }

    /// Build one structural scalar page while retaining already-decoded slot
    /// rows for SQL-only projection materialization.
    #[cfg(feature = "sql")]
    #[must_use]
    pub(in crate::db::executor) const fn new_with_slot_rows(
        slot_rows: Vec<Vec<Option<Value>>>,
        row_count: usize,
        next_cursor: Option<crate::db::executor::pipeline::contracts::PageCursor>,
    ) -> Self {
        Self {
            data_rows: Vec::new(),
            row_count,
            slot_rows: Some(slot_rows),
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
        (self.slot_rows, self.data_rows)
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
    pub(in crate::db::executor) stream_order_contract_safe: bool,
    pub(in crate::db::executor) validate_projection: bool,
    pub(in crate::db::executor) retain_slot_rows: bool,
    pub(in crate::db::executor) cursor_emission: CursorEmissionMode,
    pub(in crate::db::executor) consistency: MissingRowPolicy,
    pub(in crate::db::executor) continuation: ScalarContinuationBindings<'a>,
}

///
/// RowCollectorMaterializationRequest
///
/// Structural short-path materialization envelope for the cursorless
/// row-collector lane.
///

pub(in crate::db::executor) struct RowCollectorMaterializationRequest<'a> {
    pub(in crate::db::executor) plan: &'a AccessPlannedQuery,
    pub(in crate::db::executor) scan_budget_hint: Option<usize>,
    pub(in crate::db::executor) stream_order_contract_safe: bool,
    pub(in crate::db::executor) continuation: ScalarContinuationBindings<'a>,
    pub(in crate::db::executor) cursor_boundary: Option<&'a CursorBoundary>,
    pub(in crate::db::executor) predicate_slots: Option<&'a PredicateProgram>,
    pub(in crate::db::executor) validate_projection: bool,
    pub(in crate::db::executor) retain_slot_rows: bool,
    pub(in crate::db::executor) key_stream: &'a mut dyn OrderedKeyStream,
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
    ) -> Result<OrderedKeyStreamBox, InternalError>;

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
}

impl ExecutionRuntimeAdapterCore<'_> {
    const fn new<'a>(
        access: &'a crate::db::access::AccessPlan<crate::value::Value>,
        runtime: ErasedRuntimeBindings,
        model: &'static EntityModel,
        scalar_row_runtime: Option<ScalarRowRuntimeState>,
    ) -> ExecutionRuntimeAdapterCore<'a> {
        ExecutionRuntimeAdapterCore {
            runtime,
            access,
            model,
            scalar_row_runtime,
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
    ) -> Result<OrderedKeyStreamBox, InternalError> {
        self.runtime.fallback_execution_keys(
            self.access,
            bindings,
            physical_fetch_hint,
            index_predicate_execution,
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
    ) -> Result<OrderedKeyStreamBox, InternalError> {
        self.core.resolve_fallback_execution_key_stream(
            bindings,
            physical_fetch_hint,
            index_predicate_execution,
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
                stream_order_contract_safe: request.stream_order_contract_safe,
                validate_projection: request.validate_projection,
                retain_slot_rows: request.retain_slot_rows,
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
    validate_projection: bool,
    retain_slot_rows: bool,
    emit_cursor: bool,
}

impl<'a> ExecutionInputs<'a> {
    /// Construct one scalar execution-input projection payload.
    #[must_use]
    pub(in crate::db::executor) const fn new(
        runtime: &'a dyn ExecutionRuntime,
        plan: &'a AccessPlannedQuery,
        stream_bindings: AccessStreamBindings<'a>,
        execution_preparation: &'a ExecutionPreparation,
        validate_projection: bool,
        retain_slot_rows: bool,
        emit_cursor: bool,
    ) -> Self {
        Self {
            runtime,
            plan,
            stream_bindings,
            execution_preparation,
            validate_projection,
            retain_slot_rows,
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
        self.validate_projection
    }

    /// Return whether this execution attempt should retain decoded slot rows
    /// for an immediate SQL projection materialization step.
    #[must_use]
    pub(in crate::db::executor) const fn retain_slot_rows(&self) -> bool {
        self.retain_slot_rows
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
