//! Module: db::executor::pipeline::contracts::inputs
//! Responsibility: module-local ownership and contracts for db::executor::pipeline::contracts::inputs.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
        cursor::CursorBoundary,
        data::{DataKey, DataRow},
        direction::Direction,
        executor::pipeline::contracts::FastPathKeyResult,
        executor::{
            AccessStreamBindings, Context, ExecutableAccess, ExecutionKernel, ExecutionPreparation,
            ExecutorError, LoadExecutor, OrderedKeyStream, OrderedKeyStreamBox,
            ScalarContinuationBindings,
            preparation::resolved_index_slots_for_access_path,
            route::RoutedKeyStreamRequest,
            terminal::{
                RowDecoder, RowLayout,
                page::{
                    KernelPageMaterializationRequest, ScalarRowRuntimeHandle,
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
    traits::{EntityKind, EntityValue, Path},
};

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
    fn new(store: StoreHandle, model: &'static EntityModel) -> Self {
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
            MissingRowPolicy::Error => row.map(Some).ok_or_else(|| {
                InternalError::from(ExecutorError::store_corruption(format!(
                    "missing row: {key}"
                )))
            }),
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
    predicate_preapplied: bool,
    predicate_slots: Option<&PredicateProgram>,
) -> Result<Option<crate::db::executor::terminal::page::KernelRow>, InternalError> {
    let state = unsafe { &mut *state.cast::<ScalarRowRuntimeState>() };
    let Some(row) = state.read_row(consistency, key)? else {
        return Ok(None);
    };
    let data_row = (key.clone(), row);
    let kernel_row = state.row_decoder.decode(&state.row_layout, data_row)?;
    if predicate_preapplied
        && let Some(predicate_program) = predicate_slots
        && !predicate_program.eval_with_slot_reader(&mut |slot| kernel_row.slot(slot))
    {
        return Ok(None);
    }

    Ok(Some(kernel_row))
}

// Drop one erased typed scalar runtime state allocated by the row-runtime
// handle constructor.
unsafe fn structural_scalar_drop_state(state: *mut ()) {
    drop(unsafe { Box::from_raw(state.cast::<ScalarRowRuntimeState>()) });
}

// Build the erased scalar row-runtime vtable once for one typed boundary.
const fn scalar_row_runtime_vtable() -> ScalarRowRuntimeVTable {
    ScalarRowRuntimeVTable {
        read_kernel_row: structural_scalar_read_kernel_row,
        drop_state: structural_scalar_drop_state,
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
            data_rows,
            next_cursor,
        }
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
    pub(in crate::db::executor) consistency: MissingRowPolicy,
    pub(in crate::db::executor) continuation: ScalarContinuationBindings<'a>,
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
    fn try_materialize_load_via_row_collector(
        &self,
        plan: &AccessPlannedQuery,
        cursor_boundary: Option<&CursorBoundary>,
        key_stream: &mut dyn OrderedKeyStream,
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
/// Typed runtime adapter that captures recovered context plus typed access
/// helpers once at the execution boundary and exposes one monomorphic runtime
/// trait surface to shared executor code.
///

pub(in crate::db::executor) struct ExecutionRuntimeAdapter<'ctx, 'a, E>
where
    E: EntityKind + EntityValue,
{
    ctx: &'a Context<'ctx, E>,
    access: &'a crate::db::access::AccessPlan<E::Key>,
    model: &'static EntityModel,
    slot_map: Option<Vec<usize>>,
    scalar_row_runtime: ScalarRowRuntimeState,
}

impl<'ctx, 'a, E> ExecutionRuntimeAdapter<'ctx, 'a, E>
where
    E: EntityKind + EntityValue,
{
    /// Build one typed runtime adapter from recovered context plus typed access sidecar.
    pub(in crate::db::executor) fn new(
        ctx: &'a Context<'ctx, E>,
        access: &'a crate::db::access::AccessPlan<E::Key>,
    ) -> Result<Self, InternalError> {
        let model = E::MODEL;
        let slot_map =
            resolved_index_slots_for_access_path(model, access.resolve_strategy().executable());
        let store = ctx
            .db
            .with_store_registry(|reg| reg.try_get_store(E::Store::PATH))?;

        Ok(Self {
            ctx,
            access,
            model,
            slot_map,
            scalar_row_runtime: ScalarRowRuntimeState::new(store, model),
        })
    }

    /// Borrow the precomputed slot map for this typed adapter.
    #[must_use]
    pub(in crate::db::executor) fn slot_map(&self) -> Option<&[usize]> {
        self.slot_map.as_deref()
    }
}

impl<E> ExecutionRuntime for ExecutionRuntimeAdapter<'_, '_, E>
where
    E: EntityKind + EntityValue,
{
    fn try_execute_pk_order_stream(
        &self,
        plan: &AccessPlannedQuery,
        direction: Direction,
        physical_fetch_hint: Option<usize>,
    ) -> Result<Option<FastPathKeyResult>, InternalError> {
        LoadExecutor::<E>::try_execute_pk_order_stream(
            self.ctx,
            plan,
            direction,
            physical_fetch_hint,
        )
    }

    fn try_execute_secondary_index_order_stream(
        &self,
        plan: &AccessPlannedQuery,
        index_prefix_spec: Option<&crate::db::executor::LoweredIndexPrefixSpec>,
        direction: Direction,
        physical_fetch_hint: Option<usize>,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<Option<FastPathKeyResult>, InternalError> {
        LoadExecutor::<E>::try_execute_secondary_index_order_stream(
            self.ctx,
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
        LoadExecutor::<E>::try_execute_index_range_limit_pushdown_stream(
            self.ctx,
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
        let access = ExecutableAccess::new(
            self.access,
            bindings,
            physical_fetch_hint,
            index_predicate_execution,
        );

        LoadExecutor::<E>::resolve_routed_key_stream(
            self.ctx,
            RoutedKeyStreamRequest::ExecutableAccess(access),
        )
    }

    fn try_materialize_load_via_row_collector(
        &self,
        plan: &AccessPlannedQuery,
        cursor_boundary: Option<&CursorBoundary>,
        key_stream: &mut dyn OrderedKeyStream,
    ) -> Result<Option<StructuralRowCollectorPayload>, InternalError> {
        let mut row_runtime = ScalarRowRuntimeHandle::new(
            self.scalar_row_runtime.clone(),
            scalar_row_runtime_vtable(),
        );

        ExecutionKernel::try_materialize_load_via_row_collector(
            plan,
            self.model,
            cursor_boundary,
            key_stream,
            &mut row_runtime,
        )
    }

    fn materialize_key_stream_into_structural_page(
        &self,
        request: RuntimePageMaterializationRequest<'_>,
    ) -> Result<StructuralRowCollectorPayload, InternalError> {
        let mut row_runtime = ScalarRowRuntimeHandle::new(
            self.scalar_row_runtime.clone(),
            scalar_row_runtime_vtable(),
        );

        materialize_key_stream_into_structural_page(
            KernelPageMaterializationRequest {
                model: self.model,
                plan: request.plan,
                predicate_slots: request.predicate_slots,
                key_stream: request.key_stream,
                scan_budget_hint: request.scan_budget_hint,
                stream_order_contract_safe: request.stream_order_contract_safe,
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
}

impl<'a> ExecutionInputs<'a> {
    /// Construct one scalar execution-input projection payload.
    #[must_use]
    pub(in crate::db::executor) const fn new(
        runtime: &'a dyn ExecutionRuntime,
        plan: &'a AccessPlannedQuery,
        stream_bindings: AccessStreamBindings<'a>,
        execution_preparation: &'a ExecutionPreparation,
    ) -> Self {
        Self {
            runtime,
            plan,
            stream_bindings,
            execution_preparation,
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

    /// Return row-read missing-row policy for this execution attempt.
    #[must_use]
    pub(in crate::db::executor) const fn consistency(&self) -> MissingRowPolicy {
        row_read_consistency_for_plan(self.plan)
    }
}
