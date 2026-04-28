//! Module: executor::pipeline::runtime::adapter
//! Responsibility: runtime adapters for stream resolution and scalar materialization.
//! Does not own: execution-input DTO construction or planning semantics.
//! Boundary: executes already-assembled execution contracts through runtime owners.

use crate::{
    db::{
        access::ExecutableAccessPlan,
        direction::Direction,
        executor::{
            AccessScanContinuationInput, AccessStreamBindings, EntityAuthority, ExecutableAccess,
            ExecutionKernel, LoweredIndexRangeSpec, OrderedKeyStreamBox, ScalarContinuationContext,
            pipeline::contracts::{
                CursorEmissionMode, FastPathKeyResult, FastStreamRouteKind, FastStreamRouteRequest,
                KernelPageMaterializationRequest, KernelRowsExecutionAttempt,
                MaterializedExecutionPayload, RowCollectorMaterializationRequest,
                RuntimePageMaterializationRequest, ScalarMaterializationCapabilities,
            },
            scan::execute_fast_stream_route,
            stream::access::TraversalRuntime,
            terminal::page::{
                ScalarRowRuntimeHandle, ScalarRowRuntimeState,
                materialize_key_stream_into_execution_payload,
                materialize_key_stream_into_kernel_rows,
            },
        },
        index::predicate::IndexPredicateExecution,
        predicate::MissingRowPolicy,
        query::plan::{AccessPlannedQuery, EffectiveRuntimeFilterProgram},
        registry::StoreHandle,
    },
    error::InternalError,
    value::Value,
};

type MaterializedExecutionPayloadResult = (MaterializedExecutionPayload, usize, usize);

///
/// ExecutionMaterializationContract
///
/// ExecutionMaterializationContract captures the execution-input fields shared
/// by the row-collector and runtime-page materialization requests.
/// Runtime materialization consumes this once so the two outward request shapes
/// do not re-spell predicate/projection/retained-slot wiring.
///

#[derive(Clone, Copy)]
pub(in crate::db::executor) struct ExecutionMaterializationContract<'a> {
    pub(in crate::db::executor) plan: &'a AccessPlannedQuery,
    pub(in crate::db::executor) residual_filter_program: Option<&'a EffectiveRuntimeFilterProgram>,
    pub(in crate::db::executor) scan_budget_hint: Option<usize>,
    pub(in crate::db::executor) load_order_route_contract:
        crate::db::executor::route::LoadOrderRouteContract,
    pub(in crate::db::executor) validate_projection: bool,
    pub(in crate::db::executor) retain_slot_rows: bool,
    pub(in crate::db::executor) retained_slot_layout: Option<&'a RetainedSlotLayout>,
    pub(in crate::db::executor) prepared_projection_validation:
        Option<&'a crate::db::executor::projection::PreparedSlotProjectionValidation>,
}

impl<'a> ExecutionMaterializationContract<'a> {
    // Materialize one resolved scalar key stream through the aligned
    // row-collector or canonical page runtime lane without rebuilding the
    // shared predicate/projection/retained-slot contract twice.
    pub(in crate::db::executor) fn materialize_resolved_execution_stream(
        &self,
        runtime: &'a ExecutionRuntimeAdapter,
        emit_cursor: bool,
        consistency: MissingRowPolicy,
        continuation: &'a ScalarContinuationContext,
        direction: Direction,
        key_stream: &'a mut OrderedKeyStreamBox,
    ) -> Result<MaterializedExecutionPayloadResult, InternalError> {
        runtime.materialize_resolved_execution_stream(
            self,
            emit_cursor,
            consistency,
            continuation,
            direction,
            key_stream,
        )
    }

    // Materialize one resolved scalar key stream through post-access/window
    // processing while stopping before structural page payload construction.
    pub(in crate::db::executor) fn materialize_resolved_execution_stream_to_kernel_rows(
        &self,
        runtime: &'a ExecutionRuntimeAdapter,
        consistency: MissingRowPolicy,
        continuation: &'a ScalarContinuationContext,
        direction: Direction,
        key_stream: &'a mut OrderedKeyStreamBox,
    ) -> Result<KernelRowsExecutionAttempt, InternalError> {
        runtime.materialize_resolved_execution_stream_to_kernel_rows(
            self,
            consistency,
            continuation,
            direction,
            key_stream,
        )
    }

    // Build the cursorless row-collector materialization request from one
    // already-aligned scalar materialization contract.
    const fn row_collector_request(
        &self,
        continuation: &'a ScalarContinuationContext,
        key_stream: &'a mut OrderedKeyStreamBox,
    ) -> RowCollectorMaterializationRequest<'a> {
        RowCollectorMaterializationRequest {
            plan: self.plan,
            scan_budget_hint: self.scan_budget_hint,
            load_order_route_contract: self.load_order_route_contract,
            continuation,
            cursor_boundary: continuation.cursor_boundary(),
            capabilities: ScalarMaterializationCapabilities {
                residual_filter_program: self.residual_filter_program,
                validate_projection: self.validate_projection,
                retain_slot_rows: self.retain_slot_rows,
                retained_slot_layout: self.retained_slot_layout,
                prepared_projection_validation: self.prepared_projection_validation,
                cursor_emission: CursorEmissionMode::Suppress,
            },
            key_stream,
        }
    }

    // Build the canonical scalar page materialization request from one
    // already-aligned scalar materialization contract.
    const fn runtime_page_request(
        &self,
        emit_cursor: bool,
        consistency: MissingRowPolicy,
        continuation: &'a ScalarContinuationContext,
        direction: Direction,
        key_stream: &'a mut OrderedKeyStreamBox,
    ) -> RuntimePageMaterializationRequest<'a> {
        RuntimePageMaterializationRequest {
            plan: self.plan,
            key_stream,
            scan_budget_hint: self.scan_budget_hint,
            load_order_route_contract: self.load_order_route_contract,
            capabilities: ScalarMaterializationCapabilities {
                residual_filter_program: self.residual_filter_program,
                validate_projection: self.validate_projection,
                retain_slot_rows: self.retain_slot_rows,
                retained_slot_layout: self.retained_slot_layout,
                prepared_projection_validation: self.prepared_projection_validation,
                cursor_emission: if emit_cursor {
                    CursorEmissionMode::Emit
                } else {
                    CursorEmissionMode::Suppress
                },
            },
            consistency,
            continuation,
            direction,
        }
    }
}

///
/// ExecutionRuntimeAdapter
///
/// Typed runtime adapter that captures recovered context plus structural
/// runtime helpers once at the execution boundary and exposes one monomorphic
/// runtime surface to shared executor code.
///

pub(in crate::db::executor) struct ExecutionRuntimeAdapter {
    runtime: TraversalRuntime,
    authority: Option<EntityAuthority>,
    scalar_row_runtime: Option<ScalarRowRuntimeState>,
}

impl ExecutionRuntimeAdapter {
    /// Build one structural runtime adapter for scalar execution paths.
    pub(in crate::db::executor) const fn from_scalar_runtime_parts(
        runtime: TraversalRuntime,
        store: StoreHandle,
        authority: EntityAuthority,
    ) -> Self {
        Self {
            runtime,
            authority: Some(authority),
            scalar_row_runtime: Some(ScalarRowRuntimeState::new(store, authority.row_layout())),
        }
    }

    /// Build one stream-only runtime adapter for key-stream resolution paths
    /// that never materialize scalar rows.
    pub(in crate::db::executor) const fn from_stream_runtime_parts(
        runtime: TraversalRuntime,
    ) -> Self {
        Self {
            runtime,
            authority: None,
            scalar_row_runtime: None,
        }
    }

    // Require the scalar materialization runtime when the caller enters one
    // scalar-only row materialization path through the shared execution spine.
    fn scalar_row_runtime(&self) -> Result<&ScalarRowRuntimeState, InternalError> {
        self.scalar_row_runtime.as_ref().ok_or_else(|| {
            InternalError::query_executor_invariant(
                "scalar row runtime is required for scalar materialization paths",
            )
        })
    }

    // Require structural entity authority only for runtime paths that still
    // materialize rows or covering projections through shared scalar kernels.
    fn authority(&self) -> Result<EntityAuthority, InternalError> {
        self.authority.ok_or_else(|| {
            InternalError::query_executor_invariant(
                "structural entity authority is required for row materialization paths",
            )
        })
    }

    // Reuse the adapter-owned scalar row runtime for one materialization call
    // so callers do not each rebuild the same borrowed runtime-handle shell.
    fn with_scalar_row_runtime_handle<'a, T>(
        &'a self,
        run: impl FnOnce(&mut ScalarRowRuntimeHandle<'a>) -> Result<T, InternalError>,
    ) -> Result<T, InternalError> {
        let scalar_row_runtime = self.scalar_row_runtime()?;
        let mut row_runtime = ScalarRowRuntimeHandle::from_borrowed(scalar_row_runtime);

        run(&mut row_runtime)
    }

    // Materialize one resolved scalar key stream through the aligned
    // row-collector or canonical page runtime lane owned by this runtime
    // adapter.
    fn materialize_resolved_execution_stream<'a>(
        &'a self,
        contract: &ExecutionMaterializationContract<'a>,
        emit_cursor: bool,
        consistency: MissingRowPolicy,
        continuation: &'a ScalarContinuationContext,
        direction: Direction,
        key_stream: &'a mut OrderedKeyStreamBox,
    ) -> Result<MaterializedExecutionPayloadResult, InternalError> {
        if !emit_cursor
            && let Some(materialized) = self.try_materialize_load_via_row_collector(
                contract.row_collector_request(continuation, key_stream),
            )?
        {
            return Ok(materialized);
        }

        self.materialize_key_stream_into_structural_page(contract.runtime_page_request(
            emit_cursor,
            consistency,
            continuation,
            direction,
            key_stream,
        ))
    }

    // Materialize one ordered key stream into post-access scalar kernel rows for
    // aggregate sinks that do not need an outward cursor page.
    fn materialize_resolved_execution_stream_to_kernel_rows<'a>(
        &'a self,
        contract: &ExecutionMaterializationContract<'a>,
        consistency: MissingRowPolicy,
        continuation: &'a ScalarContinuationContext,
        direction: Direction,
        key_stream: &'a mut OrderedKeyStreamBox,
    ) -> Result<KernelRowsExecutionAttempt, InternalError> {
        self.materialize_key_stream_into_kernel_rows(contract.runtime_page_request(
            false,
            consistency,
            continuation,
            direction,
            key_stream,
        ))
    }

    /// Resolve one primary-key fast path when the route is already verified.
    pub(in crate::db::executor) fn try_execute_pk_order_stream(
        &self,
        plan: &AccessPlannedQuery,
        executable_access: ExecutableAccessPlan<'_, Value>,
        direction: Direction,
        physical_fetch_hint: Option<usize>,
    ) -> Result<Option<FastPathKeyResult>, InternalError> {
        execute_fast_stream_route(
            &self.runtime,
            FastStreamRouteKind::PrimaryKey,
            FastStreamRouteRequest::PrimaryKey {
                plan,
                executable_access: &executable_access,
                stream_direction: direction,
                probe_fetch_hint: physical_fetch_hint,
            },
        )
    }

    /// Resolve one verified secondary-prefix fast path.
    pub(in crate::db::executor) fn try_execute_secondary_index_order_stream(
        &self,
        plan: &AccessPlannedQuery,
        executable_access: ExecutableAccessPlan<'_, Value>,
        index_prefix_spec: Option<&crate::db::executor::LoweredIndexPrefixSpec>,
        direction: Direction,
        physical_fetch_hint: Option<usize>,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<Option<FastPathKeyResult>, InternalError> {
        execute_fast_stream_route(
            &self.runtime,
            FastStreamRouteKind::SecondaryIndex,
            FastStreamRouteRequest::SecondaryIndex {
                plan,
                executable_access: &executable_access,
                index_prefix_spec,
                stream_direction: direction,
                probe_fetch_hint: physical_fetch_hint,
                index_predicate_execution,
            },
        )
    }

    /// Resolve one verified index-range limit-pushdown fast path.
    pub(in crate::db::executor) fn try_execute_index_range_limit_pushdown_stream(
        &self,
        plan: &AccessPlannedQuery,
        executable_access: ExecutableAccessPlan<'_, Value>,
        index_range_spec: Option<&LoweredIndexRangeSpec>,
        continuation: AccessScanContinuationInput<'_>,
        fetch: usize,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<Option<FastPathKeyResult>, InternalError> {
        execute_fast_stream_route(
            &self.runtime,
            FastStreamRouteKind::IndexRangeLimitPushdown,
            FastStreamRouteRequest::IndexRangeLimitPushdown {
                plan,
                executable_access: &executable_access,
                index_range_spec,
                continuation,
                effective_fetch: fetch,
                index_predicate_execution,
            },
        )
    }

    /// Resolve the canonical fallback routed key stream for this execution attempt.
    pub(in crate::db::executor) fn resolve_fallback_execution_key_stream(
        &self,
        executable_access: ExecutableAccessPlan<'_, Value>,
        bindings: AccessStreamBindings<'_>,
        physical_fetch_hint: Option<usize>,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
        preserve_leaf_index_order: bool,
    ) -> Result<OrderedKeyStreamBox, InternalError> {
        let mut access = ExecutableAccess::from_executable_plan(
            executable_access,
            bindings,
            physical_fetch_hint,
            index_predicate_execution,
        );
        if preserve_leaf_index_order {
            access = access.with_preserved_leaf_index_order();
        }

        self.runtime.ordered_key_stream_from_runtime_access(access)
    }

    /// Attempt the cursorless row-collector short path and erase the typed page result.
    fn try_materialize_load_via_row_collector<'req>(
        &'req self,
        request: RowCollectorMaterializationRequest<'req>,
    ) -> Result<Option<MaterializedExecutionPayloadResult>, InternalError> {
        self.with_scalar_row_runtime_handle(|row_runtime| {
            ExecutionKernel::try_materialize_load_via_row_collector(request, row_runtime)
        })
    }

    /// Materialize one ordered key stream into one structural scalar page payload.
    fn materialize_key_stream_into_structural_page(
        &self,
        request: RuntimePageMaterializationRequest<'_>,
    ) -> Result<MaterializedExecutionPayloadResult, InternalError> {
        let authority = self.authority()?;

        self.with_scalar_row_runtime_handle(|row_runtime| {
            materialize_key_stream_into_execution_payload(
                KernelPageMaterializationRequest {
                    authority,
                    plan: request.plan,
                    key_stream: request.key_stream,
                    scan_budget_hint: request.scan_budget_hint,
                    load_order_route_contract: request.load_order_route_contract,
                    capabilities: request.capabilities,
                    consistency: request.consistency,
                    continuation: request.continuation,
                    direction: request.direction,
                },
                row_runtime,
            )
        })
    }

    /// Materialize one ordered key stream into post-access kernel rows.
    fn materialize_key_stream_into_kernel_rows(
        &self,
        request: RuntimePageMaterializationRequest<'_>,
    ) -> Result<KernelRowsExecutionAttempt, InternalError> {
        let authority = self.authority()?;

        self.with_scalar_row_runtime_handle(|row_runtime| {
            materialize_key_stream_into_kernel_rows(
                KernelPageMaterializationRequest {
                    authority,
                    plan: request.plan,
                    key_stream: request.key_stream,
                    scan_budget_hint: request.scan_budget_hint,
                    load_order_route_contract: request.load_order_route_contract,
                    capabilities: request.capabilities,
                    consistency: request.consistency,
                    continuation: request.continuation,
                    direction: request.direction,
                },
                row_runtime,
            )
        })
    }
}

type RetainedSlotLayout = crate::db::executor::terminal::RetainedSlotLayout;
