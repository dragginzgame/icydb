//! Module: db::executor::pipeline::contracts::inputs
//! Defines prepared execution inputs shared by scalar pipeline entrypoints.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use std::sync::Arc;

use crate::{
    db::{
        cursor::CursorBoundary,
        data::DataRow,
        direction::Direction,
        executor::pipeline::contracts::{FastPathKeyResult, MaterializedExecutionPayload},
        executor::{
            AccessStreamBindings, EntityAuthority, ExecutableAccess, ExecutionKernel,
            ExecutionPlan, ExecutionPreparation, OrderedKeyStream, OrderedKeyStreamBox,
            ScalarContinuationContext,
            projection::PreparedSlotProjectionValidation,
            route::LoadOrderRouteContract,
            route::access_order_satisfied_by_route_contract,
            scan::{FastStreamRouteKind, FastStreamRouteRequest, execute_fast_stream_route},
            stream::access::TraversalRuntime,
            terminal::{
                RetainedSlotLayout, RetainedSlotRow,
                page::{
                    KernelPageMaterializationRequest, ScalarMaterializationCapabilities,
                    ScalarRowRuntimeHandle, ScalarRowRuntimeState,
                    materialize_key_stream_into_execution_payload,
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
    model::{
        entity::{EntityModel, resolve_field_slot},
        index::IndexKeyItemsRef,
    },
};

type MaterializedExecutionPayloadResult = (MaterializedExecutionPayload, usize, usize);

///
/// PreparedExecutionProjection
///
/// PreparedExecutionProjection is the executor-owned fixed projection state
/// recovered once before execution begins. It freezes only the projection
/// metadata that the chosen execution lane actually consumes so the hot path
/// does not rebuild unused validation shape from the logical plan.
///

pub(in crate::db::executor) struct PreparedExecutionProjection {
    retained_slot_layout: Option<RetainedSlotLayout>,
    projection_validation: Option<Arc<PreparedSlotProjectionValidation>>,
}

impl PreparedExecutionProjection {
    /// Build one empty projection bundle for execution paths that only need
    /// key-stream resolution and never materialize rows through the shared
    /// scalar page kernel.
    #[must_use]
    pub(in crate::db::executor) const fn empty() -> Self {
        Self {
            retained_slot_layout: None,
            projection_validation: None,
        }
    }

    /// Build one executor-owned prepared projection bundle from one validated
    /// plan, compiled predicate, and optional route-owned covering contract.
    pub(in crate::db::executor) fn compile(
        authority: EntityAuthority,
        plan: &AccessPlannedQuery,
        prepared_projection_validation: Option<Arc<PreparedSlotProjectionValidation>>,
        prepared_retained_slot_layout: Option<RetainedSlotLayout>,
        projection_materialization: ProjectionMaterializationMode,
        cursor_emission: CursorEmissionMode,
    ) -> Self {
        // Phase 1: projection validation is only meaningful when the frozen
        // projection is not already model identity. Identity projections would
        // immediately no-op inside the validator, so skip building projection
        // validation state and projection-driven retained slots for that case.
        let projection_validation_enabled = projection_materialization.validate_projection()
            && !plan.projection_is_model_identity();

        // Phase 2: build prepared projection validation only when the shared
        // validation pass will actually consume it. Retained-slot row paths
        // keep their slot layout separately and do not read the prepared
        // projection shape back through this contract.
        let projection_validation = if projection_validation_enabled {
            Some(prepared_projection_validation.expect(
                "shared scalar execution requires one frozen prepared projection validation shape",
            ))
        } else {
            None
        };

        // Phase 3: reuse one frozen retained-slot layout whenever the
        // prepared-plan boundary already compiled the canonical scalar
        // execution shape. Non-prepared callers still compile on demand.
        let retained_slot_layout = prepared_retained_slot_layout.or_else(|| {
            compile_retained_slot_layout_for_mode(
                authority.model(),
                plan,
                projection_materialization,
                cursor_emission,
            )
        });
        Self {
            retained_slot_layout,
            projection_validation,
        }
    }

    #[must_use]
    pub(in crate::db::executor) const fn retained_slot_layout(
        &self,
    ) -> Option<&RetainedSlotLayout> {
        self.retained_slot_layout.as_ref()
    }

    #[must_use]
    pub(in crate::db::executor) fn projection_validation(
        &self,
    ) -> Option<&PreparedSlotProjectionValidation> {
        self.projection_validation.as_deref()
    }

    #[must_use]
    pub(in crate::db::executor) const fn projection_validation_enabled(&self) -> bool {
        self.projection_validation.is_some()
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

pub(in crate::db) struct StructuralCursorPage {
    payload: StructuralCursorPagePayload,
    next_cursor: Option<crate::db::executor::pipeline::contracts::PageCursor>,
}

///
/// StructuralCursorPagePayload
///
/// StructuralCursorPagePayload keeps the scalar page on exactly one payload
/// shape at a time instead of carrying several mutually exclusive vectors in
/// the same envelope.
///

pub(in crate::db) enum StructuralCursorPagePayload {
    DataRows(Vec<DataRow>),
    #[cfg(feature = "sql")]
    SlotRows(Vec<RetainedSlotRow>),
}

impl StructuralCursorPage {
    /// Build one structural scalar page from canonical data rows plus cursor state.
    #[must_use]
    pub(in crate::db) const fn new(
        data_rows: Vec<DataRow>,
        next_cursor: Option<crate::db::executor::pipeline::contracts::PageCursor>,
    ) -> Self {
        Self {
            payload: StructuralCursorPagePayload::DataRows(data_rows),
            next_cursor,
        }
    }

    /// Build one structural scalar page while retaining already-decoded slot
    /// rows for one structural consumer over the executor boundary.
    #[cfg(feature = "sql")]
    #[must_use]
    pub(in crate::db) const fn new_with_slot_rows(
        slot_rows: Vec<RetainedSlotRow>,
        next_cursor: Option<crate::db::executor::pipeline::contracts::PageCursor>,
    ) -> Self {
        Self {
            payload: StructuralCursorPagePayload::SlotRows(slot_rows),
            next_cursor,
        }
    }

    /// Return the number of structural rows carried by this page.
    #[must_use]
    pub(in crate::db) const fn row_count(&self) -> usize {
        match &self.payload {
            StructuralCursorPagePayload::DataRows(data_rows) => data_rows.len(),
            #[cfg(feature = "sql")]
            StructuralCursorPagePayload::SlotRows(slot_rows) => slot_rows.len(),
        }
    }

    /// Borrow structural scalar rows without forcing typed response assembly.
    #[must_use]
    pub(in crate::db) const fn data_rows(&self) -> &[DataRow] {
        match &self.payload {
            StructuralCursorPagePayload::DataRows(data_rows) => data_rows.as_slice(),
            #[cfg(feature = "sql")]
            StructuralCursorPagePayload::SlotRows(_) => &[],
        }
    }

    /// Consume one structural scalar page into its single owned payload shape.
    #[must_use]
    pub(in crate::db) fn into_payload(self) -> StructuralCursorPagePayload {
        self.payload
    }

    /// Consume one structural scalar page into rows plus cursor state.
    #[must_use]
    pub(in crate::db) fn into_parts(
        self,
    ) -> (
        Vec<DataRow>,
        Option<crate::db::executor::pipeline::contracts::PageCursor>,
    ) {
        let data_rows = match self.payload {
            StructuralCursorPagePayload::DataRows(data_rows) => data_rows,
            #[cfg(feature = "sql")]
            StructuralCursorPagePayload::SlotRows(_) => Vec::new(),
        };

        (data_rows, self.next_cursor)
    }
}

///
/// CursorEmissionMode
///
/// Cursor emission contract for structural page materialization.
/// Shared scalar execution uses this to keep no-cursor structural consumers
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
/// ProjectionMaterializationMode keeps structural projection-retention
/// behavior explicit at the execution boundary instead of scattering multiple
/// interdependent bool flags across kernel/runtime contracts.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) enum ProjectionMaterializationMode {
    None,
    SharedValidation,
    RetainSlotRows,
}

impl ProjectionMaterializationMode {
    /// Return whether this execution attempt still requires the shared
    /// projection-validation pass before surface-owned materialization.
    #[must_use]
    pub(in crate::db::executor) const fn validate_projection(self) -> bool {
        matches!(self, Self::SharedValidation)
    }

    /// Return whether this execution attempt should retain decoded slot rows
    /// for one outer surface-owned projection materialization step.
    #[must_use]
    pub(in crate::db::executor) const fn retain_slot_rows(self) -> bool {
        matches!(self, Self::RetainSlotRows)
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
    pub(in crate::db::executor) key_stream: &'a mut dyn OrderedKeyStream,
    pub(in crate::db::executor) scan_budget_hint: Option<usize>,
    pub(in crate::db::executor) load_order_route_contract: LoadOrderRouteContract,
    pub(in crate::db::executor) capabilities: ScalarMaterializationCapabilities<'a>,
    pub(in crate::db::executor) consistency: MissingRowPolicy,
    pub(in crate::db::executor) continuation: &'a ScalarContinuationContext,
    pub(in crate::db::executor) direction: Direction,
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
    pub(in crate::db::executor) continuation: &'a ScalarContinuationContext,
    pub(in crate::db::executor) cursor_boundary: Option<&'a CursorBoundary>,
    pub(in crate::db::executor) predicate_slots: Option<&'a PredicateProgram>,
    pub(in crate::db::executor) validate_projection: bool,
    pub(in crate::db::executor) retain_slot_rows: bool,
    pub(in crate::db::executor) retained_slot_layout: Option<&'a RetainedSlotLayout>,
    pub(in crate::db::executor) prepared_projection_validation:
        Option<&'a PreparedSlotProjectionValidation>,
    pub(in crate::db::executor) key_stream: &'a mut dyn OrderedKeyStream,
}

///
/// ExecutionMaterializationContract
///
/// ExecutionMaterializationContract captures the execution-input fields shared
/// by the row-collector and runtime-page materialization requests.
/// `ExecutionInputs` builds this once so the two outward request shapes do not
/// re-spell the same predicate/projection/retained-slot contract fields.
///

struct ExecutionMaterializationContract<'a> {
    plan: &'a AccessPlannedQuery,
    predicate_slots: Option<&'a PredicateProgram>,
    scan_budget_hint: Option<usize>,
    load_order_route_contract: LoadOrderRouteContract,
    validate_projection: bool,
    retain_slot_rows: bool,
    retained_slot_layout: Option<&'a RetainedSlotLayout>,
    prepared_projection_validation: Option<&'a PreparedSlotProjectionValidation>,
}

impl<'a> ExecutionMaterializationContract<'a> {
    // Build the cursorless row-collector materialization request from one
    // already-aligned scalar materialization contract.
    fn row_collector_request(
        self,
        continuation: &'a ScalarContinuationContext,
        key_stream: &'a mut dyn OrderedKeyStream,
    ) -> RowCollectorMaterializationRequest<'a> {
        RowCollectorMaterializationRequest {
            plan: self.plan,
            scan_budget_hint: self.scan_budget_hint,
            load_order_route_contract: self.load_order_route_contract,
            continuation,
            cursor_boundary: continuation.post_access_cursor_boundary(),
            predicate_slots: self.predicate_slots,
            validate_projection: self.validate_projection,
            retain_slot_rows: self.retain_slot_rows,
            retained_slot_layout: self.retained_slot_layout,
            prepared_projection_validation: self.prepared_projection_validation,
            key_stream,
        }
    }

    // Build the canonical scalar page materialization request from one
    // already-aligned scalar materialization contract.
    fn runtime_page_request(
        self,
        emit_cursor: bool,
        consistency: MissingRowPolicy,
        continuation: &'a ScalarContinuationContext,
        direction: Direction,
        key_stream: &'a mut dyn OrderedKeyStream,
    ) -> RuntimePageMaterializationRequest<'a> {
        RuntimePageMaterializationRequest {
            plan: self.plan,
            key_stream,
            scan_budget_hint: self.scan_budget_hint,
            load_order_route_contract: self.load_order_route_contract,
            capabilities: ScalarMaterializationCapabilities {
                predicate_slots: self.predicate_slots,
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

pub(in crate::db::executor) struct ExecutionRuntimeAdapter<'a> {
    runtime: TraversalRuntime,
    access: &'a crate::db::access::AccessPlan<crate::value::Value>,
    authority: Option<EntityAuthority>,
    scalar_row_runtime: Option<ScalarRowRuntimeState>,
}

impl<'a> ExecutionRuntimeAdapter<'a> {
    /// Build one structural runtime adapter for scalar execution paths.
    pub(in crate::db::executor) const fn from_scalar_runtime_parts(
        access: &'a crate::db::access::AccessPlan<crate::value::Value>,
        runtime: TraversalRuntime,
        store: StoreHandle,
        authority: EntityAuthority,
    ) -> Self {
        Self {
            runtime,
            access,
            authority: Some(authority),
            scalar_row_runtime: Some(ScalarRowRuntimeState::new(store, authority.row_layout())),
        }
    }

    /// Build one stream-only runtime adapter for key-stream resolution paths
    /// that never materialize scalar rows.
    pub(in crate::db::executor) const fn from_stream_runtime_parts(
        access: &'a crate::db::access::AccessPlan<crate::value::Value>,
        runtime: TraversalRuntime,
    ) -> Self {
        Self {
            runtime,
            access,
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
    fn with_scalar_row_runtime_handle<T>(
        &'a self,
        run: impl FnOnce(&mut ScalarRowRuntimeHandle<'a>) -> Result<T, InternalError>,
    ) -> Result<T, InternalError> {
        let scalar_row_runtime = self.scalar_row_runtime()?;
        let mut row_runtime = ScalarRowRuntimeHandle::from_borrowed(scalar_row_runtime);

        run(&mut row_runtime)
    }

    /// Resolve one primary-key fast path when the route is already verified.
    pub(in crate::db::executor) fn try_execute_pk_order_stream(
        &self,
        plan: &AccessPlannedQuery,
        direction: Direction,
        physical_fetch_hint: Option<usize>,
    ) -> Result<Option<FastPathKeyResult>, InternalError> {
        execute_fast_stream_route(
            &self.runtime,
            FastStreamRouteKind::PrimaryKey,
            FastStreamRouteRequest::PrimaryKey {
                plan,
                stream_direction: direction,
                probe_fetch_hint: physical_fetch_hint,
            },
        )
    }

    /// Resolve one verified secondary-prefix fast path.
    pub(in crate::db::executor) fn try_execute_secondary_index_order_stream(
        &self,
        plan: &AccessPlannedQuery,
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
        index_range_spec: Option<&crate::db::executor::LoweredIndexRangeSpec>,
        continuation: crate::db::executor::AccessScanContinuationInput<'_>,
        fetch: usize,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<Option<FastPathKeyResult>, InternalError> {
        execute_fast_stream_route(
            &self.runtime,
            FastStreamRouteKind::IndexRangeLimitPushdown,
            FastStreamRouteRequest::IndexRangeLimitPushdown {
                plan,
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
        bindings: AccessStreamBindings<'_>,
        physical_fetch_hint: Option<usize>,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
        preserve_leaf_index_order: bool,
    ) -> Result<OrderedKeyStreamBox, InternalError> {
        let access = if preserve_leaf_index_order {
            ExecutableAccess::new_with_preserved_leaf_index_order(
                self.access,
                bindings,
                physical_fetch_hint,
                index_predicate_execution,
            )
        } else {
            ExecutableAccess::new(
                self.access,
                bindings,
                physical_fetch_hint,
                index_predicate_execution,
            )
        };

        self.runtime.ordered_key_stream_from_runtime_access(access)
    }

    /// Attempt the cursorless row-collector short path and erase the typed page result.
    pub(in crate::db::executor) fn try_materialize_load_via_row_collector<'req>(
        &'req self,
        request: RowCollectorMaterializationRequest<'req>,
    ) -> Result<Option<MaterializedExecutionPayloadResult>, InternalError> {
        self.with_scalar_row_runtime_handle(|row_runtime| {
            ExecutionKernel::try_materialize_load_via_row_collector(request, row_runtime)
        })
    }

    /// Materialize one ordered key stream into one structural scalar page payload.
    pub(in crate::db::executor) fn materialize_key_stream_into_structural_page(
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
}

///
/// ExecutionInputs
///
/// Shared immutable execution inputs for one load execution attempt.
/// Keeps shared execution code monomorphic by carrying plan shape, runtime
/// bindings, and the pre-resolved runtime adapter instead of typed entity params.
///

pub(in crate::db::executor) struct ExecutionInputs<'a> {
    runtime: &'a ExecutionRuntimeAdapter<'a>,
    plan: &'a AccessPlannedQuery,
    stream_bindings: AccessStreamBindings<'a>,
    execution_preparation: &'a ExecutionPreparation,
    prepared_projection: PreparedExecutionProjection,
    retain_slot_rows: bool,
    emit_cursor: bool,
}

impl<'a> ExecutionInputs<'a> {
    /// Construct one scalar execution-input payload from already-prepared
    /// execution and projection state.
    pub(in crate::db::executor) const fn new_prepared(
        runtime: &'a ExecutionRuntimeAdapter<'a>,
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
            prepared_projection,
            retain_slot_rows: projection_materialization.retain_slot_rows(),
            emit_cursor,
        }
    }

    /// Borrow the resolved runtime adapter for this execution attempt.
    #[must_use]
    pub(in crate::db::executor) const fn runtime(&self) -> &ExecutionRuntimeAdapter<'a> {
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
        self.prepared_projection.projection_validation_enabled()
    }

    /// Return whether this execution attempt should retain decoded slot rows
    /// for one outer surface-owned projection materialization step.
    #[must_use]
    pub(in crate::db::executor) const fn retain_slot_rows(&self) -> bool {
        self.retain_slot_rows
    }

    /// Borrow the precomputed retained-slot layout when this execution shape
    /// keeps slot rows for one outer structural consumer.
    #[must_use]
    pub(in crate::db::executor) const fn retained_slot_layout(
        &self,
    ) -> Option<&RetainedSlotLayout> {
        self.prepared_projection.retained_slot_layout()
    }

    /// Borrow one prepared slot-row projection validation bundle when this
    /// execution attempt still requires shared projection validation.
    #[must_use]
    pub(in crate::db::executor) fn prepared_projection_validation(
        &self,
    ) -> Option<&PreparedSlotProjectionValidation> {
        self.prepared_projection.projection_validation()
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

    // Build the shared materialization contract once so the two outward
    // request shapes stay aligned on predicate/projection/retained-slot wiring.
    fn materialization_contract<'req>(
        &'req self,
        route_plan: &ExecutionPlan,
    ) -> ExecutionMaterializationContract<'req> {
        ExecutionMaterializationContract {
            plan: self.plan(),
            predicate_slots: self.execution_preparation().compiled_predicate(),
            scan_budget_hint: route_plan.scan_hints.load_scan_budget_hint,
            load_order_route_contract: route_plan.load_order_route_contract(),
            validate_projection: self.validate_projection(),
            retain_slot_rows: self.retain_slot_rows(),
            retained_slot_layout: self.retained_slot_layout(),
            prepared_projection_validation: self.prepared_projection_validation(),
        }
    }

    /// Build the cursorless row-collector materialization request owned by
    /// this execution-input boundary so kernel callers do not reconstruct the
    /// same projection and predicate contract ad hoc.
    pub(in crate::db::executor) fn row_collector_materialization_request<'req>(
        &'req self,
        route_plan: &ExecutionPlan,
        continuation: &'req ScalarContinuationContext,
        key_stream: &'req mut dyn OrderedKeyStream,
    ) -> RowCollectorMaterializationRequest<'req> {
        self.materialization_contract(route_plan)
            .row_collector_request(continuation, key_stream)
    }

    /// Build the canonical scalar page materialization request from the
    /// already-prepared execution inputs and route-owned scan hints.
    pub(in crate::db::executor) fn runtime_page_materialization_request<'req>(
        &'req self,
        route_plan: &ExecutionPlan,
        continuation: &'req ScalarContinuationContext,
        key_stream: &'req mut dyn OrderedKeyStream,
    ) -> RuntimePageMaterializationRequest<'req> {
        self.materialization_contract(route_plan)
            .runtime_page_request(
                self.emit_cursor(),
                self.consistency(),
                continuation,
                route_plan.direction(),
                key_stream,
            )
    }
}

/// Compile the canonical retained-slot layout for one explicit scalar
/// projection and cursor-emission mode pair.
pub(in crate::db::executor) fn compile_retained_slot_layout_for_mode(
    model: &EntityModel,
    plan: &AccessPlannedQuery,
    projection_materialization: ProjectionMaterializationMode,
    cursor_emission: CursorEmissionMode,
) -> Option<RetainedSlotLayout> {
    let projection_validation_enabled =
        projection_materialization.validate_projection() && !plan.projection_is_model_identity();
    let retain_slot_rows = projection_materialization.retain_slot_rows();

    compile_retained_slot_layout(
        model,
        plan,
        plan.effective_runtime_compiled_predicate(),
        projection_validation_enabled,
        retain_slot_rows,
        cursor_emission,
    )
}

// Compile the canonical retained-slot layout once per execution shape so
// shared scalar row materialization does not rebuild
// projection/predicate/order/cursor reachability ad hoc at each execution
// boundary.
fn compile_retained_slot_layout(
    model: &EntityModel,
    plan: &AccessPlannedQuery,
    compiled_predicate: Option<&PredicateProgram>,
    projection_validation_enabled: bool,
    retain_slot_rows: bool,
    cursor_emission: CursorEmissionMode,
) -> Option<RetainedSlotLayout> {
    let mut required_slots = RetainedSlotRequirements::new(model.fields().len());

    // Phase 1: projection validation and retained-slot materialization both
    // need one stable slot set for later structural slot reads.
    if projection_validation_enabled || retain_slot_rows {
        required_slots.mark_slots(plan.projection_referenced_slots().iter().copied());
    }

    // Phase 2: residual predicate filtering still runs on retained slot rows
    // before the outer projection materializer consumes them.
    if plan.has_residual_predicate()
        && let Some(predicate_program) = compiled_predicate
    {
        predicate_program.mark_referenced_slots(required_slots.flags_mut());
    }

    // Phase 3: ordering slots are needed for in-memory ordering and also for
    // cursor boundary assembly on route-ordered load paths.
    if plan.scalar_plan().order.as_ref().is_some()
        && let Some(order_slots) = plan.order_referenced_slots()
    {
        let route_needs_order_slots =
            !access_order_satisfied_by_route_contract(plan) || cursor_emission.enabled();

        if route_needs_order_slots {
            required_slots.mark_slots(order_slots.iter().copied());
        }
    }

    // Phase 4: index-range cursor anchors need the complete index key item
    // slot set, not only the outward order slots. Model-identity projections
    // no longer force shared validation state, so keep these slots explicit
    // for cursor-emitting index-range paths.
    if cursor_emission.enabled()
        && let Some((index, _, _, _)) = plan.access.as_index_range_path()
    {
        required_slots.mark_index_key_item_slots(model, index.key_items());
    }

    let required_slots = required_slots.into_slots();

    if required_slots.is_empty() && !retain_slot_rows {
        return None;
    }

    Some(RetainedSlotLayout::compile(
        model.fields().len(),
        required_slots,
    ))
}

///
/// RetainedSlotRequirements
///
/// RetainedSlotRequirements collects the canonical retained-slot requirement
/// set for one scalar execution shape.
/// It exists so projection, predicate, ordering, and index-anchor slot needs
/// can all contribute through one owner-local boundary instead of mutating the
/// raw bitset directly in several separate loops.
///

struct RetainedSlotRequirements {
    flags: Vec<bool>,
}

impl RetainedSlotRequirements {
    // Build one empty retained-slot requirement set sized to the model field
    // count for the current execution shape.
    fn new(field_count: usize) -> Self {
        Self {
            flags: vec![false; field_count],
        }
    }

    // Borrow the raw bitset when an existing helper already knows how to mark
    // referenced slots in place.
    fn flags_mut(&mut self) -> &mut [bool] {
        self.flags.as_mut_slice()
    }

    // Mark one iterator of already-resolved field slots as required.
    fn mark_slots(&mut self, slots: impl IntoIterator<Item = usize>) {
        for slot in slots {
            self.flags[slot] = true;
        }
    }

    // Mark the slots needed to reconstruct index-range cursor anchors from the
    // full index key item set instead of only the outward order fields.
    fn mark_index_key_item_slots(&mut self, model: &EntityModel, key_items: IndexKeyItemsRef) {
        match key_items {
            IndexKeyItemsRef::Fields(fields) => {
                for field in fields {
                    if let Some(slot) = resolve_field_slot(model, field) {
                        self.flags[slot] = true;
                    }
                }
            }
            IndexKeyItemsRef::Items(items) => {
                for key_item in items {
                    if let Some(slot) = resolve_field_slot(model, key_item.field()) {
                        self.flags[slot] = true;
                    }
                }
            }
        }
    }

    // Consume the requirement set into the final sorted retained-slot vector
    // used by the compiled layout contract.
    fn into_slots(self) -> Vec<usize> {
        self.flags
            .into_iter()
            .enumerate()
            .filter_map(|(slot, required)| required.then_some(slot))
            .collect()
    }
}
