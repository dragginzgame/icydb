//! Module: db::executor::pipeline::contracts::inputs
//! Responsibility: module-local ownership and contracts for db::executor::pipeline::contracts::inputs.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
        cursor::CursorBoundary,
        data::DataRow,
        direction::Direction,
        executor::pipeline::contracts::{FastPathKeyResult, MaterializedExecutionPayload},
        executor::{
            AccessStreamBindings, EntityAuthority, ExecutableAccess, ExecutionKernel,
            ExecutionPreparation, OrderedKeyStream, OrderedKeyStreamBox,
            ScalarContinuationBindings,
            pipeline::operators::PreparedSqlExecutionProjection,
            projection::{
                PreparedProjectionShape, PreparedSlotProjectionValidation,
                prepare_projection_shape_from_plan,
            },
            route::access_order_satisfied_by_route_contract,
            route::{LoadOrderRouteContract, LoadTerminalFastPathContract},
            scan::{FastStreamRouteKind, FastStreamRouteRequest, execute_fast_stream_route},
            stream::access::TraversalRuntime,
            terminal::{
                RetainedSlotLayout, RetainedSlotRow,
                page::{
                    KernelPageMaterializationRequest, ScalarRowRuntimeHandle,
                    ScalarRowRuntimeState, materialize_key_stream_into_execution_payload,
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
    prepared_shape: Option<PreparedProjectionShape>,
    projection_validation_enabled: bool,
    #[cfg(feature = "sql")]
    sql: Option<PreparedSqlExecutionProjection>,
}

impl PreparedExecutionProjection {
    /// Build one empty projection bundle for execution paths that only need
    /// key-stream resolution and never materialize rows through the shared
    /// scalar page kernel.
    #[must_use]
    pub(in crate::db::executor) const fn empty() -> Self {
        Self {
            retained_slot_layout: None,
            prepared_shape: None,
            projection_validation_enabled: false,
            #[cfg(feature = "sql")]
            sql: None,
        }
    }

    /// Build one executor-owned prepared projection bundle from one validated
    /// plan, compiled predicate, and optional route-owned covering contract.
    pub(in crate::db::executor) fn compile(
        authority: EntityAuthority,
        plan: &AccessPlannedQuery,
        compiled_predicate: Option<&PredicateProgram>,
        projection_materialization: ProjectionMaterializationMode,
        load_terminal_fast_path: Option<&LoadTerminalFastPathContract>,
    ) -> Result<Self, InternalError> {
        let retained_slot_layout = compile_retained_slot_layout(
            plan.projected_slot_mask().len(),
            plan,
            compiled_predicate,
            projection_materialization,
        );
        let projection_validation_enabled = projection_materialization.validate_projection();
        let prepared_shape = (projection_validation_enabled
            || projection_materialization.retain_slot_rows())
        .then(|| prepare_projection_shape_from_plan(authority.row_layout().field_count(), plan));

        #[cfg(feature = "sql")]
        let sql = crate::db::executor::pipeline::operators::prepare_sql_execution_projection(
            authority.row_layout(),
            plan,
            compiled_predicate,
            projection_materialization,
            load_terminal_fast_path,
        )?;

        Ok(Self {
            retained_slot_layout,
            prepared_shape,
            projection_validation_enabled,
            #[cfg(feature = "sql")]
            sql,
        })
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
        self.projection_validation_enabled
            .then_some(())
            .and(self.prepared_shape.as_ref())
    }

    #[must_use]
    pub(in crate::db::executor) const fn prepared_shape(&self) -> Option<&PreparedProjectionShape> {
        self.prepared_shape.as_ref()
    }

    #[cfg(feature = "sql")]
    #[must_use]
    pub(in crate::db::executor) const fn sql(&self) -> Option<&PreparedSqlExecutionProjection> {
        self.sql.as_ref()
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

///
/// StructuralCursorPage
///
/// StructuralCursorPage is the shared scalar page payload emitted by the
/// monomorphic scalar runtime before typed response reconstruction.
/// It preserves post-access row order and the next-page cursor while keeping
/// final entity decode at the outer typed boundary only.
///

pub(in crate::db::executor) struct StructuralCursorPage {
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

pub(in crate::db::executor) enum StructuralCursorPagePayload {
    DataRows(Vec<DataRow>),
    #[cfg(feature = "sql")]
    SlotRows(Vec<RetainedSlotRow>),
}

impl StructuralCursorPage {
    /// Build one structural scalar page from canonical data rows plus cursor state.
    #[must_use]
    pub(in crate::db::executor) const fn new(
        data_rows: Vec<DataRow>,
        next_cursor: Option<crate::db::executor::pipeline::contracts::PageCursor>,
    ) -> Self {
        Self {
            payload: StructuralCursorPagePayload::DataRows(data_rows),
            next_cursor,
        }
    }

    /// Build one structural scalar page while retaining already-decoded slot
    /// rows for SQL-only projection materialization.
    #[cfg(feature = "sql")]
    #[must_use]
    pub(in crate::db::executor) const fn new_with_slot_rows(
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
    pub(in crate::db::executor) const fn row_count(&self) -> usize {
        match &self.payload {
            StructuralCursorPagePayload::DataRows(data_rows) => data_rows.len(),
            #[cfg(feature = "sql")]
            StructuralCursorPagePayload::SlotRows(slot_rows) => slot_rows.len(),
        }
    }

    /// Borrow structural scalar rows without forcing typed response assembly.
    #[must_use]
    pub(in crate::db::executor) const fn data_rows(&self) -> &[DataRow] {
        match &self.payload {
            StructuralCursorPagePayload::DataRows(data_rows) => data_rows.as_slice(),
            #[cfg(feature = "sql")]
            StructuralCursorPagePayload::SlotRows(_) => &[],
        }
    }

    /// Consume one structural scalar page into its single owned payload shape.
    #[must_use]
    #[cfg(any(test, feature = "perf-attribution"))]
    pub(in crate::db::executor) fn into_payload(self) -> StructuralCursorPagePayload {
        self.payload
    }

    /// Consume one structural scalar page into rows plus cursor state.
    #[must_use]
    pub(in crate::db::executor) fn into_parts(
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
    pub(in crate::db::executor) retained_slot_layout: Option<&'a RetainedSlotLayout>,
    pub(in crate::db::executor) prepared_projection_shape: Option<&'a PreparedProjectionShape>,
    pub(in crate::db::executor) prepared_projection_validation:
        Option<&'a PreparedSlotProjectionValidation>,
    #[cfg(feature = "sql")]
    pub(in crate::db::executor) prepared_sql_projection: Option<&'a PreparedSqlExecutionProjection>,
    pub(in crate::db::executor) projection_materialization: ProjectionMaterializationMode,
    pub(in crate::db::executor) fuse_immediate_sql_terminal: bool,
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
    pub(in crate::db::executor) retained_slot_layout: Option<&'a RetainedSlotLayout>,
    pub(in crate::db::executor) prepared_projection_shape: Option<&'a PreparedProjectionShape>,
    pub(in crate::db::executor) prepared_projection_validation:
        Option<&'a PreparedSlotProjectionValidation>,
    #[cfg(feature = "sql")]
    pub(in crate::db::executor) prepared_sql_projection: Option<&'a PreparedSqlExecutionProjection>,
    pub(in crate::db::executor) projection_materialization: ProjectionMaterializationMode,
    pub(in crate::db::executor) fuse_immediate_sql_terminal: bool,
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
    pub(in crate::db::executor) prepared_projection_shape: Option<&'a PreparedProjectionShape>,
    pub(in crate::db::executor) prepared_projection_validation:
        Option<&'a PreparedSlotProjectionValidation>,
    #[cfg(feature = "sql")]
    pub(in crate::db::executor) prepared_sql_projection: Option<&'a PreparedSqlExecutionProjection>,
    pub(in crate::db::executor) projection_materialization: ProjectionMaterializationMode,
    pub(in crate::db::executor) fuse_immediate_sql_terminal: bool,
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
    covering_component_scan: Option<CoveringComponentScanState<'a>>,
}

impl<'a> ExecutionRuntimeAdapter<'a> {
    /// Build one structural runtime adapter from structural runtime authority plus access plan.
    pub(in crate::db::executor) const fn from_runtime_parts(
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
            covering_component_scan: None,
        }
    }

    /// Build one structural runtime adapter for scalar execution paths that
    /// may consume route-owned covering-read component scans.
    pub(in crate::db::executor) const fn from_scalar_runtime_parts(
        access: &'a crate::db::access::AccessPlan<crate::value::Value>,
        runtime: TraversalRuntime,
        store: StoreHandle,
        authority: EntityAuthority,
        covering_component_scan: CoveringComponentScanState<'a>,
    ) -> Self {
        Self {
            runtime,
            access,
            authority: Some(authority),
            scalar_row_runtime: Some(ScalarRowRuntimeState::new(store, authority.row_layout())),
            covering_component_scan: Some(covering_component_scan),
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
            covering_component_scan: None,
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

    /// Attempt the cursorless direct covering-scan short path before generic
    /// key-stream resolution when the same terminal-owned covering contract
    /// can already materialize the final structural page without a key stream.
    pub(in crate::db::executor) fn try_materialize_load_via_direct_covering_scan<'req>(
        &'req self,
        request: DirectCoveringScanMaterializationRequest<'req>,
    ) -> Result<Option<MaterializedExecutionPayloadResult>, InternalError> {
        ExecutionKernel::try_materialize_load_via_direct_covering_scan(
            request,
            self.scalar_row_runtime()?.store(),
            self.covering_component_scan,
        )
    }

    /// Attempt the cursorless row-collector short path and erase the typed page result.
    pub(in crate::db::executor) fn try_materialize_load_via_row_collector<'req>(
        &'req self,
        request: RowCollectorMaterializationRequest<'req>,
    ) -> Result<Option<MaterializedExecutionPayloadResult>, InternalError> {
        // Reuse the adapter-owned structural row-runtime state for the whole
        // query instead of cloning and boxing the same read-only runtime
        // descriptor before every materialization call.
        let scalar_row_runtime = self.scalar_row_runtime()?;
        let mut row_runtime = ScalarRowRuntimeHandle::from_borrowed(scalar_row_runtime);

        ExecutionKernel::try_materialize_load_via_row_collector(
            request,
            &mut row_runtime,
            scalar_row_runtime.store(),
            self.covering_component_scan,
        )
    }

    /// Materialize one ordered key stream into one structural scalar page payload.
    pub(in crate::db::executor) fn materialize_key_stream_into_structural_page(
        &self,
        request: RuntimePageMaterializationRequest<'_>,
    ) -> Result<MaterializedExecutionPayloadResult, InternalError> {
        // Reuse the adapter-owned structural row-runtime state for the whole
        // query instead of cloning and boxing the same read-only runtime
        // descriptor before every materialization call.
        let scalar_row_runtime = self.scalar_row_runtime()?;
        let mut row_runtime = ScalarRowRuntimeHandle::from_borrowed(scalar_row_runtime);

        materialize_key_stream_into_execution_payload(
            KernelPageMaterializationRequest {
                authority: self.authority()?,
                plan: request.plan,
                predicate_slots: request.predicate_slots,
                key_stream: request.key_stream,
                scan_budget_hint: request.scan_budget_hint,
                load_order_route_contract: request.load_order_route_contract,
                validate_projection: request.validate_projection,
                retain_slot_rows: request.retain_slot_rows,
                retained_slot_layout: request.retained_slot_layout,
                prepared_projection_shape: request.prepared_projection_shape,
                prepared_projection_validation: request.prepared_projection_validation,
                #[cfg(feature = "sql")]
                prepared_sql_projection: request.prepared_sql_projection,
                projection_materialization: request.projection_materialization,
                fuse_immediate_sql_terminal: request.fuse_immediate_sql_terminal,
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
    runtime: &'a ExecutionRuntimeAdapter<'a>,
    plan: &'a AccessPlannedQuery,
    stream_bindings: AccessStreamBindings<'a>,
    execution_preparation: &'a ExecutionPreparation,
    projection_materialization: ProjectionMaterializationMode,
    prepared_projection: PreparedExecutionProjection,
    emit_cursor: bool,
    fuse_immediate_sql_terminal: bool,
}

///
/// ExecutionOutputOptions
///
/// ExecutionOutputOptions carries the two remaining scalar execution toggles that
/// are chosen before shared runtime dispatch.
/// This keeps the prepared execution-input constructor under the clippy
/// argument-count threshold without widening the execution contract itself.
///

pub(in crate::db::executor) struct ExecutionOutputOptions {
    emit_cursor: bool,
    fuse_immediate_sql_terminal: bool,
}

impl ExecutionOutputOptions {
    #[must_use]
    pub(in crate::db::executor) const fn new(
        emit_cursor: bool,
        fuse_immediate_sql_terminal: bool,
    ) -> Self {
        Self {
            emit_cursor,
            fuse_immediate_sql_terminal,
        }
    }
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
        flags: ExecutionOutputOptions,
    ) -> Self {
        Self {
            runtime,
            plan,
            stream_bindings,
            execution_preparation,
            projection_materialization,
            prepared_projection,
            emit_cursor: flags.emit_cursor,
            fuse_immediate_sql_terminal: flags.fuse_immediate_sql_terminal,
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
    pub(in crate::db::executor) const fn retained_slot_layout(
        &self,
    ) -> Option<&RetainedSlotLayout> {
        self.prepared_projection.retained_slot_layout()
    }

    /// Borrow prepared projection materialization shape for fused SQL terminal
    /// lanes that emit final rows directly.
    #[must_use]
    pub(in crate::db::executor) const fn prepared_projection_shape(
        &self,
    ) -> Option<&PreparedProjectionShape> {
        self.prepared_projection.prepared_shape()
    }

    /// Borrow one prepared slot-row projection validation bundle when this
    /// execution attempt still requires shared projection validation.
    #[must_use]
    pub(in crate::db::executor) fn prepared_projection_validation(
        &self,
    ) -> Option<&PreparedSlotProjectionValidation> {
        self.prepared_projection.projection_validation()
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

    /// Return whether this execution attempt should assemble one outward
    /// continuation cursor from the materialized structural page.
    #[must_use]
    pub(in crate::db::executor) const fn emit_cursor(&self) -> bool {
        self.emit_cursor
    }

    /// Borrow projection materialization mode for this execution attempt.
    #[must_use]
    pub(in crate::db::executor) const fn projection_materialization(
        &self,
    ) -> ProjectionMaterializationMode {
        self.projection_materialization
    }

    /// Return whether the immediate SQL terminal may bypass the structural page
    /// envelope and emit final rows directly.
    #[must_use]
    pub(in crate::db::executor) const fn fuse_immediate_sql_terminal(&self) -> bool {
        self.fuse_immediate_sql_terminal
    }

    /// Return row-read missing-row policy for this execution attempt.
    #[must_use]
    pub(in crate::db::executor) const fn consistency(&self) -> MissingRowPolicy {
        row_read_consistency_for_plan(self.plan)
    }
}

// Compile the canonical retained-slot layout once per execution shape so
// shared scalar row materialization does not rebuild
// projection/predicate/order/cursor reachability ad hoc at each execution
// boundary.
fn compile_retained_slot_layout(
    field_count: usize,
    plan: &AccessPlannedQuery,
    compiled_predicate: Option<&PredicateProgram>,
    projection_materialization: ProjectionMaterializationMode,
) -> Option<RetainedSlotLayout> {
    let mut required_slots = vec![false; field_count];

    // Phase 1: projection validation and SQL immediate materialization both
    // need one stable slot set for later structural slot reads.
    if projection_materialization.validate_projection()
        || projection_materialization.retain_slot_rows()
    {
        for &slot in plan.projection_referenced_slots() {
            required_slots[slot] = true;
        }
    }

    // Phase 2: residual predicate filtering still runs on retained slot rows
    // before the outer SQL materializer consumes them.
    if plan.has_residual_predicate()
        && let Some(predicate_program) = compiled_predicate
    {
        predicate_program.mark_referenced_slots(&mut required_slots);
    }

    // Phase 3: ordering slots are needed for in-memory ordering and also for
    // cursor boundary assembly on route-ordered load paths.
    if plan.scalar_plan().order.as_ref().is_some()
        && let Some(order_slots) = plan.order_referenced_slots()
    {
        let route_needs_order_slots = !access_order_satisfied_by_route_contract(plan)
            || projection_materialization.emit_cursor();

        if route_needs_order_slots {
            for &slot in order_slots {
                required_slots[slot] = true;
            }
        }
    }

    let required_slots = required_slots
        .into_iter()
        .enumerate()
        .filter_map(|(slot, required)| required.then_some(slot))
        .collect::<Vec<_>>();

    if required_slots.is_empty() && !projection_materialization.retain_slot_rows() {
        return None;
    }

    Some(RetainedSlotLayout::compile(field_count, required_slots))
}
