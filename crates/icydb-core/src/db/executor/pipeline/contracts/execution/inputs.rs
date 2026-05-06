//! Module: db::executor::pipeline::contracts::inputs
//! Defines prepared execution inputs shared by scalar pipeline entrypoints.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use std::sync::Arc;

use crate::{
    db::{
        access::ExecutableAccessPlan,
        cursor::CursorBoundary,
        data::DataRow,
        direction::Direction,
        executor::{
            AccessStreamBindings, EntityAuthority, ExecutionPreparation, OrderedKeyStreamBox,
            ScalarContinuationContext,
            pipeline::{
                contracts::ScalarMaterializationCapabilities,
                runtime::{ExecutionRuntimeAdapter, compile_retained_slot_layout_for_mode},
            },
            projection::PreparedSlotProjectionValidation,
            route::LoadOrderRouteContract,
            terminal::{RetainedSlotLayout, RetainedSlotRow},
            traversal::row_read_consistency_for_plan,
        },
        predicate::MissingRowPolicy,
        query::plan::{AccessPlannedQuery, EffectiveRuntimeFilterProgram},
    },
    value::Value,
};

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
                &authority,
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

pub(in crate::db::executor) enum StructuralCursorPagePayload {
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

    /// Dispatch one structural projection consumer onto the page's concrete row
    /// payload without exposing the payload enum to the session boundary.
    #[cfg(feature = "sql")]
    pub(in crate::db) fn consume_projection_rows<T>(
        self,
        handle_slot_rows: impl FnOnce(Vec<RetainedSlotRow>) -> T,
        handle_data_rows: impl FnOnce(Vec<DataRow>) -> T,
    ) -> T {
        match self.payload {
            StructuralCursorPagePayload::DataRows(data_rows) => handle_data_rows(data_rows),
            StructuralCursorPagePayload::SlotRows(slot_rows) => handle_slot_rows(slot_rows),
        }
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
    pub(in crate::db::executor) key_stream: &'a mut OrderedKeyStreamBox,
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
    pub(in crate::db::executor) capabilities: ScalarMaterializationCapabilities<'a>,
    pub(in crate::db::executor) key_stream: &'a mut OrderedKeyStreamBox,
}

///
/// PreparedExecutionInputParts
///
/// PreparedExecutionInputParts bundles the constructor-only inputs for one
/// prepared scalar execution attempt. It keeps the runtime, access,
/// execution-preparation, projection, and cursor handoff explicit without
/// growing the `ExecutionInputs::new_prepared` signature.
///

pub(in crate::db::executor) struct PreparedExecutionInputParts<'a> {
    pub(in crate::db::executor) runtime: &'a ExecutionRuntimeAdapter,
    pub(in crate::db::executor) plan: &'a AccessPlannedQuery,
    pub(in crate::db::executor) executable_access: ExecutableAccessPlan<'a, Value>,
    pub(in crate::db::executor) stream_bindings: AccessStreamBindings<'a>,
    pub(in crate::db::executor) execution_preparation: &'a ExecutionPreparation,
    pub(in crate::db::executor) projection_materialization: ProjectionMaterializationMode,
    pub(in crate::db::executor) prepared_projection: PreparedExecutionProjection,
    pub(in crate::db::executor) emit_cursor: bool,
}

///
/// ExecutionInputs
///
/// Shared immutable execution inputs for one load execution attempt.
/// Keeps shared execution code monomorphic by carrying plan shape, runtime
/// bindings, and the pre-resolved runtime adapter instead of typed entity params.
///

pub(in crate::db::executor) struct ExecutionInputs<'a> {
    runtime: &'a ExecutionRuntimeAdapter,
    plan: &'a AccessPlannedQuery,
    executable_access: ExecutableAccessPlan<'a, Value>,
    stream_bindings: AccessStreamBindings<'a>,
    execution_preparation: &'a ExecutionPreparation,
    residual_filter_program: Option<&'a EffectiveRuntimeFilterProgram>,
    prepared_projection: PreparedExecutionProjection,
    retain_slot_rows: bool,
    emit_cursor: bool,
    consistency: MissingRowPolicy,
}

impl<'a> ExecutionInputs<'a> {
    /// Construct one scalar execution-input payload from already-prepared
    /// execution and projection state.
    pub(in crate::db::executor) fn new_prepared(parts: PreparedExecutionInputParts<'a>) -> Self {
        let PreparedExecutionInputParts {
            runtime,
            plan,
            executable_access,
            stream_bindings,
            execution_preparation,
            projection_materialization,
            prepared_projection,
            emit_cursor,
        } = parts;

        Self {
            runtime,
            plan,
            executable_access,
            stream_bindings,
            execution_preparation,
            residual_filter_program: execution_preparation.effective_runtime_filter_program(),
            prepared_projection,
            retain_slot_rows: projection_materialization.retain_slot_rows(),
            emit_cursor,
            consistency: row_read_consistency_for_plan(plan),
        }
    }

    /// Borrow the resolved runtime adapter for this execution attempt.
    #[must_use]
    pub(in crate::db::executor) const fn runtime(&self) -> &ExecutionRuntimeAdapter {
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

    /// Borrow the executable access shape prepared for this execution attempt.
    #[must_use]
    pub(in crate::db::executor) const fn executable_access(
        &self,
    ) -> &ExecutableAccessPlan<'a, Value> {
        &self.executable_access
    }

    /// Borrow precomputed execution-preparation payloads.
    #[must_use]
    pub(in crate::db::executor) const fn execution_preparation(&self) -> &ExecutionPreparation {
        self.execution_preparation
    }

    /// Borrow the compiled residual filter program prepared for this execution
    /// attempt, if one exists.
    #[must_use]
    pub(in crate::db::executor) const fn residual_filter_program(
        &self,
    ) -> Option<&EffectiveRuntimeFilterProgram> {
        self.residual_filter_program
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
        self.consistency
    }
}
