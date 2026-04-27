//! Module: executor::projection::facade
//! Responsibility: executor-owned structural projection execution coordination.
//! Does not own: SQL DTO shaping, projection label policy, or diagnostic counter storage.
//! Boundary: accepts prepared projection intent and returns structural projected rows.

use crate::{
    db::{
        Db,
        executor::{
            CoveringProjectionMetricsRecorder, ProjectionMaterializationMetricsRecorder,
            SharedPreparedExecutionPlan, SharedPreparedProjectionRuntimeParts,
            pipeline::execute_initial_scalar_retained_slot_page_from_runtime_parts_for_canister,
            projection::{
                MaterializedProjectionRows, project_distinct_structural_projection_page,
                project_structural_projection_page,
                try_execute_covering_projection_rows_for_canister,
            },
            saturating_u32_len,
        },
    },
    error::InternalError,
    traits::CanisterKind,
    value::Value,
};

///
/// StructuralProjectionRequest
///
/// StructuralProjectionRequest carries the generic-free projection execution
/// intent needed after admission/lowering has produced one shared prepared plan.
/// Adapter layers may attach diagnostic callbacks, but executor owns all path
/// selection and row materialization decisions after this boundary.
///

#[cfg(feature = "sql")]
pub(in crate::db) struct StructuralProjectionRequest {
    debug: bool,
    prepared_plan: SharedPreparedExecutionPlan,
    covering_metrics: CoveringProjectionMetricsRecorder,
    materialization_metrics: ProjectionMaterializationMetricsRecorder,
}

#[cfg(feature = "sql")]
impl StructuralProjectionRequest {
    /// Build one structural projection request from adapter-provided runtime
    /// switches and diagnostic callback bundles.
    pub(in crate::db) const fn new(
        debug: bool,
        prepared_plan: SharedPreparedExecutionPlan,
        covering_metrics: CoveringProjectionMetricsRecorder,
        materialization_metrics: ProjectionMaterializationMetricsRecorder,
    ) -> Self {
        Self {
            debug,
            prepared_plan,
            covering_metrics,
            materialization_metrics,
        }
    }
}

///
/// StructuralProjectionResult
///
/// StructuralProjectionResult is the executor-owned transport wrapper for one
/// completed projection execution. It keeps the projected row matrix opaque
/// until an adapter consumes it for response DTO shaping.
///

#[cfg(feature = "sql")]
#[derive(Debug)]
pub(in crate::db) struct StructuralProjectionResult {
    rows: MaterializedProjectionRows,
}

#[cfg(feature = "sql")]
impl StructuralProjectionResult {
    /// Build one structural projection result from executor-materialized rows.
    const fn new(rows: MaterializedProjectionRows) -> Self {
        Self { rows }
    }

    /// Consume this structural result into row values for adapter DTO shaping.
    #[must_use]
    pub(in crate::db) fn into_value_rows(self) -> Vec<Vec<Value>> {
        self.rows.into_value_rows()
    }

    /// Return the number of structural projection rows produced by execution.
    #[must_use]
    pub(in crate::db) fn row_count(&self) -> u32 {
        saturating_u32_len(self.rows.len())
    }
}

/// Execute one prepared structural projection request through the executor-owned
/// projection coordinator.
#[cfg(feature = "sql")]
pub(in crate::db) fn execute_structural_projection_result<C>(
    db: &Db<C>,
    request: StructuralProjectionRequest,
) -> Result<StructuralProjectionResult, InternalError>
where
    C: CanisterKind,
{
    let StructuralProjectionRequest {
        debug,
        prepared_plan,
        covering_metrics,
        materialization_metrics,
    } = request;
    let SharedPreparedProjectionRuntimeParts {
        authority,
        prepared_projection_shape,
        scalar_runtime,
    } = prepared_plan.into_projection_runtime_parts()?;
    let distinct = scalar_runtime.plan_core.plan().scalar_plan().distinct;
    let distinct_plan = distinct.then(|| scalar_runtime.plan_core.plan().clone());
    let scalar_runtime = if distinct {
        scalar_runtime.into_scalar_page_suppressed()
    } else {
        scalar_runtime
    };
    let execution_plan = scalar_runtime.plan_core.plan();

    // Phase 1: choose the covering projection lane only for non-DISTINCT
    // requests. DISTINCT must see final projected rows in scalar execution order
    // before executor-owned deduplication and windowing.
    if !distinct
        && let Some(projected) = try_execute_covering_projection_rows_for_canister(
            db,
            authority,
            execution_plan,
            covering_metrics,
        )?
    {
        let rows = MaterializedProjectionRows::from_value_rows(projected.into_value_rows());

        return Ok(StructuralProjectionResult::new(rows));
    }

    // Phase 2: execute the canonical scalar retained-slot path and let the
    // projection materializer choose slot-row, data-row, or scalar fallback
    // shaping behind the executor boundary.
    let row_layout = authority.row_layout();
    let prepared_projection = prepared_projection_shape.as_deref().ok_or_else(|| {
        InternalError::query_executor_invariant(
            "structural projection runtime requires one frozen projection shape",
        )
    })?;
    let page = execute_initial_scalar_retained_slot_page_from_runtime_parts_for_canister(
        db,
        debug,
        scalar_runtime,
        distinct,
    )?;

    let rows = if distinct {
        project_distinct_structural_projection_page(
            row_layout,
            prepared_projection,
            distinct_plan.as_ref().ok_or_else(|| {
                InternalError::query_executor_invariant(
                    "distinct projection materialization requires logical plan metadata",
                )
            })?,
            page,
            materialization_metrics,
        )?
    } else {
        project_structural_projection_page(
            row_layout,
            prepared_projection,
            page,
            materialization_metrics,
        )?
    };

    Ok(StructuralProjectionResult::new(rows))
}
