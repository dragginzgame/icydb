//! Module: db::session::sql::projection::runtime
//! Responsibility: session-owned SQL projection payload handoff over
//! executor-owned structural projection rows.
//! Does not own: shared projection validation, projection execution, or scalar
//! execution mechanics.
//! Boundary: consumes structural projection results from the executor and
//! performs SQL response-payload shaping above that boundary.

mod materialize;
#[cfg(all(feature = "sql", test))]
mod tests;

#[cfg(any(test, feature = "diagnostics"))]
use crate::db::executor::{
    CoveringProjectionMetricsRecorder, ProjectionMaterializationMetricsRecorder,
};
#[cfg(feature = "sql")]
use crate::{
    db::{
        Db,
        executor::{
            SharedPreparedExecutionPlan, SharedPreparedProjectionRuntimeParts,
            pipeline::execute_initial_scalar_retained_slot_page_for_canister,
            project_distinct_structural_projection_page, project_structural_projection_page,
            try_execute_covering_projection_rows_for_canister,
        },
        query::plan::LogicalPlan,
    },
    error::InternalError,
    traits::CanisterKind,
    value::Value,
};

#[cfg(all(test, not(feature = "diagnostics")))]
pub(crate) use crate::db::session::sql::projection::runtime::materialize::{
    SqlProjectionMaterializationMetrics, with_sql_projection_materialization_metrics,
};
#[cfg(feature = "diagnostics")]
pub use crate::db::session::sql::projection::runtime::materialize::{
    SqlProjectionMaterializationMetrics, with_sql_projection_materialization_metrics,
};

///
/// SqlProjectionRows
///
/// Generic-free SQL projection row payload emitted after structural projection
/// execution hands rows back to the SQL adapter.
/// Keeps SQL row materialization out of typed `ProjectionResponse<E>` so SQL
/// execution can render value rows without reintroducing entity-specific ids.
///

#[cfg(feature = "sql")]
#[derive(Debug)]
pub(in crate::db) struct SqlProjectionRows {
    rows: Vec<Vec<Value>>,
    row_count: u32,
}

#[cfg(feature = "sql")]
impl SqlProjectionRows {
    #[must_use]
    pub(in crate::db) const fn new(rows: Vec<Vec<Value>>, row_count: u32) -> Self {
        Self { rows, row_count }
    }

    #[must_use]
    pub(in crate::db) fn into_parts(self) -> (Vec<Vec<Value>>, u32) {
        (self.rows, self.row_count)
    }
}

#[cfg(all(feature = "sql", feature = "diagnostics"))]
pub(in crate::db) fn current_pure_covering_decode_local_instructions() -> u64 {
    crate::db::executor::current_pure_covering_decode_local_instructions()
}

#[cfg(all(feature = "sql", feature = "diagnostics"))]
pub(in crate::db) fn current_pure_covering_row_assembly_local_instructions() -> u64 {
    crate::db::executor::current_pure_covering_row_assembly_local_instructions()
}

#[cfg(any(test, feature = "diagnostics"))]
fn covering_projection_metrics_recorder() -> CoveringProjectionMetricsRecorder {
    CoveringProjectionMetricsRecorder::new(
        materialize::record_sql_projection_hybrid_covering_path_hit,
        materialize::record_sql_projection_hybrid_covering_index_field_access,
        materialize::record_sql_projection_hybrid_covering_row_field_access,
    )
}

#[cfg(not(any(test, feature = "diagnostics")))]
const fn covering_projection_metrics_recorder()
-> crate::db::executor::CoveringProjectionMetricsRecorder {
    crate::db::executor::CoveringProjectionMetricsRecorder::new()
}

#[cfg(any(test, feature = "diagnostics"))]
fn projection_materialization_metrics_recorder() -> ProjectionMaterializationMetricsRecorder {
    ProjectionMaterializationMetricsRecorder::new(
        materialize::record_sql_projection_slot_rows_path_hit,
        materialize::record_sql_projection_data_rows_path_hit,
        materialize::record_sql_projection_data_rows_scalar_fallback_hit,
        materialize::record_sql_projection_data_rows_slot_access,
        materialize::record_sql_projection_distinct_candidate_row,
        materialize::record_sql_projection_distinct_bounded_stop,
    )
}

#[cfg(not(any(test, feature = "diagnostics")))]
const fn projection_materialization_metrics_recorder()
-> crate::db::executor::ProjectionMaterializationMetricsRecorder {
    crate::db::executor::ProjectionMaterializationMetricsRecorder::new()
}

#[cfg(feature = "sql")]
/// Execute one scalar load plan through executor-owned structural projection
/// materialization and return adapter-shaped SQL values.
pub(in crate::db) fn execute_sql_projection_rows_for_canister<C>(
    db: &Db<C>,
    debug: bool,
    prepared_plan: SharedPreparedExecutionPlan,
) -> Result<SqlProjectionRows, InternalError>
where
    C: CanisterKind,
{
    let SharedPreparedProjectionRuntimeParts {
        authority,
        plan,
        prepared_projection_shape,
    } = prepared_plan.into_projection_runtime_parts();
    // DISTINCT applies paging after projected-row deduplication, so raw-row
    // paging is removed before shared scalar execution. The executor projection
    // materializer applies the bounded DISTINCT window after rows are projected.
    let mut execution_plan = plan.clone();
    if execution_plan.scalar_plan().distinct {
        match &mut execution_plan.logical {
            LogicalPlan::Scalar(scalar) => scalar.page = None,
            LogicalPlan::Grouped(grouped) => grouped.scalar.page = None,
        }
    }

    // Non-DISTINCT projections may still call the executor-owned covering
    // projection boundary. DISTINCT deliberately falls through to the shared
    // scalar executor first so executor projection materialization can dedupe
    // projected rows in final execution order.
    let distinct = execution_plan.scalar_plan().distinct;
    if !distinct
        && let Some(projected) = try_execute_covering_projection_rows_for_canister(
            db,
            authority,
            &execution_plan,
            covering_projection_metrics_recorder(),
        )?
    {
        let projected = projected.into_value_rows();
        let row_count = u32::try_from(projected.len()).unwrap_or(u32::MAX);

        return Ok(SqlProjectionRows::new(projected, row_count));
    }

    let row_layout = authority.row_layout();
    let prepared_projection = prepared_projection_shape.as_deref().ok_or_else(|| {
        InternalError::query_executor_invariant(
            "SQL projection runtime requires one frozen structural projection shape",
        )
    })?;

    // Execute the canonical scalar runtime, then hand structural row shaping to
    // executor projection materialization before SQL final response shaping.
    let page = execute_initial_scalar_retained_slot_page_for_canister(
        db,
        debug,
        authority,
        execution_plan,
    )?;
    let projected = if distinct {
        project_distinct_structural_projection_page(
            row_layout,
            prepared_projection,
            &plan,
            page,
            projection_materialization_metrics_recorder(),
        )?
        .into_value_rows()
    } else {
        let projected = project_structural_projection_page(
            row_layout,
            prepared_projection,
            page,
            projection_materialization_metrics_recorder(),
        )?;
        projected.into_value_rows()
    };
    let row_count = u32::try_from(projected.len()).unwrap_or(u32::MAX);

    Ok(SqlProjectionRows::new(projected, row_count))
}
