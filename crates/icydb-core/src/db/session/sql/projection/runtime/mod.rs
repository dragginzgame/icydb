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
            SharedPreparedExecutionPlan, StructuralProjectionRequest,
            execute_structural_projection_result,
        },
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
    let result = execute_structural_projection_result(
        db,
        StructuralProjectionRequest::new(
            debug,
            prepared_plan,
            covering_projection_metrics_recorder(),
            projection_materialization_metrics_recorder(),
        ),
    )?;
    let row_count = result.row_count();
    let projected = result.into_value_rows();

    Ok(SqlProjectionRows::new(projected, row_count))
}
