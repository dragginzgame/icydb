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
#[cfg(all(feature = "sql", feature = "diagnostics"))]
use crate::db::{DirectDataRowAttribution, KernelRowAttribution};
#[cfg(feature = "sql")]
use crate::{
    db::{
        Db,
        executor::{
            SharedPreparedExecutionPlan, StructuralProjectionRequest,
            StructuralProjectionScanBudget, execute_structural_projection_rows,
        },
    },
    error::InternalError,
    traits::CanisterKind,
    value::Value,
};

#[cfg(all(feature = "sql", feature = "diagnostics"))]
type SqlProjectionRowsWithDirectAttribution = (
    (Vec<Vec<Value>>, u32),
    Option<DirectDataRowAttribution>,
    Option<KernelRowAttribution>,
);

#[cfg(all(test, not(feature = "diagnostics")))]
pub(crate) use crate::db::session::sql::projection::runtime::materialize::{
    SqlProjectionMaterializationMetrics, with_sql_projection_materialization_metrics,
};
#[cfg(feature = "diagnostics")]
pub use crate::db::session::sql::projection::runtime::materialize::{
    SqlProjectionMaterializationMetrics, with_sql_projection_materialization_metrics,
};

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
) -> Result<(Vec<Vec<Value>>, u32), InternalError>
where
    C: CanisterKind,
{
    let rows = execute_structural_projection_rows(
        db,
        StructuralProjectionRequest::new(
            debug,
            prepared_plan,
            covering_projection_metrics_recorder(),
            projection_materialization_metrics_recorder(),
        ),
    )?;
    let row_count = rows.row_count();
    let projected = rows.into_value_rows();

    Ok((projected, row_count))
}

#[cfg(feature = "sql")]
/// Execute one SQL projection under a fail-closed scanned-key ceiling.
pub(in crate::db) fn execute_sql_projection_rows_for_canister_with_scan_budget<C>(
    db: &Db<C>,
    debug: bool,
    prepared_plan: SharedPreparedExecutionPlan,
    scan_budget: StructuralProjectionScanBudget,
) -> Result<(Vec<Vec<Value>>, u32), InternalError>
where
    C: CanisterKind,
{
    let rows = execute_structural_projection_rows(
        db,
        StructuralProjectionRequest::new(
            debug,
            prepared_plan,
            covering_projection_metrics_recorder(),
            projection_materialization_metrics_recorder(),
        )
        .with_scan_budget(scan_budget),
    )?;
    let row_count = rows.row_count();
    let projected = rows.into_value_rows();

    Ok((projected, row_count))
}

#[cfg(all(feature = "sql", feature = "diagnostics"))]
/// Execute one SQL scalar projection while collecting direct data-row subphase
/// counters for perf-audit attribution.
pub(in crate::db) fn execute_sql_projection_rows_for_canister_with_direct_data_row_attribution<C>(
    db: &Db<C>,
    debug: bool,
    prepared_plan: SharedPreparedExecutionPlan,
) -> Result<SqlProjectionRowsWithDirectAttribution, InternalError>
where
    C: CanisterKind,
{
    let ((rows, direct_data_row), kernel_row) =
        crate::db::executor::with_kernel_row_phase_attribution(|| {
            crate::db::executor::with_direct_data_row_phase_attribution(|| {
                execute_sql_projection_rows_for_canister(db, debug, prepared_plan)
            })
        });
    let rows = rows?;
    let direct_data_row = DirectDataRowAttribution::from_captured_phase(direct_data_row);
    let kernel_row = KernelRowAttribution::from_captured_phase(kernel_row);

    Ok((rows, direct_data_row, kernel_row))
}
