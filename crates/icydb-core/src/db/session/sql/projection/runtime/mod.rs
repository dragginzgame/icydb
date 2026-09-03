//! Module: db::session::sql::projection::runtime
//! Responsibility: session-owned SQL projection execution handoff over
//! executor-owned structural projection rows.
//! Does not own: shared projection validation, projection execution, or scalar
//! execution mechanics.
//! Boundary: consumes structural projection results from the executor and
//! returns runtime value rows for the shared structural payload.

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
use icydb_diagnostic_code::DiagnosticExecutionLane;

#[cfg(feature = "sql")]
/// Execute one scalar load plan through executor-owned structural projection
/// materialization and return adapter-shaped SQL values.
pub(in crate::db) fn execute_sql_projection_rows_for_canister<C>(
    db: &Db<C>,
    prepared_plan: SharedPreparedExecutionPlan,
) -> Result<(Vec<Vec<Value>>, u32), InternalError>
where
    C: CanisterKind,
{
    let rows = execute_structural_projection_rows(
        db,
        StructuralProjectionRequest::new(prepared_plan, DiagnosticExecutionLane::TrustedRead),
    )?;
    let row_count = rows.row_count();
    let projected = rows.into_value_rows();

    Ok((projected, row_count))
}

#[cfg(feature = "sql")]
/// Execute one SQL projection under a fail-closed scanned-key ceiling.
pub(in crate::db) fn execute_sql_projection_rows_for_canister_with_scan_budget<C>(
    db: &Db<C>,
    prepared_plan: SharedPreparedExecutionPlan,
    scan_budget: StructuralProjectionScanBudget,
) -> Result<(Vec<Vec<Value>>, u32), InternalError>
where
    C: CanisterKind,
{
    let rows = execute_structural_projection_rows(
        db,
        StructuralProjectionRequest::new(prepared_plan, DiagnosticExecutionLane::Mutation)
            .with_scan_budget(scan_budget),
    )?;
    let row_count = rows.row_count();
    let projected = rows.into_value_rows();

    Ok((projected, row_count))
}
