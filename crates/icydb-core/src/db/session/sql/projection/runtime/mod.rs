//! Module: db::session::sql::projection::runtime
//! Responsibility: session-owned SQL projection row shaping over structural
//! executor pages.
//! Does not own: shared projection validation or scalar execution mechanics.
//! Boundary: consumes structural pages from the executor and performs the
//! SQL-specific value/text shaping above that boundary.

mod covering;
mod materialize;
#[cfg(all(feature = "sql", test))]
mod tests;

#[cfg(feature = "sql")]
use crate::{
    db::{
        Db,
        executor::{
            SharedPreparedExecutionPlan, SharedPreparedProjectionRuntimeParts,
            pipeline::execute_initial_scalar_retained_slot_page_for_canister,
        },
        query::plan::LogicalPlan,
        session::sql::projection::runtime::{
            covering::{
                try_execute_covering_sql_projection_rows_for_canister,
                try_execute_hybrid_covering_sql_projection_rows_for_canister,
            },
            materialize::{finalize_sql_projection_rows, project_structural_sql_projection_page},
        },
    },
    error::InternalError,
    traits::CanisterKind,
    value::Value,
};
#[cfg(all(feature = "sql", feature = "diagnostics"))]
use std::cell::Cell;

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
/// Generic-free SQL projection row payload emitted by executor-owned structural
/// projection execution helpers.
/// Keeps SQL row materialization out of typed `ProjectionResponse<E>` so SQL
/// SQL execution can render value rows without reintroducing entity-specific ids.
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
std::thread_local! {
    static PURE_COVERING_DECODE_LOCAL_INSTRUCTIONS: Cell<u64> = const { Cell::new(0) };
    static PURE_COVERING_ROW_ASSEMBLY_LOCAL_INSTRUCTIONS: Cell<u64> = const { Cell::new(0) };
}

#[cfg(all(feature = "sql", feature = "diagnostics"))]
pub(in crate::db::session::sql::projection::runtime) fn record_pure_covering_decode_local_instructions(
    delta: u64,
) {
    if delta == 0 {
        return;
    }

    PURE_COVERING_DECODE_LOCAL_INSTRUCTIONS.with(|counter| {
        counter.set(counter.get().saturating_add(delta));
    });
}

#[cfg(all(feature = "sql", feature = "diagnostics"))]
pub(in crate::db::session::sql::projection::runtime) fn record_pure_covering_row_assembly_local_instructions(
    delta: u64,
) {
    if delta == 0 {
        return;
    }

    PURE_COVERING_ROW_ASSEMBLY_LOCAL_INSTRUCTIONS.with(|counter| {
        counter.set(counter.get().saturating_add(delta));
    });
}

#[cfg(all(feature = "sql", feature = "diagnostics"))]
pub(in crate::db) fn current_pure_covering_decode_local_instructions() -> u64 {
    PURE_COVERING_DECODE_LOCAL_INSTRUCTIONS.with(Cell::get)
}

#[cfg(all(feature = "sql", feature = "diagnostics"))]
pub(in crate::db) fn current_pure_covering_row_assembly_local_instructions() -> u64 {
    PURE_COVERING_ROW_ASSEMBLY_LOCAL_INSTRUCTIONS.with(Cell::get)
}

#[cfg(all(feature = "sql", feature = "diagnostics", target_arch = "wasm32"))]
fn read_local_instruction_counter() -> u64 {
    canic_cdk::api::performance_counter(1)
}

#[cfg(all(feature = "sql", feature = "diagnostics", not(target_arch = "wasm32")))]
const fn read_local_instruction_counter() -> u64 {
    0
}

#[cfg(all(feature = "sql", feature = "diagnostics"))]
pub(in crate::db::session::sql::projection::runtime) fn measure_structural_result<T, E>(
    run: impl FnOnce() -> Result<T, E>,
) -> (u64, Result<T, E>) {
    let start = read_local_instruction_counter();
    let result = run();
    let delta = read_local_instruction_counter().saturating_sub(start);

    (delta, result)
}

#[cfg(feature = "sql")]
/// Execute one scalar load plan through the shared structural SQL projection
/// path and return only projected SQL values.
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
    // SQL projection DISTINCT applies paging after projected-row
    // deduplication, so the executor must materialize the full ordered scalar
    // stream here and leave LIMIT/OFFSET to final SQL projection shaping.
    let mut execution_plan = plan.clone();
    if execution_plan.scalar_plan().distinct {
        match &mut execution_plan.logical {
            LogicalPlan::Scalar(scalar) => scalar.page = None,
            LogicalPlan::Grouped(grouped) => grouped.scalar.page = None,
        }
    }

    if let Some(projected) =
        try_execute_covering_sql_projection_rows_for_canister(db, authority, &execution_plan)?
    {
        let projected = finalize_sql_projection_rows(&plan, projected)?;
        let row_count = u32::try_from(projected.len()).unwrap_or(u32::MAX);

        return Ok(SqlProjectionRows::new(projected, row_count));
    }

    if let Some(projected) = try_execute_hybrid_covering_sql_projection_rows_for_canister(
        db,
        authority,
        &execution_plan,
    )? {
        let projected = finalize_sql_projection_rows(&plan, projected)?;
        let row_count = u32::try_from(projected.len()).unwrap_or(u32::MAX);

        return Ok(SqlProjectionRows::new(projected, row_count));
    }

    let row_layout = authority.row_layout();
    let prepared_projection = prepared_projection_shape.as_deref().ok_or_else(|| {
        InternalError::query_executor_invariant(
            "structural SQL projection execution requires one frozen scalar projection shape",
        )
    })?;

    // Execute the canonical scalar runtime and then shape the resulting
    // structural page into projected SQL values.
    let page = execute_initial_scalar_retained_slot_page_for_canister(
        db,
        debug,
        authority,
        execution_plan,
    )?;
    let projected = project_structural_sql_projection_page(row_layout, prepared_projection, page)?;
    let projected = finalize_sql_projection_rows(&plan, projected)?;
    let row_count = u32::try_from(projected.len()).unwrap_or(u32::MAX);

    Ok(SqlProjectionRows::new(projected, row_count))
}
