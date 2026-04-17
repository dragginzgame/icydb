//! Module: db::session::sql::projection::runtime
//! Responsibility: session-owned SQL projection row shaping over structural
//! executor pages.
//! Does not own: shared projection validation or scalar execution mechanics.
//! Boundary: consumes structural pages from the executor and performs the
//! SQL-specific value/text shaping above that boundary.

mod covering;
mod materialize;
mod render;

#[cfg(all(feature = "sql", feature = "diagnostics"))]
use crate::db::{
    executor::{
        EntityAuthority, projection::PreparedProjectionShape,
        projection::prepare_projection_shape_from_plan,
    },
    query::plan::AccessPlannedQuery,
    session::sql::projection::runtime::render::render_projected_sql_rows_text,
};
#[cfg(feature = "sql")]
use crate::{
    db::{
        Db,
        executor::{
            SharedPreparedExecutionPlan,
            pipeline::execute_initial_scalar_retained_slot_page_for_canister,
        },
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

#[allow(unused_imports)]
#[cfg(all(test, not(feature = "diagnostics")))]
pub(crate) use crate::db::session::sql::projection::runtime::materialize::{
    SqlProjectionMaterializationMetrics, with_sql_projection_materialization_metrics,
};
#[allow(unused_imports)]
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

///
/// SqlProjectionTextExecutorAttribution
///
/// SqlProjectionTextExecutorAttribution breaks the rendered SQL projection
/// executor path into structural prepare, scalar runtime, projection
/// materialization, and final row-payload packaging.
/// This lets perf harnesses separate fixed executor setup from the terminal
/// fast path without reopening the session or SQL layers above it.
///

#[cfg(all(feature = "sql", feature = "diagnostics"))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SqlProjectionTextExecutorAttribution {
    pub prepare_projection: u64,
    pub scalar_runtime: u64,
    pub materialize_projection: u64,
    pub result_rows: u64,
    pub total: u64,
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

#[cfg(all(feature = "sql", feature = "diagnostics"))]
/// Execute one scalar load plan through the shared rendered SQL projection
/// path and return both the rendered rows and one executor-only phase split.
pub(in crate::db) fn attribute_sql_projection_text_rows_for_canister<C>(
    db: &Db<C>,
    debug: bool,
    authority: EntityAuthority,
    plan: AccessPlannedQuery,
) -> Result<SqlProjectionTextExecutorAttribution, InternalError>
where
    C: CanisterKind,
{
    let row_layout = authority.row_layout();

    // Phase 1: freeze the executor-owned structural projection contract.
    let (prepare_projection_local_instructions, prepared_projection) =
        measure_structural_result(|| {
            Ok::<PreparedProjectionShape, InternalError>(prepare_projection_shape_from_plan(
                authority.model(),
                &plan,
            ))
        });
    let prepared_projection = prepared_projection?;

    // Phase 2: execute the scalar runtime and preserve one structural slot-row
    // page for later SQL-specific shaping.
    let runtime_plan = plan.clone();
    let (scalar_runtime_local_instructions, page) = measure_structural_result(|| {
        execute_initial_scalar_retained_slot_page_for_canister(db, debug, authority, runtime_plan)
    });
    let page = page?;

    // Phase 3: project or preserve the structural page into rendered SQL rows.
    let (materialize_projection_local_instructions, rendered_rows) =
        measure_structural_result(|| {
            let projected =
                project_structural_sql_projection_page(row_layout, &prepared_projection, page)?;
            let projected = finalize_sql_projection_rows(&plan, projected)?;

            Ok::<Vec<Vec<String>>, InternalError>(render_projected_sql_rows_text(projected))
        });
    let rendered_rows = rendered_rows?;

    // Phase 4: package the rendered rows onto the stable SQL projection text
    // payload boundary.
    let (result_rows_local_instructions, row_count) = measure_structural_result(|| {
        Ok::<u32, InternalError>(u32::try_from(rendered_rows.len()).unwrap_or(u32::MAX))
    });
    let _row_count = row_count?;

    let total_local_instructions = prepare_projection_local_instructions
        .saturating_add(scalar_runtime_local_instructions)
        .saturating_add(materialize_projection_local_instructions)
        .saturating_add(result_rows_local_instructions);

    Ok(SqlProjectionTextExecutorAttribution {
        prepare_projection: prepare_projection_local_instructions,
        scalar_runtime: scalar_runtime_local_instructions,
        materialize_projection: materialize_projection_local_instructions,
        result_rows: result_rows_local_instructions,
        total: total_local_instructions,
    })
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
    let authority = prepared_plan.authority();
    let plan = prepared_plan.logical_plan();

    if let Some(projected) =
        try_execute_covering_sql_projection_rows_for_canister(db, authority, plan)?
    {
        let projected = finalize_sql_projection_rows(plan, projected)?;
        let row_count = u32::try_from(projected.len()).unwrap_or(u32::MAX);

        return Ok(SqlProjectionRows::new(projected, row_count));
    }

    if let Some(projected) =
        try_execute_hybrid_covering_sql_projection_rows_for_canister(db, authority, plan)?
    {
        let projected = finalize_sql_projection_rows(plan, projected)?;
        let row_count = u32::try_from(projected.len()).unwrap_or(u32::MAX);

        return Ok(SqlProjectionRows::new(projected, row_count));
    }

    let row_layout = authority.row_layout();
    let prepared_projection = prepared_plan.prepared_projection_shape().ok_or_else(|| {
        InternalError::query_executor_invariant(
            "structural SQL projection execution requires one frozen scalar projection shape",
        )
    })?;

    // Execute the canonical scalar runtime and then shape the resulting
    // structural page into projected SQL values.
    let page =
        execute_initial_scalar_retained_slot_page_for_canister(db, debug, authority, plan.clone())?;
    let projected = project_structural_sql_projection_page(row_layout, prepared_projection, page)?;
    let projected = finalize_sql_projection_rows(plan, projected)?;
    let row_count = u32::try_from(projected.len()).unwrap_or(u32::MAX);

    Ok(SqlProjectionRows::new(projected, row_count))
}
