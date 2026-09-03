//! Module: executor::aggregate::scalar_terminals
//! Responsibility: scalar-window aggregate terminals over retained-slot rows.
//! Does not own: adapter lowering, grouped DISTINCT policy, or response DTO shaping.
//! Boundary: consumes prepared scalar access/window plans plus uncached terminal metadata.

mod expr_cache;
mod reducer;
mod request;
mod terminal;

use crate::{
    db::{
        Db,
        executor::{
            SharedPreparedExecutionPlan,
            aggregate::scalar_terminals::{
                reducer::ScalarAggregateReducerRuntime,
                request::CompiledStructuralAggregateRequest,
                terminal::{
                    PreparedScalarAggregateTerminalSet,
                    compile_structural_scalar_aggregate_terminal,
                },
            },
            budget::{
                charge_runtime_value_rows, prepared_read_execution_context,
                with_read_execution_budget,
            },
            pipeline::entrypoints::execute_prepared_scalar_aggregate_kernel_row_sink_for_canister,
            projection::{GroupedRowView, evaluate_grouped_having_expr},
        },
    },
    error::InternalError,
    metrics::EntityMetricsSpan,
    traits::CanisterKind,
    value::Value,
};
use icydb_diagnostic_code::DiagnosticExecutionLane;
use std::borrow::Cow;

pub(in crate::db::executor::aggregate) use reducer::scalar_distinct_conservative_unit_work;
pub(in crate::db) use request::StructuralAggregateRequest;
#[cfg(feature = "sql")]
pub(in crate::db) use terminal::{StructuralAggregateTerminal, StructuralAggregateTerminalKind};

/// Execute one structural global aggregate request over a shared prepared scalar plan.
pub(in crate::db) fn execute_structural_aggregate_rows_for_canister<C>(
    db: &Db<C>,
    shared_plan: SharedPreparedExecutionPlan,
    request: StructuralAggregateRequest,
) -> Result<Vec<Vec<Value>>, InternalError>
where
    C: CanisterKind,
{
    let context =
        prepared_read_execution_context(&shared_plan, DiagnosticExecutionLane::TrustedRead);
    with_read_execution_budget(db.request_execution_scope(), context, || {
        execute_structural_aggregate_rows_inner(db, shared_plan, request)
    })
}

fn execute_structural_aggregate_rows_inner<C>(
    db: &Db<C>,
    shared_plan: SharedPreparedExecutionPlan,
    request: StructuralAggregateRequest,
) -> Result<Vec<Vec<Value>>, InternalError>
where
    C: CanisterKind,
{
    let entity_path = shared_plan.authority_ref().entity_path_handle();
    let _metrics_span = EntityMetricsSpan::new(entity_path.as_ref());
    let compiled = CompiledStructuralAggregateRequest::compile(&request)?;
    let terminal_count = request.terminals().len();
    let terminals = request
        .terminals()
        .iter()
        .map(|terminal| {
            compile_structural_scalar_aggregate_terminal(request.schema_info(), terminal)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let ordered_values = execute_scalar_aggregate_terminals(
        db,
        shared_plan,
        PreparedScalarAggregateTerminalSet::new(terminals),
    )?;
    if ordered_values.len() != terminal_count {
        return Err(InternalError::query_executor_invariant());
    }

    let grouped_row = GroupedRowView::new(&[], ordered_values.as_slice());
    if let Some(expr) = compiled.having()
        && !evaluate_grouped_having_expr(expr, &grouped_row)
            .map_err(|_err| InternalError::query_executor_invariant())?
    {
        let rows = Vec::new();
        charge_runtime_value_rows(&rows)?;
        return Ok(rows);
    }

    let mut row = Vec::with_capacity(compiled.projection().len());
    for expr in compiled.projection() {
        row.push(
            expr.evaluate(&grouped_row)
                .map(Cow::into_owned)
                .map_err(|_err| InternalError::query_executor_invariant())?,
        );
    }

    let rows = vec![row];
    charge_runtime_value_rows(&rows)?;

    Ok(rows)
}

fn execute_scalar_aggregate_terminals<C>(
    db: &Db<C>,
    plan: SharedPreparedExecutionPlan,
    terminals: PreparedScalarAggregateTerminalSet,
) -> Result<Vec<Value>, InternalError>
where
    C: CanisterKind,
{
    if terminals.is_empty() {
        return Ok(Vec::new());
    }

    let plan = plan.into_prepared_load_plan();
    let authority = plan.authority();
    let retained_slot_layout = terminals.retained_slot_layout(&authority, plan.logical_plan())?;
    let aggregate_route_plan = terminals
        .single_field_extrema_route_candidate()
        .map(|(kind, field)| plan.aggregate_execution_route_plan(kind, field))
        .transpose()?
        .flatten();

    let mut reducer_runtime = ScalarAggregateReducerRuntime::new(terminals);

    execute_prepared_scalar_aggregate_kernel_row_sink_for_canister(
        db,
        plan,
        retained_slot_layout,
        aggregate_route_plan,
        |row| reducer_runtime.ingest_row(row),
    )?;

    reducer_runtime.finalize()
}
