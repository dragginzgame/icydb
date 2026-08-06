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
    traits::CanisterKind,
    value::Value,
};
use icydb_diagnostic_code::DiagnosticExecutionLane;
use std::borrow::Cow;

#[cfg(feature = "diagnostics")]
use crate::db::executor::aggregate::terminal_attribution::{
    ScalarAggregateTerminalAttribution, measure_phase, record_scalar_aggregate_terminal_attribution,
};
pub(in crate::db) use request::StructuralAggregateRequest;
#[cfg(feature = "sql")]
pub(in crate::db) use terminal::{StructuralAggregateTerminal, StructuralAggregateTerminalKind};

/// Execute one structural global aggregate request over a shared prepared scalar plan.
pub(in crate::db) fn execute_structural_aggregate_rows_for_canister<C>(
    db: &Db<C>,
    debug: bool,
    shared_plan: SharedPreparedExecutionPlan,
    request: StructuralAggregateRequest,
) -> Result<Vec<Vec<Value>>, InternalError>
where
    C: CanisterKind,
{
    let context =
        prepared_read_execution_context(&shared_plan, DiagnosticExecutionLane::TrustedRead);
    with_read_execution_budget(context, || {
        execute_structural_aggregate_rows_inner(db, debug, shared_plan, request)
    })
}

fn execute_structural_aggregate_rows_inner<C>(
    db: &Db<C>,
    debug: bool,
    shared_plan: SharedPreparedExecutionPlan,
    request: StructuralAggregateRequest,
) -> Result<Vec<Vec<Value>>, InternalError>
where
    C: CanisterKind,
{
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
        debug,
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
    debug: bool,
    plan: SharedPreparedExecutionPlan,
    terminals: PreparedScalarAggregateTerminalSet,
) -> Result<Vec<Value>, InternalError>
where
    C: CanisterKind,
{
    if terminals.is_empty() {
        return Ok(Vec::new());
    }
    #[cfg(feature = "diagnostics")]
    let mut terminal_attribution = ScalarAggregateTerminalAttribution::from_terminal_counts(
        terminals.terminal_count(),
        terminals.input_expr_count(),
        terminals.filter_expr_count(),
    );

    let plan = plan.into_prepared_load_plan();
    let authority = plan.authority();
    let retained_slot_layout = terminals.retained_slot_layout(&authority, plan.logical_plan())?;

    let mut reducer_runtime = ScalarAggregateReducerRuntime::new(terminals);
    #[cfg(feature = "diagnostics")]
    {
        let (total_local_instructions, execution) = measure_phase(|| {
            execute_prepared_scalar_aggregate_kernel_row_sink_for_canister(
                db,
                debug,
                plan,
                retained_slot_layout,
                |row| reducer_runtime.ingest_row(row),
            )
        });
        execution?;
        let runtime_attribution = reducer_runtime.attribution();
        terminal_attribution.merge_runtime(runtime_attribution);
        terminal_attribution.base_row_local_instructions = total_local_instructions
            .saturating_sub(terminal_attribution.reducer_fold_local_instructions);
        record_scalar_aggregate_terminal_attribution(terminal_attribution);
    }
    #[cfg(not(feature = "diagnostics"))]
    execute_prepared_scalar_aggregate_kernel_row_sink_for_canister(
        db,
        debug,
        plan,
        retained_slot_layout,
        |row| reducer_runtime.ingest_row(row),
    )?;

    reducer_runtime.finalize()
}
