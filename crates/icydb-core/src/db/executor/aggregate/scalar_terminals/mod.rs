//! Module: executor::aggregate::scalar_terminals
//! Responsibility: scalar-window aggregate terminals over retained-slot rows.
//! Does not own: adapter lowering, grouped DISTINCT policy, or response DTO shaping.
//! Boundary: consumes prepared scalar access/window plans plus uncached terminal metadata.

#[cfg(feature = "diagnostics")]
mod diagnostics;
mod expr_cache;
mod reducer;
mod request;
mod terminal;

use crate::{
    db::{
        executor::{
            PreparedExecutionPlan, SharedPreparedExecutionPlan,
            aggregate::{
                reducer_core::finalize_count,
                scalar_terminals::{
                    reducer::ScalarAggregateReducerRuntime,
                    request::CompiledStructuralAggregateRequest,
                    terminal::{
                        PreparedScalarAggregateTerminalSet,
                        compile_structural_scalar_aggregate_terminal,
                    },
                },
            },
            pipeline::{
                contracts::LoadExecutor,
                entrypoints::execute_prepared_scalar_aggregate_kernel_row_sink_for_canister,
            },
            projection::{GroupedRowView, evaluate_grouped_having_expr},
        },
        query::builder::aggregate::ScalarTerminalBoundaryRequest,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
    value::Value,
};
use std::borrow::Cow;

#[cfg(feature = "diagnostics")]
pub(in crate::db) use diagnostics::{
    ScalarAggregateTerminalAttribution, with_scalar_aggregate_terminal_attribution,
};
pub(in crate::db) use request::{StructuralAggregateRequest, StructuralAggregateResult};
pub(in crate::db) use terminal::{StructuralAggregateTerminal, StructuralAggregateTerminalKind};

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    /// Execute one structural global aggregate request over a shared prepared scalar plan.
    pub(in crate::db) fn execute_structural_aggregate_result(
        &self,
        shared_plan: &SharedPreparedExecutionPlan,
        request: StructuralAggregateRequest,
    ) -> Result<StructuralAggregateResult, InternalError> {
        let compiled = CompiledStructuralAggregateRequest::compile(&request)?;
        let terminal_count = request.terminals().len();
        let mut unique_values = vec![None; terminal_count];
        let mut scalar_aggregate_terminals = Vec::with_capacity(terminal_count);
        let mut scalar_aggregate_terminal_positions = Vec::with_capacity(terminal_count);

        // Phase 1: route count-equivalent terminals through the shared scalar
        // count boundary and stage all remaining terminals for the aggregate
        // reducer sink. Both paths stay under executor ownership.
        for (terminal_index, terminal) in request.terminals().iter().enumerate() {
            if terminal.uses_shared_count_terminal(E::MODEL) {
                let count = self
                    .execute_scalar_terminal_request(
                        shared_plan.typed_clone::<E>(),
                        ScalarTerminalBoundaryRequest::Count,
                    )?
                    .into_count()?;
                unique_values[terminal_index] = Some(finalize_count(u64::from(count)));
            } else {
                scalar_aggregate_terminals.push(compile_structural_scalar_aggregate_terminal(
                    E::MODEL,
                    terminal,
                )?);
                scalar_aggregate_terminal_positions.push(terminal_index);
            }
        }

        // Phase 2: reduce every non-count-equivalent terminal through the
        // scalar aggregate terminal sink so row decoding, filter evaluation,
        // expression evaluation, DISTINCT, and reducer finalization remain
        // executor-owned.
        if !scalar_aggregate_terminals.is_empty() {
            let terminal_values = self.execute_scalar_aggregate_terminals(
                shared_plan.typed_clone::<E>(),
                PreparedScalarAggregateTerminalSet::new(scalar_aggregate_terminals),
            )?;
            if terminal_values.len() != scalar_aggregate_terminal_positions.len() {
                return Err(InternalError::query_executor_invariant(
                    "structural aggregate terminal output count must match staged terminals",
                ));
            }

            for (terminal_index, value) in scalar_aggregate_terminal_positions
                .into_iter()
                .zip(terminal_values)
            {
                unique_values[terminal_index] = Some(value);
            }
        }
        let mut ordered_values = Vec::with_capacity(unique_values.len());
        for value in unique_values {
            let value = value.ok_or_else(|| {
                InternalError::query_executor_invariant(
                    "structural aggregate terminal did not produce a reduced value",
                )
            })?;
            ordered_values.push(value);
        }

        // Phase 3: evaluate global aggregate HAVING and final projection
        // against the implicit single aggregate row. Adapter layers only see
        // the completed structural row payload.
        let grouped_row = GroupedRowView::new(
            &[],
            ordered_values.as_slice(),
            &[],
            compiled.aggregate_execution_specs(),
        );
        if let Some(expr) = compiled.having()
            && !evaluate_grouped_having_expr(expr, &grouped_row).map_err(|err| {
                InternalError::query_executor_invariant(format!(
                    "structural aggregate HAVING evaluation failed: {err}",
                ))
            })?
        {
            return Ok(StructuralAggregateResult::new(Vec::new()));
        }

        let mut row = Vec::with_capacity(compiled.projection().len());
        for expr in compiled.projection() {
            row.push(
                expr.evaluate(&grouped_row)
                    .map(Cow::into_owned)
                    .map_err(|err| {
                        InternalError::query_executor_invariant(format!(
                            "structural aggregate projection evaluation failed: {err}",
                        ))
                    })?,
            );
        }

        Ok(StructuralAggregateResult::new(vec![row]))
    }

    /// Execute scalar aggregate terminals over one prepared scalar access/window plan.
    fn execute_scalar_aggregate_terminals(
        &self,
        plan: PreparedExecutionPlan<E>,
        terminals: PreparedScalarAggregateTerminalSet,
    ) -> Result<Vec<Value>, InternalError> {
        if terminals.is_empty() {
            return Ok(Vec::new());
        }
        #[cfg(feature = "diagnostics")]
        let mut terminal_attribution =
            ScalarAggregateTerminalAttribution::from_terminal_set(&terminals);

        // Phase 1: prepare the scalar plan with an execution-local retained-slot
        // layout that includes aggregate input and filter slots.
        let plan = plan.into_prepared_load_plan();
        let retained_slot_layout =
            terminals.retained_slot_layout(plan.authority().model(), plan.logical_plan())?;

        // Phase 2: reduce every terminal as the scalar runtime emits its final
        // post-access/windowed row boundary, without constructing a retained-slot
        // response page for adapter-owned aggregate code to consume.
        let mut reducer_runtime = ScalarAggregateReducerRuntime::new(terminals);
        #[cfg(feature = "diagnostics")]
        {
            let (total_local_instructions, execution) = diagnostics::measure_phase(|| {
                execute_prepared_scalar_aggregate_kernel_row_sink_for_canister(
                    &self.db,
                    self.debug,
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
            diagnostics::record_scalar_aggregate_terminal_attribution(terminal_attribution);
        }
        #[cfg(not(feature = "diagnostics"))]
        execute_prepared_scalar_aggregate_kernel_row_sink_for_canister(
            &self.db,
            self.debug,
            plan,
            retained_slot_layout,
            |row| reducer_runtime.ingest_row(row),
        )?;

        reducer_runtime.finalize()
    }
}
