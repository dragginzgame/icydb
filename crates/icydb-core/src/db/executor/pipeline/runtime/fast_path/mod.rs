//! Module: executor::pipeline::runtime::fast_path
//! Responsibility: fast-path decision and fallback key-stream resolution policy.
//! Does not own: page materialization or execution-trace finalization.
//! Boundary: internal helper boundary for `executor::pipeline::runtime`.

mod strategy;

use crate::{
    db::{
        executor::{
            ExecutionKernel, ExecutionOptimization, ExecutionPlan,
            pipeline::contracts::{ExecutionInputs, ResolvedExecutionKeyStream},
        },
        index::{
            IndexCompilePolicy, compile_index_program, compile_index_program_for_targets,
            predicate::IndexPredicateExecution,
        },
    },
    error::InternalError,
};
use std::cell::Cell;

use crate::db::executor::pipeline::runtime::fast_path::strategy::{
    FastPathDecision, FastPathResolutionStrategy,
};

impl ExecutionKernel {
    /// Resolve one canonical execution key stream in fast-path precedence order.
    ///
    /// This is the single shared load key-stream resolver boundary.
    pub(in crate::db::executor) fn resolve_execution_key_stream_without_distinct(
        inputs: &ExecutionInputs<'_>,
        route_plan: &ExecutionPlan,
        predicate_compile_mode: IndexCompilePolicy,
    ) -> Result<ResolvedExecutionKeyStream, InternalError> {
        // Phase 0: reuse precompiled runtime index predicates when the
        // execution-preparation boundary already owns the requested mode, and
        // only fall back to one on-demand compile when it does not.
        let precompiled_index_predicate = match predicate_compile_mode {
            IndexCompilePolicy::ConservativeSubset => {
                inputs.execution_preparation().conservative_mode()
            }
            IndexCompilePolicy::StrictAllOrNone => inputs.execution_preparation().strict_mode(),
        };
        let compiled_index_predicate = if precompiled_index_predicate.is_none() {
            inputs
                .execution_preparation()
                .compiled_predicate()
                .and_then(|compiled_predicate| {
                    if let Some(compile_targets) = inputs.execution_preparation().compile_targets()
                    {
                        return compile_index_program_for_targets(
                            compiled_predicate.executable(),
                            compile_targets,
                            predicate_compile_mode,
                        );
                    }

                    let slot_map = inputs.execution_preparation().slot_map()?;

                    compile_index_program(
                        compiled_predicate.executable(),
                        slot_map,
                        predicate_compile_mode,
                    )
                })
        } else {
            None
        };
        let index_predicate_program =
            precompiled_index_predicate.or(compiled_index_predicate.as_ref());
        let index_predicate_applied = index_predicate_program.is_some();
        let index_predicate_rejected_counter = Cell::new(0u64);
        let index_predicate_execution =
            index_predicate_program.map(|program| IndexPredicateExecution {
                program,
                rejected_keys_counter: Some(&index_predicate_rejected_counter),
            });

        // Phase 1: select fast-path resolution strategy once from route shape.
        let fast_path_strategy = FastPathResolutionStrategy::for_route(route_plan);
        let fast_path_decision = fast_path_strategy.resolve_fast_path_decision(
            inputs,
            route_plan,
            index_predicate_execution,
        )?;

        // Phase 2: materialize from fast-path hit or canonical fallback stream.
        Self::resolve_execution_key_stream_from_decision(
            fast_path_decision,
            inputs,
            route_plan,
            index_predicate_execution,
            index_predicate_applied,
            &index_predicate_rejected_counter,
        )
    }

    // Resolve one canonical key stream from fast-path decision output.
    fn resolve_execution_key_stream_from_decision(
        fast_path_decision: FastPathDecision,
        inputs: &ExecutionInputs<'_>,
        route_plan: &ExecutionPlan,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
        index_predicate_applied: bool,
        index_predicate_rejected_counter: &Cell<u64>,
    ) -> Result<ResolvedExecutionKeyStream, InternalError> {
        match fast_path_decision {
            FastPathDecision::Hit(fast) => Ok(ResolvedExecutionKeyStream::new(
                fast.ordered_key_stream,
                Some(Self::decorate_fast_path_optimization_for_route(
                    fast.optimization,
                    route_plan,
                )),
                Some(fast.rows_scanned),
                index_predicate_applied,
                index_predicate_rejected_counter.get(),
                None,
            )),
            FastPathDecision::None => Self::resolve_fallback_execution_key_stream(
                inputs,
                route_plan,
                index_predicate_execution,
                index_predicate_applied,
                index_predicate_rejected_counter,
            ),
        }
    }

    // Resolve canonical fallback access stream when no fast path produced rows.
    fn resolve_fallback_execution_key_stream(
        inputs: &ExecutionInputs<'_>,
        route_plan: &ExecutionPlan,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
        index_predicate_applied: bool,
        index_predicate_rejected_counter: &Cell<u64>,
    ) -> Result<ResolvedExecutionKeyStream, InternalError> {
        let fallback_fetch_hint =
            route_plan.fallback_physical_fetch_hint(inputs.stream_bindings().direction());
        let preserve_leaf_index_order = route_plan.secondary_fast_path_eligible();
        let key_stream = inputs.runtime().resolve_fallback_execution_key_stream(
            *inputs.stream_bindings(),
            fallback_fetch_hint,
            index_predicate_execution,
            preserve_leaf_index_order,
        )?;

        Ok(ResolvedExecutionKeyStream::new(
            key_stream,
            None,
            None,
            index_predicate_applied,
            index_predicate_rejected_counter.get(),
            None,
        ))
    }

    // Project one fast-path optimization label through route-level top-N seek
    // metadata so trace taxonomy keeps top-N assisted fast paths explicit.
    const fn decorate_fast_path_optimization_for_route(
        optimization: ExecutionOptimization,
        route_plan: &ExecutionPlan,
    ) -> ExecutionOptimization {
        if route_plan.top_n_seek_spec().is_none() {
            return optimization;
        }

        match optimization {
            ExecutionOptimization::PrimaryKey => ExecutionOptimization::PrimaryKeyTopNSeek,
            ExecutionOptimization::SecondaryOrderPushdown => {
                ExecutionOptimization::SecondaryOrderTopNSeek
            }
            ExecutionOptimization::PrimaryKeyTopNSeek
            | ExecutionOptimization::SecondaryOrderTopNSeek
            | ExecutionOptimization::IndexRangeLimitPushdown => optimization,
        }
    }
}
