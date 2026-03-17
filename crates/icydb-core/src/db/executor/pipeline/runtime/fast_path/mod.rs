//! Module: executor::pipeline::runtime::fast_path
//! Responsibility: fast-path decision and fallback key-stream resolution policy.
//! Does not own: page materialization or execution-trace finalization.
//! Boundary: internal helper boundary for `executor::pipeline::runtime`.

mod strategy;

use crate::{
    db::{
        executor::{
            ExecutableAccess, ExecutionOptimization, ExecutionPlan,
            pipeline::contracts::{ExecutionInputs, LoadExecutor, ResolvedExecutionKeyStream},
            route::RoutedKeyStreamRequest,
        },
        index::{IndexCompilePolicy, compile_index_program, predicate::IndexPredicateExecution},
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};
use std::cell::Cell;

use crate::db::executor::pipeline::runtime::fast_path::strategy::{
    FastPathDecision, FastPathResolutionStrategy,
};

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    /// Resolve one canonical execution key stream in fast-path precedence order.
    ///
    /// This is the single shared load key-stream resolver boundary.
    pub(in crate::db::executor) fn resolve_execution_key_stream_without_distinct(
        inputs: &ExecutionInputs<'_, E>,
        route_plan: &ExecutionPlan,
        predicate_compile_mode: IndexCompilePolicy,
    ) -> Result<ResolvedExecutionKeyStream, InternalError> {
        // Phase 0: compile optional index predicate execution program.
        let index_predicate_program = inputs
            .execution_preparation()
            .compiled_predicate()
            .and_then(|compiled_predicate| {
                let slot_map = inputs.execution_preparation().slot_map()?;

                compile_index_program(
                    compiled_predicate.resolved(),
                    slot_map,
                    predicate_compile_mode,
                )
            });
        let index_predicate_applied = index_predicate_program.is_some();
        let index_predicate_rejected_counter = Cell::new(0u64);
        let index_predicate_execution =
            index_predicate_program
                .as_ref()
                .map(|program| IndexPredicateExecution {
                    program,
                    rejected_keys_counter: Some(&index_predicate_rejected_counter),
                });

        // Phase 1: select fast-path resolution strategy once from route shape.
        let fast_path_strategy = FastPathResolutionStrategy::for_route(route_plan);
        let fast_path_decision = fast_path_strategy.resolve_fast_path_decision::<E>(
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
        inputs: &ExecutionInputs<'_, E>,
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
        inputs: &ExecutionInputs<'_, E>,
        route_plan: &ExecutionPlan,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
        index_predicate_applied: bool,
        index_predicate_rejected_counter: &Cell<u64>,
    ) -> Result<ResolvedExecutionKeyStream, InternalError> {
        let fallback_fetch_hint =
            route_plan.fallback_physical_fetch_hint(inputs.stream_bindings().direction());
        let access = ExecutableAccess::new(
            &inputs.plan().access,
            *inputs.stream_bindings(),
            fallback_fetch_hint,
            index_predicate_execution,
        );
        let key_stream = Self::resolve_routed_key_stream(
            inputs.ctx(),
            RoutedKeyStreamRequest::ExecutableAccess(access),
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
