//! Module: executor::load::execute::fast_path
//! Responsibility: fast-path decision and fallback key-stream resolution policy.
//! Does not own: page materialization or execution-trace finalization.
//! Boundary: internal helper boundary for `executor::load::execute`.

use crate::{
    db::{
        executor::load::{
            ExecutionInputsProjection, FastPathKeyResult, LoadExecutor, ResolvedExecutionKeyStream,
        },
        executor::{
            AccessExecutionDescriptor, ExecutionOptimization, ExecutionPlan,
            route::{
                FastPathOrder, RoutedKeyStreamRequest, ensure_load_fast_path_spec_arity,
                try_first_verified_fast_path_hit,
            },
        },
        index::{IndexCompilePolicy, compile_index_program, predicate::IndexPredicateExecution},
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};
use std::cell::Cell;

///
/// FastPathDecision
///
/// Canonical fast-path routing decision for one execution attempt.
///

enum FastPathDecision {
    Hit(FastPathKeyResult),
    None,
}

///
/// FastPathResolutionStrategy
///
/// Strategy selected once from route shape so key-stream resolution does not
/// branch inline on fast-path eligibility policy.
///

enum FastPathResolutionStrategy {
    StreamingFastPathFirst,
    FallbackOnly,
}

impl FastPathResolutionStrategy {
    const fn for_route(route_plan: &ExecutionPlan) -> Self {
        if route_plan.shape().is_streaming() {
            Self::StreamingFastPathFirst
        } else {
            Self::FallbackOnly
        }
    }

    fn resolve_fast_path_decision<E, I>(
        self,
        inputs: &I,
        route_plan: &ExecutionPlan,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<FastPathDecision, InternalError>
    where
        E: EntityKind + EntityValue,
        I: ExecutionInputsProjection<E>,
    {
        match self {
            Self::StreamingFastPathFirst => {
                LoadExecutor::<E>::evaluate_fast_path(inputs, route_plan, index_predicate_execution)
            }
            Self::FallbackOnly => Ok(FastPathDecision::None),
        }
    }
}

///
/// FastPathRouteHandler
///
/// Strategy selected once from verified fast-path route so route-specific stream
/// execution stays centralized.
///

enum FastPathRouteHandler {
    PrimaryKey,
    SecondaryPrefix,
    IndexRange { fetch: Option<usize> },
    None,
}

impl FastPathRouteHandler {
    fn resolve(route_plan: &ExecutionPlan, verified_route: FastPathOrder) -> Self {
        match verified_route {
            FastPathOrder::PrimaryKey => Self::PrimaryKey,
            FastPathOrder::SecondaryPrefix => Self::SecondaryPrefix,
            FastPathOrder::IndexRange => Self::IndexRange {
                fetch: route_plan
                    .index_range_limit_spec
                    .as_ref()
                    .map(|spec| spec.fetch),
            },
            FastPathOrder::PrimaryScan | FastPathOrder::Composite => Self::None,
        }
    }

    fn execute<E, I>(
        self,
        inputs: &I,
        route_plan: &ExecutionPlan,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<Option<FastPathKeyResult>, InternalError>
    where
        E: EntityKind + EntityValue,
        I: ExecutionInputsProjection<E>,
    {
        match self {
            Self::PrimaryKey => LoadExecutor::<E>::try_execute_pk_order_stream(
                inputs.ctx(),
                inputs.plan(),
                inputs.stream_bindings().direction(),
                route_plan.scan_hints.physical_fetch_hint,
            ),
            Self::SecondaryPrefix => LoadExecutor::<E>::try_execute_secondary_index_order_stream(
                inputs.ctx(),
                inputs.plan(),
                inputs.stream_bindings().index_prefix_specs.first(),
                inputs.stream_bindings().direction(),
                route_plan.scan_hints.physical_fetch_hint,
                index_predicate_execution,
            ),
            Self::IndexRange { fetch: Some(fetch) } => {
                LoadExecutor::<E>::try_execute_index_range_limit_pushdown_stream(
                    inputs.ctx(),
                    inputs.plan(),
                    inputs.stream_bindings().index_range_specs.first(),
                    inputs.stream_bindings().continuation,
                    fetch,
                    index_predicate_execution,
                )
            }
            Self::IndexRange { fetch: None } | Self::None => Ok(None),
        }
    }
}

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    /// Resolve one canonical execution key stream in fast-path precedence order.
    ///
    /// This is the single shared load key-stream resolver boundary.
    pub(in crate::db::executor) fn resolve_execution_key_stream_without_distinct<I>(
        inputs: &I,
        route_plan: &ExecutionPlan,
        predicate_compile_mode: IndexCompilePolicy,
    ) -> Result<ResolvedExecutionKeyStream, InternalError>
    where
        I: ExecutionInputsProjection<E>,
    {
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
        let fast_path_decision = fast_path_strategy.resolve_fast_path_decision::<E, I>(
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
    fn resolve_execution_key_stream_from_decision<I>(
        fast_path_decision: FastPathDecision,
        inputs: &I,
        route_plan: &ExecutionPlan,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
        index_predicate_applied: bool,
        index_predicate_rejected_counter: &Cell<u64>,
    ) -> Result<ResolvedExecutionKeyStream, InternalError>
    where
        I: ExecutionInputsProjection<E>,
    {
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
    fn resolve_fallback_execution_key_stream<I>(
        inputs: &I,
        route_plan: &ExecutionPlan,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
        index_predicate_applied: bool,
        index_predicate_rejected_counter: &Cell<u64>,
    ) -> Result<ResolvedExecutionKeyStream, InternalError>
    where
        I: ExecutionInputsProjection<E>,
    {
        let fallback_fetch_hint =
            route_plan.fallback_physical_fetch_hint(inputs.stream_bindings().direction());
        let descriptor = AccessExecutionDescriptor::from_bindings(
            &inputs.plan().access,
            *inputs.stream_bindings(),
            fallback_fetch_hint,
            index_predicate_execution,
        );
        let key_stream = Self::resolve_routed_key_stream(
            inputs.ctx(),
            RoutedKeyStreamRequest::AccessDescriptor(descriptor),
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

    /// Evaluate fast-path routes in canonical precedence and return one decision.
    fn evaluate_fast_path<I>(
        inputs: &I,
        route_plan: &ExecutionPlan,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<FastPathDecision, InternalError>
    where
        I: ExecutionInputsProjection<E>,
    {
        // Guard fast-path spec arity up front so plan/runtime traversal drift
        // cannot silently consume the wrong spec in release builds.
        ensure_load_fast_path_spec_arity(
            route_plan.secondary_fast_path_eligible(),
            inputs.stream_bindings().index_prefix_specs.len(),
            route_plan.index_range_limit_fast_path_enabled(),
            inputs.stream_bindings().index_range_specs.len(),
        )?;

        let fast = try_first_verified_fast_path_hit(
            route_plan.fast_path_order(),
            |route| {
                Ok(route_plan
                    .load_fast_path_route_eligible(route)
                    .then_some(route))
            },
            |verified_route| {
                Self::try_execute_verified_load_fast_path(
                    inputs,
                    route_plan,
                    index_predicate_execution,
                    verified_route,
                )
            },
        )?;

        Ok(fast.map_or(FastPathDecision::None, FastPathDecision::Hit))
    }

    // Execute one verified fast-path route and return keys if the route produces them.
    fn try_execute_verified_load_fast_path<I>(
        inputs: &I,
        route_plan: &ExecutionPlan,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
        verified_route: FastPathOrder,
    ) -> Result<Option<FastPathKeyResult>, InternalError>
    where
        I: ExecutionInputsProjection<E>,
    {
        let handler = FastPathRouteHandler::resolve(route_plan, verified_route);

        handler.execute::<E, I>(inputs, route_plan, index_predicate_execution)
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
