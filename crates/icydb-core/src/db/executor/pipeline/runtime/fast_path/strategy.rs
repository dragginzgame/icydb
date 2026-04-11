//! Module: db::executor::pipeline::runtime::fast_path::strategy
//! Resolves scalar pipeline fast-path eligibility before falling back to the
//! generic kernel.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
        executor::{
            ExecutionPlan,
            pipeline::contracts::{ExecutionInputs, FastPathKeyResult},
            route::{
                FastPathOrder, ensure_load_fast_path_spec_arity, try_first_verified_fast_path_hit,
            },
        },
        index::predicate::IndexPredicateExecution,
    },
    error::InternalError,
};

///
/// FastPathResolutionStrategy
///
/// Strategy selected once from route shape so key-stream resolution does not
/// branch inline on fast-path eligibility policy.
///

pub(super) enum FastPathResolutionStrategy {
    StreamingFastPathFirst,
    FallbackOnly,
}

impl FastPathResolutionStrategy {
    pub(super) const fn for_route(route_plan: &ExecutionPlan) -> Self {
        if route_plan.is_streaming() {
            Self::StreamingFastPathFirst
        } else {
            Self::FallbackOnly
        }
    }

    pub(super) fn resolve_fast_path_decision(
        self,
        inputs: &ExecutionInputs<'_>,
        route_plan: &ExecutionPlan,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<Option<FastPathKeyResult>, InternalError> {
        match self {
            Self::StreamingFastPathFirst => {
                evaluate_fast_path(inputs, route_plan, index_predicate_execution)
            }
            Self::FallbackOnly => Ok(None),
        }
    }
}

///
/// FastPathRouteHandler
///
/// Strategy selected once from verified fast-path route so route-specific stream
/// execution stays centralized.
///

pub(super) enum FastPathRouteHandler {
    PrimaryKey,
    SecondaryPrefix,
    IndexRange { fetch: Option<usize> },
    None,
}

impl FastPathRouteHandler {
    pub(super) fn resolve(route_plan: &ExecutionPlan, verified_route: FastPathOrder) -> Self {
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

    pub(super) fn execute(
        self,
        inputs: &ExecutionInputs<'_>,
        route_plan: &ExecutionPlan,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<Option<FastPathKeyResult>, InternalError> {
        match self {
            Self::PrimaryKey => inputs.runtime().try_execute_pk_order_stream(
                inputs.plan(),
                inputs.stream_bindings().direction(),
                route_plan.scan_hints.physical_fetch_hint,
            ),
            Self::SecondaryPrefix => inputs.runtime().try_execute_secondary_index_order_stream(
                inputs.plan(),
                inputs.stream_bindings().index_prefix_specs.first(),
                inputs.stream_bindings().direction(),
                route_plan.scan_hints.physical_fetch_hint,
                index_predicate_execution,
            ),
            Self::IndexRange { fetch: Some(fetch) } => inputs
                .runtime()
                .try_execute_index_range_limit_pushdown_stream(
                    inputs.plan(),
                    inputs.stream_bindings().index_range_specs.first(),
                    inputs.stream_bindings().continuation,
                    fetch,
                    index_predicate_execution,
                ),
            Self::IndexRange { fetch: None } | Self::None => Ok(None),
        }
    }
}

/// Evaluate fast-path routes in canonical precedence and return one decision.
pub(super) fn evaluate_fast_path(
    inputs: &ExecutionInputs<'_>,
    route_plan: &ExecutionPlan,
    index_predicate_execution: Option<IndexPredicateExecution<'_>>,
) -> Result<Option<FastPathKeyResult>, InternalError> {
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
            try_execute_verified_load_fast_path(
                inputs,
                route_plan,
                index_predicate_execution,
                verified_route,
            )
        },
    )?;

    Ok(fast)
}

// Execute one verified fast-path route and return keys if the route produces them.
fn try_execute_verified_load_fast_path(
    inputs: &ExecutionInputs<'_>,
    route_plan: &ExecutionPlan,
    index_predicate_execution: Option<IndexPredicateExecution<'_>>,
    verified_route: FastPathOrder,
) -> Result<Option<FastPathKeyResult>, InternalError> {
    let handler = FastPathRouteHandler::resolve(route_plan, verified_route);

    handler.execute(inputs, route_plan, index_predicate_execution)
}
