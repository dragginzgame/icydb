use crate::{
    db::{
        cursor::ContinuationSignature,
        direction::Direction,
        executor::{
            ContinuationEngine, ExecutionTrace, load::PageCursor,
            plan_metrics::GroupedPlanMetricsStrategy,
        },
        query::plan::GroupedContinuationWindow,
    },
    error::InternalError,
    value::Value,
};

///
/// GroupedPaginationWindow
///
/// Runtime grouped pagination projection consumed by grouped fold/page stages.
/// Separates grouped paging primitives from route/fold call signatures so grouped
/// continuation window semantics flow through one runtime boundary object.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::executor::load) struct GroupedPaginationWindow {
    limit: Option<usize>,
    initial_offset_for_page: usize,
    selection_bound: Option<usize>,
    resume_initial_offset: u32,
    resume_boundary: Option<Value>,
}

impl GroupedPaginationWindow {
    /// Build runtime grouped pagination projection from planner continuation window contract.
    #[must_use]
    pub(in crate::db::executor::load) fn from_contract(window: GroupedContinuationWindow) -> Self {
        let (
            limit,
            initial_offset_for_page,
            selection_bound,
            resume_initial_offset,
            resume_boundary,
        ) = window.into_parts();

        Self {
            limit,
            initial_offset_for_page,
            selection_bound,
            resume_initial_offset,
            resume_boundary,
        }
    }

    /// Return grouped page limit for this execution window.
    #[must_use]
    pub(in crate::db::executor::load) const fn limit(&self) -> Option<usize> {
        self.limit
    }

    /// Return grouped page-initial offset for this execution window.
    #[must_use]
    pub(in crate::db::executor::load) const fn initial_offset_for_page(&self) -> usize {
        self.initial_offset_for_page
    }

    /// Return bounded grouped candidate selection cap (`offset + limit + 1`) when active.
    #[must_use]
    pub(in crate::db::executor::load) const fn selection_bound(&self) -> Option<usize> {
        self.selection_bound
    }

    /// Return resume offset encoded into grouped continuation tokens.
    #[must_use]
    pub(in crate::db::executor::load) const fn resume_initial_offset(&self) -> u32 {
        self.resume_initial_offset
    }

    /// Borrow optional grouped resume boundary value for continuation filtering.
    #[must_use]
    pub(in crate::db::executor::load) const fn resume_boundary(&self) -> Option<&Value> {
        self.resume_boundary.as_ref()
    }
}

///
/// GroupedContinuationContext
///
/// Runtime grouped continuation context derived from immutable continuation
/// contracts. Carries grouped continuation signature, boundary arity, and one
/// grouped pagination projection bundle consumed by grouped runtime stages.
///

pub(in crate::db::executor::load) struct GroupedContinuationContext {
    continuation_signature: ContinuationSignature,
    continuation_boundary_arity: usize,
    grouped_pagination_window: GroupedPaginationWindow,
}

impl GroupedContinuationContext {
    /// Construct grouped continuation runtime context from grouped contract values.
    #[must_use]
    pub(in crate::db::executor::load) const fn new(
        continuation_signature: ContinuationSignature,
        continuation_boundary_arity: usize,
        grouped_pagination_window: GroupedPaginationWindow,
    ) -> Self {
        Self {
            continuation_signature,
            continuation_boundary_arity,
            grouped_pagination_window,
        }
    }

    /// Borrow grouped runtime pagination projection.
    #[must_use]
    pub(in crate::db::executor::load) const fn grouped_pagination_window(
        &self,
    ) -> &GroupedPaginationWindow {
        &self.grouped_pagination_window
    }

    /// Build one grouped next cursor after validating grouped boundary arity.
    pub(in crate::db::executor::load) fn grouped_next_cursor(
        &self,
        last_group_key: Vec<Value>,
    ) -> Result<PageCursor, InternalError> {
        if last_group_key.len() != self.continuation_boundary_arity {
            return Err(invariant(format!(
                "grouped continuation boundary arity mismatch: expected {}, found {}",
                self.continuation_boundary_arity,
                last_group_key.len()
            )));
        }

        Ok(PageCursor::Grouped(
            ContinuationEngine::grouped_next_cursor_token(
                self.continuation_signature,
                last_group_key,
                self.grouped_pagination_window.resume_initial_offset(),
            ),
        ))
    }
}

///
/// GroupedRuntimeProjection
///
/// Runtime grouped execution projection shared across grouped stream/fold/output
/// stages. Keeps routed direction, grouped plan-metrics strategy, and optional
/// execution trace under one runtime-boundary object.
///

pub(in crate::db::executor::load) struct GroupedRuntimeProjection {
    direction: Direction,
    grouped_plan_metrics_strategy: GroupedPlanMetricsStrategy,
    execution_trace: Option<ExecutionTrace>,
}

impl GroupedRuntimeProjection {
    /// Construct grouped runtime projection from routed direction/metrics/trace.
    #[must_use]
    pub(in crate::db::executor::load) const fn new(
        direction: Direction,
        grouped_plan_metrics_strategy: GroupedPlanMetricsStrategy,
        execution_trace: Option<ExecutionTrace>,
    ) -> Self {
        Self {
            direction,
            grouped_plan_metrics_strategy,
            execution_trace,
        }
    }

    /// Return routed grouped stream direction.
    #[must_use]
    pub(in crate::db::executor::load) const fn direction(&self) -> Direction {
        self.direction
    }

    /// Return grouped plan-metrics strategy for grouped stream observability.
    #[must_use]
    pub(in crate::db::executor::load) const fn grouped_plan_metrics_strategy(
        &self,
    ) -> GroupedPlanMetricsStrategy {
        self.grouped_plan_metrics_strategy
    }

    /// Borrow optional grouped execution trace for observability mutation.
    pub(in crate::db::executor::load) const fn execution_trace_mut(
        &mut self,
    ) -> &mut Option<ExecutionTrace> {
        &mut self.execution_trace
    }

    /// Consume projection and return final grouped execution trace payload.
    pub(in crate::db::executor::load) const fn into_execution_trace(
        self,
    ) -> Option<ExecutionTrace> {
        self.execution_trace
    }
}

///
/// GroupedExecutionContext
///
/// Grouped runtime execution context artifacts derived at grouped route stage.
/// Keeps cursor/runtime direction, continuation signature, trace, and grouped
/// metrics strategy together for grouped stream/fold/output stages.
///

pub(in crate::db::executor::load) struct GroupedExecutionContext {
    continuation: GroupedContinuationContext,
    runtime: GroupedRuntimeProjection,
}

impl GroupedExecutionContext {
    /// Construct grouped execution context from continuation + runtime projection.
    #[must_use]
    pub(in crate::db::executor::load) const fn new(
        continuation: GroupedContinuationContext,
        runtime: GroupedRuntimeProjection,
    ) -> Self {
        Self {
            continuation,
            runtime,
        }
    }

    /// Return routed grouped stream direction.
    #[must_use]
    pub(in crate::db::executor::load) const fn direction(&self) -> Direction {
        self.runtime.direction()
    }

    /// Return grouped plan-metrics strategy for grouped stream observability.
    #[must_use]
    pub(in crate::db::executor::load) const fn grouped_plan_metrics_strategy(
        &self,
    ) -> GroupedPlanMetricsStrategy {
        self.runtime.grouped_plan_metrics_strategy()
    }

    /// Borrow grouped continuation context.
    #[must_use]
    pub(in crate::db::executor::load) const fn continuation(&self) -> &GroupedContinuationContext {
        &self.continuation
    }

    /// Borrow optional grouped execution trace for observability mutation.
    pub(in crate::db::executor::load) const fn execution_trace_mut(
        &mut self,
    ) -> &mut Option<ExecutionTrace> {
        self.runtime.execution_trace_mut()
    }

    /// Consume grouped execution context and return final grouped execution trace payload.
    pub(in crate::db::executor::load) fn into_execution_trace(self) -> Option<ExecutionTrace> {
        self.runtime.into_execution_trace()
    }
}

fn invariant(message: impl Into<String>) -> InternalError {
    InternalError::query_executor_invariant(message)
}
