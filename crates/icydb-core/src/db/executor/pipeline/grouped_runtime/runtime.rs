//! Module: db::executor::pipeline::grouped_runtime::runtime
//! Responsibility: module-local ownership and contracts for db::executor::pipeline::grouped_runtime::runtime.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::db::{
    direction::Direction,
    executor::{
        ExecutionTrace, GroupedContinuationContext, plan_metrics::GroupedPlanMetricsStrategy,
    },
};

///
/// GroupedExecutionContext
///
/// Grouped runtime execution context artifacts derived at grouped route stage.
/// Keeps cursor/runtime direction, continuation signature, trace, and grouped
/// metrics strategy together for grouped stream/fold/output stages.
///

pub(in crate::db::executor) struct GroupedExecutionContext {
    continuation: GroupedContinuationContext,
    direction: Direction,
    grouped_plan_metrics_strategy: GroupedPlanMetricsStrategy,
    execution_trace: Option<ExecutionTrace>,
}

impl GroupedExecutionContext {
    /// Construct grouped execution context from continuation + route/runtime artifacts.
    #[must_use]
    pub(in crate::db::executor) const fn new(
        continuation: GroupedContinuationContext,
        direction: Direction,
        grouped_plan_metrics_strategy: GroupedPlanMetricsStrategy,
        execution_trace: Option<ExecutionTrace>,
    ) -> Self {
        Self {
            continuation,
            direction,
            grouped_plan_metrics_strategy,
            execution_trace,
        }
    }

    /// Return routed grouped stream direction.
    #[must_use]
    pub(in crate::db::executor) const fn direction(&self) -> Direction {
        self.direction
    }

    /// Return grouped plan-metrics strategy for grouped stream observability.
    #[must_use]
    pub(in crate::db::executor) const fn grouped_plan_metrics_strategy(
        &self,
    ) -> GroupedPlanMetricsStrategy {
        self.grouped_plan_metrics_strategy
    }

    /// Borrow grouped continuation context.
    #[must_use]
    pub(in crate::db::executor) const fn continuation(&self) -> &GroupedContinuationContext {
        &self.continuation
    }

    /// Borrow optional grouped execution trace for observability mutation.
    pub(in crate::db::executor) const fn execution_trace_mut(
        &mut self,
    ) -> &mut Option<ExecutionTrace> {
        &mut self.execution_trace
    }

    /// Consume grouped execution context and return final grouped execution trace payload.
    pub(in crate::db::executor) fn into_execution_trace(self) -> Option<ExecutionTrace> {
        self.execution_trace
    }
}
