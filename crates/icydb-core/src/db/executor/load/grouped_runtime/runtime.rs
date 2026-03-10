//! Module: db::executor::load::grouped_runtime::runtime
//! Responsibility: module-local ownership and contracts for db::executor::load::grouped_runtime::runtime.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::db::{
    direction::Direction,
    executor::{
        ExecutionTrace, load::grouped_runtime::GroupedContinuationContext,
        plan_metrics::GroupedPlanMetricsStrategy,
    },
};

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
