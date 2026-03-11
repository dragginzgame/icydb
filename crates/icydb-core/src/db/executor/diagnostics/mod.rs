//! Module: db::executor::diagnostics
//! Responsibility: executor-scoped diagnostics contracts for node/counter correlation.
//! Does not own: explain rendering, metrics sink persistence, or route behavior.
//! Boundary: additive observability types consumed by executor-local diagnostics paths.

pub(crate) mod counters;
pub(crate) mod node;

pub(in crate::db::executor) use crate::db::diagnostics::ExecutionOptimization;
pub(in crate::db::executor) use crate::db::diagnostics::ExecutionOptimizationCounter;
pub(in crate::db::executor) use crate::db::diagnostics::ExecutionTrace;
pub(in crate::db::executor) use crate::db::diagnostics::record_execution_optimization_hit_for_tests;
#[cfg(test)]
pub(in crate::db::executor) use crate::db::diagnostics::take_execution_optimization_hits_for_tests;
