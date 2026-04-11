//! Module: db::executor::diagnostics
//! Responsibility: executor-scoped diagnostics contracts for node/counter correlation.
//! Does not own: explain rendering, metrics sink persistence, or route behavior.
//! Boundary: additive observability types consumed by executor-local diagnostics paths.

#[cfg(test)]
pub(crate) mod counters;
#[cfg(test)]
pub(crate) mod node;

pub(in crate::db::executor) use crate::db::diagnostics::ExecutionOptimization;
pub(in crate::db::executor) use crate::db::diagnostics::ExecutionTrace;
