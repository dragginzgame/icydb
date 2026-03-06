//! Module: query::trace
//! Responsibility: lightweight, deterministic trace projections for planned queries.
//! Does not own: query semantics, plan hashing primitives, or executor routing policy.
//! Boundary: read-only diagnostics surface assembled at query/session boundaries.

use crate::db::query::explain::ExplainPlan;

///
/// TraceExecutionStrategy
///
/// Trace-surface execution-shape label derived from executor strategy selection.
/// Keeps high-level route shape visible without exposing executor internals.
///
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TraceExecutionStrategy {
    PrimaryKey,
    Ordered,
    Grouped,
}

///
/// QueryTracePlan
///
/// Lightweight trace payload for one planned query.
/// Includes plan hash, selected access strategy summary, and logical explain output.
///
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryTracePlan {
    pub plan_hash: String,
    pub access_strategy: String,
    pub execution_strategy: Option<TraceExecutionStrategy>,
    pub explain: ExplainPlan,
}

impl QueryTracePlan {
    #[must_use]
    pub(in crate::db) const fn new(
        plan_hash: String,
        access_strategy: String,
        execution_strategy: Option<TraceExecutionStrategy>,
        explain: ExplainPlan,
    ) -> Self {
        Self {
            plan_hash,
            access_strategy,
            execution_strategy,
            explain,
        }
    }
}
