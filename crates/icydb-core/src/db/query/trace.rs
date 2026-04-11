//! Module: query::trace
//! Responsibility: lightweight, deterministic trace projections for planned queries.
//! Does not own: query semantics, plan hashing primitives, or executor routing policy.
//! Boundary: read-only diagnostics surface assembled at query/session boundaries.

use crate::db::{executor::ExecutionFamily, query::explain::ExplainPlan};

///
/// TraceExecutionFamily
///
/// Trace-surface execution-family label derived from executor family selection.
/// Keeps high-level route shape visible without exposing executor internals.
///
pub type TraceExecutionFamily = ExecutionFamily;

///
/// QueryTracePlan
///
/// Lightweight trace payload for one planned query.
/// Includes plan hash, selected access strategy summary, and logical explain output.
///
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryTracePlan {
    pub(crate) plan_hash: String,
    pub(crate) access_strategy: String,
    pub(crate) execution_family: Option<TraceExecutionFamily>,
    pub(crate) explain: ExplainPlan,
}

impl QueryTracePlan {
    /// Construct one query trace payload.
    #[must_use]
    pub const fn new(
        plan_hash: String,
        access_strategy: String,
        execution_family: Option<TraceExecutionFamily>,
        explain: ExplainPlan,
    ) -> Self {
        Self {
            plan_hash,
            access_strategy,
            execution_family,
            explain,
        }
    }

    /// Borrow the canonical explain fingerprint hash.
    #[must_use]
    pub const fn plan_hash(&self) -> &str {
        self.plan_hash.as_str()
    }

    /// Borrow the rendered access strategy summary.
    #[must_use]
    pub const fn access_strategy(&self) -> &str {
        self.access_strategy.as_str()
    }

    /// Return the selected execution family classification.
    #[must_use]
    pub const fn execution_family(&self) -> Option<TraceExecutionFamily> {
        self.execution_family
    }

    /// Borrow planner explain output carried in this trace payload.
    #[must_use]
    pub const fn explain(&self) -> &ExplainPlan {
        &self.explain
    }
}
