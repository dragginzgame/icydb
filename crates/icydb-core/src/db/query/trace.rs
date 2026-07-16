//! Module: query::trace
//! Responsibility: lightweight, deterministic trace projections for planned queries.
//! Does not own: query semantics, plan hashing primitives, or executor routing policy.
//! Boundary: read-only diagnostics surface assembled at query/session boundaries.

use crate::db::query::explain::ExplainPlan;

///
/// TraceExecutionFamily
///
/// TraceExecutionFamily is the query-facing execution-family label derived at
/// the session boundary after executor route selection.
/// It keeps high-level trace shape visible without making query diagnostics
/// depend on executor-owned route types.
///
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TraceExecutionFamily {
    PrimaryKey,
    Ordered,
    Grouped,
}

///
/// TraceReuseEvent
///
/// Trace-surface semantic reuse result for one query planning attempt.
/// Reuse always refers to the shared prepared query plan, so the event owns
/// only the exact-match hit or miss outcome.
///
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TraceReuseEvent {
    /// The shared prepared query plan matched the current semantic identity.
    Hit,
    /// No shared prepared query plan matched the current semantic identity.
    Miss,
}

impl TraceReuseEvent {
    /// Return true when this event represents a semantic-reuse hit.
    #[must_use]
    pub const fn is_hit(self) -> bool {
        matches!(self, Self::Hit)
    }
}

///
/// QueryTracePlan
///
/// Lightweight trace payload for one planned query.
/// Includes plan hash, selected access strategy summary, reuse attribution,
/// and logical explain output.
///
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryTracePlan {
    plan_hash: String,
    access_strategy: String,
    execution_family: Option<TraceExecutionFamily>,
    reuse: TraceReuseEvent,
    explain: ExplainPlan,
}

impl QueryTracePlan {
    /// Construct one query trace payload.
    #[must_use]
    pub const fn new(
        plan_hash: String,
        access_strategy: String,
        execution_family: Option<TraceExecutionFamily>,
        reuse: TraceReuseEvent,
        explain: ExplainPlan,
    ) -> Self {
        Self {
            plan_hash,
            access_strategy,
            execution_family,
            reuse,
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

    /// Return semantic-reuse attribution for this trace build.
    #[must_use]
    pub const fn reuse(&self) -> TraceReuseEvent {
        self.reuse
    }

    /// Borrow planner explain output carried in this trace payload.
    #[must_use]
    pub const fn explain(&self) -> &ExplainPlan {
        &self.explain
    }
}
