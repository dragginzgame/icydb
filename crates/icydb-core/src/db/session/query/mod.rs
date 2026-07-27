//! Module: db::session::query
//! Responsibility: session-bound query planning, explain, and cursor execution
//! helpers that recover store visibility before delegating to query-owned logic.
//! Does not own: query intent construction or executor runtime semantics.
//! Boundary: resolves session visibility and cursor policy before handing work to the planner/executor.

mod cache;
#[cfg(feature = "diagnostics")]
mod diagnostics;
#[cfg(feature = "sql")]
mod dynamic;
#[cfg(feature = "sql")]
mod grouped;

use crate::db::{QueryError, executor::ExecutorPlanError};

pub(in crate::db) use cache::QueryPlanCacheAttribution;
#[cfg(feature = "diagnostics")]
pub(in crate::db) use cache::QueryPlanCompilePhaseAttribution;
#[cfg(any(test, feature = "sql-explain"))]
pub(in crate::db::session) use cache::query_plan_cache_reuse_event;
#[cfg(feature = "diagnostics")]
pub use diagnostics::{
    DirectDataRowAttribution, GroupedCountAttribution, GroupedExecutionAttribution,
    KernelRowAttribution, ScalarAggregateAttribution,
};

// Convert executor plan-surface failures at the session boundary so query
// errors do not import executor-owned error enums.
pub(in crate::db::session) fn query_error_from_executor_plan_error(
    err: ExecutorPlanError,
) -> QueryError {
    match err {
        ExecutorPlanError::Cursor(err) => QueryError::from_cursor_plan_error(*err),
    }
}
