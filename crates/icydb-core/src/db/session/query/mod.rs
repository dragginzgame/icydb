//! Module: db::session::query
//! Responsibility: session-bound query planning, explain, and cursor execution
//! helpers that recover store visibility before delegating to query-owned logic.
//! Does not own: query intent construction or executor runtime semantics.
//! Boundary: resolves session visibility and cursor policy before handing work to the planner/executor.

mod cache;
#[cfg(feature = "diagnostics")]
mod diagnostics;
mod execution;
mod explain;
mod fluent;
mod paging;
mod planning;

pub(in crate::db) use cache::QueryPlanCacheAttribution;
#[cfg(test)]
pub(in crate::db) use cache::QueryPlanVisibility;
pub(in crate::db::session) use cache::query_plan_cache_reuse_event;
#[cfg(feature = "diagnostics")]
pub use diagnostics::{
    DirectDataRowAttribution, GroupedCountAttribution, GroupedExecutionAttribution,
    QueryExecutionAttribution,
};
pub(in crate::db::session) use execution::query_error_from_executor_plan_error;
#[cfg(feature = "diagnostics")]
pub(in crate::db::session::query) use execution::{
    PreparedQueryExecutionOutcome, PreparedQueryExecutionOutput,
};
