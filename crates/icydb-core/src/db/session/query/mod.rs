//! Module: db::session::query
//! Responsibility: session-bound query planning, explain, and cursor execution
//! helpers that recover store visibility before delegating to query-owned logic.
//! Does not own: query intent construction or executor runtime semantics.
//! Boundary: resolves session visibility and cursor policy before handing work to the planner/executor.

mod cache;
#[cfg(all(feature = "sql", feature = "diagnostics"))]
mod diagnostics;
mod dynamic;
mod exact_key;
mod grouped;
mod projection;

use crate::db::{QueryError, executor::ExecutorPlanError};

pub(in crate::db) use cache::QueryPlanCacheAttribution;
#[cfg(all(feature = "sql", feature = "diagnostics"))]
pub(in crate::db) use cache::QueryPlanCompilePhaseAttribution;
#[cfg(feature = "sql")]
pub(in crate::db::session) use cache::query_plan_cache_reuse_event;
#[cfg(all(test, feature = "sql", feature = "diagnostics"))]
pub(in crate::db) use cache::shared_query_plan_cache_len_for_tests;
#[cfg(all(feature = "sql", feature = "diagnostics"))]
pub use diagnostics::{
    DirectDataRowAttribution, GroupedCountAttribution, GroupedExecutionAttribution,
    KernelRowAttribution, ScalarAggregateAttribution,
};
#[doc(hidden)]
pub use exact_key::{
    MAX_TYPED_EXACT_KEY_BATCH_INPUT_BYTES, MAX_TYPED_EXACT_KEY_BATCH_ITEMS,
    MAX_TYPED_EXACT_KEY_BATCH_RESULT_BYTES, MAX_TYPED_EXACT_KEY_BATCH_STORED_BYTES,
};
#[cfg(feature = "sql")]
pub(in crate::db) use projection::StructuralProjectionContract;
#[cfg(feature = "sql")]
pub(in crate::db::session) use projection::StructuralProjectionPayload;
#[cfg(feature = "sql")]
pub(in crate::db::session) use projection::projection_labels_from_projection_spec;

// Convert executor plan-surface failures at the session boundary so query
// errors do not import executor-owned error enums.
pub(in crate::db::session) fn query_error_from_executor_plan_error(
    err: ExecutorPlanError,
) -> QueryError {
    match err {
        ExecutorPlanError::Cursor(err) => QueryError::from_cursor_plan_error(*err),
    }
}
