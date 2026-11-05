//! Module: db::session::query
//! Responsibility: session-bound query planning, explain, and cursor execution
//! helpers that recover store visibility before delegating to query-owned logic.
//! Does not own: query intent construction or executor runtime semantics.
//! Boundary: resolves session visibility and cursor policy before handing work to the planner/executor.

mod cache;
mod cardinality_tiebreak;
mod dynamic;
mod exact_count;
mod exact_key;
mod grouped;
mod projection;

#[cfg(feature = "sql")]
pub(in crate::db) use cache::QueryPlanCacheReuse;
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

///
/// TESTS
///

#[cfg(test)]
mod tests;
