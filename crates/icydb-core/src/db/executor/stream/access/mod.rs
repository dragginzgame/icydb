//! Module: executor::stream::access
//! Responsibility: physical access-plan traversal into ordered key streams.
//! Does not own: logical planning decisions or post-access row semantics.
//! Boundary: exclusive executor path for store/index iteration.

#[cfg(test)]
mod tests;

mod bindings;
mod physical;
mod scan;
mod traversal;

pub(in crate::db::executor) use bindings::{
    AccessStreamExecutionPolicy, ExecutableAccess, IndexLeafOrderPolicy,
};
pub(in crate::db::executor) use physical::{IndexRangeKeyStream, PrimaryRangeKeyStream};
pub(in crate::db::executor) use scan::{
    ACCESS_SCAN_CHUNK_ENTRIES, IndexScan, PrimaryScan, active_lowered_index_prefix_specs,
    apply_index_scan_chunk_progress, branch_stream_chunk_entries,
    index_predicate_rejects_prefix_components, index_stream_chunk_entries_for_remaining,
    index_stream_output_limit_for_chunk,
};
pub(in crate::db::executor) use traversal::TraversalRuntime;
