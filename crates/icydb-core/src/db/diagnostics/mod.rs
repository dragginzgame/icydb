//! Module: diagnostics
//! Responsibility: read-only storage footprint and execution diagnostics.
//! Does not own: recovery, write-path mutation, or query planning semantics.
//! Boundary: consumes `Db`/store read APIs and returns DTO snapshots.

mod execution_trace;
mod local_instructions;
mod model;
#[cfg(all(feature = "sql", feature = "diagnostics"))]
mod sql_structural;
mod storage_report;
#[cfg(all(feature = "sql", feature = "diagnostics"))]
mod store_counters;
pub use execution_trace::{
    ExecutionAccessPathVariant, ExecutionMetrics, ExecutionOptimization, ExecutionStats,
    ExecutionTrace,
};
pub(in crate::db) use local_instructions::measure_local_instruction_delta;
#[cfg(feature = "diagnostics")]
pub(in crate::db) use local_instructions::read_local_instruction_counter;
pub use model::{
    DataStoreSnapshot, EntitySnapshot, IndexStoreSnapshot, SchemaStoreSnapshot, StorageReport,
    StoreSnapshotStorageMode,
};
use model::{
    IndexStoreSnapshotStats, StoreSnapshotAllocationIdentity, StoreSnapshotSchemaMetadata,
};
#[cfg(all(feature = "sql", feature = "diagnostics"))]
pub use sql_structural::SqlStructuralWorkAttribution;
#[cfg(all(feature = "sql", feature = "diagnostics"))]
pub(in crate::db) use sql_structural::{
    begin_sql_structural_work_attribution, finish_sql_structural_work_attribution,
    record_sql_membership_authored, record_sql_membership_canonicalization,
    record_sql_membership_normalized, record_sql_prefix_branch_cap,
    record_sql_prefix_branch_deduplication, record_sql_prefix_exclusion_pruning,
    record_sql_range_conjunction, record_sql_range_physical_child,
    record_sql_residual_predicate_evaluation,
};
pub(in crate::db) use storage_report::{storage_report, storage_report_default};
#[cfg(all(feature = "sql", feature = "diagnostics"))]
pub(in crate::db) use store_counters::StoreCounterSnapshot;
