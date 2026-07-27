//! Module: diagnostics
//! Responsibility: read-only storage footprint and execution diagnostics.
//! Does not own: recovery, write-path mutation, or query planning semantics.
//! Boundary: consumes `Db`/store read APIs and returns DTO snapshots.

#[cfg(any(test, feature = "sql"))]
mod execution_trace;
#[cfg(any(feature = "diagnostics", feature = "sql"))]
mod local_instructions;
mod model;
mod storage_report;
#[cfg(all(feature = "sql", feature = "diagnostics"))]
mod store_counters;
#[cfg(any(test, feature = "sql"))]
pub use execution_trace::{
    ExecutionAccessPathVariant, ExecutionMetrics, ExecutionOptimization, ExecutionStats,
    ExecutionTrace,
};
#[cfg(any(test, feature = "sql"))]
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
pub(in crate::db) use storage_report::{storage_report, storage_report_default};
#[cfg(all(feature = "sql", feature = "diagnostics"))]
pub(in crate::db) use store_counters::StoreCounterSnapshot;
