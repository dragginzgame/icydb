//! Module: diagnostics
//! Responsibility: read-only storage footprint and execution diagnostics.
//! Does not own: recovery, write-path mutation, or query planning semantics.
//! Boundary: consumes `Db`/store read APIs and returns DTO snapshots.

mod execution_trace;
mod model;
mod storage_report;
pub use execution_trace::{
    ExecutionAccessPathVariant, ExecutionMetrics, ExecutionOptimization, ExecutionStats,
    ExecutionTrace,
};
pub use model::{
    DataStoreSnapshot, EntitySnapshot, IndexStoreSnapshot, SchemaStoreSnapshot, StorageReport,
    StoreSnapshotStorageMode,
};
use model::{
    IndexStoreSnapshotStats, StoreSnapshotAllocationIdentity, StoreSnapshotSchemaMetadata,
};
pub(in crate::db) use storage_report::{storage_report, storage_report_default};
