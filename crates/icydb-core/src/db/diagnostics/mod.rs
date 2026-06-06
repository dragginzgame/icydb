//! Module: diagnostics
//! Responsibility: read-only storage footprint and integrity snapshots.
//! Does not own: recovery, write-path mutation, or query planning semantics.
//! Boundary: consumes `Db`/store read APIs and returns DTO snapshots.

pub(in crate::db) mod execution_trace;
mod integrity;
#[cfg(any(feature = "diagnostics", feature = "sql"))]
mod local_instructions;
pub(in crate::db) mod model;
mod storage_report;
#[cfg(test)]
mod tests;

pub(in crate::db) use execution_trace::{
    ExecutionAccessPathVariant, ExecutionMetrics, ExecutionOptimization, ExecutionStats,
    ExecutionTrace,
};
pub(in crate::db) use integrity::integrity_report;
pub(in crate::db) use integrity::integrity_report_after_recovery;
#[cfg(any(feature = "diagnostics", feature = "sql"))]
pub(in crate::db) use local_instructions::measure_local_instruction_delta;
#[cfg(feature = "diagnostics")]
pub(in crate::db) use local_instructions::read_local_instruction_counter;
pub(in crate::db) use model::{
    DataStoreSnapshot, EntitySnapshot, IndexStoreSnapshot, IntegrityReport, IntegrityStoreSnapshot,
    IntegrityTotals, SchemaStoreSnapshot, StorageReport, StoreSnapshotStorageMode,
};
pub(crate) use model::{
    IndexStoreSnapshotStats, StoreSnapshotAllocationIdentity, StoreSnapshotSchemaMetadata,
};
pub(in crate::db) use storage_report::{storage_report, storage_report_default};
