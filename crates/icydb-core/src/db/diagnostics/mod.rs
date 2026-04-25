//! Module: diagnostics
//! Responsibility: read-only storage footprint and integrity snapshots.
//! Does not own: recovery, write-path mutation, or query planning semantics.
//! Boundary: consumes `Db`/store read APIs and returns DTO snapshots.

mod execution_trace;
mod integrity;
mod model;
mod storage_report;
#[cfg(test)]
mod tests;

pub use execution_trace::{
    ExecutionAccessPathVariant, ExecutionMetrics, ExecutionOptimization, ExecutionStats,
    ExecutionTrace,
};
pub(crate) use integrity::integrity_report;
pub(in crate::db) use integrity::integrity_report_after_recovery;
pub use model::{
    DataStoreSnapshot, EntitySnapshot, IndexStoreSnapshot, IntegrityReport, IntegrityStoreSnapshot,
    IntegrityTotals, StorageReport,
};
pub(crate) use storage_report::{storage_report, storage_report_default};
