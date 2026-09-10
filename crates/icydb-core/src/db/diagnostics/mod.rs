//! Module: diagnostics
//! Responsibility: read-only storage footprint and execution diagnostics.
//! Does not own: recovery, write-path mutation, or query planning semantics.
//! Boundary: consumes `Db`/store read APIs and returns DTO snapshots.

mod memory_allocations;
mod model;
mod storage_report;

use model::{
    IndexStoreSnapshotStats, StoreSnapshotAllocationIdentity, StoreSnapshotSchemaMetadata,
};

pub use memory_allocations::{
    MemoryAllocation, MemoryAllocationBinding, MemoryAllocationRangeClaim, MemoryAllocations,
    MemoryExtent,
};
pub use model::{
    DataStoreSnapshot, EntitySnapshot, IndexStoreSnapshot, SchemaStoreSnapshot, StorageReport,
    StoreSnapshotStorageMode,
};
pub(in crate::db) use storage_report::{storage_report, storage_report_default};
