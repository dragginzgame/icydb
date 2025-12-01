//! Observability: runtime event telemetry (metrics) and storage snapshots.

pub mod metrics;
pub mod snapshot;

// Convenient re-exports
pub use metrics::{
    EntityCounters, EntitySummary, EventOps, EventPerf, EventReport, EventSelect, EventState, Span,
    report as event_report, reset_all as event_reset_all,
};
pub use snapshot::{
    DataStoreSnapshot, EntitySnapshot, IndexStoreSnapshot, StorageReport, storage_report,
};
