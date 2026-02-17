//! Observability: runtime event telemetry (metrics) and storage snapshots.

pub(crate) mod metrics;
pub(crate) mod sink;
pub(crate) mod snapshot;

// re-exports
pub use metrics::EventReport;
pub use sink::{MetricsSink, metrics_report, metrics_reset_all};
pub use snapshot::{StorageReport, storage_report};
