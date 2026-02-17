//! Observability: runtime telemetry (metrics) and sink abstractions.
//!
//! This module does not access storage internals directly.
//! Engine-level storage inspection lives in `db`.

pub(crate) mod metrics;
pub(crate) mod sink;

use crate::{db::Db, error::InternalError, traits::CanisterKind};

// re-exports
pub use crate::db::StorageReport;
pub use metrics::EventReport;
pub use sink::{MetricsSink, metrics_report, metrics_reset_all};

/// Build a point-in-time storage report for observability surfaces.
pub fn storage_report<C: CanisterKind>(
    db: &Db<C>,
    name_to_path: &[(&'static str, &'static str)],
) -> Result<StorageReport, InternalError> {
    db.storage_report(name_to_path)
}
