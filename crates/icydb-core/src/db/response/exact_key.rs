//! Module: response::exact_key
//! Responsibility: planner-free exact-key projection handoff.
//! Does not own: typed adapter decoding, admission limits, or store access.
//! Boundary: keeps one decoded projection per distinct input key plus the
//! original-position mapping required by public typed reads.

use crate::value::OutputValue;

/// Internal projection from one bounded exact-key batch.
#[doc(hidden)]
#[derive(Debug)]
pub struct ExactKeyBatchProjectionOutput {
    /// Accepted entity name used for the read.
    pub entity: String,
    /// Accepted fields in physical-slot order.
    pub columns: Vec<String>,
    /// One optional decoded row for each distinct input key.
    pub distinct_rows: Vec<Option<Vec<OutputValue>>>,
    /// Distinct-key index for each original input position.
    pub positions: Vec<u32>,
}
