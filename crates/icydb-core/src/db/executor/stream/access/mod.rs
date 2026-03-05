//! Module: executor::stream::access
//! Responsibility: physical access-plan traversal into ordered key streams.
//! Does not own: logical planning decisions or post-access row semantics.
//! Boundary: exclusive executor path for store/index iteration.

#[cfg(test)]
mod tests;

mod bindings;
mod physical;
mod scan;
mod traversal;

pub(in crate::db) use self::bindings::AccessScanContinuationInput;
pub(in crate::db::executor) use self::bindings::{AccessExecutionDescriptor, AccessStreamBindings};
#[cfg(test)]
pub(in crate::db) use self::bindings::{IndexStreamConstraints, StreamExecutionHints};
pub(in crate::db::executor) use self::scan::{IndexScan, PrimaryScan};
