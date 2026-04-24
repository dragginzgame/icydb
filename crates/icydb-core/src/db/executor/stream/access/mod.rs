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

pub(in crate::db::executor) use bindings::ExecutableAccess;
pub(in crate::db::executor) use physical::{IndexRangeKeyStream, PrimaryRangeKeyStream};
pub(in crate::db::executor) use scan::{IndexScan, PrimaryScan};
pub(in crate::db::executor) use traversal::TraversalRuntime;
