//! Module: db::index::plan::private
//! Responsibility: module-local ownership and contracts for db::index::plan::private.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::traits::{EntityKind, EntityValue};

///
/// SealedPrimaryRowReader
///
/// Internal marker used to seal `PrimaryRowReader` implementations.
///

pub(in crate::db) trait SealedPrimaryRowReader<E: EntityKind + EntityValue> {}

///
/// SealedIndexEntryReader
///
/// Internal marker used to seal `IndexEntryReader` implementations.
///

pub(in crate::db) trait SealedIndexEntryReader<E: EntityKind + EntityValue> {}
