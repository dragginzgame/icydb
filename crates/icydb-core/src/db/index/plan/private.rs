//! Module: db::index::plan::private
//! Defines private helper types used to assemble index-planning decisions
//! before exposing canonical plan shapes.
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
/// SealedStructuralPrimaryRowReader
///
/// Internal marker used to seal nongeneric structural primary-row readers.
///

pub(in crate::db) trait SealedStructuralPrimaryRowReader {}

///
/// SealedIndexEntryReader
///
/// Internal marker used to seal `IndexEntryReader` implementations.
///

pub(in crate::db) trait SealedIndexEntryReader<E: EntityKind + EntityValue> {}

///
/// SealedStructuralIndexEntryReader
///
/// Internal marker used to seal nongeneric structural index-entry readers.
///

pub(in crate::db) trait SealedStructuralIndexEntryReader {}
