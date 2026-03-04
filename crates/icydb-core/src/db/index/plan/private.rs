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
