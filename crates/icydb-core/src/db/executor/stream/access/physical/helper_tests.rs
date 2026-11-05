//! Test-only constructors and adapters for the parent module.

use super::*;

impl PrimaryRangeKeyStream {
    pub(in crate::db::executor) fn new(
        store: StoreHandle,
        start: DecodedDataStoreKey,
        end: DecodedDataStoreKey,
        direction: Direction,
        limit: Option<usize>,
    ) -> Result<Self, InternalError> {
        Self::new_bounds(
            store,
            start.entity_tag(),
            Some(start),
            Some(end),
            direction,
            limit,
        )
    }
}
