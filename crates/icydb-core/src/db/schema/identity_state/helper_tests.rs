//! Test-only constructors and adapters for the parent module.

use super::*;

impl IdentityStatementCursor {
    #[must_use]
    pub(in crate::db) const fn owner(&self) -> IdentityStateOwner {
        self.owner
    }

    #[must_use]
    pub(in crate::db) const fn expected_high_water(&self) -> u128 {
        self.expected_high_water
    }

    #[must_use]
    pub(in crate::db) const fn tentative_high_water(&self) -> u128 {
        self.tentative_high_water
    }

    #[must_use]
    pub(in crate::db) const fn allocation_count(&self) -> u32 {
        self.allocation_count
    }
}
