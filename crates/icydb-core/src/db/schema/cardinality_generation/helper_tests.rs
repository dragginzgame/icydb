//! Test-only constructors and adapters for the parent module.

use super::*;

impl CardinalitySourceIdentity {
    /// Derive one source identity from current runtime authorities.
    pub(in crate::db) fn derive(
        database_incarnation: DatabaseIncarnationId,
        allocations: StoreAllocationIdentities,
        accepted_root: Option<CardinalityAcceptedRootIdentity>,
        accepted_indexes: impl IntoIterator<Item = IndexId>,
        fold_watermark: FoldWatermark,
    ) -> Result<Self, InternalError> {
        let accepted_index_set = CardinalityAcceptedIndexSetIdentity::derive(accepted_indexes)?;
        let store_allocation = CardinalityStoreAllocationIdentity::derive(allocations)?;
        Self::derive_with_bound_identities(
            database_incarnation,
            store_allocation,
            accepted_root,
            accepted_index_set,
            fold_watermark,
        )
    }
}

impl CardinalityBuildTotals {
    #[must_use]
    pub(in crate::db) const fn source_entries(self) -> u64 {
        self.source_entries
    }

    #[must_use]
    pub(in crate::db) const fn source_bytes(self) -> u64 {
        self.source_bytes
    }

    #[must_use]
    pub(in crate::db) const fn prefix_updates(self) -> u64 {
        self.prefix_updates
    }

    #[must_use]
    pub(in crate::db) const fn distinct_count_keys(self) -> u64 {
        self.distinct_count_keys
    }
}
