//! Module: diagnostics::store_counters
//! Responsibility: shared diagnostics snapshots of physical store/index counters.
//! Does not own: store counter mutation or query attribution DTO shaping.
//! Boundary: reads store-global counters and returns saturating per-call deltas.

use crate::db::{DataStore, IndexStore};

///
/// StoreCounterSnapshot
///
/// StoreCounterSnapshot captures the physical store/index counter state at one
/// diagnostics boundary and can later produce a saturating delta from that
/// boundary. SQL and fluent attribution both use this shape so store work is
/// reported consistently across frontends.
///

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(in crate::db) struct StoreCounterSnapshot {
    pub(in crate::db) data_store_get_calls: u64,
    pub(in crate::db) index_store_get_calls: u64,
    pub(in crate::db) index_store_range_scan_calls: u64,
    pub(in crate::db) index_store_entry_reads: u64,
}

impl StoreCounterSnapshot {
    #[must_use]
    pub(in crate::db) fn capture() -> Self {
        Self {
            data_store_get_calls: DataStore::current_get_call_count(),
            index_store_get_calls: IndexStore::current_get_call_count(),
            index_store_range_scan_calls: IndexStore::current_range_scan_call_count(),
            index_store_entry_reads: IndexStore::current_entry_read_count(),
        }
    }

    #[must_use]
    pub(in crate::db) fn delta_since(self) -> Self {
        let current = Self::capture();

        Self {
            data_store_get_calls: current
                .data_store_get_calls
                .saturating_sub(self.data_store_get_calls),
            index_store_get_calls: current
                .index_store_get_calls
                .saturating_sub(self.index_store_get_calls),
            index_store_range_scan_calls: current
                .index_store_range_scan_calls
                .saturating_sub(self.index_store_range_scan_calls),
            index_store_entry_reads: current
                .index_store_entry_reads
                .saturating_sub(self.index_store_entry_reads),
        }
    }
}
