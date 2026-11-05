//! Test-only constructors and adapters for the parent module.

use super::*;

impl IndexStore {
    /// Validate and publish one positioned index effect for direct store tests.
    pub(in crate::db) fn publish_positioned_journal_entry(
        &mut self,
        key: RawIndexStoreKey,
        value: Option<IndexEntryValue>,
        position: JournalOverlayPosition,
    ) -> Result<Option<IndexEntryValue>, crate::error::InternalError> {
        self.preflight_positioned_journal_entry(&key, position)?;
        self.publish_preflighted_journal_entry(key, value, position)
    }

    /// Return the monotonic perf-only count of index entries yielded by traversal.
    pub(in crate::db) fn current_entry_read_count() -> u64 {
        INDEX_STORE_ENTRY_READ_COUNT.with(Cell::get)
    }
}

pub(super) fn record_index_store_entry_read() {
    INDEX_STORE_ENTRY_READ_COUNT.with(|count| {
        count.set(count.get().saturating_add(1));
    });
}

thread_local! {
    pub(super) static INDEX_STORE_ENTRY_READ_COUNT: Cell<u64> = const { Cell::new(0) };
}
