//! Test-only constructors and adapters for the parent module.

use super::*;

impl DataStore {
    /// Insert one raw row directly for corruption-focused test setup only.
    pub(in crate::db) fn insert_raw_for_test(
        &mut self,
        key: RawDataStoreKey,
        row: RawRow,
    ) -> Result<Option<RawRow>, InternalError> {
        self.insert_raw(key, row)
    }

    /// Validate and publish one positioned row for direct store tests.
    pub(in crate::db) fn publish_positioned_journal_entry(
        &mut self,
        key: RawDataStoreKey,
        row: Option<RawRow>,
        position: JournalOverlayPosition,
    ) -> Result<(), crate::error::InternalError> {
        self.preflight_positioned_journal_entry(&key, position)?;
        self.publish_preflighted_journal_entry(key, row, position)
    }

    /// Return an exact row count for tests without exposing the backing map.
    #[must_use]
    pub(in crate::db) fn len(&self) -> u64 {
        match &self.backend {
            DataStoreBackend::Heap(map) => u64::try_from(map.len()).unwrap_or(u64::MAX),
            DataStoreBackend::Journaled { .. } => {
                let mut count = 0_u64;
                self.visit_entries(|_key, _row| {
                    count = count.saturating_add(1);
                    Ok::<_, crate::error::InternalError>(StoreVisit::Continue)
                })
                .expect("fixture traversal should succeed");
                count
            }
        }
    }

    /// Independent physical byte baseline for storage-report tests.
    pub(in crate::db) fn memory_bytes(&self) -> u64 {
        // Report map footprint as key bytes + row bytes per entry.
        let mut bytes = 0u64;
        self.visit_entries(|key, row| {
            bytes = bytes.saturating_add(key.as_bytes().len() as u64 + row.len() as u64);
            Ok::<_, crate::error::InternalError>(StoreVisit::Continue)
        })
        .expect("fixture traversal should succeed");
        bytes
    }

    pub(super) fn rebuild_entity_cardinality_from_entries(&mut self) -> Result<(), InternalError> {
        let mut cardinality = EntityCardinality::empty();
        self.visit_entries(|key, _row| {
            cardinality.apply_present_entity(key.entity_tag_prefix());
            Ok::<_, crate::error::InternalError>(StoreVisit::Continue)
        })?;
        self.entity_cardinality = cardinality;
        Ok(())
    }

    /// Return the monotonic perf-only count of stable row fetches seen by this process.
    pub(in crate::db) fn current_get_call_count() -> u64 {
        DATA_STORE_GET_CALL_COUNT.with(Cell::get)
    }
}

pub(super) fn record_data_store_get_call() {
    DATA_STORE_GET_CALL_COUNT.with(|count| {
        count.set(count.get().saturating_add(1));
    });
}

thread_local! {
    pub(super) static DATA_STORE_GET_CALL_COUNT: Cell<u64> = const { Cell::new(0) };
}
