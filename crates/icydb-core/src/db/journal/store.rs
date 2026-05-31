//! Module: db::journal::store
//! Responsibility: stable BTreeMap-backed journal-tail append/read storage.
//! Does not own: journal codec semantics, recovery replay, or fold publication.
//! Boundary: future journaled store wrappers -> committed journal tail.

use crate::{
    db::journal::{JournalBatch, JournalSequence, codec::RawJournalBatch},
    error::InternalError,
};
use ic_memory::stable_structures::{
    BTreeMap as StableBTreeMap, DefaultMemoryImpl, memory_manager::VirtualMemory,
};
use std::collections::BTreeSet;
use std::ops::Bound::{Excluded, Unbounded};

/// Control-flow result for journal-tail traversal visitors.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum JournalTailVisit {
    Continue,
    Stop,
}

impl JournalTailVisit {
    const fn should_stop(self) -> bool {
        matches!(self, Self::Stop)
    }
}

///
/// JournalTailStore
///
/// Stable journal-tail storage keyed by monotonic `journal_sequence`.
/// Values are complete encoded `JournalBatch` envelopes.
///
pub struct JournalTailStore {
    map: StableBTreeMap<JournalSequence, RawJournalBatch, VirtualMemory<DefaultMemoryImpl>>,
}

impl JournalTailStore {
    /// Initialize a journal-tail store with the provided backing memory.
    #[must_use]
    pub fn init(memory: VirtualMemory<DefaultMemoryImpl>) -> Self {
        Self {
            map: StableBTreeMap::init(memory),
        }
    }

    /// Append one complete marker-bound journal batch.
    ///
    /// Re-appending identical bytes for the same sequence is idempotent.
    /// Reusing a sequence for different bytes fails closed.
    pub(in crate::db) fn append_batch(
        &mut self,
        batch: &JournalBatch,
    ) -> Result<(), InternalError> {
        let key = batch.journal_sequence();
        let raw = RawJournalBatch::from_batch(batch)?;

        if let Some(existing) = self.map.get(&key) {
            if existing.as_bytes() == raw.as_bytes() {
                return Ok(());
            }

            return Err(InternalError::store_corruption(format!(
                "journal tail sequence {} already maps to different batch bytes",
                key.get(),
            )));
        }

        self.map.insert(key, raw);

        Ok(())
    }

    /// Visit complete batches after the durable fold watermark in replay order.
    ///
    /// This read boundary validates the first journal-tail invariants needed by
    /// recovery: encoded sequence must match physical key, sequences above the
    /// watermark are contiguous, and batch IDs do not repeat across sequences.
    pub(in crate::db) fn visit_batches_after(
        &self,
        watermark: JournalSequence,
        mut visitor: impl FnMut(&JournalBatch) -> Result<JournalTailVisit, InternalError>,
    ) -> Result<(), InternalError> {
        let mut expected = watermark.next();
        let mut seen_batch_ids = BTreeSet::new();

        for entry in self.map.range((Excluded(watermark), Unbounded)) {
            let key = entry.key();
            let expected_sequence = expected.ok_or_else(|| {
                InternalError::store_corruption(
                    "journal tail contains batch after maximum fold watermark",
                )
            })?;
            if *key != expected_sequence {
                return Err(InternalError::store_corruption(format!(
                    "journal tail sequence gap after watermark: expected {}, found {}",
                    expected_sequence.get(),
                    key.get(),
                )));
            }

            let batch = entry.value().decode()?;
            if batch.journal_sequence() != *key {
                return Err(InternalError::store_corruption(format!(
                    "journal batch sequence {} disagrees with journal tail key {}",
                    batch.journal_sequence().get(),
                    key.get(),
                )));
            }
            if !seen_batch_ids.insert(batch.batch_id()) {
                return Err(InternalError::store_corruption(
                    "journal tail contains duplicate batch id above fold watermark",
                ));
            }

            if visitor(&batch)?.should_stop() {
                break;
            }
            expected = key.next();
        }

        Ok(())
    }

    /// Return the number of complete journal-tail batches.
    #[must_use]
    pub(in crate::db) fn len(&self) -> u64 {
        self.map.len()
    }

    /// Return whether the journal tail is currently empty.
    #[must_use]
    pub(in crate::db) fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}
