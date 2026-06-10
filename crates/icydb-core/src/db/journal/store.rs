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
use std::ops::Bound::{Excluded, Included, Unbounded};

const FOLD_WATERMARK_CONTROL_SEQUENCE: JournalSequence = JournalSequence::new(0);
const FOLD_WATERMARK_MAGIC: &[u8] = b"ICYDB-FOLD-WATERMARK";
const FOLD_WATERMARK_VERSION: u8 = 1;
const FOLD_WATERMARK_BYTES: usize = FOLD_WATERMARK_MAGIC.len() + 1 + 8 + 8;

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

/// Durable replay boundary for a journal tail.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct FoldWatermark {
    highest_folded_journal_sequence: JournalSequence,
    fold_epoch: u64,
}

impl FoldWatermark {
    #[must_use]
    pub(in crate::db) const fn initial() -> Self {
        Self {
            highest_folded_journal_sequence: JournalSequence::new(0),
            fold_epoch: 0,
        }
    }

    #[must_use]
    pub(in crate::db) const fn new(
        highest_folded_journal_sequence: JournalSequence,
        fold_epoch: u64,
    ) -> Self {
        Self {
            highest_folded_journal_sequence,
            fold_epoch,
        }
    }

    #[must_use]
    pub(in crate::db) const fn highest_folded_journal_sequence(self) -> JournalSequence {
        self.highest_folded_journal_sequence
    }

    #[must_use]
    pub(in crate::db) const fn fold_epoch(self) -> u64 {
        self.fold_epoch
    }
}

///
/// JournalTailStore
///
/// Stable journal-tail storage keyed by monotonic `journal_sequence`.
/// Values above sequence `0` are complete encoded `JournalBatch` envelopes.
/// Sequence `0` is reserved for the fold-watermark control entry, so real
/// journal batches start at sequence `1`.
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
        if key == FOLD_WATERMARK_CONTROL_SEQUENCE {
            return Err(journal_tail_corruption());
        }
        let raw = RawJournalBatch::from_batch(batch)?;

        if let Some(existing) = self.map.get(&key) {
            if existing.as_bytes() == raw.as_bytes() {
                return Ok(());
            }

            return Err(journal_tail_corruption());
        }

        self.map.insert(key, raw);

        Ok(())
    }

    /// Return the next contiguous append sequence for this tail.
    pub(in crate::db) fn next_append_sequence(&self) -> Result<JournalSequence, InternalError> {
        let watermark = self.fold_watermark()?;
        let mut last_sequence = watermark.highest_folded_journal_sequence();

        for entry in self.map.iter().rev() {
            let key = *entry.key();
            if key == FOLD_WATERMARK_CONTROL_SEQUENCE {
                continue;
            }
            if key > last_sequence {
                last_sequence = key;
            }
            break;
        }

        last_sequence.next().ok_or_else(journal_tail_corruption)
    }

    /// Return the durable replay boundary encoded in the journal-tail memory.
    pub(in crate::db) fn fold_watermark(&self) -> Result<FoldWatermark, InternalError> {
        self.map
            .get(&FOLD_WATERMARK_CONTROL_SEQUENCE)
            .map_or(Ok(FoldWatermark::initial()), |raw| {
                decode_fold_watermark(raw.as_bytes())
            })
    }

    /// Persist a new durable fold watermark.
    ///
    /// Watermarks may advance or be rewritten idempotently, but they never
    /// move backward. The journal tail itself is the replay-boundary authority;
    /// no extra stable memory ID is required.
    pub(in crate::db) fn persist_fold_watermark(
        &mut self,
        watermark: FoldWatermark,
    ) -> Result<(), InternalError> {
        let current = self.fold_watermark()?;
        if watermark.highest_folded_journal_sequence() < current.highest_folded_journal_sequence()
            || (watermark.highest_folded_journal_sequence()
                == current.highest_folded_journal_sequence()
                && watermark.fold_epoch() < current.fold_epoch())
        {
            return Err(journal_tail_corruption());
        }

        self.map.insert(
            FOLD_WATERMARK_CONTROL_SEQUENCE,
            RawJournalBatch::from_control_bytes(encode_fold_watermark(watermark)),
        );

        Ok(())
    }

    /// Remove folded journal batches through the provided sequence.
    ///
    /// The persisted fold watermark remains authoritative if cleanup is
    /// interrupted after the watermark is advanced.
    pub(in crate::db) fn clear_batches_through(&mut self, watermark: JournalSequence) {
        if watermark == FOLD_WATERMARK_CONTROL_SEQUENCE {
            return;
        }

        let keys = self
            .map
            .range((Included(JournalSequence::new(1)), Included(watermark)))
            .map(|entry| *entry.key())
            .collect::<Vec<_>>();
        for key in keys {
            let _ = self.map.remove(&key);
        }
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
            let expected_sequence = expected.ok_or_else(journal_tail_corruption)?;
            if *key != expected_sequence {
                return Err(journal_tail_corruption());
            }

            let batch = entry.value().decode()?;
            if batch.journal_sequence() != *key {
                return Err(journal_tail_corruption());
            }
            if !seen_batch_ids.insert(batch.batch_id()) {
                return Err(journal_tail_corruption());
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
        self.map.len().saturating_sub(u64::from(
            self.map.contains_key(&FOLD_WATERMARK_CONTROL_SEQUENCE),
        ))
    }

    /// Return whether the journal tail is currently empty.
    #[must_use]
    pub(in crate::db) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Clear all journal-tail batches from this test store.
    #[cfg(test)]
    pub(in crate::db) fn clear(&mut self) {
        self.map.clear_new();
    }
}

fn encode_fold_watermark(watermark: FoldWatermark) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(FOLD_WATERMARK_BYTES);
    bytes.extend_from_slice(FOLD_WATERMARK_MAGIC);
    bytes.push(FOLD_WATERMARK_VERSION);
    bytes.extend_from_slice(
        &watermark
            .highest_folded_journal_sequence()
            .get()
            .to_be_bytes(),
    );
    bytes.extend_from_slice(&watermark.fold_epoch().to_be_bytes());
    bytes
}

fn decode_fold_watermark(bytes: &[u8]) -> Result<FoldWatermark, InternalError> {
    if bytes.len() != FOLD_WATERMARK_BYTES {
        return Err(journal_tail_corruption());
    }
    if !bytes.starts_with(FOLD_WATERMARK_MAGIC) {
        return Err(journal_tail_corruption());
    }

    let version_index = FOLD_WATERMARK_MAGIC.len();
    let version = bytes[version_index];
    if version != FOLD_WATERMARK_VERSION {
        return Err(journal_tail_corruption());
    }

    let sequence_start = version_index + 1;
    let epoch_start = sequence_start + 8;
    let mut sequence_bytes = [0u8; 8];
    let mut epoch_bytes = [0u8; 8];
    sequence_bytes.copy_from_slice(&bytes[sequence_start..epoch_start]);
    epoch_bytes.copy_from_slice(&bytes[epoch_start..]);

    Ok(FoldWatermark::new(
        JournalSequence::new(u64::from_be_bytes(sequence_bytes)),
        u64::from_be_bytes(epoch_bytes),
    ))
}

fn journal_tail_corruption() -> InternalError {
    InternalError::store_corruption()
}
