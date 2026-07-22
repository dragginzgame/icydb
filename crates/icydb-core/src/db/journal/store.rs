//! Module: db::journal::store
//! Responsibility: stable BTreeMap-backed journal-tail append/read storage.
//! Does not own: journal codec semantics, recovery replay, or fold publication.
//! Boundary: future journaled store wrappers -> committed journal tail.

use crate::{
    db::journal::{
        JournalBatch, JournalSequence,
        codec::{MAX_JOURNAL_BATCH_BYTES, RawJournalBatch},
    },
    error::InternalError,
};
use ic_stable_structures::{
    BTreeMap as StableBTreeMap, DefaultMemoryImpl, Storable, memory_manager::VirtualMemory,
    storable::Bound as StorableBound,
};
use std::ops::Bound::{Included, Unbounded};
use std::{borrow::Cow, collections::BTreeSet};

const FOLD_WATERMARK_CONTROL_SEQUENCE: JournalSequence = JournalSequence::new(0);
const FOLD_WATERMARK_MAGIC: &[u8] = b"ICYDB-FOLD-WATERMARK";
const FOLD_WATERMARK_VERSION: u8 = 1;
const FOLD_WATERMARK_BYTES: usize = FOLD_WATERMARK_MAGIC.len() + 1 + 8 + 8;
pub(in crate::db::journal) const JOURNAL_TAIL_CHUNK_BYTES: u32 = 64 * 1024;
const JOURNAL_TAIL_KEY_BYTES: u32 = 12;

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
    map: StableBTreeMap<JournalTailKey, RawJournalChunk, VirtualMemory<DefaultMemoryImpl>>,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct JournalTailKey {
    sequence: JournalSequence,
    chunk_index: u32,
}

impl JournalTailKey {
    const fn new(sequence: JournalSequence, chunk_index: u32) -> Self {
        Self {
            sequence,
            chunk_index,
        }
    }

    const fn fold_watermark() -> Self {
        Self::new(FOLD_WATERMARK_CONTROL_SEQUENCE, 0)
    }
}

impl Storable for JournalTailKey {
    fn to_bytes(&self) -> Cow<'_, [u8]> {
        let mut bytes = Vec::with_capacity(JOURNAL_TAIL_KEY_BYTES as usize);
        bytes.extend_from_slice(&self.sequence.get().to_be_bytes());
        bytes.extend_from_slice(&self.chunk_index.to_be_bytes());

        Cow::Owned(bytes)
    }

    fn from_bytes(bytes: Cow<'_, [u8]>) -> Self {
        debug_assert_eq!(
            bytes.len(),
            JOURNAL_TAIL_KEY_BYTES as usize,
            "JournalTailKey::from_bytes received unexpected byte length",
        );

        let mut sequence = [0u8; 8];
        let mut chunk_index = [0u8; 4];
        if bytes.len() == JOURNAL_TAIL_KEY_BYTES as usize {
            sequence.copy_from_slice(&bytes.as_ref()[..8]);
            chunk_index.copy_from_slice(&bytes.as_ref()[8..]);
        }

        Self::new(
            JournalSequence::new(u64::from_be_bytes(sequence)),
            u32::from_be_bytes(chunk_index),
        )
    }

    fn into_bytes(self) -> Vec<u8> {
        self.to_bytes().into_owned()
    }

    const BOUND: StorableBound = StorableBound::Bounded {
        max_size: JOURNAL_TAIL_KEY_BYTES,
        is_fixed_size: true,
    };
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::journal) struct RawJournalChunk(Vec<u8>);

impl RawJournalChunk {
    const fn from_bytes(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }

    const fn as_bytes(&self) -> &[u8] {
        self.0.as_slice()
    }
}

impl Storable for RawJournalChunk {
    fn to_bytes(&self) -> Cow<'_, [u8]> {
        Cow::Borrowed(self.as_bytes())
    }

    fn from_bytes(bytes: Cow<'_, [u8]>) -> Self {
        Self(bytes.into_owned())
    }

    fn into_bytes(self) -> Vec<u8> {
        self.0
    }

    const BOUND: StorableBound = StorableBound::Bounded {
        max_size: JOURNAL_TAIL_CHUNK_BYTES,
        is_fixed_size: false,
    };
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

        self.append_raw_batch(key, raw.as_bytes())
    }

    /// Return the next contiguous append sequence for this tail.
    pub(in crate::db) fn next_append_sequence(&self) -> Result<JournalSequence, InternalError> {
        let watermark = self.fold_watermark()?;
        let mut last_sequence = watermark.highest_folded_journal_sequence();

        for entry in self.map.iter().rev() {
            let key = entry.key();
            if key.sequence == FOLD_WATERMARK_CONTROL_SEQUENCE {
                continue;
            }
            if key.sequence > last_sequence {
                last_sequence = key.sequence;
            }
            break;
        }

        last_sequence.next().ok_or_else(journal_tail_corruption)
    }

    /// Reserve the next mutation sequence while retaining its successor as
    /// the representable durable post-commit revision.
    pub(in crate::db) fn next_mutation_append_sequence(
        &self,
    ) -> Result<JournalSequence, InternalError> {
        let sequence = self.next_append_sequence()?;
        let _ = sequence
            .next()
            .ok_or_else(InternalError::journal_mutation_revision_exhausted)?;

        Ok(sequence)
    }

    /// Return the durable replay boundary encoded in the journal-tail memory.
    pub(in crate::db) fn fold_watermark(&self) -> Result<FoldWatermark, InternalError> {
        self.map
            .get(&JournalTailKey::fold_watermark())
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
            JournalTailKey::fold_watermark(),
            RawJournalChunk::from_bytes(encode_fold_watermark(watermark)),
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
            .range((
                Included(JournalTailKey::new(JournalSequence::new(1), 0)),
                Included(JournalTailKey::new(watermark, u32::MAX)),
            ))
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
        mut visitor: impl FnMut(&JournalBatch) -> Result<(), InternalError>,
    ) -> Result<(), InternalError> {
        let mut expected = watermark.next();
        let mut seen_batch_ids = BTreeSet::new();

        loop {
            let expected_sequence = expected.ok_or_else(journal_tail_corruption)?;
            let Some(entry) = self
                .map
                .range((
                    Included(JournalTailKey::new(expected_sequence, 0)),
                    Unbounded,
                ))
                .next()
            else {
                break;
            };
            if entry.key().sequence != expected_sequence {
                return Err(journal_tail_corruption());
            }

            let batch = RawJournalBatch::from_control_bytes(
                self.raw_batch_bytes_for_sequence(expected_sequence)?
                    .ok_or_else(journal_tail_corruption)?,
            )
            .decode()?;
            if batch.journal_sequence() != expected_sequence {
                return Err(journal_tail_corruption());
            }
            if !seen_batch_ids.insert(batch.batch_id()) {
                return Err(journal_tail_corruption());
            }

            visitor(&batch)?;
            expected = expected_sequence.next();
        }

        Ok(())
    }

    /// Return the number of complete journal-tail batches.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db) fn len(&self) -> u64 {
        self.map
            .iter()
            .filter_map(|entry| {
                let sequence = entry.key().sequence;
                (sequence != FOLD_WATERMARK_CONTROL_SEQUENCE).then_some(sequence)
            })
            .collect::<BTreeSet<_>>()
            .len() as u64
    }

    /// Return whether the journal tail is currently empty.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Clear all journal-tail batches from this test store.
    #[cfg(test)]
    pub(in crate::db) fn clear(&mut self) {
        self.map.clear_new();
    }

    /// Insert raw journal-tail bytes for persisted-corruption tests.
    #[cfg(test)]
    pub(in crate::db) fn insert_raw_batch_for_tests(
        &mut self,
        sequence: JournalSequence,
        bytes: Vec<u8>,
    ) -> Result<(), InternalError> {
        self.append_raw_batch(sequence, bytes.as_slice())
    }

    fn append_raw_batch(
        &mut self,
        sequence: JournalSequence,
        bytes: &[u8],
    ) -> Result<(), InternalError> {
        if sequence == FOLD_WATERMARK_CONTROL_SEQUENCE {
            return Err(journal_tail_corruption());
        }
        if bytes.is_empty() || bytes.len() > MAX_JOURNAL_BATCH_BYTES as usize {
            return Err(journal_tail_corruption());
        }

        if let Some(existing) = self.raw_batch_bytes_for_sequence(sequence)? {
            if existing == bytes {
                return Ok(());
            }
            if existing.len() > bytes.len() || !bytes.starts_with(existing.as_slice()) {
                return Err(journal_tail_corruption());
            }
        }

        for (chunk_index, chunk) in bytes.chunks(JOURNAL_TAIL_CHUNK_BYTES as usize).enumerate() {
            let chunk_index = u32::try_from(chunk_index).map_err(|_| journal_tail_corruption())?;
            let key = JournalTailKey::new(sequence, chunk_index);
            if self.map.contains_key(&key) {
                continue;
            }
            self.map
                .insert(key, RawJournalChunk::from_bytes(chunk.to_vec()));
        }

        Ok(())
    }

    fn raw_batch_bytes_for_sequence(
        &self,
        sequence: JournalSequence,
    ) -> Result<Option<Vec<u8>>, InternalError> {
        let mut bytes = Vec::new();
        let mut expected_chunk = 0u32;

        for entry in self.map.range((
            Included(JournalTailKey::new(sequence, 0)),
            Included(JournalTailKey::new(sequence, u32::MAX)),
        )) {
            let key = entry.key();
            if key.chunk_index != expected_chunk {
                return Err(journal_tail_corruption());
            }
            let next_len = bytes
                .len()
                .checked_add(entry.value().as_bytes().len())
                .ok_or_else(journal_tail_corruption)?;
            if next_len > MAX_JOURNAL_BATCH_BYTES as usize {
                return Err(journal_tail_corruption());
            }
            bytes.extend_from_slice(entry.value().as_bytes());
            expected_chunk = expected_chunk
                .checked_add(1)
                .ok_or_else(journal_tail_corruption)?;
        }

        if expected_chunk == 0 {
            return Ok(None);
        }

        Ok(Some(bytes))
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
