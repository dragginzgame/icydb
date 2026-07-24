//! Module: db::journal::store
//! Responsibility: stable BTreeMap-backed journal-tail append/read storage.
//! Does not own: journal codec semantics, recovery replay, or fold publication.
//! Boundary: future journaled store wrappers -> committed journal tail.

use crate::{
    db::journal::{
        JournalBatch, JournalRecord, JournalSequence,
        codec::{MAX_JOURNAL_BATCH_BYTES, RawJournalBatch},
    },
    error::{ErrorClass, InternalError},
};
use candid::CandidType;
use ic_stable_structures::{
    BTreeMap as StableBTreeMap, DefaultMemoryImpl, Storable, memory_manager::VirtualMemory,
    storable::Bound as StorableBound,
};
use serde::Deserialize;
use std::ops::Bound::{Included, Unbounded};
use std::{borrow::Cow, collections::BTreeSet};

const FOLD_WATERMARK_CONTROL_SEQUENCE: JournalSequence = JournalSequence::new(0);
const DATA_MUTATION_REVISION_CONTROL_CHUNK: u32 = 1;
const FOLD_WATERMARK_MAGIC: &[u8] = b"ICYDB-FOLD-WATERMARK";
const FOLD_WATERMARK_VERSION: u8 = 1;
const FOLD_WATERMARK_BYTES: usize = FOLD_WATERMARK_MAGIC.len() + 1 + 8 + 8;
const DATA_MUTATION_REVISION_MAGIC: &[u8] = b"ICYDB-DATA-REVISION";
const DATA_MUTATION_REVISION_VERSION: u8 = 1;
const DATA_MUTATION_REVISION_BYTES: usize = DATA_MUTATION_REVISION_MAGIC.len() + 1 + 8;
pub(in crate::db::journal) const JOURNAL_TAIL_CHUNK_BYTES: u32 = 64 * 1024;
const JOURNAL_TAIL_KEY_BYTES: u32 = 12;
const MAX_JOURNAL_INSPECTION_BATCHES_PER_PAGE: usize = 2;
const MAX_JOURNAL_INSPECTION_BYTES_PER_PAGE: usize =
    (MAX_JOURNAL_BATCH_BYTES as usize) * MAX_JOURNAL_INSPECTION_BATCHES_PER_PAGE;

/// Exact private continuation within one physical journal tail.
///
/// Duplicate batch IDs require comparing a newly decoded batch with every
/// earlier live tail batch. `CheckingBatchIdentity` makes that proof resumable
/// without retaining an unbounded set of IDs.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub(in crate::db) enum JournalInspectionCheckpoint {
    /// No live tail batch has been classified.
    BeforeFirst,
    /// A preceding sequence gap was classified; inspect this exact next batch.
    BeforeBatch { sequence: u64 },
    /// The candidate batch is valid while earlier batch identities remain.
    CheckingBatchIdentity {
        sequence: u64,
        batch_id: [u8; 16],
        next_prior_sequence: u64,
    },
    /// The named batch and all earlier identity comparisons are complete.
    AfterBatch { sequence: u64 },
}

/// Definite progressable journal-tail invariant failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum JournalIntegrityIssue {
    /// One physical batch/chunk envelope is not current-form decodable.
    MalformedBatch {
        sequence: u64,
        diagnostic_code: u16,
        incompatible_format: bool,
    },
    /// One or more expected sequence values have no physical batch.
    SequenceGap {
        expected_sequence: u64,
        next_present_sequence: u64,
    },
    /// Two distinct physical sequences carry the same batch identity.
    DuplicateBatchIdentity { sequence: u64, prior_sequence: u64 },
}

/// Hard bounds for one journal-tail inspection page.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct JournalInspectionLimits {
    decoded_batches: usize,
    decoded_bytes: usize,
}

impl JournalInspectionLimits {
    /// Return the maintained production journal-page bounds.
    #[must_use]
    pub(in crate::db) const fn standard() -> Self {
        Self {
            decoded_batches: MAX_JOURNAL_INSPECTION_BATCHES_PER_PAGE,
            decoded_bytes: MAX_JOURNAL_INSPECTION_BYTES_PER_PAGE,
        }
    }

    #[cfg(test)]
    #[must_use]
    pub(in crate::db) const fn for_tests(decoded_batches: usize, decoded_bytes: usize) -> Self {
        Self {
            decoded_batches,
            decoded_bytes,
        }
    }

    fn validate(self) -> Result<Self, InternalError> {
        if self.decoded_batches < 2 || self.decoded_bytes < MAX_JOURNAL_BATCH_BYTES as usize {
            return Err(InternalError::store_invariant());
        }
        Ok(self)
    }
}

/// Stable proof inputs for one physical journal tail.
#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
pub(in crate::db) struct JournalTailProofIdentity {
    data_mutation_revision: u64,
    fold_sequence: u64,
    fold_epoch: u64,
    next_append_sequence: u64,
    physical_record_count: u64,
}

impl JournalTailProofIdentity {
    /// Return the durable logical row-mutation revision.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn data_mutation_revision(self) -> u64 {
        self.data_mutation_revision
    }

    /// Return the highest durably folded batch sequence.
    #[must_use]
    pub(in crate::db) const fn fold_sequence(self) -> u64 {
        self.fold_sequence
    }

    /// Return the fold topology epoch.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn fold_epoch(self) -> u64 {
        self.fold_epoch
    }

    /// Return the next sequence that a valid append would consume.
    #[must_use]
    pub(in crate::db) const fn next_append_sequence(self) -> u64 {
        self.next_append_sequence
    }

    /// Return the complete physical map-record count, including control records.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn physical_record_count(self) -> u64 {
        self.physical_record_count
    }

    /// Return whether decoded proof fields can describe one maintained tail.
    #[must_use]
    pub(in crate::db) const fn is_well_formed(self) -> bool {
        self.data_mutation_revision > 0
            && self.data_mutation_revision <= self.next_append_sequence
            && self.next_append_sequence > self.fold_sequence
    }
}

/// One bounded exact page from a physical journal tail.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct JournalIntegrityPage {
    checkpoint: JournalInspectionCheckpoint,
    exhausted: bool,
    issue: Option<JournalIntegrityIssue>,
    batch_identity_blocked: bool,
}

impl JournalIntegrityPage {
    /// Borrow the exact next private checkpoint.
    #[must_use]
    pub(in crate::db) const fn checkpoint(&self) -> &JournalInspectionCheckpoint {
        &self.checkpoint
    }

    /// Return whether the live tail interval was authoritatively exhausted.
    #[must_use]
    pub(in crate::db) const fn exhausted(&self) -> bool {
        self.exhausted
    }

    /// Return the one bounded definite issue classified by this page.
    #[must_use]
    pub(in crate::db) const fn issue(&self) -> Option<JournalIntegrityIssue> {
        self.issue
    }

    /// Return whether malformed prior state blocked complete batch-ID proof.
    #[must_use]
    pub(in crate::db) const fn batch_identity_blocked(&self) -> bool {
        self.batch_identity_blocked
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

    const fn data_mutation_revision() -> Self {
        Self::new(
            FOLD_WATERMARK_CONTROL_SEQUENCE,
            DATA_MUTATION_REVISION_CONTROL_CHUNK,
        )
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
        self.append_raw_batch(key, raw.as_bytes())?;
        if batch.records().iter().any(|record| {
            matches!(
                record,
                JournalRecord::RowPut { .. } | JournalRecord::RowDelete { .. }
            )
        }) {
            self.persist_data_mutation_revision(key)?;
        }
        Ok(())
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

    /// Return the stable row-mutation revision without counting schema or
    /// validation-job journal records.
    pub(in crate::db) fn data_mutation_revision(&self) -> Result<u64, InternalError> {
        let highest_row_sequence = self
            .map
            .get(&JournalTailKey::data_mutation_revision())
            .map_or(Ok(JournalSequence::new(0)), |raw| {
                decode_data_mutation_revision(raw.as_bytes())
            })?;
        highest_row_sequence
            .next()
            .map(JournalSequence::get)
            .ok_or_else(InternalError::journal_mutation_revision_exhausted)
    }

    /// Capture the exact durable and physical identity inspected by Deep.
    pub(in crate::db) fn proof_identity(&self) -> Result<JournalTailProofIdentity, InternalError> {
        let watermark = self.fold_watermark()?;
        Ok(JournalTailProofIdentity {
            data_mutation_revision: self.data_mutation_revision()?,
            fold_sequence: watermark.highest_folded_journal_sequence().get(),
            fold_epoch: watermark.fold_epoch(),
            next_append_sequence: self.next_append_sequence()?.get(),
            physical_record_count: self.map.len(),
        })
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

    /// Return whether any physical journal batch remains in this tail.
    ///
    /// Recovery uses this single-lookup boundary after advancing the fold
    /// watermark. A retained batch below or above that watermark means cleanup
    /// is incomplete, so marker authority must remain published.
    #[must_use]
    pub(in crate::db) fn has_stored_batch(&self) -> bool {
        self.map
            .range((
                Included(JournalTailKey::new(JournalSequence::new(1), 0)),
                Unbounded,
            ))
            .next()
            .is_some()
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

    /// Inspect one bounded exact journal-tail page.
    ///
    /// The page validates complete current-form batch envelopes and sequence
    /// continuity. Duplicate batch identity is checked through a resumable
    /// comparison against every earlier live batch, so memory use does not
    /// grow with tail length.
    pub(in crate::db) fn inspect_page(
        &self,
        checkpoint: JournalInspectionCheckpoint,
        limits: JournalInspectionLimits,
    ) -> Result<JournalIntegrityPage, InternalError> {
        let limits = limits.validate()?;
        let watermark = self.fold_watermark()?.highest_folded_journal_sequence();
        let mut accumulator = JournalInspectionAccumulator::new(limits);

        match checkpoint {
            JournalInspectionCheckpoint::BeforeFirst => {
                let sequence = watermark.next().ok_or_else(journal_tail_corruption)?;
                self.start_inspection_batch(watermark, sequence, &mut accumulator)
            }
            JournalInspectionCheckpoint::BeforeBatch { sequence } => {
                let sequence = JournalSequence::new(sequence);
                if sequence <= watermark {
                    return Err(journal_tail_corruption());
                }
                self.start_inspection_batch(watermark, sequence, &mut accumulator)
            }
            JournalInspectionCheckpoint::AfterBatch { sequence } => {
                if sequence < watermark.get() {
                    return Err(journal_tail_corruption());
                }
                let sequence = JournalSequence::new(sequence)
                    .next()
                    .ok_or_else(journal_tail_corruption)?;
                self.start_inspection_batch(watermark, sequence, &mut accumulator)
            }
            JournalInspectionCheckpoint::CheckingBatchIdentity {
                sequence,
                batch_id,
                next_prior_sequence,
            } => self.continue_batch_identity_check(
                watermark,
                JournalSequence::new(sequence),
                batch_id,
                JournalSequence::new(next_prior_sequence),
                &mut accumulator,
            ),
        }
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

    /// Corrupt the first envelope byte without changing proof-vector counters.
    #[cfg(test)]
    pub(in crate::db) fn corrupt_batch_envelope_for_tests(
        &mut self,
        sequence: JournalSequence,
    ) -> Result<(), InternalError> {
        let key = JournalTailKey::new(sequence, 0);
        let Some(raw) = self.map.get(&key) else {
            return Err(journal_tail_corruption());
        };
        let mut bytes = raw.into_bytes();
        let Some(first) = bytes.first_mut() else {
            return Err(journal_tail_corruption());
        };
        *first ^= u8::MAX;
        self.map.insert(key, RawJournalChunk::from_bytes(bytes));
        Ok(())
    }

    /// Corrupt the bounded fold-control envelope for Quick classification tests.
    #[cfg(test)]
    pub(in crate::db) fn corrupt_fold_watermark_for_tests(&mut self) -> Result<(), InternalError> {
        let key = JournalTailKey::fold_watermark();
        let mut raw = self.map.get(&key).unwrap_or_else(|| {
            RawJournalChunk::from_bytes(encode_fold_watermark(FoldWatermark::initial()))
        });
        let Some(first) = raw.0.first_mut() else {
            return Err(journal_tail_corruption());
        };
        *first ^= u8::MAX;
        self.map.insert(key, raw);
        Ok(())
    }

    /// Persist a valid but tail-inconsistent row revision for Quick tests.
    #[cfg(test)]
    pub(in crate::db) fn diverge_data_mutation_revision_for_tests(
        &mut self,
        sequence: JournalSequence,
    ) -> Result<(), InternalError> {
        self.persist_data_mutation_revision(sequence)
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

    fn persist_data_mutation_revision(
        &mut self,
        sequence: JournalSequence,
    ) -> Result<(), InternalError> {
        let current = self
            .map
            .get(&JournalTailKey::data_mutation_revision())
            .map_or(Ok(JournalSequence::new(0)), |raw| {
                decode_data_mutation_revision(raw.as_bytes())
            })?;
        if sequence <= current {
            return Ok(());
        }
        let _ = sequence
            .next()
            .ok_or_else(InternalError::journal_mutation_revision_exhausted)?;
        self.map.insert(
            JournalTailKey::data_mutation_revision(),
            RawJournalChunk::from_bytes(encode_data_mutation_revision(sequence)),
        );
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

    fn start_inspection_batch(
        &self,
        watermark: JournalSequence,
        sequence: JournalSequence,
        accumulator: &mut JournalInspectionAccumulator,
    ) -> Result<JournalIntegrityPage, InternalError> {
        let batch = match self.decode_inspection_batch(sequence, accumulator) {
            Ok(Some(batch)) => batch,
            Ok(None) => {
                let Some(next) = self.next_batch_sequence_at_or_after(sequence) else {
                    let prior = sequence
                        .get()
                        .checked_sub(1)
                        .ok_or_else(journal_tail_corruption)?;
                    return Ok(JournalInspectionAccumulator::finish(
                        JournalInspectionCheckpoint::AfterBatch { sequence: prior },
                        true,
                        None,
                        false,
                    ));
                };
                return Ok(JournalInspectionAccumulator::finish(
                    JournalInspectionCheckpoint::BeforeBatch {
                        sequence: next.get(),
                    },
                    false,
                    Some(JournalIntegrityIssue::SequenceGap {
                        expected_sequence: sequence.get(),
                        next_present_sequence: next.get(),
                    }),
                    true,
                ));
            }
            Err(error) if progressable_journal_unit_error(&error) => {
                return Ok(JournalInspectionAccumulator::finish(
                    JournalInspectionCheckpoint::AfterBatch {
                        sequence: sequence.get(),
                    },
                    !self.has_batch_after(sequence),
                    Some(malformed_batch_issue(sequence, &error)),
                    true,
                ));
            }
            Err(error) => return Err(error),
        };
        let first_live_sequence = watermark.next().ok_or_else(journal_tail_corruption)?;
        if sequence == first_live_sequence {
            return Ok(JournalInspectionAccumulator::finish(
                JournalInspectionCheckpoint::AfterBatch {
                    sequence: sequence.get(),
                },
                !self.has_batch_after(sequence),
                None,
                false,
            ));
        }

        self.compare_prior_batch_identities(
            sequence,
            batch.batch_id(),
            first_live_sequence,
            accumulator,
        )
    }

    fn continue_batch_identity_check(
        &self,
        watermark: JournalSequence,
        sequence: JournalSequence,
        batch_id: [u8; 16],
        prior_sequence: JournalSequence,
        accumulator: &mut JournalInspectionAccumulator,
    ) -> Result<JournalIntegrityPage, InternalError> {
        if sequence <= watermark || prior_sequence <= watermark || prior_sequence >= sequence {
            return Err(journal_tail_corruption());
        }

        let candidate = match self.decode_inspection_batch(sequence, accumulator) {
            Ok(Some(candidate)) => candidate,
            Ok(None) => return Err(journal_tail_corruption()),
            Err(error) if progressable_journal_unit_error(&error) => {
                return Ok(JournalInspectionAccumulator::finish(
                    JournalInspectionCheckpoint::AfterBatch {
                        sequence: sequence.get(),
                    },
                    !self.has_batch_after(sequence),
                    Some(malformed_batch_issue(sequence, &error)),
                    true,
                ));
            }
            Err(error) => return Err(error),
        };
        if candidate.batch_id() != batch_id {
            return Err(journal_tail_corruption());
        }

        self.compare_prior_batch_identities(sequence, batch_id, prior_sequence, accumulator)
    }

    fn compare_prior_batch_identities(
        &self,
        sequence: JournalSequence,
        batch_id: [u8; 16],
        mut prior_sequence: JournalSequence,
        accumulator: &mut JournalInspectionAccumulator,
    ) -> Result<JournalIntegrityPage, InternalError> {
        while prior_sequence < sequence {
            if !accumulator.can_decode_another_batch() {
                return Ok(JournalInspectionAccumulator::finish(
                    JournalInspectionCheckpoint::CheckingBatchIdentity {
                        sequence: sequence.get(),
                        batch_id,
                        next_prior_sequence: prior_sequence.get(),
                    },
                    false,
                    None,
                    false,
                ));
            }
            let prior = match self.decode_inspection_batch(prior_sequence, accumulator) {
                Ok(Some(prior)) => prior,
                Ok(None) => {
                    return self.blocked_identity_progress(sequence, batch_id, prior_sequence);
                }
                Err(error) if progressable_journal_unit_error(&error) => {
                    return self.blocked_identity_progress(sequence, batch_id, prior_sequence);
                }
                Err(error) => return Err(error),
            };
            if prior.batch_id() == batch_id {
                return Ok(JournalInspectionAccumulator::finish(
                    JournalInspectionCheckpoint::AfterBatch {
                        sequence: sequence.get(),
                    },
                    !self.has_batch_after(sequence),
                    Some(JournalIntegrityIssue::DuplicateBatchIdentity {
                        sequence: sequence.get(),
                        prior_sequence: prior_sequence.get(),
                    }),
                    false,
                ));
            }
            prior_sequence = prior_sequence.next().ok_or_else(journal_tail_corruption)?;
        }

        Ok(JournalInspectionAccumulator::finish(
            JournalInspectionCheckpoint::AfterBatch {
                sequence: sequence.get(),
            },
            !self.has_batch_after(sequence),
            None,
            false,
        ))
    }

    fn decode_inspection_batch(
        &self,
        sequence: JournalSequence,
        accumulator: &mut JournalInspectionAccumulator,
    ) -> Result<Option<JournalBatch>, InternalError> {
        let Some(bytes) = self.raw_batch_bytes_for_sequence(sequence)? else {
            return Ok(None);
        };
        accumulator.consume_batch(bytes.len())?;
        let batch = RawJournalBatch::from_control_bytes(bytes).decode()?;
        if batch.journal_sequence() != sequence {
            return Err(journal_tail_corruption());
        }
        Ok(Some(batch))
    }

    fn has_batch_after(&self, sequence: JournalSequence) -> bool {
        let Some(next) = sequence.next() else {
            return false;
        };
        self.map
            .range((Included(JournalTailKey::new(next, 0)), Unbounded))
            .next()
            .is_some()
    }

    fn next_batch_sequence_at_or_after(
        &self,
        sequence: JournalSequence,
    ) -> Option<JournalSequence> {
        self.map
            .range((Included(JournalTailKey::new(sequence, 0)), Unbounded))
            .map(|entry| entry.key().sequence)
            .find(|candidate| *candidate != FOLD_WATERMARK_CONTROL_SEQUENCE)
    }

    fn blocked_identity_progress(
        &self,
        sequence: JournalSequence,
        batch_id: [u8; 16],
        prior_sequence: JournalSequence,
    ) -> Result<JournalIntegrityPage, InternalError> {
        let next = prior_sequence.next().ok_or_else(journal_tail_corruption)?;
        let next = self
            .next_batch_sequence_at_or_after(next)
            .filter(|next| *next < sequence);
        Ok(match next {
            Some(next) => JournalInspectionAccumulator::finish(
                JournalInspectionCheckpoint::CheckingBatchIdentity {
                    sequence: sequence.get(),
                    batch_id,
                    next_prior_sequence: next.get(),
                },
                false,
                None,
                true,
            ),
            None => JournalInspectionAccumulator::finish(
                JournalInspectionCheckpoint::AfterBatch {
                    sequence: sequence.get(),
                },
                !self.has_batch_after(sequence),
                None,
                true,
            ),
        })
    }
}

struct JournalInspectionAccumulator {
    limits: JournalInspectionLimits,
    decoded_batches: usize,
    decoded_bytes: usize,
}

impl JournalInspectionAccumulator {
    const fn new(limits: JournalInspectionLimits) -> Self {
        Self {
            limits,
            decoded_batches: 0,
            decoded_bytes: 0,
        }
    }

    const fn can_decode_another_batch(&self) -> bool {
        self.decoded_batches < self.limits.decoded_batches
    }

    fn consume_batch(&mut self, bytes: usize) -> Result<(), InternalError> {
        if !self.can_decode_another_batch() {
            return Err(InternalError::store_invariant());
        }
        let decoded_bytes = self
            .decoded_bytes
            .checked_add(bytes)
            .ok_or_else(InternalError::store_invariant)?;
        if decoded_bytes > self.limits.decoded_bytes {
            return Err(InternalError::store_invariant());
        }
        self.decoded_batches = self
            .decoded_batches
            .checked_add(1)
            .ok_or_else(InternalError::store_invariant)?;
        self.decoded_bytes = decoded_bytes;
        Ok(())
    }

    const fn finish(
        checkpoint: JournalInspectionCheckpoint,
        exhausted: bool,
        issue: Option<JournalIntegrityIssue>,
        batch_identity_blocked: bool,
    ) -> JournalIntegrityPage {
        JournalIntegrityPage {
            checkpoint,
            exhausted,
            issue,
            batch_identity_blocked,
        }
    }
}

const fn progressable_journal_unit_error(error: &InternalError) -> bool {
    matches!(
        error.class(),
        ErrorClass::Corruption | ErrorClass::IncompatiblePersistedFormat
    )
}

fn malformed_batch_issue(
    sequence: JournalSequence,
    error: &InternalError,
) -> JournalIntegrityIssue {
    JournalIntegrityIssue::MalformedBatch {
        sequence: sequence.get(),
        diagnostic_code: error.diagnostic_code().error_code().raw(),
        incompatible_format: error.class() == ErrorClass::IncompatiblePersistedFormat,
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

fn encode_data_mutation_revision(sequence: JournalSequence) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(DATA_MUTATION_REVISION_BYTES);
    bytes.extend_from_slice(DATA_MUTATION_REVISION_MAGIC);
    bytes.push(DATA_MUTATION_REVISION_VERSION);
    bytes.extend_from_slice(&sequence.get().to_be_bytes());
    bytes
}

fn decode_data_mutation_revision(bytes: &[u8]) -> Result<JournalSequence, InternalError> {
    if bytes.len() != DATA_MUTATION_REVISION_BYTES
        || !bytes.starts_with(DATA_MUTATION_REVISION_MAGIC)
        || bytes[DATA_MUTATION_REVISION_MAGIC.len()] != DATA_MUTATION_REVISION_VERSION
    {
        return Err(journal_tail_corruption());
    }
    let revision_start = DATA_MUTATION_REVISION_MAGIC.len() + 1;
    let mut revision = [0u8; size_of::<u64>()];
    revision.copy_from_slice(&bytes[revision_start..]);
    let sequence = JournalSequence::new(u64::from_be_bytes(revision));
    if sequence == FOLD_WATERMARK_CONTROL_SEQUENCE {
        return Err(journal_tail_corruption());
    }
    Ok(sequence)
}

fn journal_tail_corruption() -> InternalError {
    InternalError::store_corruption()
}
