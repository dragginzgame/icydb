//! Module: db::journal::store
//! Responsibility: stable BTreeMap-backed journal-tail append/read storage.
//! Does not own: journal codec semantics, recovery replay, or fold publication.
//! Boundary: future journaled store wrappers -> committed journal tail.

use crate::{
    db::journal::{
        DatabaseCommitSequence, JournalBatch, JournalRecord, JournalSequence,
        codec::{
            JOURNAL_BATCH_FIXED_HEADER_BYTES, MAX_JOURNAL_BATCH_BYTES, MAX_JOURNAL_BATCH_RECORDS,
            RawJournalBatch, RawJournalBatchFixedHeader, inspect_raw_journal_batch_fixed_header,
        },
    },
    error::{ErrorClass, InternalError},
};
use ic_stable_structures::{
    BTreeMap as StableBTreeMap, DefaultMemoryImpl, Storable, memory_manager::VirtualMemory,
    storable::Bound as StorableBound,
};
use std::borrow::Cow;
#[cfg(test)]
use std::collections::BTreeSet;
use std::ops::Bound::{Included, Unbounded};

const FOLD_WATERMARK_CONTROL_SEQUENCE: JournalSequence = JournalSequence::new(0);
const DATA_MUTATION_REVISION_CONTROL_CHUNK: u32 = 1;
const ACCESS_STATE_REVISION_CONTROL_CHUNK: u32 = 2;
const TAIL_CONVERGENCE_CONTROL_CHUNK: u32 = 3;
const FOLD_WATERMARK_MAGIC: &[u8] = b"ICYDB-FOLD-WATERMARK";
const FOLD_WATERMARK_VERSION: u8 = 1;
const FOLD_WATERMARK_BYTES: usize = FOLD_WATERMARK_MAGIC.len() + 1 + 8 + 8;
const DATA_MUTATION_REVISION_MAGIC: &[u8] = b"ICYDB-DATA-REVISION";
const DATA_MUTATION_REVISION_VERSION: u8 = 1;
const DATA_MUTATION_REVISION_BYTES: usize = DATA_MUTATION_REVISION_MAGIC.len() + 1 + 8;
const ACCESS_STATE_REVISION_MAGIC: &[u8] = b"ICYDB-ACCESS-REVISION";
const ACCESS_STATE_REVISION_VERSION: u8 = 1;
const ACCESS_STATE_REVISION_BYTES: usize = ACCESS_STATE_REVISION_MAGIC.len() + 1 + 8;
const TAIL_CONVERGENCE_MAGIC: &[u8] = b"ICYDB-TAIL-CONTROL";
const TAIL_CONVERGENCE_VERSION: u8 = 1;
const TAIL_CONVERGENCE_BYTES: usize = TAIL_CONVERGENCE_MAGIC.len() + 1 + 8 + 8 + 8 + 1 + 8;
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
#[derive(Clone, Debug, Eq, PartialEq)]
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
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct JournalTailProofIdentity {
    data_mutation_revision: u64,
    fold_sequence: u64,
    fold_epoch: u64,
    next_append_sequence: u64,
    physical_record_count: u64,
}

impl JournalTailProofIdentity {
    /// Reconstruct one current-form proof from bounded persisted components.
    #[must_use]
    pub(in crate::db) const fn from_persisted_parts(
        data_mutation_revision: u64,
        fold_sequence: u64,
        fold_epoch: u64,
        next_append_sequence: u64,
        physical_record_count: u64,
    ) -> Self {
        Self {
            data_mutation_revision,
            fold_sequence,
            fold_epoch,
            next_append_sequence,
            physical_record_count,
        }
    }

    /// Return the durable logical row-mutation revision.
    #[must_use]
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

/// Exact current-format retained-tail contribution owned by one journal store.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct JournalTailControl {
    batch_count: u64,
    record_count: u64,
    encoded_batch_bytes: u64,
    head_database_commit_sequence: Option<DatabaseCommitSequence>,
}

impl JournalTailControl {
    #[must_use]
    pub(in crate::db) const fn empty() -> Self {
        Self {
            batch_count: 0,
            record_count: 0,
            encoded_batch_bytes: 0,
            head_database_commit_sequence: None,
        }
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn batch_count(self) -> u64 {
        self.batch_count
    }

    #[must_use]
    pub(in crate::db) const fn record_count(self) -> u64 {
        self.record_count
    }

    #[must_use]
    pub(in crate::db) const fn encoded_batch_bytes(self) -> u64 {
        self.encoded_batch_bytes
    }

    #[must_use]
    pub(in crate::db) const fn head_database_commit_sequence(
        self,
    ) -> Option<DatabaseCommitSequence> {
        self.head_database_commit_sequence
    }

    #[must_use]
    pub(in crate::db) const fn is_empty(self) -> bool {
        self.batch_count == 0
            && self.record_count == 0
            && self.encoded_batch_bytes == 0
            && self.head_database_commit_sequence.is_none()
    }
}

/// Fully preflighted exact retirement applied only after canonical batch mutation.
pub(in crate::db) struct PreparedJournalBatchRetirement {
    next_watermark: FoldWatermark,
    next_control: JournalTailControl,
    batch_keys: Vec<JournalTailKey>,
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

    const fn access_state_revision() -> Self {
        Self::new(
            FOLD_WATERMARK_CONTROL_SEQUENCE,
            ACCESS_STATE_REVISION_CONTROL_CHUNK,
        )
    }

    const fn tail_convergence_control() -> Self {
        Self::new(
            FOLD_WATERMARK_CONTROL_SEQUENCE,
            TAIL_CONVERGENCE_CONTROL_CHUNK,
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

    /// Initialize the exact current-format tail control on an empty predecessor tail.
    #[cfg(test)]
    pub(in crate::db) fn initialize_current_tail_control(&mut self) -> Result<(), InternalError> {
        if !self.preflight_current_tail_control_initialization()? {
            return Ok(());
        }
        self.apply_current_tail_control_initialization();
        Ok(())
    }

    /// Preflight zero-control initialization without mutating the journal allocation.
    pub(in crate::db) fn preflight_current_tail_control_initialization(
        &self,
    ) -> Result<bool, InternalError> {
        if self
            .map
            .contains_key(&JournalTailKey::tail_convergence_control())
        {
            let control = self.current_tail_control()?;
            if !control.is_empty() || self.has_stored_batch() {
                return Err(journal_tail_corruption());
            }
            return Ok(false);
        }
        if self.has_stored_batch() {
            return Err(InternalError::store_unsupported());
        }
        Ok(true)
    }

    /// Mechanically publish an already-preflighted empty current tail control.
    pub(in crate::db) fn apply_current_tail_control_initialization(&mut self) {
        self.map.insert(
            JournalTailKey::tail_convergence_control(),
            RawJournalChunk::from_bytes(encode_tail_control(JournalTailControl::empty())),
        );
    }

    /// Return whether current-format exact tail authority is present.
    #[must_use]
    pub(in crate::db) fn has_current_tail_control(&self) -> bool {
        self.map
            .contains_key(&JournalTailKey::tail_convergence_control())
    }

    /// Load the exact current-format retained-tail contribution.
    pub(in crate::db) fn current_tail_control(&self) -> Result<JournalTailControl, InternalError> {
        self.map
            .get(&JournalTailKey::tail_convergence_control())
            .ok_or_else(journal_tail_corruption)
            .and_then(|raw| decode_tail_control(raw.as_bytes()))
    }

    /// Validate current fixed control authority against the physical tail head.
    ///
    /// Routine startup intentionally does not recompute aggregate equality;
    /// that history-sized comparison belongs to explicit integrity work.
    pub(in crate::db) fn validate_current_tail_authority(
        &self,
    ) -> Result<JournalTailControl, InternalError> {
        let control = self.current_tail_control()?;
        let watermark = self.fold_watermark()?;
        let expected_head = watermark
            .highest_folded_journal_sequence()
            .next()
            .ok_or_else(journal_tail_corruption)?;
        let first = self
            .map
            .range((
                Included(JournalTailKey::new(JournalSequence::new(1), 0)),
                Unbounded,
            ))
            .next();
        match (control.is_empty(), first) {
            (true, None) => Ok(control),
            (false, Some(entry))
                if entry.key().sequence == expected_head && entry.key().chunk_index == 0 =>
            {
                let header = inspect_raw_journal_batch_fixed_header(entry.value().as_bytes())?;
                if header.journal_sequence() != expected_head
                    || control.head_database_commit_sequence()
                        != Some(header.database_commit_sequence())
                {
                    return Err(journal_tail_corruption());
                }
                Ok(control)
            }
            _ => Err(journal_tail_corruption()),
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
        let raw = RawJournalBatch::from_batch(batch)?;
        self.append_batch_bytes(batch, raw.as_bytes())
    }

    /// Append the exact batch envelope already persisted in the live marker.
    ///
    /// The fixed identity facts are rechecked, but the fingerprint is not
    /// recomputed: recovery owns verification if this message does not finish.
    pub(in crate::db) fn append_marker_encoded_batch(
        &mut self,
        batch: &JournalBatch,
        bytes: &[u8],
    ) -> Result<(), InternalError> {
        self.append_batch_bytes(batch, bytes)
    }

    fn append_batch_bytes(
        &mut self,
        batch: &JournalBatch,
        bytes: &[u8],
    ) -> Result<(), InternalError> {
        let key = batch.journal_sequence();
        if key == FOLD_WATERMARK_CONTROL_SEQUENCE {
            return Err(journal_tail_corruption());
        }
        let header = validate_encoded_batch_header(batch, bytes)?;
        let existing_prefix = self.raw_batch_prefix_bytes_for_sequence(key)?;
        let repairing_prefix = match existing_prefix.as_deref() {
            Some(existing) if existing == bytes => {
                self.validate_replayed_batch_against_control(header)?;
                return Ok(());
            }
            Some(existing) if existing.len() < bytes.len() && bytes.starts_with(existing) => true,
            Some(_) => return Err(journal_tail_corruption()),
            None => false,
        };
        #[cfg(test)]
        if !self.has_current_tail_control() {
            if repairing_prefix {
                self.apply_current_tail_control_initialization();
            } else {
                self.initialize_current_tail_control()?;
            }
        }
        let current_control = self.current_tail_control()?;
        let expected_sequence = if repairing_prefix {
            self.fold_watermark()?
                .highest_folded_journal_sequence()
                .next()
                .ok_or_else(journal_tail_corruption)?
        } else {
            self.next_append_sequence()?
        };
        if key != expected_sequence {
            return Err(journal_tail_corruption());
        }
        if !repairing_prefix
            && let Some(last_database_sequence) = self.last_database_commit_sequence()?
            && header.database_commit_sequence() <= last_database_sequence
        {
            return Err(journal_tail_corruption());
        }
        let next_control = prepare_appended_tail_control(current_control, header)?;
        let chunks = prepare_raw_batch_chunks(key, bytes)?;
        let row_mutation = batch.records().iter().any(|record| match record {
            JournalRecord::RowPut { .. } | JournalRecord::RowDelete { .. } => true,
            #[cfg(any(test, feature = "migration"))]
            JournalRecord::SchemaMigrationRowPut { .. } => true,
            _ => false,
        });
        let next_data_mutation_revision = if row_mutation {
            self.prepare_data_mutation_revision(key)?
        } else {
            None
        };

        for (chunk_key, chunk) in chunks {
            self.map
                .insert(chunk_key, RawJournalChunk::from_bytes(chunk));
        }
        if let Some(revision) = next_data_mutation_revision {
            self.map.insert(
                JournalTailKey::data_mutation_revision(),
                RawJournalChunk::from_bytes(encode_data_mutation_revision(revision)),
            );
        }
        self.map.insert(
            JournalTailKey::tail_convergence_control(),
            RawJournalChunk::from_bytes(encode_tail_control(next_control)),
        );
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

    /// Return the durable revision of physical access readiness for this store.
    pub(in crate::db) fn access_state_revision(&self) -> Result<u64, InternalError> {
        self.map
            .get(&JournalTailKey::access_state_revision())
            .map_or(Ok(1), |raw| decode_access_state_revision(raw.as_bytes()))
    }

    /// Advance physical access readiness after one actual lifecycle transition.
    pub(in crate::db) fn advance_access_state_revision(&mut self) -> Result<u64, InternalError> {
        let revision = self
            .access_state_revision()?
            .checked_add(1)
            .ok_or_else(journal_tail_corruption)?;
        self.map.insert(
            JournalTailKey::access_state_revision(),
            RawJournalChunk::from_bytes(encode_access_state_revision(revision)),
        );
        Ok(revision)
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
        self.validate_current_control_records()?;
        self.map
            .get(&JournalTailKey::fold_watermark())
            .map_or(Ok(FoldWatermark::initial()), |raw| {
                decode_fold_watermark(raw.as_bytes())
            })
    }

    fn validate_current_control_records(&self) -> Result<(), InternalError> {
        for entry in self.map.range((
            Included(JournalTailKey::new(FOLD_WATERMARK_CONTROL_SEQUENCE, 0)),
            Included(JournalTailKey::new(
                FOLD_WATERMARK_CONTROL_SEQUENCE,
                u32::MAX,
            )),
        )) {
            match entry.key().chunk_index {
                0 => {
                    let _watermark = decode_fold_watermark(entry.value().as_bytes())?;
                }
                DATA_MUTATION_REVISION_CONTROL_CHUNK => {
                    let _revision = decode_data_mutation_revision(entry.value().as_bytes())?;
                }
                ACCESS_STATE_REVISION_CONTROL_CHUNK => {
                    let _revision = decode_access_state_revision(entry.value().as_bytes())?;
                }
                TAIL_CONVERGENCE_CONTROL_CHUNK => {
                    let _control = decode_tail_control(entry.value().as_bytes())?;
                }
                _ => return Err(journal_tail_corruption()),
            }
        }
        Ok(())
    }

    /// Persist a new durable fold watermark.
    ///
    /// Watermarks may advance or be rewritten idempotently, but they never
    /// move backward. The journal tail itself is the replay-boundary authority;
    /// no extra stable memory ID is required.
    #[cfg(test)]
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
    #[cfg(test)]
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

    /// Preflight exact complete-batch retirement before canonical mutation begins.
    pub(in crate::db) fn prepare_batch_retirement(
        &self,
        batch: &JournalBatch,
        next_watermark: FoldWatermark,
    ) -> Result<PreparedJournalBatchRetirement, InternalError> {
        let current_watermark = self.fold_watermark()?;
        if current_watermark.highest_folded_journal_sequence().next()
            != Some(batch.journal_sequence())
            || next_watermark.highest_folded_journal_sequence() != batch.journal_sequence()
            || Some(next_watermark.fold_epoch()) != current_watermark.fold_epoch().checked_add(1)
        {
            return Err(journal_tail_corruption());
        }
        let bytes = self
            .raw_batch_bytes_for_sequence(batch.journal_sequence())?
            .ok_or_else(journal_tail_corruption)?;
        let header = validate_encoded_batch_header(batch, &bytes)?;
        let current_control = self.current_tail_control()?;
        if current_control.head_database_commit_sequence() != Some(batch.database_commit_sequence())
        {
            return Err(journal_tail_corruption());
        }
        let next_control = self.prepare_retired_tail_control(current_control, header, batch)?;
        let batch_keys = self
            .map
            .range((
                Included(JournalTailKey::new(batch.journal_sequence(), 0)),
                Included(JournalTailKey::new(batch.journal_sequence(), u32::MAX)),
            ))
            .map(|entry| *entry.key())
            .collect();
        Ok(PreparedJournalBatchRetirement {
            next_watermark,
            next_control,
            batch_keys,
        })
    }

    /// Mechanically publish one already-preflighted watermark/control/retirement boundary.
    pub(in crate::db) fn apply_prepared_batch_retirement(
        &mut self,
        retirement: PreparedJournalBatchRetirement,
    ) {
        self.map.insert(
            JournalTailKey::fold_watermark(),
            RawJournalChunk::from_bytes(encode_fold_watermark(retirement.next_watermark)),
        );
        self.map.insert(
            JournalTailKey::tail_convergence_control(),
            RawJournalChunk::from_bytes(encode_tail_control(retirement.next_control)),
        );
        for key in retirement.batch_keys {
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
    #[cfg(test)]
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

    /// Load the next complete batch after one durable fold watermark.
    ///
    /// Recovery uses this single-batch boundary to make fold progress durable
    /// between messages without materializing the remaining journal tail.
    pub(in crate::db) fn next_batch_after(
        &self,
        watermark: JournalSequence,
    ) -> Result<Option<JournalBatch>, InternalError> {
        let expected_sequence = watermark.next().ok_or_else(journal_tail_corruption)?;
        let Some(entry) = self
            .map
            .range((
                Included(JournalTailKey::new(expected_sequence, 0)),
                Unbounded,
            ))
            .next()
        else {
            return Ok(None);
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

        Ok(Some(batch))
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

    /// Insert raw journal-tail bytes for persisted-corruption tests.
    #[cfg(test)]
    pub(in crate::db) fn insert_raw_batch_for_tests(
        &mut self,
        sequence: JournalSequence,
        bytes: Vec<u8>,
    ) -> Result<(), InternalError> {
        self.append_raw_batch(sequence, bytes.as_slice())
    }

    #[cfg(test)]
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

        if let Some(existing) = self.raw_batch_prefix_bytes_for_sequence(sequence)? {
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

    fn prepare_data_mutation_revision(
        &self,
        sequence: JournalSequence,
    ) -> Result<Option<JournalSequence>, InternalError> {
        let current = self
            .map
            .get(&JournalTailKey::data_mutation_revision())
            .map_or(Ok(JournalSequence::new(0)), |raw| {
                decode_data_mutation_revision(raw.as_bytes())
            })?;
        if sequence <= current {
            return Ok(None);
        }
        let _ = sequence
            .next()
            .ok_or_else(InternalError::journal_mutation_revision_exhausted)?;
        Ok(Some(sequence))
    }

    fn validate_replayed_batch_against_control(
        &self,
        header: RawJournalBatchFixedHeader,
    ) -> Result<(), InternalError> {
        let control = self.validate_current_tail_authority()?;
        if control.is_empty()
            || control.record_count() < u64::from(header.record_count())
            || control.encoded_batch_bytes()
                < u64::try_from(header.total_len()).map_err(|_| journal_tail_corruption())?
        {
            return Err(journal_tail_corruption());
        }
        Ok(())
    }

    fn last_database_commit_sequence(
        &self,
    ) -> Result<Option<DatabaseCommitSequence>, InternalError> {
        let Some(entry) = self
            .map
            .range((
                Included(JournalTailKey::new(JournalSequence::new(1), 0)),
                Unbounded,
            ))
            .rev()
            .find(|entry| entry.key().sequence != FOLD_WATERMARK_CONTROL_SEQUENCE)
        else {
            return Ok(None);
        };
        let sequence = entry.key().sequence;
        let first = self
            .map
            .get(&JournalTailKey::new(sequence, 0))
            .ok_or_else(journal_tail_corruption)?;
        inspect_raw_journal_batch_fixed_header(first.as_bytes())
            .map(|header| Some(header.database_commit_sequence()))
    }

    fn prepare_retired_tail_control(
        &self,
        current: JournalTailControl,
        header: RawJournalBatchFixedHeader,
        batch: &JournalBatch,
    ) -> Result<JournalTailControl, InternalError> {
        let batch_count = current
            .batch_count
            .checked_sub(1)
            .ok_or_else(journal_tail_corruption)?;
        let record_count = current
            .record_count
            .checked_sub(u64::from(header.record_count()))
            .ok_or_else(journal_tail_corruption)?;
        let encoded_batch_bytes = current
            .encoded_batch_bytes
            .checked_sub(u64::try_from(header.total_len()).map_err(|_| journal_tail_corruption())?)
            .ok_or_else(journal_tail_corruption)?;
        let next_sequence = batch
            .journal_sequence()
            .next()
            .ok_or_else(journal_tail_corruption)?;
        let next_head = self
            .raw_batch_bytes_for_sequence(next_sequence)?
            .map(|bytes| inspect_raw_journal_batch_fixed_header(&bytes))
            .transpose()?
            .map(RawJournalBatchFixedHeader::database_commit_sequence);
        if batch_count == 0 {
            if record_count != 0 || encoded_batch_bytes != 0 || next_head.is_some() {
                return Err(journal_tail_corruption());
            }
        } else if next_head.is_none() {
            return Err(journal_tail_corruption());
        }
        Ok(JournalTailControl {
            batch_count,
            record_count,
            encoded_batch_bytes,
            head_database_commit_sequence: next_head,
        })
    }

    fn raw_batch_bytes_for_sequence(
        &self,
        sequence: JournalSequence,
    ) -> Result<Option<Vec<u8>>, InternalError> {
        let mut bytes = Vec::new();
        let mut expected_len = None;
        let mut expected_chunk = 0u32;

        for entry in self.map.range((
            Included(JournalTailKey::new(sequence, 0)),
            Included(JournalTailKey::new(sequence, u32::MAX)),
        )) {
            let key = entry.key();
            if key.chunk_index != expected_chunk {
                return Err(journal_tail_corruption());
            }
            if expected_chunk == 0 {
                let fixed_header =
                    inspect_raw_journal_batch_fixed_header(entry.value().as_bytes())?;
                if fixed_header.journal_sequence() != sequence {
                    return Err(journal_tail_corruption());
                }
                let total_len = fixed_header.total_len();
                bytes = Vec::with_capacity(total_len);
                expected_len = Some(total_len);
            }
            let total_len = expected_len.ok_or_else(journal_tail_corruption)?;
            let next_len = bytes
                .len()
                .checked_add(entry.value().as_bytes().len())
                .ok_or_else(journal_tail_corruption)?;
            if next_len > total_len {
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
        if bytes.len() != expected_len.ok_or_else(journal_tail_corruption)? {
            return Err(journal_tail_corruption());
        }

        Ok(Some(bytes))
    }

    fn raw_batch_prefix_bytes_for_sequence(
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

fn encode_access_state_revision(revision: u64) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(ACCESS_STATE_REVISION_BYTES);
    bytes.extend_from_slice(ACCESS_STATE_REVISION_MAGIC);
    bytes.push(ACCESS_STATE_REVISION_VERSION);
    bytes.extend_from_slice(&revision.to_be_bytes());
    bytes
}

fn decode_access_state_revision(bytes: &[u8]) -> Result<u64, InternalError> {
    if bytes.len() != ACCESS_STATE_REVISION_BYTES
        || !bytes.starts_with(ACCESS_STATE_REVISION_MAGIC)
        || bytes[ACCESS_STATE_REVISION_MAGIC.len()] != ACCESS_STATE_REVISION_VERSION
    {
        return Err(journal_tail_corruption());
    }
    let revision_start = ACCESS_STATE_REVISION_MAGIC.len() + 1;
    let mut revision = [0; size_of::<u64>()];
    revision.copy_from_slice(&bytes[revision_start..]);
    let revision = u64::from_be_bytes(revision);
    if revision == 0 {
        return Err(journal_tail_corruption());
    }
    Ok(revision)
}

fn encode_tail_control(control: JournalTailControl) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(TAIL_CONVERGENCE_BYTES);
    bytes.extend_from_slice(TAIL_CONVERGENCE_MAGIC);
    bytes.push(TAIL_CONVERGENCE_VERSION);
    bytes.extend_from_slice(&control.batch_count.to_be_bytes());
    bytes.extend_from_slice(&control.record_count.to_be_bytes());
    bytes.extend_from_slice(&control.encoded_batch_bytes.to_be_bytes());
    match control.head_database_commit_sequence {
        None => {
            bytes.push(0);
            bytes.extend_from_slice(&0_u64.to_be_bytes());
        }
        Some(sequence) => {
            bytes.push(1);
            bytes.extend_from_slice(&sequence.get().to_be_bytes());
        }
    }
    bytes
}

fn decode_tail_control(bytes: &[u8]) -> Result<JournalTailControl, InternalError> {
    if bytes.len() != TAIL_CONVERGENCE_BYTES
        || !bytes.starts_with(TAIL_CONVERGENCE_MAGIC)
        || bytes[TAIL_CONVERGENCE_MAGIC.len()] != TAIL_CONVERGENCE_VERSION
    {
        return Err(journal_tail_corruption());
    }
    let mut cursor = TAIL_CONVERGENCE_MAGIC.len() + 1;
    let batch_count = read_control_u64(bytes, &mut cursor)?;
    let record_count = read_control_u64(bytes, &mut cursor)?;
    let encoded_batch_bytes = read_control_u64(bytes, &mut cursor)?;
    let head_tag = *bytes.get(cursor).ok_or_else(journal_tail_corruption)?;
    cursor = cursor.saturating_add(1);
    let head_value = read_control_u64(bytes, &mut cursor)?;
    let head_database_commit_sequence = match (head_tag, head_value) {
        (0, 0) => None,
        (1, value) if value != 0 => Some(DatabaseCommitSequence::new(value)),
        _ => return Err(journal_tail_corruption()),
    };
    let control = JournalTailControl {
        batch_count,
        record_count,
        encoded_batch_bytes,
        head_database_commit_sequence,
    };
    let maximum_record_count = u64::try_from(MAX_JOURNAL_BATCH_RECORDS)
        .ok()
        .and_then(|maximum| maximum.checked_mul(batch_count));
    let minimum_encoded_bytes = u64::try_from(JOURNAL_BATCH_FIXED_HEADER_BYTES)
        .ok()
        .and_then(|minimum| minimum.checked_mul(batch_count));
    if control.is_empty()
        || (batch_count > 0
            && maximum_record_count.is_some_and(|maximum| record_count <= maximum)
            && minimum_encoded_bytes.is_some_and(|minimum| encoded_batch_bytes >= minimum)
            && head_database_commit_sequence.is_some())
    {
        return Ok(control);
    }
    Err(journal_tail_corruption())
}

fn read_control_u64(bytes: &[u8], cursor: &mut usize) -> Result<u64, InternalError> {
    let end = cursor.saturating_add(size_of::<u64>());
    let encoded = bytes
        .get(*cursor..end)
        .ok_or_else(journal_tail_corruption)?
        .try_into()
        .map_err(|_| journal_tail_corruption())?;
    *cursor = end;
    Ok(u64::from_be_bytes(encoded))
}

fn prepare_appended_tail_control(
    current: JournalTailControl,
    header: RawJournalBatchFixedHeader,
) -> Result<JournalTailControl, InternalError> {
    let batch_count = current
        .batch_count
        .checked_add(1)
        .ok_or_else(journal_tail_corruption)?;
    let record_count = current
        .record_count
        .checked_add(u64::from(header.record_count()))
        .ok_or_else(journal_tail_corruption)?;
    let encoded_batch_bytes = current
        .encoded_batch_bytes
        .checked_add(u64::try_from(header.total_len()).map_err(|_| journal_tail_corruption())?)
        .ok_or_else(journal_tail_corruption)?;
    Ok(JournalTailControl {
        batch_count,
        record_count,
        encoded_batch_bytes,
        head_database_commit_sequence: current
            .head_database_commit_sequence
            .or_else(|| Some(header.database_commit_sequence())),
    })
}

fn prepare_raw_batch_chunks(
    sequence: JournalSequence,
    bytes: &[u8],
) -> Result<Vec<(JournalTailKey, Vec<u8>)>, InternalError> {
    if bytes.is_empty() || bytes.len() > MAX_JOURNAL_BATCH_BYTES as usize {
        return Err(journal_tail_corruption());
    }
    bytes
        .chunks(JOURNAL_TAIL_CHUNK_BYTES as usize)
        .enumerate()
        .map(|(chunk_index, chunk)| {
            let chunk_index = u32::try_from(chunk_index).map_err(|_| journal_tail_corruption())?;
            Ok((JournalTailKey::new(sequence, chunk_index), chunk.to_vec()))
        })
        .collect()
}

fn validate_encoded_batch_header(
    batch: &JournalBatch,
    bytes: &[u8],
) -> Result<RawJournalBatchFixedHeader, InternalError> {
    let header = inspect_raw_journal_batch_fixed_header(bytes)?;
    let record_count =
        u32::try_from(batch.records().len()).map_err(|_| journal_tail_corruption())?;
    let observed = (
        header.total_len(),
        header.batch_id(),
        header.commit_marker_id(),
        header.journal_sequence(),
        header.database_commit_sequence(),
        header.record_count(),
    );
    let expected = (
        bytes.len(),
        batch.batch_id(),
        batch.commit_marker_id(),
        batch.journal_sequence(),
        batch.database_commit_sequence(),
        record_count,
    );
    if observed != expected {
        return Err(journal_tail_corruption());
    }
    Ok(header)
}

fn journal_tail_corruption() -> InternalError {
    InternalError::store_corruption()
}

#[cfg(test)]
mod convergence_control_tests {
    use super::*;
    use crate::{
        db::{
            journal::{
                DatabaseCommitSequence, JournalBatch, encode_journal_batch,
                journal_batch_encoded_len, journal_record_payload_len,
            },
            schema::MAX_SCHEMA_SNAPSHOT_BYTES,
        },
        error::{ErrorClass, ErrorOrigin},
        testing::test_memory,
    };

    fn batch(journal_sequence: u64, database_commit_sequence: u64) -> JournalBatch {
        let batch_id = u8::try_from(journal_sequence).expect("test sequence should fit u8");
        JournalBatch::new_with_database_commit_sequence(
            [batch_id; 16],
            [0xA5; 16],
            JournalSequence::new(journal_sequence),
            DatabaseCommitSequence::new(database_commit_sequence),
            Vec::new(),
        )
        .expect("test journal batch should build")
    }

    fn set_control(store: &mut JournalTailStore, control: JournalTailControl) {
        store.map.insert(
            JournalTailKey::tail_convergence_control(),
            RawJournalChunk::from_bytes(encode_tail_control(control)),
        );
    }

    #[test]
    fn exact_controls_append_replay_retire_and_reopen() {
        let memory = test_memory(176);
        let mut store = JournalTailStore::init(memory.clone());
        store
            .initialize_current_tail_control()
            .expect("empty current control should initialize");
        let first = batch(1, 7);
        let second = batch(2, 8);
        let first_bytes = encode_journal_batch(&first).expect("first batch should encode");
        let second_bytes = encode_journal_batch(&second).expect("second batch should encode");

        store
            .append_batch(&first)
            .expect("first batch should append");
        let first_control = store.current_tail_control().expect("control should decode");
        assert_eq!(first_control.batch_count(), 1);
        assert_eq!(first_control.record_count(), 0);
        assert_eq!(
            first_control.encoded_batch_bytes(),
            first_bytes.len() as u64
        );
        assert_eq!(
            first_control.head_database_commit_sequence(),
            Some(DatabaseCommitSequence::new(7)),
        );

        store
            .append_batch(&first)
            .expect("identical replay should be idempotent");
        assert_eq!(store.current_tail_control().unwrap(), first_control);
        store
            .append_batch(&second)
            .expect("second batch should append");
        let both = store.current_tail_control().expect("control should decode");
        assert_eq!(both.batch_count(), 2);
        assert_eq!(
            both.encoded_batch_bytes(),
            (first_bytes.len() + second_bytes.len()) as u64
        );
        assert_eq!(
            both.head_database_commit_sequence(),
            Some(DatabaseCommitSequence::new(7)),
        );

        let retirement = store
            .prepare_batch_retirement(&first, FoldWatermark::new(JournalSequence::new(1), 1))
            .expect("first retirement should preflight");
        store.apply_prepared_batch_retirement(retirement);
        drop(store);

        let mut reopened = JournalTailStore::init(memory);
        let remaining = reopened
            .current_tail_control()
            .expect("control should reopen");
        assert_eq!(remaining.batch_count(), 1);
        assert_eq!(remaining.encoded_batch_bytes(), second_bytes.len() as u64);
        assert_eq!(
            remaining.head_database_commit_sequence(),
            Some(DatabaseCommitSequence::new(8)),
        );
        let retirement = reopened
            .prepare_batch_retirement(&second, FoldWatermark::new(JournalSequence::new(2), 2))
            .expect("second retirement should preflight");
        reopened.apply_prepared_batch_retirement(retirement);
        assert_eq!(
            reopened
                .current_tail_control()
                .expect("control should decode"),
            JournalTailControl::empty(),
        );
        assert!(!reopened.has_stored_batch());
    }

    #[test]
    fn exact_maximum_encoded_batch_append_is_counted_without_approximation() {
        let mut records = (0..31)
            .map(|ordinal| {
                JournalRecord::schema_put(
                    format!("test::MaximumStore{ordinal}"),
                    vec![0xA5; MAX_SCHEMA_SNAPSHOT_BYTES as usize],
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        let final_empty = JournalRecord::schema_put("test::MaximumStoreFinal", Vec::new()).unwrap();
        let used = JOURNAL_BATCH_FIXED_HEADER_BYTES
            + records
                .iter()
                .map(journal_record_payload_len)
                .sum::<usize>();
        let final_payload_len = (MAX_JOURNAL_BATCH_BYTES as usize)
            .checked_sub(used)
            .and_then(|remaining| remaining.checked_sub(journal_record_payload_len(&final_empty)))
            .expect("maximum envelope should retain one final schema payload");
        assert!(final_payload_len <= MAX_SCHEMA_SNAPSHOT_BYTES as usize);
        records.push(
            JournalRecord::schema_put("test::MaximumStoreFinal", vec![0x5A; final_payload_len])
                .unwrap(),
        );
        let batch = JournalBatch::new_with_database_commit_sequence(
            [0xC1; 16],
            [0xC2; 16],
            JournalSequence::new(1),
            DatabaseCommitSequence::new(1),
            records,
        )
        .unwrap();
        assert_eq!(
            journal_batch_encoded_len(&batch),
            MAX_JOURNAL_BATCH_BYTES as usize
        );

        let mut store = JournalTailStore::init(test_memory(174));
        store.initialize_current_tail_control().unwrap();
        store.append_batch(&batch).unwrap();
        let control = store.current_tail_control().unwrap();
        assert_eq!(control.batch_count(), 1);
        assert_eq!(control.record_count(), 32);
        assert_eq!(
            control.encoded_batch_bytes(),
            u64::from(MAX_JOURNAL_BATCH_BYTES),
        );
    }

    #[test]
    fn append_rejects_nonmonotonic_database_sequence_before_tail_mutation() {
        let mut store = JournalTailStore::init(test_memory(177));
        store.initialize_current_tail_control().unwrap();
        store.append_batch(&batch(1, 10)).unwrap();

        let error = store
            .append_batch(&batch(2, 9))
            .expect_err("database order must advance across local appends");
        assert_eq!(error.class(), ErrorClass::Corruption);
        assert_eq!(error.origin(), ErrorOrigin::Store);
        assert_eq!(store.current_tail_control().unwrap().batch_count(), 1);
    }

    #[test]
    fn aggregate_overflow_and_retirement_underflow_fail_closed() {
        let mut overflow = JournalTailStore::init(test_memory(178));
        overflow.initialize_current_tail_control().unwrap();
        set_control(
            &mut overflow,
            JournalTailControl {
                batch_count: u64::MAX,
                record_count: 0,
                encoded_batch_bytes: u64::MAX,
                head_database_commit_sequence: Some(DatabaseCommitSequence::new(1)),
            },
        );
        assert!(overflow.append_batch(&batch(1, 2)).is_err());
        assert!(!overflow.has_stored_batch());

        let mut underflow = JournalTailStore::init(test_memory(179));
        underflow.initialize_current_tail_control().unwrap();
        let retained = batch(1, 3);
        underflow.append_batch(&retained).unwrap();
        let encoded_len = encode_journal_batch(&retained).unwrap().len() as u64;
        set_control(
            &mut underflow,
            JournalTailControl {
                batch_count: 1,
                record_count: 0,
                encoded_batch_bytes: encoded_len - 1,
                head_database_commit_sequence: Some(DatabaseCommitSequence::new(3)),
            },
        );
        assert!(
            underflow
                .prepare_batch_retirement(
                    &retained,
                    FoldWatermark::new(JournalSequence::new(1), 1),
                )
                .is_err()
        );
        assert!(underflow.has_stored_batch());
    }

    #[test]
    fn missing_malformed_and_mismatched_head_controls_fail_closed() {
        let mut store = JournalTailStore::init(test_memory(180));
        assert!(store.current_tail_control().is_err());
        store.map.insert(
            JournalTailKey::tail_convergence_control(),
            RawJournalChunk::from_bytes(vec![0xFF; TAIL_CONVERGENCE_BYTES]),
        );
        assert!(store.current_tail_control().is_err());

        let mut mismatch = JournalTailStore::init(test_memory(175));
        mismatch.initialize_current_tail_control().unwrap();
        let retained = batch(1, 4);
        mismatch.append_batch(&retained).unwrap();
        let mut control = mismatch.current_tail_control().unwrap();
        control.head_database_commit_sequence = Some(DatabaseCommitSequence::new(5));
        set_control(&mut mismatch, control);
        assert!(mismatch.append_batch(&retained).is_err());
        assert!(
            mismatch
                .prepare_batch_retirement(
                    &retained,
                    FoldWatermark::new(JournalSequence::new(1), 1),
                )
                .is_err()
        );
    }
}
