//! Module: db::integrity::progress_store
//! Responsibility: independently persist one bounded record per Deep job.
//! Does not own: inspected database state, commit markers, journals, or advancement semantics.
//! Boundary: current-form job codec -> physically separate stable BTreeMap allocation.

use crate::{
    db::{
        codec::{finalize_hash_sha256, new_hash_sha256_prefixed},
        database_format::crc32c,
        integrity::{
            IntegrityJob, IntegrityJobError, IntegrityJobId, IntegrityJobOwner, IntegrityJobState,
            progress_codec::{
                MAX_INTEGRITY_JOB_PAYLOAD_BYTES, decode_integrity_job_payload,
                encode_integrity_job_payload,
            },
        },
        mutation_job::{
            MAX_MUTATION_JOB_RECORD_BYTES, MutationJobError, MutationJobId, MutationJobRecord,
            MutationJobStatus, decode_mutation_job_payload, encode_mutation_job_payload,
        },
        resumable_job::{
            ResumableJobError, ResumableJobId, ResumableJobRecord, ResumableJobStatus,
            decode_resumable_job_payload, encode_resumable_job_payload,
        },
    },
    error::InternalError,
    traits::CanisterKind,
};
use candid::CandidType;
#[cfg(not(test))]
use ic_memory::open_default_memory_manager_memory;
use ic_stable_structures::{
    BTreeMap as StableBTreeMap, DefaultMemoryImpl, Storable, memory_manager::VirtualMemory,
    storable::Bound,
};
use serde::Deserialize;
use sha2::Digest;
use std::borrow::Cow;
#[cfg(test)]
use std::cell::RefCell;
use std::ops::Bound::{Excluded, Unbounded};

const PROGRESS_HEADER_KEY: ProgressRecordKey = ProgressRecordKey([0; 32]);
const PROGRESS_HEADER_MAGIC: &[u8; 8] = b"ICYIPROG";
const PROGRESS_HEADER_VERSION: u8 = 1;
const PROGRESS_HEADER_BYTES: usize = 8 + 1 + 4;
const JOB_RECORD_MAGIC: &[u8; 8] = b"ICYIJPTH";
const JOB_RECORD_VERSION: u8 = 1;
const JOB_RECORD_HEADER_BYTES: usize = 8 + 1 + 4 + 4;
const RESUMABLE_JOB_KEY_DOMAIN: &[u8] = b"icydb.resumable-job.progress-key.v1";
const RESUMABLE_JOB_RECORD_MAGIC: &[u8; 8] = b"ICYRJOB1";
const RESUMABLE_JOB_RECORD_VERSION: u8 = 1;
const RESUMABLE_JOB_RECORD_HEADER_BYTES: usize = 8 + 1 + 4 + 4;
const MUTATION_JOB_KEY_DOMAIN: &[u8] = b"icydb.mutation-job.progress-key.v1";
const MUTATION_JOB_RECORD_MAGIC: &[u8; 8] = b"ICYMJOB1";
const MUTATION_JOB_RECORD_VERSION: u8 = 1;
const MUTATION_JOB_RECORD_HEADER_BYTES: usize = 8 + 1 + 4 + 4;
const MUTATION_PROGRESS_BEFORE_DIGEST_DOMAIN: &[u8] = b"icydb.mutation-job.progress-before.v1";
const MAX_PROGRESS_RECORD_BYTES: u32 = 512 * 1024;
const MAX_PROGRESS_JOBS_GLOBAL: u64 = 64;
const MAX_PROGRESS_JOBS_NON_INTEGRITY: u64 = 56;
const PROGRESS_JOBS_INTEGRITY_RESERVATION: u64 =
    MAX_PROGRESS_JOBS_GLOBAL - MAX_PROGRESS_JOBS_NON_INTEGRITY;
const MAX_PROGRESS_JOBS_PER_OWNER: u64 = 8;

/// Stable family of one retained record in the shared progress allocation.
#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
pub enum ProgressJobFamily {
    /// Engine-owned Deep integrity inspection.
    Integrity,
    /// Application-owned generic resumable job.
    Resumable,
    /// Engine-owned fixed SQL mutation job.
    Mutation,
}

/// Bounded normalized lifecycle of one retained progress record.
#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
pub enum ProgressJobLifecycle {
    /// More work may advance.
    Active,
    /// Integrity advancement is frozen before its terminal receipt is published.
    TerminalPending,
    /// Successful exhaustion is retained.
    Completed,
    /// Protected source authority changed.
    Invalidated,
    /// Mutation authority or policy requires a fresh job.
    RestartRequired,
    /// Another integrity terminal outcome is retained.
    Terminal,
}

/// One privacy-bounded retained-record inventory entry.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct ProgressJobInventoryRecord {
    /// Durable progress family.
    pub family: ProgressJobFamily,
    /// Family-owned job identity bytes.
    pub job_id: [u8; 32],
    /// Normalized retained lifecycle.
    pub lifecycle: ProgressJobLifecycle,
    /// Family sequence when its current record carries one.
    pub sequence: Option<u64>,
}

/// Complete bounded inventory of the shared excluded progress allocation.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct ProgressJobInventory {
    /// Total retained records across all families.
    pub retained_count: u64,
    /// Hard retained-record capacity.
    pub hard_limit: u64,
    /// Slots protected for integrity work from non-integrity starts.
    pub reserved_integrity_headroom: u64,
    /// Retained Deep integrity records.
    pub integrity_count: u64,
    /// Retained generic resumable records.
    pub resumable_count: u64,
    /// Retained fixed SQL mutation records.
    pub mutation_count: u64,
    /// Every validated retained record in stable key order.
    pub records: Vec<ProgressJobInventoryRecord>,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ProgressRecordKey([u8; 32]);

impl ProgressRecordKey {
    const fn from_job_id(job_id: IntegrityJobId) -> Self {
        Self(job_id.to_bytes())
    }

    fn from_resumable_job_id(job_id: ResumableJobId) -> Result<Self, ResumableJobError> {
        let mut hasher = new_hash_sha256_prefixed(RESUMABLE_JOB_KEY_DOMAIN);
        hasher.update(job_id.to_bytes());
        let key = finalize_hash_sha256(hasher);
        if key == PROGRESS_HEADER_KEY.0 {
            return Err(ResumableJobError::InvalidJobId);
        }
        Ok(Self(key))
    }

    fn from_mutation_job_id(job_id: MutationJobId) -> Result<Self, MutationJobError> {
        job_id.validate()?;
        let mut hasher = new_hash_sha256_prefixed(MUTATION_JOB_KEY_DOMAIN);
        hasher.update(job_id.to_bytes());
        let key = finalize_hash_sha256(hasher);
        if key == PROGRESS_HEADER_KEY.0 {
            return Err(MutationJobError::InvalidJobId);
        }
        Ok(Self(key))
    }

    const fn to_bytes(self) -> [u8; 32] {
        self.0
    }
}

impl Storable for ProgressRecordKey {
    fn to_bytes(&self) -> Cow<'_, [u8]> {
        Cow::Borrowed(&self.0)
    }

    fn from_bytes(bytes: Cow<'_, [u8]>) -> Self {
        let mut key = [0; 32];
        if bytes.len() == key.len() {
            key.copy_from_slice(bytes.as_ref());
        }
        Self(key)
    }

    fn into_bytes(self) -> Vec<u8> {
        self.0.to_vec()
    }

    const BOUND: Bound = Bound::Bounded {
        max_size: 32,
        is_fixed_size: true,
    };
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ProgressRecordBytes(Vec<u8>);

impl Storable for ProgressRecordBytes {
    fn to_bytes(&self) -> Cow<'_, [u8]> {
        Cow::Borrowed(self.0.as_slice())
    }

    fn from_bytes(bytes: Cow<'_, [u8]>) -> Self {
        Self(bytes.into_owned())
    }

    fn into_bytes(self) -> Vec<u8> {
        self.0
    }

    const BOUND: Bound = Bound::Bounded {
        max_size: MAX_PROGRESS_RECORD_BYTES,
        is_fixed_size: false,
    };
}

pub(super) enum InsertJobResult {
    Inserted,
    Occupied(Box<IntegrityJob>),
}

pub(in crate::db) enum InsertMutationJobResult {
    Inserted,
    Occupied(Box<MutationJobRecord>),
}

/// Exact mutation-progress compare-and-replace effect carried by one commit marker.
///
/// Every constructor validates the complete before/after proof. Store
/// preflight and apply therefore compare only the already-canonical bytes and
/// do not repeat bounded record decoding on the commit hot path.
#[derive(Clone, Debug)]
pub(in crate::db) struct MutationProgressRecordOp {
    key: ProgressRecordKey,
    job_id: MutationJobId,
    expected_sequence: u64,
    expected_before_digest: [u8; 32],
    before: Vec<u8>,
    after: Vec<u8>,
}

impl MutationProgressRecordOp {
    /// Build one exact successor replacement from two validated current records.
    pub(in crate::db) fn replace(
        before: &MutationJobRecord,
        after: &MutationJobRecord,
    ) -> Result<Self, MutationJobError> {
        let job_id = before.state().job_id;
        let expected_sequence = before.state().sequence;
        let key = ProgressRecordKey::from_mutation_job_id(job_id)?;
        let before = encode_mutation_job_record(before)?;
        Self::from_encoded(
            key.to_bytes(),
            job_id,
            expected_sequence,
            mutation_progress_before_digest(&before),
            before,
            encode_mutation_job_record(after)?,
        )
    }

    /// Reconstruct and validate one current marker-owned replacement.
    pub(in crate::db) fn from_encoded(
        key: [u8; 32],
        job_id: MutationJobId,
        expected_sequence: u64,
        expected_before_digest: [u8; 32],
        before: Vec<u8>,
        after: Vec<u8>,
    ) -> Result<Self, MutationJobError> {
        let operation = Self {
            key: ProgressRecordKey(key),
            job_id,
            expected_sequence,
            expected_before_digest,
            before,
            after,
        };
        operation.validate()?;
        Ok(operation)
    }

    pub(in crate::db) const fn key(&self) -> [u8; 32] {
        self.key.to_bytes()
    }

    pub(in crate::db) const fn job_id(&self) -> MutationJobId {
        self.job_id
    }

    pub(in crate::db) const fn expected_sequence(&self) -> u64 {
        self.expected_sequence
    }

    pub(in crate::db) const fn expected_before_digest(&self) -> [u8; 32] {
        self.expected_before_digest
    }

    pub(in crate::db) const fn before_bytes(&self) -> &[u8] {
        self.before.as_slice()
    }

    pub(in crate::db) const fn after_bytes(&self) -> &[u8] {
        self.after.as_slice()
    }

    pub(in crate::db) fn validate(&self) -> Result<(), MutationJobError> {
        self.job_id.validate()?;
        if ProgressRecordKey::from_mutation_job_id(self.job_id)? != self.key
            || mutation_progress_before_digest(&self.before) != self.expected_before_digest
        {
            return Err(MutationJobError::CorruptProgressStore);
        }
        let before = decode_mutation_job_record(&self.before, self.job_id)?;
        let after = decode_mutation_job_record(&self.after, self.job_id)?;
        let Some(expected_after_sequence) = self.expected_sequence.checked_add(1) else {
            return Err(MutationJobError::CounterOverflow);
        };
        if before.state().sequence != self.expected_sequence
            || after.state().sequence != expected_after_sequence
            || before.canonical_intent() != after.canonical_intent()
        {
            return Err(MutationJobError::CorruptProgressStore);
        }
        Ok(())
    }
}

pub(super) struct ProgressScanPage {
    pub(super) job_ids: Vec<IntegrityJobId>,
    pub(super) exhausted: bool,
}

pub(in crate::db) struct InspectionProgressStore {
    map: StableBTreeMap<ProgressRecordKey, ProgressRecordBytes, VirtualMemory<DefaultMemoryImpl>>,
}

impl InspectionProgressStore {
    fn open(memory: VirtualMemory<DefaultMemoryImpl>) -> Result<Self, IntegrityJobError> {
        let mut store = Self {
            map: StableBTreeMap::init(memory),
        };
        if store.map.is_empty() {
            store.map.insert(
                PROGRESS_HEADER_KEY,
                ProgressRecordBytes(encode_progress_header()),
            );
        } else {
            let header = store
                .map
                .get(&PROGRESS_HEADER_KEY)
                .ok_or(IntegrityJobError::CorruptProgressHeader)?;
            decode_progress_header(&header.0)?;
            if store.job_count()? > MAX_PROGRESS_JOBS_GLOBAL {
                return Err(IntegrityJobError::CorruptProgressHeader);
            }
        }
        Ok(store)
    }

    pub(super) fn load(&self, job_id: IntegrityJobId) -> Result<IntegrityJob, IntegrityJobError> {
        let raw = self
            .map
            .get(&ProgressRecordKey::from_job_id(job_id))
            .ok_or(IntegrityJobError::JobNotFound)?;
        decode_job_record(&raw.0, job_id)
    }

    pub(super) fn insert_new(
        &mut self,
        job: &IntegrityJob,
    ) -> Result<InsertJobResult, IntegrityJobError> {
        job.validate()?;
        let key = ProgressRecordKey::from_job_id(job.id);
        if key == PROGRESS_HEADER_KEY {
            return Err(IntegrityJobError::CorruptProgressRecord);
        }
        if let Some(raw) = self.map.get(&key) {
            return decode_job_record(&raw.0, job.id)
                .map(Box::new)
                .map(InsertJobResult::Occupied);
        }
        if self.job_count()? >= MAX_PROGRESS_JOBS_GLOBAL
            || self.owner_job_count(&job.owner)? >= MAX_PROGRESS_JOBS_PER_OWNER
        {
            return Err(IntegrityJobError::CapacityExceeded);
        }
        self.map
            .insert(key, ProgressRecordBytes(encode_job_record(job)?));
        Ok(InsertJobResult::Inserted)
    }

    pub(super) fn replace(&mut self, job: &IntegrityJob) -> Result<(), IntegrityJobError> {
        job.validate()?;
        let key = ProgressRecordKey::from_job_id(job.id);
        if !self.map.contains_key(&key) {
            return Err(IntegrityJobError::JobNotFound);
        }
        self.map
            .insert(key, ProgressRecordBytes(encode_job_record(job)?));
        Ok(())
    }

    pub(in crate::db) fn load_resumable(
        &self,
        job_id: ResumableJobId,
    ) -> Result<ResumableJobRecord, ResumableJobError> {
        let key = ProgressRecordKey::from_resumable_job_id(job_id)?;
        let raw = self.map.get(&key).ok_or(ResumableJobError::NotFound)?;
        decode_resumable_job_record(&raw.0, job_id)
    }

    pub(in crate::db) fn insert_resumable(
        &mut self,
        record: &ResumableJobRecord,
    ) -> Result<(), ResumableJobError> {
        record.validate()?;
        let key = ProgressRecordKey::from_resumable_job_id(record.state().job_id)?;
        if self.map.contains_key(&key) {
            return Err(ResumableJobError::AlreadyExists);
        }
        if self.job_count().map_err(map_integrity_store_error)? >= MAX_PROGRESS_JOBS_NON_INTEGRITY {
            return Err(ResumableJobError::CapacityExceeded);
        }
        self.map.insert(
            key,
            ProgressRecordBytes(encode_resumable_job_record(record)?),
        );
        Ok(())
    }

    pub(in crate::db) fn replace_resumable(
        &mut self,
        record: &ResumableJobRecord,
    ) -> Result<(), ResumableJobError> {
        record.validate()?;
        let key = ProgressRecordKey::from_resumable_job_id(record.state().job_id)?;
        if !self.map.contains_key(&key) {
            return Err(ResumableJobError::NotFound);
        }
        self.map.insert(
            key,
            ProgressRecordBytes(encode_resumable_job_record(record)?),
        );
        Ok(())
    }

    pub(in crate::db) fn remove_resumable(
        &mut self,
        job_id: ResumableJobId,
    ) -> Result<(), ResumableJobError> {
        let key = ProgressRecordKey::from_resumable_job_id(job_id)?;
        let Some(raw) = self.map.get(&key) else {
            return Ok(());
        };
        decode_resumable_job_record(&raw.0, job_id)?;
        let _ = self.map.remove(&key);
        Ok(())
    }

    pub(in crate::db) fn load_mutation(
        &self,
        job_id: MutationJobId,
    ) -> Result<MutationJobRecord, MutationJobError> {
        let key = ProgressRecordKey::from_mutation_job_id(job_id)?;
        let raw = self.map.get(&key).ok_or(MutationJobError::NotFound)?;
        decode_mutation_job_record(&raw.0, job_id)
    }

    pub(in crate::db) fn insert_mutation(
        &mut self,
        record: &MutationJobRecord,
    ) -> Result<InsertMutationJobResult, MutationJobError> {
        record.validate()?;
        let key = ProgressRecordKey::from_mutation_job_id(record.state().job_id)?;
        if let Some(raw) = self.map.get(&key) {
            return decode_mutation_job_record(&raw.0, record.state().job_id)
                .map(Box::new)
                .map(InsertMutationJobResult::Occupied);
        }
        if self.job_count().map_err(map_mutation_store_error)? >= MAX_PROGRESS_JOBS_NON_INTEGRITY {
            return Err(MutationJobError::CapacityExceeded);
        }
        self.map.insert(
            key,
            ProgressRecordBytes(encode_mutation_job_record(record)?),
        );
        Ok(InsertMutationJobResult::Inserted)
    }

    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "mutation-job advancement owns sequence-checked replacement"
        )
    )]
    pub(in crate::db) fn replace_mutation(
        &mut self,
        record: &MutationJobRecord,
    ) -> Result<(), MutationJobError> {
        record.validate()?;
        let key = ProgressRecordKey::from_mutation_job_id(record.state().job_id)?;
        if !self.map.contains_key(&key) {
            return Err(MutationJobError::NotFound);
        }
        self.map.insert(
            key,
            ProgressRecordBytes(encode_mutation_job_record(record)?),
        );
        Ok(())
    }

    fn preflight_mutation_progress(
        &self,
        operation: &MutationProgressRecordOp,
    ) -> Result<(), MutationJobError> {
        let current = self
            .map
            .get(&operation.key)
            .ok_or(MutationJobError::CorruptProgressStore)?;
        if current.0 != operation.before {
            return Err(MutationJobError::CorruptProgressStore);
        }
        Ok(())
    }

    fn apply_mutation_progress(
        &mut self,
        operation: &MutationProgressRecordOp,
    ) -> Result<(), MutationJobError> {
        let current = self
            .map
            .get(&operation.key)
            .ok_or(MutationJobError::CorruptProgressStore)?;
        if current.0 == operation.after {
            return Ok(());
        }
        if current.0 != operation.before {
            return Err(MutationJobError::CorruptProgressStore);
        }
        self.map
            .insert(operation.key, ProgressRecordBytes(operation.after.clone()));
        Ok(())
    }

    fn apply_preflighted_mutation_progress(&mut self, operation: &MutationProgressRecordOp) {
        self.map
            .insert(operation.key, ProgressRecordBytes(operation.after.clone()));
    }

    /// Replace one mutation record without target-row work using the same exact
    /// before/after proof as marker-owned mutation progress.
    pub(in crate::db) fn replace_mutation_progress(
        &mut self,
        operation: &MutationProgressRecordOp,
    ) -> Result<(), MutationJobError> {
        self.apply_mutation_progress(operation)
    }

    fn verify_mutation_progress(
        &self,
        operation: &MutationProgressRecordOp,
    ) -> Result<(), MutationJobError> {
        operation.validate()?;
        let current = self
            .map
            .get(&operation.key)
            .ok_or(MutationJobError::CorruptProgressStore)?;
        if current.0 != operation.after {
            return Err(MutationJobError::CorruptProgressStore);
        }
        Ok(())
    }

    pub(in crate::db) fn acknowledge_mutation(
        &mut self,
        job_id: MutationJobId,
        expected_sequence: u64,
    ) -> Result<(), MutationJobError> {
        let key = ProgressRecordKey::from_mutation_job_id(job_id)?;
        let Some(raw) = self.map.get(&key) else {
            return Ok(());
        };
        let record = decode_mutation_job_record(&raw.0, job_id)?;
        if record.state().sequence != expected_sequence {
            return Err(MutationJobError::StaleSequence {
                expected: expected_sequence,
                actual: record.state().sequence,
            });
        }
        if record.state().status == crate::db::MutationJobStatus::Active {
            return Err(MutationJobError::Active);
        }
        let _ = self.map.remove(&key);
        Ok(())
    }

    /// Remove exactly one current initial mutation record.
    ///
    /// The caller supplies the engine-format validator so the shared store
    /// remains independent of SQL continuation semantics while still making
    /// validation and removal one indivisible store borrow.
    #[cfg(feature = "sql")]
    pub(in crate::db) fn cancel_unadvanced_mutation(
        &mut self,
        job_id: MutationJobId,
        expected_sequence: u64,
        validate_initial_continuation: impl FnOnce(&[u8]) -> Result<(), MutationJobError>,
    ) -> Result<(), MutationJobError> {
        let key = ProgressRecordKey::from_mutation_job_id(job_id)?;
        let Some(raw) = self.map.get(&key) else {
            return Ok(());
        };
        let record = decode_mutation_job_record(&raw.0, job_id)?;
        let continuation = record.ensure_cancelable_at_sequence(expected_sequence)?;
        validate_initial_continuation(continuation)?;
        let _ = self.map.remove(&key);
        Ok(())
    }

    /// Decode every retained slot before returning one complete bounded inventory.
    pub(in crate::db) fn inventory(&self) -> Result<ProgressJobInventory, MutationJobError> {
        let retained_count = self.job_count().map_err(map_mutation_store_error)?;
        let record_capacity =
            usize::try_from(retained_count).map_err(|_| MutationJobError::CorruptProgressStore)?;
        let mut records = Vec::with_capacity(record_capacity);
        let mut integrity_count = 0_u64;
        let mut resumable_count = 0_u64;
        let mut mutation_count = 0_u64;

        for entry in self.map.iter() {
            let key = *entry.key();
            if key == PROGRESS_HEADER_KEY {
                continue;
            }
            let bytes = &entry.value().0;
            let record = if bytes.starts_with(JOB_RECORD_MAGIC) {
                let job_id = IntegrityJobId::try_from_bytes(key.to_bytes())
                    .map_err(|_| MutationJobError::CorruptProgressStore)?;
                let job = decode_job_record(bytes, job_id)
                    .map_err(|_| MutationJobError::CorruptProgressStore)?;
                integrity_count = integrity_count
                    .checked_add(1)
                    .ok_or(MutationJobError::CorruptProgressStore)?;
                ProgressJobInventoryRecord {
                    family: ProgressJobFamily::Integrity,
                    job_id: job.id.to_bytes(),
                    lifecycle: match job.state {
                        IntegrityJobState::InProgress => ProgressJobLifecycle::Active,
                        IntegrityJobState::TerminalPending(_) => {
                            ProgressJobLifecycle::TerminalPending
                        }
                        IntegrityJobState::Terminal { .. } => ProgressJobLifecycle::Terminal,
                    },
                    sequence: Some(job.pages_completed),
                }
            } else if bytes.starts_with(RESUMABLE_JOB_RECORD_MAGIC) {
                let job = decode_resumable_job_record_for_inventory(bytes, key)?;
                resumable_count = resumable_count
                    .checked_add(1)
                    .ok_or(MutationJobError::CorruptProgressStore)?;
                ProgressJobInventoryRecord {
                    family: ProgressJobFamily::Resumable,
                    job_id: job.state().job_id.to_bytes(),
                    lifecycle: match job.state().status {
                        ResumableJobStatus::Active => ProgressJobLifecycle::Active,
                        ResumableJobStatus::Completed => ProgressJobLifecycle::Completed,
                        ResumableJobStatus::Invalidated => ProgressJobLifecycle::Invalidated,
                    },
                    sequence: Some(job.state().sequence),
                }
            } else if bytes.starts_with(MUTATION_JOB_RECORD_MAGIC) {
                let job = decode_mutation_job_record_for_inventory(bytes, key)?;
                mutation_count = mutation_count
                    .checked_add(1)
                    .ok_or(MutationJobError::CorruptProgressStore)?;
                ProgressJobInventoryRecord {
                    family: ProgressJobFamily::Mutation,
                    job_id: job.state().job_id.to_bytes(),
                    lifecycle: match job.state().status {
                        MutationJobStatus::Active => ProgressJobLifecycle::Active,
                        MutationJobStatus::Completed => ProgressJobLifecycle::Completed,
                        MutationJobStatus::RestartRequired(_) => {
                            ProgressJobLifecycle::RestartRequired
                        }
                    },
                    sequence: Some(job.state().sequence),
                }
            } else {
                return Err(MutationJobError::CorruptProgressStore);
            };
            records.push(record);
        }

        let decoded_count = integrity_count
            .checked_add(resumable_count)
            .and_then(|count| count.checked_add(mutation_count))
            .ok_or(MutationJobError::CorruptProgressStore)?;
        if decoded_count != retained_count || u64::try_from(records.len()) != Ok(retained_count) {
            return Err(MutationJobError::CorruptProgressStore);
        }

        Ok(ProgressJobInventory {
            retained_count,
            hard_limit: MAX_PROGRESS_JOBS_GLOBAL,
            reserved_integrity_headroom: PROGRESS_JOBS_INTEGRITY_RESERVATION,
            integrity_count,
            resumable_count,
            mutation_count,
            records,
        })
    }

    pub(super) fn remove(&mut self, job_id: IntegrityJobId) -> Result<(), IntegrityJobError> {
        if self
            .map
            .remove(&ProgressRecordKey::from_job_id(job_id))
            .is_none()
        {
            return Err(IntegrityJobError::JobNotFound);
        }
        Ok(())
    }

    pub(super) fn scan_after(
        &self,
        checkpoint: Option<IntegrityJobId>,
        limit: usize,
    ) -> Result<ProgressScanPage, IntegrityJobError> {
        if limit == 0 {
            return Err(IntegrityJobError::CapacityExceeded);
        }
        let lower = checkpoint.map_or(PROGRESS_HEADER_KEY, ProgressRecordKey::from_job_id);
        let mut job_ids = Vec::with_capacity(limit);
        let mut has_more = false;
        for entry in self.map.range((Excluded(lower), Unbounded)) {
            let Ok(job_id) = integrity_job_id_from_record(&entry.value().0) else {
                continue;
            };
            if job_ids.len() == limit {
                has_more = true;
                break;
            }
            job_ids.push(job_id);
        }
        Ok(ProgressScanPage {
            job_ids,
            exhausted: !has_more,
        })
    }

    fn job_count(&self) -> Result<u64, IntegrityJobError> {
        self.map
            .len()
            .checked_sub(1)
            .ok_or(IntegrityJobError::CorruptProgressHeader)
    }

    fn owner_job_count(&self, owner: &IntegrityJobOwner) -> Result<u64, IntegrityJobError> {
        let mut count = 0_u64;
        for entry in self.map.iter() {
            if *entry.key() == PROGRESS_HEADER_KEY {
                continue;
            }
            // Corrupt records already consume one slot from the global hard
            // capacity, but their owner cannot be trusted. Skipping them here
            // isolates the failed job without allowing unbounded progress
            // growth or blocking every other owner from starting work.
            let Ok(job_id) = IntegrityJobId::try_from_bytes(entry.key().0) else {
                continue;
            };
            let Ok(job) = decode_job_record(&entry.value().0, job_id) else {
                continue;
            };
            if job.owner == *owner {
                count = count
                    .checked_add(1)
                    .ok_or(IntegrityJobError::CapacityExceeded)?;
            }
        }
        Ok(count)
    }
}

fn encode_progress_header() -> Vec<u8> {
    let mut bytes = Vec::with_capacity(PROGRESS_HEADER_BYTES);
    bytes.extend_from_slice(PROGRESS_HEADER_MAGIC);
    bytes.push(PROGRESS_HEADER_VERSION);
    let checksum = crc32c(bytes.as_slice());
    bytes.extend_from_slice(&checksum.to_be_bytes());
    bytes
}

fn decode_progress_header(bytes: &[u8]) -> Result<(), IntegrityJobError> {
    if bytes.len() != PROGRESS_HEADER_BYTES
        || !bytes.starts_with(PROGRESS_HEADER_MAGIC)
        || bytes[PROGRESS_HEADER_MAGIC.len()] != PROGRESS_HEADER_VERSION
    {
        return Err(IntegrityJobError::IncompatibleProgressFormat);
    }
    let checksum_offset = PROGRESS_HEADER_MAGIC.len() + 1;
    let mut checksum = [0; 4];
    checksum.copy_from_slice(&bytes[checksum_offset..]);
    if u32::from_be_bytes(checksum) != crc32c(&bytes[..checksum_offset]) {
        return Err(IntegrityJobError::CorruptProgressHeader);
    }
    Ok(())
}

fn encode_job_record(job: &IntegrityJob) -> Result<Vec<u8>, IntegrityJobError> {
    let payload =
        encode_integrity_job_payload(job).map_err(|_| IntegrityJobError::CapacityExceeded)?;
    let total_len = JOB_RECORD_HEADER_BYTES
        .checked_add(payload.len())
        .ok_or(IntegrityJobError::CapacityExceeded)?;
    if total_len > MAX_PROGRESS_RECORD_BYTES as usize {
        return Err(IntegrityJobError::CapacityExceeded);
    }
    let payload_len =
        u32::try_from(payload.len()).map_err(|_| IntegrityJobError::CapacityExceeded)?;
    let mut bytes = Vec::with_capacity(total_len);
    bytes.extend_from_slice(JOB_RECORD_MAGIC);
    bytes.push(JOB_RECORD_VERSION);
    bytes.extend_from_slice(&payload_len.to_be_bytes());
    bytes.extend_from_slice(&crc32c(payload.as_slice()).to_be_bytes());
    bytes.extend_from_slice(&payload);
    Ok(bytes)
}

fn decode_job_record(
    bytes: &[u8],
    expected_id: IntegrityJobId,
) -> Result<IntegrityJob, IntegrityJobError> {
    if bytes.len() < JOB_RECORD_HEADER_BYTES
        || !bytes.starts_with(JOB_RECORD_MAGIC)
        || bytes[JOB_RECORD_MAGIC.len()] != JOB_RECORD_VERSION
    {
        return Err(IntegrityJobError::IncompatibleProgressFormat);
    }
    if bytes.len() > MAX_PROGRESS_RECORD_BYTES as usize {
        return Err(IntegrityJobError::CorruptProgressRecord);
    }
    let payload_len_offset = JOB_RECORD_MAGIC.len() + 1;
    let checksum_offset = payload_len_offset + 4;
    let payload_offset = checksum_offset + 4;
    let mut payload_len = [0; 4];
    payload_len.copy_from_slice(&bytes[payload_len_offset..checksum_offset]);
    if u32::from_be_bytes(payload_len) as usize != bytes.len() - payload_offset {
        return Err(IntegrityJobError::CorruptProgressRecord);
    }
    let payload = &bytes[payload_offset..];
    let mut checksum = [0; 4];
    checksum.copy_from_slice(&bytes[checksum_offset..payload_offset]);
    if u32::from_be_bytes(checksum) != crc32c(payload) {
        return Err(IntegrityJobError::CorruptProgressRecord);
    }
    if payload.len() > MAX_INTEGRITY_JOB_PAYLOAD_BYTES {
        return Err(IntegrityJobError::CorruptProgressRecord);
    }
    let job = decode_integrity_job_payload(payload)
        .map_err(|_| IntegrityJobError::CorruptProgressRecord)?;
    if job.id != expected_id {
        return Err(IntegrityJobError::CorruptProgressRecord);
    }
    Ok(job)
}

fn integrity_job_id_from_record(bytes: &[u8]) -> Result<IntegrityJobId, IntegrityJobError> {
    if bytes.len() < JOB_RECORD_HEADER_BYTES || !bytes.starts_with(JOB_RECORD_MAGIC) {
        return Err(IntegrityJobError::CorruptProgressRecord);
    }
    let payload_len_offset = JOB_RECORD_MAGIC.len() + 1;
    let checksum_offset = payload_len_offset + 4;
    let payload_offset = checksum_offset + 4;
    let payload = bytes
        .get(payload_offset..)
        .ok_or(IntegrityJobError::CorruptProgressRecord)?;
    let job = decode_integrity_job_payload(payload)
        .map_err(|_| IntegrityJobError::CorruptProgressRecord)?;
    decode_job_record(bytes, job.id).map(|job| job.id)
}

fn encode_resumable_job_record(record: &ResumableJobRecord) -> Result<Vec<u8>, ResumableJobError> {
    let payload = encode_resumable_job_payload(record)?;
    let total_len = RESUMABLE_JOB_RECORD_HEADER_BYTES
        .checked_add(payload.len())
        .ok_or(ResumableJobError::PayloadTooLarge)?;
    if total_len > MAX_PROGRESS_RECORD_BYTES as usize {
        return Err(ResumableJobError::PayloadTooLarge);
    }
    let payload_len =
        u32::try_from(payload.len()).map_err(|_| ResumableJobError::PayloadTooLarge)?;
    let mut bytes = Vec::with_capacity(total_len);
    bytes.extend_from_slice(RESUMABLE_JOB_RECORD_MAGIC);
    bytes.push(RESUMABLE_JOB_RECORD_VERSION);
    bytes.extend_from_slice(&payload_len.to_be_bytes());
    bytes.extend_from_slice(&crc32c(payload.as_slice()).to_be_bytes());
    bytes.extend_from_slice(&payload);
    Ok(bytes)
}

fn decode_resumable_job_record(
    bytes: &[u8],
    expected_id: ResumableJobId,
) -> Result<ResumableJobRecord, ResumableJobError> {
    let record = decode_resumable_job_record_unbound(bytes)?;
    if record.state().job_id != expected_id {
        return Err(ResumableJobError::CorruptProgressStore);
    }
    Ok(record)
}

fn decode_resumable_job_record_unbound(
    bytes: &[u8],
) -> Result<ResumableJobRecord, ResumableJobError> {
    if bytes.len() < RESUMABLE_JOB_RECORD_HEADER_BYTES
        || !bytes.starts_with(RESUMABLE_JOB_RECORD_MAGIC)
        || bytes[RESUMABLE_JOB_RECORD_MAGIC.len()] != RESUMABLE_JOB_RECORD_VERSION
    {
        return Err(ResumableJobError::IncompatibleProgressFormat);
    }
    if bytes.len() > MAX_PROGRESS_RECORD_BYTES as usize {
        return Err(ResumableJobError::CorruptProgressStore);
    }
    let payload_len_offset = RESUMABLE_JOB_RECORD_MAGIC.len() + 1;
    let checksum_offset = payload_len_offset + 4;
    let payload_offset = checksum_offset + 4;
    let mut payload_len = [0; 4];
    payload_len.copy_from_slice(&bytes[payload_len_offset..checksum_offset]);
    if u32::from_be_bytes(payload_len) as usize != bytes.len() - payload_offset {
        return Err(ResumableJobError::CorruptProgressStore);
    }
    let payload = &bytes[payload_offset..];
    let mut checksum = [0; 4];
    checksum.copy_from_slice(&bytes[checksum_offset..payload_offset]);
    if u32::from_be_bytes(checksum) != crc32c(payload) {
        return Err(ResumableJobError::CorruptProgressStore);
    }
    decode_resumable_job_payload(payload)
}

fn decode_resumable_job_record_for_inventory(
    bytes: &[u8],
    key: ProgressRecordKey,
) -> Result<ResumableJobRecord, MutationJobError> {
    let record = decode_resumable_job_record_unbound(bytes)
        .map_err(|_| MutationJobError::CorruptProgressStore)?;
    let expected_key = ProgressRecordKey::from_resumable_job_id(record.state().job_id)
        .map_err(|_| MutationJobError::CorruptProgressStore)?;
    if key != expected_key {
        return Err(MutationJobError::CorruptProgressStore);
    }
    Ok(record)
}

fn encode_mutation_job_record(record: &MutationJobRecord) -> Result<Vec<u8>, MutationJobError> {
    let payload = encode_mutation_job_payload(record)?;
    let total_len = MUTATION_JOB_RECORD_HEADER_BYTES
        .checked_add(payload.len())
        .ok_or(MutationJobError::CapacityExceeded)?;
    if total_len > MAX_MUTATION_JOB_RECORD_BYTES {
        return Err(mutation_record_size_error(total_len));
    }
    let payload_len =
        u32::try_from(payload.len()).map_err(|_| MutationJobError::CapacityExceeded)?;
    let mut bytes = Vec::with_capacity(total_len);
    bytes.extend_from_slice(MUTATION_JOB_RECORD_MAGIC);
    bytes.push(MUTATION_JOB_RECORD_VERSION);
    bytes.extend_from_slice(&payload_len.to_be_bytes());
    bytes.extend_from_slice(&crc32c(payload.as_slice()).to_be_bytes());
    bytes.extend_from_slice(&payload);
    Ok(bytes)
}

fn decode_mutation_job_record(
    bytes: &[u8],
    expected_id: MutationJobId,
) -> Result<MutationJobRecord, MutationJobError> {
    let record = decode_mutation_job_record_unbound(bytes)?;
    if record.state().job_id != expected_id {
        return Err(MutationJobError::CorruptProgressStore);
    }
    Ok(record)
}

fn decode_mutation_job_record_unbound(bytes: &[u8]) -> Result<MutationJobRecord, MutationJobError> {
    if bytes.len() < MUTATION_JOB_RECORD_HEADER_BYTES
        || !bytes.starts_with(MUTATION_JOB_RECORD_MAGIC)
        || bytes[MUTATION_JOB_RECORD_MAGIC.len()] != MUTATION_JOB_RECORD_VERSION
    {
        return Err(MutationJobError::IncompatibleProgressFormat);
    }
    if bytes.len() > MAX_MUTATION_JOB_RECORD_BYTES {
        return Err(MutationJobError::CorruptProgressStore);
    }
    let payload_len_offset = MUTATION_JOB_RECORD_MAGIC.len() + 1;
    let checksum_offset = payload_len_offset + 4;
    let payload_offset = checksum_offset + 4;
    let mut payload_len = [0; 4];
    payload_len.copy_from_slice(&bytes[payload_len_offset..checksum_offset]);
    if u32::from_be_bytes(payload_len) as usize != bytes.len() - payload_offset {
        return Err(MutationJobError::CorruptProgressStore);
    }
    let payload = &bytes[payload_offset..];
    let mut checksum = [0; 4];
    checksum.copy_from_slice(&bytes[checksum_offset..payload_offset]);
    if u32::from_be_bytes(checksum) != crc32c(payload) {
        return Err(MutationJobError::CorruptProgressStore);
    }
    decode_mutation_job_payload(payload)
}

fn decode_mutation_job_record_for_inventory(
    bytes: &[u8],
    key: ProgressRecordKey,
) -> Result<MutationJobRecord, MutationJobError> {
    let record = decode_mutation_job_record_unbound(bytes)
        .map_err(|_| MutationJobError::CorruptProgressStore)?;
    let expected_key = ProgressRecordKey::from_mutation_job_id(record.state().job_id)
        .map_err(|_| MutationJobError::CorruptProgressStore)?;
    if key != expected_key {
        return Err(MutationJobError::CorruptProgressStore);
    }
    Ok(record)
}

fn mutation_progress_before_digest(bytes: &[u8]) -> [u8; 32] {
    let mut hasher = new_hash_sha256_prefixed(MUTATION_PROGRESS_BEFORE_DIGEST_DOMAIN);
    hasher.update(bytes);
    finalize_hash_sha256(hasher)
}

fn mutation_record_size_error(observed: usize) -> MutationJobError {
    MutationJobError::PayloadTooLarge {
        kind: crate::db::MutationJobPayloadKind::Record,
        limit: u64::try_from(MAX_MUTATION_JOB_RECORD_BYTES).map_or(u64::MAX, |value| value),
        observed: u64::try_from(observed).map_or(u64::MAX, |value| value),
    }
}

const fn map_integrity_store_error(error: IntegrityJobError) -> ResumableJobError {
    match error {
        IntegrityJobError::IncompatibleProgressFormat => {
            ResumableJobError::IncompatibleProgressFormat
        }
        IntegrityJobError::CapacityExceeded => ResumableJobError::CapacityExceeded,
        _ => ResumableJobError::CorruptProgressStore,
    }
}

const fn map_mutation_store_error(error: IntegrityJobError) -> MutationJobError {
    match error {
        IntegrityJobError::IncompatibleProgressFormat => {
            MutationJobError::IncompatibleProgressFormat
        }
        IntegrityJobError::CapacityExceeded => MutationJobError::CapacityExceeded,
        _ => MutationJobError::CorruptProgressStore,
    }
}

pub(in crate::db) fn with_progress_store<C: CanisterKind, R>(
    f: impl FnOnce(&mut InspectionProgressStore) -> Result<R, IntegrityJobError>,
) -> Result<R, IntegrityJobError> {
    let memory = progress_memory::<C>()?;
    let mut store = InspectionProgressStore::open(memory)?;
    f(&mut store)
}

pub(in crate::db) fn with_resumable_progress_store<C: CanisterKind, R>(
    f: impl FnOnce(&mut InspectionProgressStore) -> Result<R, ResumableJobError>,
) -> Result<R, ResumableJobError> {
    let memory = progress_memory::<C>().map_err(map_integrity_store_error)?;
    let mut store = InspectionProgressStore::open(memory).map_err(map_integrity_store_error)?;
    f(&mut store)
}

pub(in crate::db) fn with_mutation_progress_store<C: CanisterKind, R>(
    f: impl FnOnce(&mut InspectionProgressStore) -> Result<R, MutationJobError>,
) -> Result<R, MutationJobError> {
    let memory = progress_memory::<C>().map_err(map_mutation_store_error)?;
    let mut store = InspectionProgressStore::open(memory).map_err(map_mutation_store_error)?;
    f(&mut store)
}

pub(in crate::db) fn preflight_mutation_progress_record_op<C: CanisterKind>(
    operation: &MutationProgressRecordOp,
) -> Result<(), InternalError> {
    with_mutation_progress_store::<C, _>(|store| store.preflight_mutation_progress(operation))
        .map_err(|_| InternalError::commit_corruption())
}

pub(in crate::db) fn apply_mutation_progress_record_op<C: CanisterKind>(
    operation: &MutationProgressRecordOp,
) -> Result<(), InternalError> {
    with_mutation_progress_store::<C, _>(|store| store.apply_mutation_progress(operation))
        .map_err(|_| InternalError::commit_corruption())
}

/// Mechanically publish one mutation-progress replacement already proved by preflight.
pub(in crate::db) fn apply_preflighted_mutation_progress_record_op<C: CanisterKind>(
    operation: &MutationProgressRecordOp,
) -> Result<(), InternalError> {
    with_mutation_progress_store::<C, _>(|store| {
        store.apply_preflighted_mutation_progress(operation);
        Ok(())
    })
    .map_err(|_| InternalError::commit_corruption())
}

pub(in crate::db) fn replace_mutation_progress_record_op<C: CanisterKind>(
    operation: &MutationProgressRecordOp,
) -> Result<(), MutationJobError> {
    with_mutation_progress_store::<C, _>(|store| store.replace_mutation_progress(operation))
}

pub(in crate::db) fn verify_mutation_progress_record_op<C: CanisterKind>(
    operation: &MutationProgressRecordOp,
) -> Result<(), InternalError> {
    with_mutation_progress_store::<C, _>(|store| store.verify_mutation_progress(operation))
        .map_err(|_| InternalError::recovery_effect_verification_failed())
}

#[cfg(test)]
fn progress_memory<C: CanisterKind>() -> Result<VirtualMemory<DefaultMemoryImpl>, IntegrityJobError>
{
    thread_local! {
        static MEMORIES: RefCell<
            Vec<(u8, &'static str, VirtualMemory<DefaultMemoryImpl>)>
        > = const { RefCell::new(Vec::new()) };
    }

    MEMORIES.with(|memories| {
        let mut memories = memories.borrow_mut();
        if let Some((_, _, memory)) = memories.iter().find(|(id, key, _)| {
            *id == C::INTEGRITY_PROGRESS_MEMORY_ID && *key == C::INTEGRITY_PROGRESS_STABLE_KEY
        }) {
            return Ok(memory.clone());
        }
        let memory = crate::testing::test_memory(C::INTEGRITY_PROGRESS_MEMORY_ID);
        memories.push((
            C::INTEGRITY_PROGRESS_MEMORY_ID,
            C::INTEGRITY_PROGRESS_STABLE_KEY,
            memory.clone(),
        ));
        Ok(memory)
    })
}

#[cfg(not(test))]
fn progress_memory<C: CanisterKind>() -> Result<VirtualMemory<DefaultMemoryImpl>, IntegrityJobError>
{
    open_default_memory_manager_memory(
        C::INTEGRITY_PROGRESS_STABLE_KEY,
        C::INTEGRITY_PROGRESS_MEMORY_ID,
    )
    .map_err(|_| IntegrityJobError::Internal)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        db::{
            MutationJobAdvanceRequest, MutationJobIdempotencyKey, MutationJobPhase,
            MutationJobRestartReason, MutationJobStatus, ReadSetRevisionProof,
            ReadSetStoreIdentity, ReadSetStoreRevision,
            integrity::progress_codec::current_job_codec_fixture,
            mutation_job::MutationJobTransition,
        },
        testing::test_memory,
    };
    use ic_stable_structures::Memory;

    fn current_resumable_record() -> ResumableJobRecord {
        let proof = ReadSetRevisionProof::from_parts(
            [1; 16],
            7,
            1,
            [2; 32],
            vec![ReadSetStoreRevision::new(
                ReadSetStoreIdentity::from_bytes([3; 32]),
                11,
                13,
            )],
        )
        .expect("bounded canonical proof should admit");
        ResumableJobRecord::new(
            ResumableJobId::try_from_bytes([4; 32])
                .expect("nonzero resumable job identity should admit"),
            proof,
            vec![5, 6],
        )
        .expect("current resumable record should admit")
    }

    fn mutation_job_id(byte: u8) -> MutationJobId {
        MutationJobId::try_from_bytes([byte; 32]).expect("nonzero mutation job id should admit")
    }

    fn current_mutation_record(byte: u8) -> MutationJobRecord {
        MutationJobRecord::new(mutation_job_id(byte), vec![1, 2, 3], vec![4, 5])
            .expect("current mutation record should admit")
    }

    fn current_integrity_record(byte: u8) -> IntegrityJob {
        let mut job = current_job_codec_fixture();
        let job_id = IntegrityJobId::try_from_bytes([byte; 32])
            .expect("nonzero integrity job id should admit");
        job.id = job_id;
        match &mut job.last_receipt.receipt {
            crate::db::IntegrityJobReceipt::Page(page) => page.job_id = job_id,
            crate::db::IntegrityJobReceipt::Abort(receipt) => receipt.job_id = job_id,
        }
        job.validate()
            .expect("rewritten integrity job should admit");
        job
    }

    fn insert_mutation_record(store: &mut InspectionProgressStore, record: &MutationJobRecord) {
        assert!(matches!(
            store
                .insert_mutation(record)
                .expect("mutation record should insert"),
            InsertMutationJobResult::Inserted,
        ));
    }

    fn mutation_request(byte: u8, sequence: u64, key: &str) -> MutationJobAdvanceRequest {
        MutationJobAdvanceRequest::new(
            mutation_job_id(byte),
            sequence,
            MutationJobIdempotencyKey::new(key).expect("bounded replay key should admit"),
        )
    }

    #[test]
    fn progress_header_rejects_future_version_and_checksum_corruption() {
        let mut future = encode_progress_header();
        future[PROGRESS_HEADER_MAGIC.len()] = PROGRESS_HEADER_VERSION + 1;
        assert_eq!(
            decode_progress_header(&future),
            Err(IntegrityJobError::IncompatibleProgressFormat),
        );

        let mut corrupt = encode_progress_header();
        let last = corrupt
            .last_mut()
            .expect("current progress header has a checksum");
        *last ^= 0xff;
        assert_eq!(
            decode_progress_header(&corrupt),
            Err(IntegrityJobError::CorruptProgressHeader),
        );
    }

    #[test]
    fn current_job_record_uses_the_direct_bounded_payload() {
        let job = current_job_codec_fixture();
        let encoded = encode_job_record(&job).expect("current job should encode");

        assert_eq!(encoded[JOB_RECORD_MAGIC.len()], 1);
        assert!(!encoded[JOB_RECORD_HEADER_BYTES..].starts_with(b"DIDL"));
        assert_eq!(
            decode_job_record(&encoded, job.id).expect("current job should decode"),
            job,
        );

        let mut corrupt = encoded;
        let last = corrupt
            .last_mut()
            .expect("current job record has a payload");
        *last ^= 0xff;
        assert_eq!(
            decode_job_record(&corrupt, job.id),
            Err(IntegrityJobError::CorruptProgressRecord),
        );
    }

    #[test]
    fn current_resumable_record_is_direct_bounded_and_checksum_protected() {
        let record = current_resumable_record();
        let encoded =
            encode_resumable_job_record(&record).expect("current resumable record should encode");

        assert_eq!(encoded.len(), 175);
        assert_eq!(encoded[RESUMABLE_JOB_RECORD_MAGIC.len()], 1);
        assert!(!encoded[RESUMABLE_JOB_RECORD_HEADER_BYTES..].starts_with(b"DIDL"));
        assert_eq!(
            decode_resumable_job_record(&encoded, record.state().job_id)
                .expect("current resumable record should decode"),
            record,
        );

        let mut future = encoded.clone();
        future[RESUMABLE_JOB_RECORD_MAGIC.len()] = RESUMABLE_JOB_RECORD_VERSION + 1;
        assert_eq!(
            decode_resumable_job_record(&future, record.state().job_id),
            Err(ResumableJobError::IncompatibleProgressFormat),
        );

        let mut corrupt = encoded;
        let last = corrupt
            .last_mut()
            .expect("current resumable record has a payload");
        *last ^= 0xff;
        assert_eq!(
            decode_resumable_job_record(&corrupt, record.state().job_id),
            Err(ResumableJobError::CorruptProgressStore),
        );
    }

    #[test]
    fn current_mutation_record_is_distinct_bounded_and_checksum_protected() {
        let record = current_mutation_record(7);
        let encoded =
            encode_mutation_job_record(&record).expect("current mutation record should encode");

        assert_eq!(encoded.len(), 97);
        assert_eq!(encoded[MUTATION_JOB_RECORD_MAGIC.len()], 1);
        assert!(!encoded[MUTATION_JOB_RECORD_HEADER_BYTES..].starts_with(b"DIDL"));
        assert_eq!(
            decode_mutation_job_record(&encoded, record.state().job_id)
                .expect("current mutation record should decode"),
            record,
        );

        let mut future = encoded.clone();
        future[MUTATION_JOB_RECORD_MAGIC.len()] = MUTATION_JOB_RECORD_VERSION + 1;
        assert_eq!(
            decode_mutation_job_record(&future, record.state().job_id),
            Err(MutationJobError::IncompatibleProgressFormat),
        );

        let mut corrupt = encoded;
        let last = corrupt
            .last_mut()
            .expect("current mutation record has a payload");
        *last ^= 0xff;
        assert_eq!(
            decode_mutation_job_record(&corrupt, record.state().job_id),
            Err(MutationJobError::CorruptProgressStore),
        );

        let mut oversized = vec![0; MAX_MUTATION_JOB_RECORD_BYTES + 1];
        oversized[..MUTATION_JOB_RECORD_MAGIC.len()].copy_from_slice(MUTATION_JOB_RECORD_MAGIC);
        oversized[MUTATION_JOB_RECORD_MAGIC.len()] = MUTATION_JOB_RECORD_VERSION;
        assert_eq!(
            decode_mutation_job_record(&oversized, record.state().job_id),
            Err(MutationJobError::CorruptProgressStore),
        );
    }

    #[test]
    fn mutation_progress_replacement_is_exact_idempotent_and_fail_closed() {
        let before = current_mutation_record(21);
        let (after, _) = before
            .apply_transition(
                &mutation_request(21, 0, "atomic-forward"),
                MutationJobTransition::new(
                    MutationJobStatus::Active,
                    MutationJobPhase::Forward,
                    vec![9],
                    8,
                    3,
                    0,
                ),
            )
            .expect("bounded atomic successor should admit");
        let operation = MutationProgressRecordOp::replace(&before, &after)
            .expect("exact mutation progress replacement should admit");
        let mut store = InspectionProgressStore::open(test_memory(252))
            .expect("isolated progress store should open");
        assert!(matches!(
            store
                .insert_mutation(&before)
                .expect("before record should insert"),
            InsertMutationJobResult::Inserted,
        ));

        store
            .preflight_mutation_progress(&operation)
            .expect("exact before bytes should preflight");
        store
            .apply_mutation_progress(&operation)
            .expect("exact before bytes should advance");
        store
            .apply_mutation_progress(&operation)
            .expect("exact after bytes should replay idempotently");
        store
            .verify_mutation_progress(&operation)
            .expect("exact after bytes should verify");
        assert_eq!(
            store
                .load_mutation(before.state().job_id)
                .expect("advanced record should load"),
            after,
        );
        assert_eq!(
            store.preflight_mutation_progress(&operation),
            Err(MutationJobError::CorruptProgressStore),
            "opening a new marker against after-state must not reset progress",
        );

        let (unexpected, _) = after
            .apply_transition(
                &mutation_request(21, 1, "unexpected"),
                MutationJobTransition::new(
                    MutationJobStatus::Active,
                    MutationJobPhase::Forward,
                    vec![10],
                    1,
                    0,
                    0,
                ),
            )
            .expect("third valid state should admit");
        store
            .replace_mutation(&unexpected)
            .expect("test should install neither-side state");
        assert_eq!(
            store.apply_mutation_progress(&operation),
            Err(MutationJobError::CorruptProgressStore),
        );
        assert_eq!(
            store.verify_mutation_progress(&operation),
            Err(MutationJobError::CorruptProgressStore),
        );
    }

    #[test]
    fn mutation_record_sizes_are_fixed_for_current_and_maximal_states() {
        let initial = current_mutation_record(8);
        let (active, _) = initial
            .apply_transition(
                &mutation_request(8, 0, "forward-0"),
                MutationJobTransition::new(
                    MutationJobStatus::Active,
                    MutationJobPhase::Verify,
                    vec![6],
                    13,
                    4,
                    0,
                ),
            )
            .expect("bounded active transition should admit");
        let (completed, _) = active
            .apply_transition(
                &mutation_request(8, 1, "verify-0"),
                MutationJobTransition::new(
                    MutationJobStatus::Completed,
                    MutationJobPhase::Verify,
                    Vec::new(),
                    9,
                    0,
                    0,
                ),
            )
            .expect("bounded completion should admit");
        let (restart, _) = initial
            .apply_transition(
                &mutation_request(8, 0, "restart"),
                MutationJobTransition::new(
                    MutationJobStatus::RestartRequired(
                        MutationJobRestartReason::AcceptedSchemaChanged,
                    ),
                    MutationJobPhase::Forward,
                    Vec::new(),
                    0,
                    0,
                    0,
                ),
            )
            .expect("bounded restart should admit");
        let maximal_initial = MutationJobRecord::new(
            mutation_job_id(9),
            vec![1; crate::db::MAX_MUTATION_JOB_INTENT_BYTES],
            vec![2; crate::db::MAX_MUTATION_JOB_CONTINUATION_BYTES],
        )
        .expect("maximum initial record should admit");
        let (maximal_active, _) = maximal_initial
            .apply_transition(
                &MutationJobAdvanceRequest::new(
                    mutation_job_id(9),
                    0,
                    MutationJobIdempotencyKey::new(
                        "k".repeat(crate::db::MAX_MUTATION_JOB_IDEMPOTENCY_KEY_BYTES),
                    )
                    .expect("maximum replay key should admit"),
                ),
                MutationJobTransition::new(
                    MutationJobStatus::Active,
                    MutationJobPhase::Forward,
                    vec![2; crate::db::MAX_MUTATION_JOB_CONTINUATION_BYTES],
                    crate::db::MAX_MUTATION_JOB_STEP_KEYS_SCANNED,
                    crate::db::MAX_MUTATION_JOB_STEP_ROWS_UPDATED,
                    0,
                ),
            )
            .expect("maximum active record should admit");

        assert_eq!(
            encode_mutation_job_record(&initial).map(|bytes| bytes.len()),
            Ok(97)
        );
        assert_eq!(
            encode_mutation_job_record(&active).map(|bytes| bytes.len()),
            Ok(167)
        );
        assert_eq!(
            encode_mutation_job_record(&completed).map(|bytes| bytes.len()),
            Ok(165),
        );
        assert_eq!(
            encode_mutation_job_record(&restart).map(|bytes| bytes.len()),
            Ok(166)
        );
        assert_eq!(
            encode_mutation_job_record(&maximal_initial).map(|bytes| bytes.len()),
            Ok(18_524),
        );
        assert_eq!(
            encode_mutation_job_record(&maximal_active).map(|bytes| bytes.len()),
            Ok(18_842),
        );
    }

    #[test]
    fn mutation_key_domain_and_shared_capacity_reservation_are_enforced() {
        let shared_bytes = [11; 32];
        let mutation_key = ProgressRecordKey::from_mutation_job_id(
            MutationJobId::try_from_bytes(shared_bytes).expect("mutation id should admit"),
        )
        .expect("mutation progress key should derive");
        let resumable_key = ProgressRecordKey::from_resumable_job_id(
            ResumableJobId::try_from_bytes(shared_bytes).expect("resumable id should admit"),
        )
        .expect("resumable progress key should derive");
        let integrity_key = ProgressRecordKey::from_job_id(
            IntegrityJobId::try_from_bytes(shared_bytes).expect("integrity id should admit"),
        );
        assert_ne!(mutation_key, resumable_key);
        assert_ne!(mutation_key, integrity_key);

        let mut store = InspectionProgressStore::open(test_memory(251))
            .expect("isolated progress store should open");
        store
            .insert_resumable(&current_resumable_record())
            .expect("generic job should consume one shared slot");
        for byte in 1..=54 {
            assert!(matches!(
                store
                    .insert_mutation(&current_mutation_record(byte))
                    .expect("record inside shared capacity should insert"),
                InsertMutationJobResult::Inserted,
            ));
        }
        assert_eq!(
            store
                .inventory()
                .expect("55 current records should inventory")
                .retained_count,
            55,
        );
        assert!(matches!(
            store
                .insert_mutation(&current_mutation_record(55))
                .expect("the 56th non-integrity record should insert"),
            InsertMutationJobResult::Inserted,
        ));
        assert_eq!(
            store
                .inventory()
                .expect("56 current records should inventory")
                .retained_count,
            56,
        );
        assert!(matches!(
            store.insert_mutation(&current_mutation_record(56)),
            Err(MutationJobError::CapacityExceeded),
        ));

        for byte in 200..=206 {
            assert!(matches!(
                store
                    .insert_new(&current_integrity_record(byte))
                    .expect("reserved integrity record should insert"),
                InsertJobResult::Inserted,
            ));
        }
        assert_eq!(
            store
                .inventory()
                .expect("63 current records should inventory")
                .retained_count,
            63,
        );
        assert!(matches!(
            store
                .insert_new(&current_integrity_record(207))
                .expect("the 64th integrity record should insert"),
            InsertJobResult::Inserted,
        ));
        let full = store.inventory().expect("full store should inventory");
        assert_eq!(full.retained_count, 64);
        assert_eq!(full.hard_limit, 64);
        assert_eq!(full.reserved_integrity_headroom, 8);
        assert_eq!(full.integrity_count, 8);
        assert_eq!(full.resumable_count, 1);
        assert_eq!(full.mutation_count, 55);
        assert!(matches!(
            store.insert_new(&current_integrity_record(208)),
            Err(IntegrityJobError::CapacityExceeded),
        ));
    }

    #[test]
    fn progress_stable_growth_is_measured_at_reservation_boundaries() {
        const STABLE_PAGE_BYTES: u64 = 65_536;

        let memory = test_memory(249);
        let mut store = InspectionProgressStore::open(memory.clone())
            .expect("isolated progress store should open");
        let mut bytes_at_fifty_five = 0;
        let mut bytes_at_fifty_six = 0;
        for byte in 1..=56 {
            assert!(matches!(
                store
                    .insert_mutation(&current_mutation_record(byte))
                    .expect("record inside shared capacity should insert"),
                InsertMutationJobResult::Inserted,
            ));
            if byte == 55 {
                bytes_at_fifty_five = memory.size() * STABLE_PAGE_BYTES;
            } else if byte == 56 {
                bytes_at_fifty_six = memory.size() * STABLE_PAGE_BYTES;
            }
        }
        let mut bytes_at_sixty_three = 0;
        for byte in 200..=207 {
            assert!(matches!(
                store
                    .insert_new(&current_integrity_record(byte))
                    .expect("record inside integrity reservation should insert"),
                InsertJobResult::Inserted,
            ));
            if byte == 206 {
                bytes_at_sixty_three = memory.size() * STABLE_PAGE_BYTES;
            }
        }
        let bytes_at_sixty_four = memory.size() * STABLE_PAGE_BYTES;

        assert_eq!(
            (
                bytes_at_fifty_five,
                bytes_at_fifty_six,
                bytes_at_sixty_three,
                bytes_at_sixty_four,
            ),
            (38_993_920, 38_993_920, 43_319_296, 43_319_296),
        );
    }

    #[test]
    fn mutation_store_load_replay_replace_and_acknowledge_are_exact() {
        let mut store = InspectionProgressStore::open(test_memory(250))
            .expect("isolated progress store should open");
        let initial = current_mutation_record(10);
        assert!(matches!(
            store
                .insert_mutation(&initial)
                .expect("initial mutation record should insert"),
            InsertMutationJobResult::Inserted,
        ));
        assert!(matches!(
            store
                .insert_mutation(&initial)
                .expect("duplicate identity should load retained record"),
            InsertMutationJobResult::Occupied(record) if *record == initial,
        ));
        assert_eq!(
            store.load_mutation(mutation_job_id(10)),
            Ok(initial.clone())
        );
        assert_eq!(
            store.acknowledge_mutation(mutation_job_id(10), 0),
            Err(MutationJobError::Active),
        );

        let request = mutation_request(10, 0, "restart");
        let (terminal, receipt) = initial
            .apply_transition(
                &request,
                MutationJobTransition::new(
                    MutationJobStatus::RestartRequired(
                        MutationJobRestartReason::BatchPolicyChanged,
                    ),
                    MutationJobPhase::Forward,
                    Vec::new(),
                    0,
                    0,
                    0,
                ),
            )
            .expect("terminal transition should admit");
        store
            .replace_mutation(&terminal)
            .expect("terminal replacement should persist");
        assert_eq!(
            store.load_mutation(mutation_job_id(10)).and_then(|record| {
                let replay = record.exact_replay(&request)?;
                Ok(replay.cloned())
            }),
            Ok(Some(receipt)),
        );
        assert_eq!(
            store.acknowledge_mutation(mutation_job_id(10), 0),
            Err(MutationJobError::StaleSequence {
                expected: 0,
                actual: 1,
            }),
        );
        assert_eq!(store.acknowledge_mutation(mutation_job_id(10), 1), Ok(()));
        assert_eq!(store.acknowledge_mutation(mutation_job_id(10), 1), Ok(()));
        assert_eq!(
            store.load_mutation(mutation_job_id(10)),
            Err(MutationJobError::NotFound),
        );
    }

    #[cfg(feature = "sql")]
    #[test]
    fn mutation_cancellation_is_exact_zero_state_and_absent_idempotent() {
        let mut store = InspectionProgressStore::open(test_memory(248))
            .expect("isolated progress store should open");
        let initial = current_mutation_record(31);
        insert_mutation_record(&mut store, &initial);
        assert_eq!(
            store.cancel_unadvanced_mutation(mutation_job_id(31), 1, |_| Ok(())),
            Err(MutationJobError::StaleSequence {
                expected: 1,
                actual: 0,
            }),
        );
        assert_eq!(
            store.cancel_unadvanced_mutation(mutation_job_id(31), 0, |_| Ok(())),
            Ok(()),
        );
        assert_eq!(
            store.cancel_unadvanced_mutation(mutation_job_id(31), 0, |_| {
                Err(MutationJobError::CorruptProgressStore)
            }),
            Ok(()),
            "an absent retry must not invoke continuation validation",
        );

        let advanced_initial = current_mutation_record(32);
        let (advanced, _) = advanced_initial
            .apply_transition(
                &mutation_request(32, 0, "advanced"),
                MutationJobTransition::new(
                    MutationJobStatus::Active,
                    MutationJobPhase::Forward,
                    vec![6],
                    1,
                    0,
                    0,
                ),
            )
            .expect("advanced state should admit");
        insert_mutation_record(&mut store, &advanced);
        for expected_sequence in [0, 1] {
            assert_eq!(
                store.cancel_unadvanced_mutation(
                    mutation_job_id(32),
                    expected_sequence,
                    |_| Ok(()),
                ),
                Err(MutationJobError::StaleSequence {
                    expected: 0,
                    actual: 1,
                }),
            );
        }

        let terminal_initial = current_mutation_record(33);
        let (terminal, _) = terminal_initial
            .apply_transition(
                &mutation_request(33, 0, "terminal"),
                MutationJobTransition::new(
                    MutationJobStatus::RestartRequired(
                        MutationJobRestartReason::BatchPolicyChanged,
                    ),
                    MutationJobPhase::Forward,
                    Vec::new(),
                    0,
                    0,
                    0,
                ),
            )
            .expect("terminal state should admit");
        insert_mutation_record(&mut store, &terminal);
        assert_eq!(
            store.cancel_unadvanced_mutation(mutation_job_id(33), 1, |_| Ok(())),
            Err(MutationJobError::StaleSequence {
                expected: 0,
                actual: 1,
            }),
        );

        let malformed_continuation = current_mutation_record(34);
        insert_mutation_record(&mut store, &malformed_continuation);
        assert_eq!(
            store.cancel_unadvanced_mutation(mutation_job_id(34), 0, |_| {
                Err(MutationJobError::CorruptProgressStore)
            }),
            Err(MutationJobError::CorruptProgressStore),
        );
        assert_eq!(
            store.load_mutation(mutation_job_id(34)),
            Ok(malformed_continuation),
            "failed validation must retain the record",
        );
    }

    #[test]
    fn progress_inventory_is_complete_family_bounded_and_fail_closed() {
        let mut store = InspectionProgressStore::open(test_memory(247))
            .expect("isolated progress store should open");
        let integrity = current_integrity_record(201);
        let resumable = current_resumable_record();
        let mutation = current_mutation_record(35);
        assert!(matches!(
            store
                .insert_new(&integrity)
                .expect("integrity record should insert"),
            InsertJobResult::Inserted,
        ));
        store
            .insert_resumable(&resumable)
            .expect("resumable record should insert");
        assert!(matches!(
            store
                .insert_mutation(&mutation)
                .expect("mutation record should insert"),
            InsertMutationJobResult::Inserted,
        ));

        let inventory = store.inventory().expect("valid records should inventory");
        assert_eq!(inventory.retained_count, 3);
        assert_eq!(inventory.hard_limit, 64);
        assert_eq!(inventory.reserved_integrity_headroom, 8);
        assert_eq!(inventory.integrity_count, 1);
        assert_eq!(inventory.resumable_count, 1);
        assert_eq!(inventory.mutation_count, 1);
        assert_eq!(inventory.records.len(), 3);
        assert!(inventory.records.iter().all(|record| {
            record.lifecycle == ProgressJobLifecycle::Active && record.sequence == Some(0)
        }));
        assert!(inventory.records.iter().any(|record| {
            record.family == ProgressJobFamily::Integrity
                && record.job_id == integrity.id.to_bytes()
        }));
        assert!(inventory.records.iter().any(|record| {
            record.family == ProgressJobFamily::Resumable
                && record.job_id == resumable.state().job_id.to_bytes()
        }));
        assert!(inventory.records.iter().any(|record| {
            record.family == ProgressJobFamily::Mutation
                && record.job_id == mutation.state().job_id.to_bytes()
        }));

        store.map.insert(
            ProgressRecordKey([202; 32]),
            ProgressRecordBytes(b"undecodable-retained-slot".to_vec()),
        );
        assert_eq!(
            store.inventory(),
            Err(MutationJobError::CorruptProgressStore),
            "one undecodable slot must fail the whole inventory",
        );
    }

    #[test]
    fn integrity_scan_skips_other_progress_record_families() {
        let mut store = InspectionProgressStore::open(test_memory(252))
            .expect("isolated progress store should open");
        let integrity = current_job_codec_fixture();
        assert!(matches!(
            store
                .insert_new(&integrity)
                .expect("integrity job should insert"),
            InsertJobResult::Inserted,
        ));
        store
            .insert_resumable(&current_resumable_record())
            .expect("generic resumable job should insert");
        assert!(matches!(
            store
                .insert_mutation(&current_mutation_record(12))
                .expect("mutation job should insert"),
            InsertMutationJobResult::Inserted,
        ));

        let page = store
            .scan_after(None, 8)
            .expect("integrity scan should ignore other record families");
        assert_eq!(page.job_ids, vec![integrity.id]);
        assert!(page.exhausted);
    }
}
