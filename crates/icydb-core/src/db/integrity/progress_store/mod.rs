//! Module: db::integrity::progress_store
//! Responsibility: independently persist one bounded record per Deep job.
//! Does not own: inspected database state, commit markers, journals, or advancement semantics.
//! Boundary: current-form job codec -> physically separate stable BTreeMap allocation.

#[cfg(not(test))]
use crate::db::open_memory;
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
        storage::open_stable_map,
    },
    error::InternalError,
    traits::CanisterKind,
};

use ic_storage::adapter::{StableBTreeMap, Storable, storable::Bound};
use ic_storage::{DefaultMemoryImpl, RuntimeMemory};
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
const JOB_RECORD_MAGIC: &[u8; 8] = b"ICYIJWIR";
const PROGRESS_JOB_RECORD_VERSION: u8 = 1;
const PROGRESS_JOB_RECORD_HEADER_BYTES: usize = 8 + 1 + 4 + 4;
const RESUMABLE_JOB_KEY_DOMAIN: &[u8] = b"icydb.resumable-job.progress-key.v1";
const RESUMABLE_JOB_RECORD_MAGIC: &[u8; 8] = b"ICYRJOB1";
const MUTATION_JOB_KEY_DOMAIN: &[u8] = b"icydb.mutation-job.progress-key.v1";
const MUTATION_JOB_RECORD_MAGIC: &[u8; 8] = b"ICYMJOB1";
const MUTATION_PROGRESS_BEFORE_DIGEST_DOMAIN: &[u8] = b"icydb.mutation-job.progress-before.v1";
const MAX_PROGRESS_RECORD_BYTES: u32 = 512 * 1024;
const MAX_PROGRESS_JOBS_GLOBAL: u64 = 64;
const MAX_PROGRESS_JOBS_NON_INTEGRITY: u64 = 56;
const PROGRESS_JOBS_INTEGRITY_RESERVATION: u64 =
    MAX_PROGRESS_JOBS_GLOBAL - MAX_PROGRESS_JOBS_NON_INTEGRITY;
const MAX_PROGRESS_JOBS_PER_OWNER: u64 = 8;

/// Stable family of one retained record in the shared progress allocation.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, serde::Serialize)]
pub enum ProgressJobFamily {
    /// Engine-owned Deep integrity inspection.
    Integrity,
    /// Application-owned generic resumable job.
    Resumable,
    /// Engine-owned fixed SQL mutation job.
    Mutation,
}

/// Bounded normalized lifecycle of one retained progress record.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, serde::Serialize)]
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
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, serde::Serialize)]
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
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, serde::Serialize)]
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
    /// Complete encoded bytes across every retained record.
    pub retained_record_bytes: u64,
    /// Every validated retained record in stable key order.
    pub records: Vec<ProgressJobInventoryRecord>,
}

/// Exact raw-record authority for explicitly abandoning retained progress.
///
/// No payload or owner data is exposed. The opaque storage key is not a family
/// job ID. Authorization must be enforced by the application's admin boundary.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, serde::Serialize)]
pub struct ProgressRetirementRecord {
    /// Exact shared-store key; the reserved header is never exposed or writable.
    pub record_key: [u8; 32],
    /// Domain-separated digest of this key and its complete retained bytes.
    pub record_digest: [u8; 32],
    /// Encoded bytes that this retirement would remove.
    pub encoded_bytes: u64,
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
    fn to_bytes(&self) -> Result<Cow<'_, [u8]>, ic_storage::storable::CodecError> {
        Ok(Cow::Borrowed(&self.0))
    }

    fn from_bytes(bytes: Cow<'_, [u8]>) -> Result<Self, ic_storage::storable::CodecError> {
        Self::check_bounds(bytes.as_ref())?;
        let mut key = [0; 32];
        if bytes.len() == key.len() {
            key.copy_from_slice(bytes.as_ref());
        }
        Ok(Self(key))
    }

    fn into_bytes(self) -> Result<Vec<u8>, ic_storage::storable::CodecError> {
        Ok(self.0.to_vec())
    }

    const BOUND: Bound = Bound::Bounded {
        max_size: 32,
        is_fixed_size: true,
    };
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ProgressRecordBytes(Vec<u8>);

impl Storable for ProgressRecordBytes {
    fn to_bytes(&self) -> Result<Cow<'_, [u8]>, ic_storage::storable::CodecError> {
        Ok(Cow::Borrowed(self.0.as_slice()))
    }

    fn from_bytes(bytes: Cow<'_, [u8]>) -> Result<Self, ic_storage::storable::CodecError> {
        Self::check_bounds(bytes.as_ref())?;
        Ok(Self(bytes.into_owned()))
    }

    fn into_bytes(self) -> Result<Vec<u8>, ic_storage::storable::CodecError> {
        Ok(self.0)
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
    pub(super) next_checkpoint: Option<IntegrityJobId>,
    pub(super) records_scanned: usize,
    pub(super) exhausted: bool,
}

pub(in crate::db) struct InspectionProgressStore {
    map: StableBTreeMap<ProgressRecordKey, ProgressRecordBytes, DefaultMemoryImpl>,
}

impl InspectionProgressStore {
    fn open(memory: RuntimeMemory<DefaultMemoryImpl>) -> Result<Self, IntegrityJobError> {
        let mut store = Self {
            map: open_stable_map(memory).map_err(|_| IntegrityJobError::CorruptProgressRecord)?,
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

    // Install fixture states outside the production before/after replacement proof.

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

    // Raw inspection deliberately does not decode a family's payload: corrupt
    // and unavailable-owner records must remain addressable for explicit repair.
    pub(in crate::db) fn retirement_inventory(
        &self,
    ) -> Result<Vec<ProgressRetirementRecord>, MutationJobError> {
        let mut records = Vec::new();
        for key in self.map.keys() {
            if key == PROGRESS_HEADER_KEY {
                continue;
            }
            if records.len() as u64 >= MAX_PROGRESS_JOBS_GLOBAL {
                return Err(MutationJobError::CorruptProgressStore);
            }
            let bytes = self
                .map
                .get(&key)
                .ok_or(MutationJobError::CorruptProgressStore)?;
            records.push(ProgressRetirementRecord {
                record_key: key.0,
                record_digest: retirement_digest(key, &bytes.0),
                encoded_bytes: bytes.0.len() as u64,
            });
        }
        Ok(records)
    }

    // Recovery admission is owned by the session before this compare-and-delete.
    // A changed record is never deleted using stale operator evidence.
    pub(in crate::db) fn retire_exact(
        &mut self,
        record: &ProgressRetirementRecord,
    ) -> Result<bool, MutationJobError> {
        let key = ProgressRecordKey(record.record_key);
        if key == PROGRESS_HEADER_KEY {
            return Err(MutationJobError::InvalidJobId);
        }
        let Some(bytes) = self.map.get(&key) else {
            return Ok(false);
        };
        if bytes.0.len() as u64 != record.encoded_bytes
            || retirement_digest(key, &bytes.0) != record.record_digest
        {
            return Err(MutationJobError::IdentityConflict);
        }
        self.map.remove(&key);
        Ok(true)
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
        let mut retained_record_bytes = 0_u64;

        for entry in &self.map {
            let key = *entry.key();
            if key == PROGRESS_HEADER_KEY {
                continue;
            }
            let bytes = &entry.value().0;
            retained_record_bytes = retained_record_bytes
                .checked_add(
                    u64::try_from(bytes.len())
                        .map_err(|_| MutationJobError::CorruptProgressStore)?,
                )
                .ok_or(MutationJobError::CorruptProgressStore)?;
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
            retained_record_bytes,
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
        let mut next_checkpoint = None;
        let mut records_scanned = 0;
        let mut entries = self.map.range((Excluded(lower), Unbounded));
        for entry in entries.by_ref().take(limit) {
            // The physical key owns lookup and pagination even when the payload
            // is corrupt. Every family consumes page work; only its owner may
            // interpret or retire its records.
            let job_id = IntegrityJobId::try_from_bytes(entry.key().0)?;
            next_checkpoint = Some(job_id);
            records_scanned += 1;
            let bytes = entry.value();
            if !bytes.0.starts_with(RESUMABLE_JOB_RECORD_MAGIC)
                && !bytes.0.starts_with(MUTATION_JOB_RECORD_MAGIC)
            {
                job_ids.push(job_id);
            }
        }
        Ok(ProgressScanPage {
            job_ids,
            next_checkpoint,
            records_scanned,
            exhausted: entries.next().is_none(),
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
        for entry in &self.map {
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

fn retirement_digest(key: ProgressRecordKey, bytes: &[u8]) -> [u8; 32] {
    let mut hasher = new_hash_sha256_prefixed(b"icydb.progress.retirement.v1");
    hasher.update(key.0);
    hasher.update((bytes.len() as u64).to_be_bytes());
    hasher.update(bytes);
    finalize_hash_sha256(hasher)
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
    encode_progress_record_payload(
        &payload,
        *JOB_RECORD_MAGIC,
        MAX_PROGRESS_RECORD_BYTES as usize,
        IntegrityJobError::CapacityExceeded,
        |_| IntegrityJobError::CapacityExceeded,
    )
}

// The envelope grammar is shared; each family retains its payload codec,
// record ceiling and distinct arithmetic/size error taxonomy.
fn encode_progress_record_payload<E>(
    payload: &[u8],
    magic: [u8; 8],
    max_bytes: usize,
    capacity_error: E,
    size_error: impl FnOnce(usize) -> E,
) -> Result<Vec<u8>, E> {
    let Some(total_len) = PROGRESS_JOB_RECORD_HEADER_BYTES.checked_add(payload.len()) else {
        return Err(capacity_error);
    };
    if total_len > max_bytes {
        return Err(size_error(total_len));
    }
    let payload_len = u32::try_from(payload.len()).map_err(|_| capacity_error)?;
    let mut bytes = Vec::with_capacity(total_len);
    bytes.extend_from_slice(&magic);
    bytes.push(PROGRESS_JOB_RECORD_VERSION);
    bytes.extend_from_slice(&payload_len.to_be_bytes());
    bytes.extend_from_slice(&crc32c(payload).to_be_bytes());
    bytes.extend_from_slice(payload);
    Ok(bytes)
}

// All job families use the same envelope. Mutation before/after bytes also travel
// in commit markers, outside Storage's checksummed map, and bind exact replay.
// Callers retain payload semantics, family ceilings and public error mapping;
// validate application framing before decoding in either context.
fn decode_progress_record_payload(
    bytes: &[u8],
    magic: [u8; 8],
    max_bytes: usize,
) -> Result<&[u8], IntegrityJobError> {
    if bytes.len() < PROGRESS_JOB_RECORD_HEADER_BYTES
        || !bytes.starts_with(&magic)
        || bytes[magic.len()] != PROGRESS_JOB_RECORD_VERSION
    {
        return Err(IntegrityJobError::IncompatibleProgressFormat);
    }
    if bytes.len() > max_bytes {
        return Err(IntegrityJobError::CorruptProgressRecord);
    }
    let payload_len_offset = magic.len() + 1;
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
    Ok(payload)
}

fn decode_job_record(
    bytes: &[u8],
    expected_id: IntegrityJobId,
) -> Result<IntegrityJob, IntegrityJobError> {
    let job = decode_job_record_unbound(bytes)?;
    if job.id != expected_id {
        return Err(IntegrityJobError::CorruptProgressRecord);
    }
    Ok(job)
}

fn decode_job_record_unbound(bytes: &[u8]) -> Result<IntegrityJob, IntegrityJobError> {
    let payload = decode_progress_record_payload(
        bytes,
        *JOB_RECORD_MAGIC,
        MAX_PROGRESS_RECORD_BYTES as usize,
    )?;
    if payload.len() > MAX_INTEGRITY_JOB_PAYLOAD_BYTES {
        return Err(IntegrityJobError::CorruptProgressRecord);
    }
    decode_integrity_job_payload(payload).map_err(|_| IntegrityJobError::CorruptProgressRecord)
}

fn encode_resumable_job_record(record: &ResumableJobRecord) -> Result<Vec<u8>, ResumableJobError> {
    let payload = encode_resumable_job_payload(record)?;
    encode_progress_record_payload(
        &payload,
        *RESUMABLE_JOB_RECORD_MAGIC,
        MAX_PROGRESS_RECORD_BYTES as usize,
        ResumableJobError::PayloadTooLarge,
        |_| ResumableJobError::PayloadTooLarge,
    )
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
    let payload = decode_progress_record_payload(
        bytes,
        *RESUMABLE_JOB_RECORD_MAGIC,
        MAX_PROGRESS_RECORD_BYTES as usize,
    )
    .map_err(map_integrity_store_error)?;
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
    encode_progress_record_payload(
        &payload,
        *MUTATION_JOB_RECORD_MAGIC,
        MAX_MUTATION_JOB_RECORD_BYTES,
        MutationJobError::CapacityExceeded,
        mutation_record_size_error,
    )
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
    let payload = decode_progress_record_payload(
        bytes,
        *MUTATION_JOB_RECORD_MAGIC,
        MAX_MUTATION_JOB_RECORD_BYTES,
    )
    .map_err(map_mutation_store_error)?;
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
        limit: u64::try_from(MAX_MUTATION_JOB_RECORD_BYTES).unwrap_or(u64::MAX),
        observed: u64::try_from(observed).unwrap_or(u64::MAX),
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

// Corrupt physical bytes at the progress owner for end-to-end request tests.
#[cfg(all(test, feature = "sql"))]
pub(in crate::db) fn replace_progress_record_for_test<C: CanisterKind>(
    key: [u8; 32],
    bytes: Vec<u8>,
) -> Result<(), IntegrityJobError> {
    with_progress_store::<C, _>(|store| {
        store
            .map
            .insert(ProgressRecordKey(key), ProgressRecordBytes(bytes));
        Ok(())
    })
}

// Preserve a valid payload/CRC while removing its matching physical key.
#[cfg(all(test, feature = "sql"))]
pub(in crate::db) fn miskey_progress_record_for_test<C: CanisterKind>(
    job_id: IntegrityJobId,
    stored_id: IntegrityJobId,
) -> Result<(), IntegrityJobError> {
    with_progress_store::<C, _>(|store| {
        let bytes = store
            .map
            .remove(&ProgressRecordKey::from_job_id(job_id))
            .ok_or(IntegrityJobError::JobNotFound)?;
        store
            .map
            .insert(ProgressRecordKey::from_job_id(stored_id), bytes);
        Ok(())
    })
}

#[cfg(not(test))]
fn progress_memory<C: CanisterKind>() -> Result<RuntimeMemory<DefaultMemoryImpl>, IntegrityJobError>
{
    open_memory(C::INTEGRITY_PROGRESS_STABLE_KEY).map_err(|_| IntegrityJobError::Internal)
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod helper_tests;

#[cfg(test)]
use helper_tests::progress_memory;
