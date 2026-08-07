//! Module: db::integrity::progress_store
//! Responsibility: independently persist one bounded record per Deep job.
//! Does not own: inspected database state, commit markers, journals, or advancement semantics.
//! Boundary: current-form job codec -> physically separate stable BTreeMap allocation.

use crate::{
    db::{
        codec::{finalize_hash_sha256, new_hash_sha256_prefixed},
        database_format::crc32c,
        integrity::{
            IntegrityJob, IntegrityJobError, IntegrityJobId, IntegrityJobOwner,
            progress_codec::{
                MAX_INTEGRITY_JOB_PAYLOAD_BYTES, decode_integrity_job_payload,
                encode_integrity_job_payload,
            },
        },
        resumable_job::{
            ResumableJobError, ResumableJobId, ResumableJobRecord, decode_resumable_job_payload,
            encode_resumable_job_payload,
        },
    },
    traits::CanisterKind,
};
#[cfg(not(test))]
use ic_memory::open_default_memory_manager_memory;
use ic_stable_structures::{
    BTreeMap as StableBTreeMap, DefaultMemoryImpl, Storable, memory_manager::VirtualMemory,
    storable::Bound,
};
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
const JOB_RECORD_VERSION: u8 = 2;
const JOB_RECORD_HEADER_BYTES: usize = 8 + 1 + 4 + 4;
const RESUMABLE_JOB_KEY_DOMAIN: &[u8] = b"icydb.resumable-job.progress-key.v1";
const RESUMABLE_JOB_RECORD_MAGIC: &[u8; 8] = b"ICYRJOB1";
const RESUMABLE_JOB_RECORD_VERSION: u8 = 1;
const RESUMABLE_JOB_RECORD_HEADER_BYTES: usize = 8 + 1 + 4 + 4;
const MAX_PROGRESS_RECORD_BYTES: u32 = 512 * 1024;
const MAX_PROGRESS_JOBS_GLOBAL: u64 = 64;
const MAX_PROGRESS_JOBS_PER_OWNER: u64 = 8;

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
        if self.job_count().map_err(map_integrity_store_error)? >= MAX_PROGRESS_JOBS_GLOBAL {
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
    let record = decode_resumable_job_payload(payload)?;
    if record.state().job_id != expected_id {
        return Err(ResumableJobError::CorruptProgressStore);
    }
    Ok(record)
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
            ReadSetRevisionProof, ReadSetStoreIdentity, ReadSetStoreRevision,
            integrity::progress_codec::current_job_codec_fixture,
        },
        testing::test_memory,
    };

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
    fn current_job_record_uses_only_the_direct_version_two_payload() {
        let job = current_job_codec_fixture();
        let encoded = encode_job_record(&job).expect("current job should encode");

        assert_eq!(encoded[JOB_RECORD_MAGIC.len()], 2);
        assert!(!encoded[JOB_RECORD_HEADER_BYTES..].starts_with(b"DIDL"));
        assert_eq!(
            decode_job_record(&encoded, job.id).expect("current job should decode"),
            job,
        );

        let mut retired_version = encoded.clone();
        retired_version[JOB_RECORD_MAGIC.len()] = 1;
        assert_eq!(
            decode_job_record(&retired_version, job.id),
            Err(IntegrityJobError::IncompatibleProgressFormat),
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
    fn integrity_scan_skips_generic_resumable_progress_records() {
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

        let page = store
            .scan_after(None, 8)
            .expect("integrity scan should ignore other record families");
        assert_eq!(page.job_ids, vec![integrity.id]);
        assert!(page.exhausted);
    }
}
