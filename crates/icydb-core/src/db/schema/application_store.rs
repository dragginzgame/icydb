//! Module: db::schema::application_store
//! Responsibility: persist bounded schema-application records in one database-control region.
//! Does not own: proposal lowering, accepted-schema publication, or activation advancement.
//! Boundary: marker-owned exact record replacement -> disjoint current-form stable BTreeMap.

use crate::{
    db::{
        commit::{MAX_COMMIT_BYTES, commit_memory_handle, current_commit_memory_allocation},
        database_format::crc32c,
        schema::{SchemaApplicationRecord, SchemaChangeReceipt},
    },
    error::InternalError,
};
use candid::{Decode, Encode};
use ic_stable_structures::{
    BTreeMap as StableBTreeMap, DefaultMemoryImpl, RestrictedMemory, Storable,
    memory_manager::VirtualMemory, storable::Bound,
};
use icydb_schema::{SchemaSubmissionKey, TargetDatabaseIdentity};
use sha2::{Digest, Sha256};
use std::borrow::Cow;

const APPLICATION_HEADER_KEY: ApplicationRecordKey = ApplicationRecordKey([0; 32]);
const APPLICATION_HEADER_MAGIC: &[u8; 8] = b"ICYSCAPP";
const APPLICATION_HEADER_VERSION: u8 = 1;
const APPLICATION_HEADER_BYTES: usize = 8 + 1 + 4;
const APPLICATION_RECORD_MAGIC: &[u8; 8] = b"ICYSCREC";
const APPLICATION_RECORD_VERSION: u8 = 1;
const APPLICATION_RECORD_HEADER_BYTES: usize = 8 + 1 + 4 + 4;
pub(in crate::db) const MAX_SCHEMA_APPLICATION_RECORD_BYTES: u32 = 64 * 1024;
const MAX_SCHEMA_APPLICATION_RECORDS: u64 = 64;
const APPLICATION_RECORD_KEY_PROFILE: &[u8] = b"icydb.schema-application.record-key.v1";
const WASM_PAGE_BYTES: u64 = 65_536;
const APPLICATION_MEMORY_START_PAGE: u64 = MAX_COMMIT_BYTES as u64 / WASM_PAGE_BYTES + 1;
const APPLICATION_MEMORY_END_PAGE: u64 = 4_096;

type ApplicationMemory = RestrictedMemory<VirtualMemory<DefaultMemoryImpl>>;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(in crate::db) struct ApplicationRecordKey([u8; 32]);

impl ApplicationRecordKey {
    pub(in crate::db) const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub(in crate::db) fn new(
        database_identity: TargetDatabaseIdentity,
        submission_key: &SchemaSubmissionKey,
    ) -> Result<Self, InternalError> {
        let mut hasher = Sha256::new();
        hasher.update(APPLICATION_RECORD_KEY_PROFILE);
        hasher.update(database_identity.to_bytes());
        let key_bytes = submission_key.as_str().as_bytes();
        let key_len =
            u32::try_from(key_bytes.len()).map_err(|_| InternalError::store_invariant())?;
        hasher.update(key_len.to_le_bytes());
        hasher.update(key_bytes);
        let key = Self(hasher.finalize().into());
        if key == APPLICATION_HEADER_KEY {
            return Err(InternalError::store_invariant());
        }
        Ok(key)
    }

    pub(in crate::db) const fn to_bytes(self) -> [u8; 32] {
        self.0
    }

    fn from_receipt(receipt: &SchemaChangeReceipt) -> Result<Self, InternalError> {
        Self::new(receipt.database_identity(), receipt.submission_key())
    }
}

///
/// SchemaApplicationRecordOp
///
/// Exact compare-and-replace effect carried by one durable commit marker.
///

#[derive(Clone, Debug)]
pub(in crate::db) struct SchemaApplicationRecordOp {
    key: ApplicationRecordKey,
    before: Option<Vec<u8>>,
    after: Vec<u8>,
}

impl SchemaApplicationRecordOp {
    #[cfg(test)]
    pub(in crate::db) fn insert(record: &SchemaApplicationRecord) -> Result<Self, InternalError> {
        let key = ApplicationRecordKey::from_receipt(record.receipt())?;
        let after = encode_application_record(record)?;
        Self::from_encoded(key, None, after)
    }

    #[cfg(test)]
    pub(in crate::db) fn replace(
        before: &SchemaApplicationRecord,
        after: &SchemaApplicationRecord,
    ) -> Result<Self, InternalError> {
        let key = ApplicationRecordKey::from_receipt(before.receipt())?;
        if ApplicationRecordKey::from_receipt(after.receipt())? != key {
            return Err(InternalError::store_invariant());
        }
        Self::from_encoded(
            key,
            Some(encode_application_record(before)?),
            encode_application_record(after)?,
        )
    }

    pub(in crate::db) fn from_encoded(
        key: ApplicationRecordKey,
        before: Option<Vec<u8>>,
        after: Vec<u8>,
    ) -> Result<Self, InternalError> {
        let operation = Self { key, before, after };
        operation.validate()?;
        Ok(operation)
    }

    pub(in crate::db) const fn key(&self) -> ApplicationRecordKey {
        self.key
    }

    pub(in crate::db) fn before_bytes(&self) -> Option<&[u8]> {
        self.before.as_deref()
    }

    pub(in crate::db) const fn after_bytes(&self) -> &[u8] {
        self.after.as_slice()
    }

    pub(in crate::db) fn validate(&self) -> Result<(), InternalError> {
        if self.key == APPLICATION_HEADER_KEY {
            return Err(InternalError::store_corruption());
        }
        let after = decode_application_record(&self.after, self.key)?;
        if let Some(before) = self.before.as_deref() {
            let before = decode_application_record(before, self.key)?;
            if before.receipt().database_identity() != after.receipt().database_identity()
                || before.receipt().submission_key() != after.receipt().submission_key()
                || before.receipt().proposal_digest() != after.receipt().proposal_digest()
                || before.receipt().prior_head() != after.receipt().prior_head()
                || !valid_record_transition(&before, &after)
            {
                return Err(InternalError::store_corruption());
            }
        }
        Ok(())
    }
}

fn valid_record_transition(
    before: &SchemaApplicationRecord,
    after: &SchemaApplicationRecord,
) -> bool {
    match (before.receipt().outcome(), after.receipt().outcome()) {
        (
            crate::db::schema::SchemaChangeOutcome::Pending { candidate_head, .. },
            crate::db::schema::SchemaChangeOutcome::Applied { accepted_head },
        ) => candidate_head == accepted_head,
        (
            crate::db::schema::SchemaChangeOutcome::Pending { .. },
            crate::db::schema::SchemaChangeOutcome::Failed { .. },
        ) => true,
        _ => false,
    }
}

impl Storable for ApplicationRecordKey {
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
struct ApplicationRecordBytes(Vec<u8>);

impl Storable for ApplicationRecordBytes {
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
        max_size: MAX_SCHEMA_APPLICATION_RECORD_BYTES,
        is_fixed_size: false,
    };
}

pub(in crate::db) struct SchemaApplicationStore {
    map: StableBTreeMap<ApplicationRecordKey, ApplicationRecordBytes, ApplicationMemory>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum SchemaApplicationRecordPreflight {
    Ready,
    AlreadyApplied,
}

impl SchemaApplicationStore {
    fn open(memory: ApplicationMemory) -> Result<Self, InternalError> {
        let mut store = Self {
            map: StableBTreeMap::init(memory),
        };
        if store.map.is_empty() {
            store.map.insert(
                APPLICATION_HEADER_KEY,
                ApplicationRecordBytes(encode_application_header()),
            );
        } else {
            let header = store
                .map
                .get(&APPLICATION_HEADER_KEY)
                .ok_or_else(InternalError::store_corruption)?;
            decode_application_header(&header.0)?;
            if store.record_count()? > MAX_SCHEMA_APPLICATION_RECORDS {
                return Err(InternalError::store_corruption());
            }
        }
        Ok(store)
    }

    pub(in crate::db) fn load(
        &self,
        database_identity: TargetDatabaseIdentity,
        submission_key: &SchemaSubmissionKey,
    ) -> Result<Option<SchemaApplicationRecord>, InternalError> {
        let key = ApplicationRecordKey::new(database_identity, submission_key)?;
        self.load_key(key)
    }

    pub(in crate::db) fn load_key(
        &self,
        key: ApplicationRecordKey,
    ) -> Result<Option<SchemaApplicationRecord>, InternalError> {
        self.map
            .get(&key)
            .map(|raw| decode_application_record(&raw.0, key))
            .transpose()
    }

    pub(in crate::db) fn apply(
        &mut self,
        operation: &SchemaApplicationRecordOp,
    ) -> Result<(), InternalError> {
        match self.preflight(operation)? {
            SchemaApplicationRecordPreflight::AlreadyApplied => return Ok(()),
            SchemaApplicationRecordPreflight::Ready => {}
        }

        self.map.insert(
            operation.key(),
            ApplicationRecordBytes(operation.after_bytes().to_vec()),
        );
        Ok(())
    }

    pub(in crate::db) fn preflight(
        &self,
        operation: &SchemaApplicationRecordOp,
    ) -> Result<SchemaApplicationRecordPreflight, InternalError> {
        operation.validate()?;
        let current = self.map.get(&operation.key());
        if current
            .as_ref()
            .is_some_and(|raw| raw.0 == operation.after_bytes())
        {
            return Ok(SchemaApplicationRecordPreflight::AlreadyApplied);
        }
        if current.as_ref().map(|raw| raw.0.as_slice()) != operation.before_bytes() {
            return Err(InternalError::store_corruption());
        }
        if current.is_none() && self.record_count()? >= MAX_SCHEMA_APPLICATION_RECORDS {
            return Err(InternalError::store_invariant());
        }
        Ok(SchemaApplicationRecordPreflight::Ready)
    }

    pub(in crate::db) fn record_matches(
        &self,
        key: ApplicationRecordKey,
        expected: &[u8],
    ) -> Result<bool, InternalError> {
        let Some(raw) = self.map.get(&key) else {
            return Ok(false);
        };
        let record = decode_application_record(&raw.0, key)?;
        Ok(ApplicationRecordKey::from_receipt(record.receipt())? == key && raw.0 == expected)
    }

    fn record_count(&self) -> Result<u64, InternalError> {
        self.map
            .len()
            .checked_sub(1)
            .ok_or_else(InternalError::store_corruption)
    }
}

pub(in crate::db) fn encode_application_record(
    record: &SchemaApplicationRecord,
) -> Result<Vec<u8>, InternalError> {
    record.validate()?;
    let payload = Encode!(record).map_err(|_| InternalError::store_invariant())?;
    let payload_len = u32::try_from(payload.len()).map_err(|_| InternalError::store_invariant())?;
    let mut encoded = Vec::with_capacity(APPLICATION_RECORD_HEADER_BYTES + payload.len());
    encoded.extend_from_slice(APPLICATION_RECORD_MAGIC);
    encoded.push(APPLICATION_RECORD_VERSION);
    encoded.extend_from_slice(&payload_len.to_le_bytes());
    encoded.extend_from_slice(&crc32c(&payload).to_le_bytes());
    encoded.extend_from_slice(&payload);
    if encoded.len() > MAX_SCHEMA_APPLICATION_RECORD_BYTES as usize {
        return Err(InternalError::store_invariant());
    }
    Ok(encoded)
}

fn decode_application_record(
    bytes: &[u8],
    expected_key: ApplicationRecordKey,
) -> Result<SchemaApplicationRecord, InternalError> {
    if bytes.len() > MAX_SCHEMA_APPLICATION_RECORD_BYTES as usize
        || bytes.len() < APPLICATION_RECORD_HEADER_BYTES
        || &bytes[..8] != APPLICATION_RECORD_MAGIC
        || bytes[8] != APPLICATION_RECORD_VERSION
    {
        return Err(InternalError::store_corruption());
    }
    let payload_len = u32::from_le_bytes(
        bytes[9..13]
            .try_into()
            .map_err(|_| InternalError::store_corruption())?,
    ) as usize;
    let expected_checksum = u32::from_le_bytes(
        bytes[13..17]
            .try_into()
            .map_err(|_| InternalError::store_corruption())?,
    );
    let payload = bytes
        .get(APPLICATION_RECORD_HEADER_BYTES..)
        .ok_or_else(InternalError::store_corruption)?;
    if payload.len() != payload_len || crc32c(payload) != expected_checksum {
        return Err(InternalError::store_corruption());
    }
    let record =
        Decode!(payload, SchemaApplicationRecord).map_err(|_| InternalError::store_corruption())?;
    record.validate()?;
    if ApplicationRecordKey::from_receipt(record.receipt())? != expected_key
        || encode_application_record(&record)? != bytes
    {
        return Err(InternalError::store_corruption());
    }
    Ok(record)
}

fn encode_application_header() -> Vec<u8> {
    let mut encoded = Vec::with_capacity(APPLICATION_HEADER_BYTES);
    encoded.extend_from_slice(APPLICATION_HEADER_MAGIC);
    encoded.push(APPLICATION_HEADER_VERSION);
    encoded.extend_from_slice(&crc32c(&encoded).to_le_bytes());
    encoded
}

fn decode_application_header(bytes: &[u8]) -> Result<(), InternalError> {
    if bytes.len() != APPLICATION_HEADER_BYTES
        || &bytes[..8] != APPLICATION_HEADER_MAGIC
        || bytes[8] != APPLICATION_HEADER_VERSION
        || crc32c(&bytes[..9])
            != u32::from_le_bytes(
                bytes[9..13]
                    .try_into()
                    .map_err(|_| InternalError::store_corruption())?,
            )
    {
        return Err(InternalError::store_corruption());
    }
    Ok(())
}

fn application_memory() -> Result<ApplicationMemory, InternalError> {
    let memory = commit_memory_handle(current_commit_memory_allocation()?)?;
    Ok(RestrictedMemory::new(
        memory,
        APPLICATION_MEMORY_START_PAGE..APPLICATION_MEMORY_END_PAGE,
    ))
}

pub(in crate::db) fn with_schema_application_store<R>(
    f: impl FnOnce(&mut SchemaApplicationStore) -> Result<R, InternalError>,
) -> Result<R, InternalError> {
    let mut store = SchemaApplicationStore::open(application_memory()?)?;
    f(&mut store)
}

pub(in crate::db) fn apply_schema_application_record_op(
    operation: &SchemaApplicationRecordOp,
) -> Result<(), InternalError> {
    with_schema_application_store(|store| store.apply(operation))
}

pub(in crate::db) fn preflight_schema_application_record_op(
    operation: &SchemaApplicationRecordOp,
) -> Result<SchemaApplicationRecordPreflight, InternalError> {
    with_schema_application_store(|store| store.preflight(operation))
}

pub(in crate::db) fn verify_schema_application_record_op(
    operation: &SchemaApplicationRecordOp,
) -> Result<(), InternalError> {
    with_schema_application_store(|store| {
        if store.record_matches(operation.key(), operation.after_bytes())? {
            Ok(())
        } else {
            Err(InternalError::recovery_effect_verification_failed())
        }
    })
}

#[cfg(test)]
mod tests {
    use super::{
        APPLICATION_HEADER_KEY, APPLICATION_MEMORY_START_PAGE, ApplicationRecordBytes,
        ApplicationRecordKey, MAX_SCHEMA_APPLICATION_RECORDS, SchemaApplicationRecordOp,
        SchemaApplicationStore, decode_application_record, encode_application_record,
    };
    use crate::{
        db::{
            commit::{CommitMarker, decode_commit_marker_payload, encode_commit_marker_payload},
            schema::{
                SchemaApplicationRecord, SchemaChangeActivation, SchemaChangeActivationKind,
                SchemaChangeJob, SchemaChangeOutcome, SchemaChangeReceipt,
                derive_schema_change_job_id,
            },
        },
        testing::test_memory,
    };
    use ic_stable_structures::RestrictedMemory;
    use icydb_schema::{
        ExpectedAcceptedHead, ExpectedSchemaFingerprint, SchemaProposalDigest, SchemaSubmissionKey,
        TargetDatabaseIdentity, TargetStoreIdentity,
    };

    fn submission_key(value: &str) -> SchemaSubmissionKey {
        SchemaSubmissionKey::try_new(value).expect("test submission key should admit")
    }

    fn pending_record(value: &str) -> SchemaApplicationRecord {
        let database_identity = TargetDatabaseIdentity::from_bytes([0x11; 32]);
        let submission_key = submission_key(value);
        let proposal_digest = SchemaProposalDigest::from_bytes([0x22; 32]);
        let prior_head = ExpectedAcceptedHead::Empty;
        let job_id = derive_schema_change_job_id(
            database_identity,
            &submission_key,
            proposal_digest,
            &prior_head,
        )
        .expect("test job identity should derive");
        let receipt = SchemaChangeReceipt::new(
            database_identity,
            submission_key,
            proposal_digest,
            prior_head,
            SchemaChangeOutcome::Pending {
                job: SchemaChangeJob::new(job_id),
                candidate_head: ExpectedAcceptedHead::Exact {
                    revision: 1,
                    fingerprint: ExpectedSchemaFingerprint::from_bytes([0x33; 32]),
                },
            },
        )
        .expect("pending receipt should admit");
        SchemaApplicationRecord::new(
            receipt,
            vec![
                SchemaChangeActivation::new(
                    TargetStoreIdentity::from_bytes([0x44; 32]),
                    7,
                    9,
                    SchemaChangeActivationKind::Check,
                )
                .expect("activation should admit"),
            ],
        )
        .expect("pending record should admit")
    }

    fn applied_record(pending: &SchemaApplicationRecord) -> SchemaApplicationRecord {
        let receipt = pending.receipt();
        SchemaApplicationRecord::new(
            SchemaChangeReceipt::new(
                receipt.database_identity(),
                receipt.submission_key().clone(),
                receipt.proposal_digest(),
                receipt.prior_head().clone(),
                SchemaChangeOutcome::Applied {
                    accepted_head: ExpectedAcceptedHead::Exact {
                        revision: 1,
                        fingerprint: ExpectedSchemaFingerprint::from_bytes([0x33; 32]),
                    },
                },
            )
            .expect("applied receipt should admit"),
            Vec::new(),
        )
        .expect("applied record should admit")
    }

    fn empty_store(memory_id: u8) -> SchemaApplicationStore {
        SchemaApplicationStore::open(RestrictedMemory::new(test_memory(memory_id), 0..2_048))
            .expect("application store should initialize")
    }

    #[test]
    fn application_record_codec_is_canonical_and_checksum_bound() {
        let record = pending_record("codec");
        let key = ApplicationRecordKey::from_receipt(record.receipt()).expect("key should derive");
        let encoded = encode_application_record(&record).expect("record should encode");
        assert_eq!(
            decode_application_record(&encoded, key).expect("record should decode"),
            record,
        );

        let mut corrupted = encoded;
        let last = corrupted
            .last_mut()
            .expect("encoded record should contain a payload");
        *last ^= 0x80;
        assert!(decode_application_record(&corrupted, key).is_err());
    }

    #[test]
    fn application_store_compare_and_replace_is_idempotent_and_reopen_safe() {
        let pending = pending_record("replace");
        let applied = applied_record(&pending);
        let insert = SchemaApplicationRecordOp::insert(&pending).expect("insert should prepare");
        let replace =
            SchemaApplicationRecordOp::replace(&pending, &applied).expect("replace should prepare");
        let memory = RestrictedMemory::new(test_memory(220), 0..128);
        let mut store =
            SchemaApplicationStore::open(memory.clone()).expect("store should initialize");

        store.apply(&insert).expect("insert should apply");
        store
            .apply(&insert)
            .expect("exact replay should be idempotent");
        assert_eq!(
            store
                .load(
                    pending.receipt().database_identity(),
                    pending.receipt().submission_key(),
                )
                .expect("record should load"),
            Some(pending.clone()),
        );
        store.apply(&replace).expect("replacement should apply");

        let reopened = SchemaApplicationStore::open(memory).expect("store should reopen");
        assert_eq!(
            reopened
                .load(
                    applied.receipt().database_identity(),
                    applied.receipt().submission_key(),
                )
                .expect("terminal record should load"),
            Some(applied),
        );
    }

    #[test]
    fn application_store_rejects_wrong_compare_value_and_corrupt_record() {
        let first = pending_record("first");
        let second = pending_record("second");
        let first_insert =
            SchemaApplicationRecordOp::insert(&first).expect("first insert should prepare");
        let wrong_replace = SchemaApplicationRecordOp::replace(&second, &applied_record(&second))
            .expect("unrelated replacement should prepare");
        let mut store = empty_store(221);
        store
            .apply(&first_insert)
            .expect("first insert should apply");
        assert!(store.apply(&wrong_replace).is_err());

        let key = ApplicationRecordKey::from_receipt(first.receipt()).expect("key should derive");
        store
            .map
            .insert(key, ApplicationRecordBytes(vec![0xFF; 32]));
        assert!(store.load_key(key).is_err());
    }

    #[test]
    fn application_record_replacement_rejects_terminal_rewrite_and_wrong_accepted_head() {
        let pending = pending_record("transition");
        let applied = applied_record(&pending);
        assert!(
            SchemaApplicationRecordOp::replace(&applied, &applied).is_err(),
            "terminal application records must be immutable",
        );

        let receipt = pending.receipt();
        let wrong_head = SchemaApplicationRecord::new(
            SchemaChangeReceipt::new(
                receipt.database_identity(),
                receipt.submission_key().clone(),
                receipt.proposal_digest(),
                receipt.prior_head().clone(),
                SchemaChangeOutcome::Applied {
                    accepted_head: ExpectedAcceptedHead::Exact {
                        revision: 2,
                        fingerprint: ExpectedSchemaFingerprint::from_bytes([0x55; 32]),
                    },
                },
            )
            .expect("terminal receipt should admit"),
            Vec::new(),
        )
        .expect("terminal record should admit");
        assert!(
            SchemaApplicationRecordOp::replace(&pending, &wrong_head).is_err(),
            "promotion must publish the candidate head reserved by the pending receipt",
        );
    }

    #[test]
    fn commit_marker_round_trips_exact_schema_application_effect() {
        let record = pending_record("marker");
        let operation =
            SchemaApplicationRecordOp::insert(&record).expect("marker effect should prepare");
        let marker = CommitMarker::from_parts_with_schema_application(
            [0x55; 16],
            Vec::new(),
            Some(operation),
        )
        .expect("marker should admit");
        let encoded = encode_commit_marker_payload(&marker).expect("marker should encode");
        let decoded = decode_commit_marker_payload(&encoded).expect("marker should decode");
        let decoded = decoded
            .schema_application()
            .expect("schema application effect should remain present");

        assert_eq!(
            decoded.key(),
            ApplicationRecordKey::from_receipt(record.receipt()).expect("key should derive"),
        );
        assert_eq!(
            decode_application_record(decoded.after_bytes(), decoded.key())
                .expect("marker record should decode"),
            record,
        );
    }

    #[test]
    fn application_header_and_control_region_are_disjoint() {
        assert_eq!(APPLICATION_MEMORY_START_PAGE, 257);
        let store = empty_store(222);
        assert!(store.map.contains_key(&APPLICATION_HEADER_KEY));
    }

    #[test]
    fn application_store_capacity_rejects_before_record_mutation() {
        let mut store = empty_store(223);
        for ordinal in 0..MAX_SCHEMA_APPLICATION_RECORDS {
            let record = pending_record(&format!("capacity-{ordinal}"));
            let operation =
                SchemaApplicationRecordOp::insert(&record).expect("insert should prepare");
            store.apply(&operation).expect("bounded record should fit");
        }
        let overflow = pending_record("capacity-overflow");
        let operation =
            SchemaApplicationRecordOp::insert(&overflow).expect("overflow should prepare");

        assert!(store.preflight(&operation).is_err());
        assert_eq!(
            store
                .record_count()
                .expect("record count should remain readable"),
            MAX_SCHEMA_APPLICATION_RECORDS,
        );
    }
}
