//! Module: db::schema::application_store
//! Responsibility: persist bounded schema-application records in one database-control region.
//! Does not own: proposal lowering, accepted-schema publication, or activation advancement.
//! Boundary: marker-owned exact record replacement -> disjoint current-form stable BTreeMap.

use crate::{
    db::{
        commit::{MAX_COMMIT_BYTES, commit_memory_handle, current_commit_memory_allocation},
        database_format::crc32c,
        schema::{
            SchemaApplicationRecord, SchemaChangeActivation, SchemaChangeJob, SchemaChangeJobId,
            SchemaChangeOutcome, SchemaChangeReceipt,
            application_receipt::MAX_SCHEMA_CHANGE_ACTIVATIONS,
            derive_schema_change_job_id,
            wire::{SchemaWireReader, SchemaWireWriter},
        },
    },
    error::InternalError,
};
use ic_stable_structures::{
    BTreeMap as StableBTreeMap, DefaultMemoryImpl, Memory, RestrictedMemory, Storable,
    memory_manager::VirtualMemory, storable::Bound,
};
use icydb_schema::{
    ExpectedAcceptedHead, ExpectedSchemaFingerprint, MAX_SCHEMA_SUBMISSION_KEY_BYTES,
    SchemaProposalDigest, SchemaSubmissionKey, TargetDatabaseIdentity, TargetStoreIdentity,
};
use sha2::{Digest, Sha256};
use std::borrow::Cow;

const APPLICATION_HEADER_KEY: ApplicationRecordKey = ApplicationRecordKey([0; 32]);
const APPLICATION_HEADER_MAGIC: &[u8; 8] = b"ICYSAH01";
const APPLICATION_HEADER_VERSION: u8 = 1;
const APPLICATION_HEADER_BYTES: usize = 8 + 1 + 4;
const APPLICATION_RECORD_MAGIC: &[u8; 8] = b"ICYSAR01";
const APPLICATION_RECORD_VERSION: u8 = 1;
const APPLICATION_RECORD_HEADER_BYTES: usize = 8 + 1 + 4 + 4;
pub(in crate::db) const MAX_SCHEMA_APPLICATION_RECORD_BYTES: u32 = 64 * 1024;
const HEAD_EMPTY_TAG: u8 = 0;
const HEAD_EXACT_TAG: u8 = 1;
const OUTCOME_NO_OP_TAG: u8 = 1;
const OUTCOME_APPLIED_TAG: u8 = 2;
const OUTCOME_PENDING_TAG: u8 = 3;
const OUTCOME_ABORTED_TAG: u8 = 4;
const MAX_SCHEMA_APPLICATION_RECORDS: u64 = 64;
const APPLICATION_RECORD_KEY_PROFILE: &[u8] = b"icydb.schema-application.record-key.v1";
const WASM_PAGE_BYTES: u64 = 65_536;
const APPLICATION_MEMORY_START_PAGE: u64 = MAX_COMMIT_BYTES as u64 / WASM_PAGE_BYTES + 1;
const APPLICATION_MEMORY_END_PAGE: u64 = 4_096;

type ApplicationMemory = RestrictedMemory<VirtualMemory<DefaultMemoryImpl>>;
type ApplicationRecordWriter = SchemaWireWriter<
    { MAX_SCHEMA_APPLICATION_RECORD_BYTES as usize - APPLICATION_RECORD_HEADER_BYTES },
>;
type ApplicationRecordReader<'a> = SchemaWireReader<'a>;

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
    pub(in crate::db) fn insert(record: &SchemaApplicationRecord) -> Result<Self, InternalError> {
        let key = ApplicationRecordKey::from_receipt(record.receipt())?;
        let after = encode_application_record(record)?;
        Self::from_encoded(key, None, after)
    }

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
            crate::db::schema::SchemaChangeOutcome::Aborted { accepted_head },
        ) => accepted_head != before.receipt().prior_head(),
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
    Ready {
        retired_terminal: Option<ApplicationRecordKey>,
    },
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

    fn open_existing(memory: ApplicationMemory) -> Result<Option<Self>, InternalError> {
        if memory.size() == 0 {
            return Ok(None);
        }
        let mut header = [0_u8; 28];
        memory.read(0, &mut header);
        if &header[..3] != b"BTR" || header[3] != 2 {
            return Err(InternalError::store_corruption());
        }
        let mut allocator_header = [0_u8; 4];
        memory.read(52, &mut allocator_header);
        if &allocator_header[..3] != b"BTA" || allocator_header[3] != 1 {
            return Err(InternalError::store_corruption());
        }
        let store = Self {
            map: StableBTreeMap::load(memory),
        };
        let application_header = store
            .map
            .get(&APPLICATION_HEADER_KEY)
            .ok_or_else(InternalError::store_corruption)?;
        decode_application_header(&application_header.0)?;
        if store.record_count()? > MAX_SCHEMA_APPLICATION_RECORDS {
            return Err(InternalError::store_corruption());
        }
        Ok(Some(store))
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

    pub(in crate::db) fn load_job(
        &self,
        job_id: SchemaChangeJobId,
    ) -> Result<Option<SchemaApplicationRecord>, InternalError> {
        let mut found = None;
        for entry in self.map.iter() {
            let key = *entry.key();
            if key == APPLICATION_HEADER_KEY {
                continue;
            }
            let record = decode_application_record(&entry.value().0, key)?;
            let matches_job =
                !matches!(record.receipt().outcome(), SchemaChangeOutcome::NoOp { .. })
                    && derive_schema_change_job_id(
                        record.receipt().database_identity(),
                        record.receipt().submission_key(),
                        record.receipt().proposal_digest(),
                        record.receipt().prior_head(),
                    )? == job_id;
            if !matches_job {
                continue;
            }
            if found.replace(record).is_some() {
                return Err(InternalError::store_corruption());
            }
        }
        Ok(found)
    }

    pub(in crate::db) fn apply(
        &mut self,
        operation: &SchemaApplicationRecordOp,
    ) -> Result<(), InternalError> {
        match self.preflight(operation)? {
            SchemaApplicationRecordPreflight::AlreadyApplied => return Ok(()),
            SchemaApplicationRecordPreflight::Ready { retired_terminal } => {
                if let Some(key) = retired_terminal
                    && self.map.remove(&key).is_none()
                {
                    return Err(InternalError::store_corruption());
                }
            }
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
        let retired_terminal =
            if current.is_none() && self.record_count()? >= MAX_SCHEMA_APPLICATION_RECORDS {
                Some(
                    self.terminal_retention_candidate()?
                        .ok_or_else(InternalError::schema_application_conflict)?,
                )
            } else {
                None
            };
        Ok(SchemaApplicationRecordPreflight::Ready { retired_terminal })
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

    fn terminal_retention_candidate(&self) -> Result<Option<ApplicationRecordKey>, InternalError> {
        for entry in self.map.iter() {
            let key = *entry.key();
            if key == APPLICATION_HEADER_KEY {
                continue;
            }
            let record = decode_application_record(&entry.value().0, key)?;
            if !matches!(
                record.receipt().outcome(),
                SchemaChangeOutcome::Pending { .. }
            ) {
                return Ok(Some(key));
            }
        }
        Ok(None)
    }
}

pub(in crate::db) fn encode_application_record(
    record: &SchemaApplicationRecord,
) -> Result<Vec<u8>, InternalError> {
    record.validate()?;
    let payload = encode_application_record_payload(record)?;
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
    {
        return Err(InternalError::store_corruption());
    }
    if bytes[8] != APPLICATION_RECORD_VERSION {
        return Err(InternalError::serialize_incompatible_persisted_format());
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
    let record = decode_application_record_payload(payload)?;
    if ApplicationRecordKey::from_receipt(record.receipt())? != expected_key
        || encode_application_record(&record)? != bytes
    {
        return Err(InternalError::store_corruption());
    }
    Ok(record)
}

fn encode_application_record_payload(
    record: &SchemaApplicationRecord,
) -> Result<Vec<u8>, InternalError> {
    let mut writer = ApplicationRecordWriter::new();
    let receipt = record.receipt();
    writer.push_bytes(&receipt.database_identity().to_bytes());
    writer.push_bounded_string(
        receipt.submission_key().as_str(),
        MAX_SCHEMA_SUBMISSION_KEY_BYTES,
    )?;
    writer.push_bytes(&receipt.proposal_digest().to_bytes());
    encode_expected_head(&mut writer, receipt.prior_head());
    encode_schema_change_outcome(&mut writer, receipt.outcome());
    writer.push_len(record.activations().len())?;
    for activation in record.activations() {
        writer.push_bytes(&activation.store().to_bytes());
        writer.push_u64(activation.entity_tag());
        writer.push_u32(activation.constraint_id());
    }
    writer.finish()
}

fn decode_application_record_payload(
    payload: &[u8],
) -> Result<SchemaApplicationRecord, InternalError> {
    let mut reader = ApplicationRecordReader::new(payload);
    let database_identity = TargetDatabaseIdentity::from_bytes(reader.read_array()?);
    let submission_key =
        SchemaSubmissionKey::try_new(reader.read_bounded_string(MAX_SCHEMA_SUBMISSION_KEY_BYTES)?)
            .map_err(|_| InternalError::store_corruption())?;
    let proposal_digest = SchemaProposalDigest::from_bytes(reader.read_array()?);
    let prior_head = decode_expected_head(&mut reader)?;
    let outcome = decode_schema_change_outcome(&mut reader)?;
    let activation_count = reader.read_bounded_count(MAX_SCHEMA_CHANGE_ACTIVATIONS)?;
    let mut activations = Vec::new();
    activations
        .try_reserve_exact(activation_count)
        .map_err(|_| InternalError::store_corruption())?;
    for _ in 0..activation_count {
        activations.push(
            SchemaChangeActivation::new(
                TargetStoreIdentity::from_bytes(reader.read_array()?),
                reader.read_u64()?,
                reader.read_u32()?,
            )
            .map_err(|_| InternalError::store_corruption())?,
        );
    }
    reader.finish()?;

    let receipt = SchemaChangeReceipt::new(
        database_identity,
        submission_key,
        proposal_digest,
        prior_head,
        outcome,
    )?;
    SchemaApplicationRecord::new(receipt, activations)
}

fn encode_expected_head(writer: &mut ApplicationRecordWriter, head: &ExpectedAcceptedHead) {
    match head {
        ExpectedAcceptedHead::Empty => writer.push_u8(HEAD_EMPTY_TAG),
        ExpectedAcceptedHead::Exact {
            revision,
            fingerprint,
        } => {
            writer.push_u8(HEAD_EXACT_TAG);
            writer.push_u64(*revision);
            writer.push_bytes(&fingerprint.to_bytes());
        }
    }
}

fn decode_expected_head(
    reader: &mut ApplicationRecordReader<'_>,
) -> Result<ExpectedAcceptedHead, InternalError> {
    match reader.read_u8()? {
        HEAD_EMPTY_TAG => Ok(ExpectedAcceptedHead::Empty),
        HEAD_EXACT_TAG => Ok(ExpectedAcceptedHead::Exact {
            revision: reader.read_u64()?,
            fingerprint: ExpectedSchemaFingerprint::from_bytes(reader.read_array()?),
        }),
        _ => Err(InternalError::store_corruption()),
    }
}

fn encode_schema_change_outcome(
    writer: &mut ApplicationRecordWriter,
    outcome: &SchemaChangeOutcome,
) {
    match outcome {
        SchemaChangeOutcome::NoOp { accepted_head } => {
            writer.push_u8(OUTCOME_NO_OP_TAG);
            encode_expected_head(writer, accepted_head);
        }
        SchemaChangeOutcome::Applied { accepted_head } => {
            writer.push_u8(OUTCOME_APPLIED_TAG);
            encode_expected_head(writer, accepted_head);
        }
        SchemaChangeOutcome::Pending {
            job,
            candidate_head,
        } => {
            writer.push_u8(OUTCOME_PENDING_TAG);
            writer.push_bytes(&job.id().to_bytes());
            encode_expected_head(writer, candidate_head);
        }
        SchemaChangeOutcome::Aborted { accepted_head } => {
            writer.push_u8(OUTCOME_ABORTED_TAG);
            encode_expected_head(writer, accepted_head);
        }
    }
}

fn decode_schema_change_outcome(
    reader: &mut ApplicationRecordReader<'_>,
) -> Result<SchemaChangeOutcome, InternalError> {
    match reader.read_u8()? {
        OUTCOME_NO_OP_TAG => Ok(SchemaChangeOutcome::NoOp {
            accepted_head: decode_expected_head(reader)?,
        }),
        OUTCOME_APPLIED_TAG => Ok(SchemaChangeOutcome::Applied {
            accepted_head: decode_expected_head(reader)?,
        }),
        OUTCOME_PENDING_TAG => Ok(SchemaChangeOutcome::Pending {
            job: SchemaChangeJob::new(SchemaChangeJobId::from_bytes(reader.read_array()?)?),
            candidate_head: decode_expected_head(reader)?,
        }),
        OUTCOME_ABORTED_TAG => Ok(SchemaChangeOutcome::Aborted {
            accepted_head: decode_expected_head(reader)?,
        }),
        _ => Err(InternalError::store_corruption()),
    }
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

pub(in crate::db) fn load_schema_application_record_read_only(
    database_identity: TargetDatabaseIdentity,
    submission_key: &SchemaSubmissionKey,
) -> Result<Option<SchemaApplicationRecord>, InternalError> {
    let Some(store) = SchemaApplicationStore::open_existing(application_memory()?)? else {
        return Ok(None);
    };
    store.load(database_identity, submission_key)
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
        APPLICATION_HEADER_KEY, APPLICATION_MEMORY_START_PAGE, APPLICATION_RECORD_HEADER_BYTES,
        APPLICATION_RECORD_MAGIC, APPLICATION_RECORD_VERSION, ApplicationRecordBytes,
        ApplicationRecordKey, MAX_SCHEMA_APPLICATION_RECORDS, SchemaApplicationRecordOp,
        SchemaApplicationStore, crc32c, decode_application_header, decode_application_record,
        encode_application_header, encode_application_record,
    };
    use crate::{
        db::schema::migration_lineage::{
            AcceptedEntitySourceLineage, AcceptedEntitySourceLineageCatalog,
        },
        db::{
            commit::{
                CommitMarker, DatabaseControlOp, decode_commit_marker_payload,
                encode_commit_marker_payload,
            },
            schema::{
                EntitySourceLineageCatalogOp, SchemaApplicationRecord, SchemaChangeActivation,
                SchemaChangeJob, SchemaChangeOutcome, SchemaChangeReceipt,
                derive_schema_change_job_id,
            },
        },
        testing::test_memory,
        types::EntityTag,
    };
    use ic_stable_structures::RestrictedMemory;
    use icydb_schema::{
        ExpectedAcceptedHead, ExpectedSchemaFingerprint, MAX_SCHEMA_SUBMISSION_KEY_BYTES,
        SchemaProposalDigest, SchemaSubmissionKey, TargetDatabaseIdentity, TargetStoreIdentity,
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
                SchemaChangeActivation::new(TargetStoreIdentity::from_bytes([0x44; 32]), 7, 9)
                    .expect("activation should admit"),
            ],
        )
        .expect("pending record should admit")
    }

    fn no_op_record(value: &str) -> SchemaApplicationRecord {
        SchemaApplicationRecord::new(
            SchemaChangeReceipt::new(
                TargetDatabaseIdentity::from_bytes([0x11; 32]),
                submission_key(value),
                SchemaProposalDigest::from_bytes([0x22; 32]),
                ExpectedAcceptedHead::Empty,
                SchemaChangeOutcome::NoOp {
                    accepted_head: ExpectedAcceptedHead::Empty,
                },
            )
            .expect("no-op receipt should admit"),
            Vec::new(),
        )
        .expect("no-op record should admit")
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

    fn aborted_record(pending: &SchemaApplicationRecord) -> SchemaApplicationRecord {
        let receipt = pending.receipt();
        SchemaApplicationRecord::new(
            SchemaChangeReceipt::new(
                receipt.database_identity(),
                receipt.submission_key().clone(),
                receipt.proposal_digest(),
                receipt.prior_head().clone(),
                SchemaChangeOutcome::Aborted {
                    accepted_head: ExpectedAcceptedHead::Exact {
                        revision: 2,
                        fingerprint: ExpectedSchemaFingerprint::from_bytes([0x66; 32]),
                    },
                },
            )
            .expect("aborted receipt should admit"),
            Vec::new(),
        )
        .expect("aborted record should admit")
    }

    fn empty_store(memory_id: u8) -> SchemaApplicationStore {
        SchemaApplicationStore::open(RestrictedMemory::new(test_memory(memory_id), 0..2_048))
            .expect("application store should initialize")
    }

    fn rewrite_application_payload(encoded: &[u8], rewrite: impl FnOnce(&mut Vec<u8>)) -> Vec<u8> {
        let mut payload = encoded[APPLICATION_RECORD_HEADER_BYTES..].to_vec();
        rewrite(&mut payload);
        let mut rewritten = APPLICATION_RECORD_MAGIC.to_vec();
        rewritten.push(APPLICATION_RECORD_VERSION);
        rewritten.extend_from_slice(
            &u32::try_from(payload.len())
                .expect("test payload should fit u32")
                .to_le_bytes(),
        );
        rewritten.extend_from_slice(&crc32c(&payload).to_le_bytes());
        rewritten.extend_from_slice(&payload);
        rewritten
    }

    #[test]
    fn application_record_codec_is_canonical_and_checksum_bound() {
        let pending = pending_record("codec");
        let records = [
            no_op_record("codec-no-op"),
            pending.clone(),
            applied_record(&pending),
            aborted_record(&pending),
        ];
        for record in records {
            let key =
                ApplicationRecordKey::from_receipt(record.receipt()).expect("key should derive");
            let encoded = encode_application_record(&record).expect("record should encode");
            assert_eq!(
                encode_application_record(&record).expect("repeat encoding should remain stable"),
                encoded,
            );
            assert_eq!(
                decode_application_record(&encoded, key).expect("record should decode"),
                record,
            );
        }

        let key = ApplicationRecordKey::from_receipt(pending.receipt()).expect("key should derive");
        let mut corrupted = encode_application_record(&pending).expect("record should encode");
        let last = corrupted
            .last_mut()
            .expect("encoded record should contain a payload");
        *last ^= 0x80;
        assert!(decode_application_record(&corrupted, key).is_err());
    }

    #[test]
    fn application_record_v1_golden_bytes_remain_stable() {
        let pending = pending_record("golden");
        let record = applied_record(&pending);
        let encoded = encode_application_record(&record).expect("golden record should encode");

        let mut payload = vec![0x11; 32];
        payload.extend_from_slice(&6_u32.to_be_bytes());
        payload.extend_from_slice(b"golden");
        payload.extend_from_slice(&[0x22; 32]);
        payload.push(0);
        payload.push(2);
        payload.push(1);
        payload.extend_from_slice(&1_u64.to_be_bytes());
        payload.extend_from_slice(&[0x33; 32]);
        payload.extend_from_slice(&0_u32.to_be_bytes());
        let mut expected = b"ICYSAR01".to_vec();
        expected.push(1);
        expected.extend_from_slice(
            &u32::try_from(payload.len())
                .expect("golden payload should fit u32")
                .to_le_bytes(),
        );
        expected.extend_from_slice(&crc32c(&payload).to_le_bytes());
        expected.extend_from_slice(&payload);

        assert_eq!(encoded, expected);
    }

    #[test]
    fn application_record_decode_rejects_truncation_tags_lengths_and_trailing_bytes() {
        let record = pending_record("malformed");
        let key = ApplicationRecordKey::from_receipt(record.receipt()).expect("key should derive");
        let encoded = encode_application_record(&record).expect("record should encode");
        for len in 0..encoded.len() {
            assert!(
                decode_application_record(&encoded[..len], key).is_err(),
                "truncated application record at {len} must fail closed",
            );
        }

        let mut bad_magic = encoded.clone();
        bad_magic[0] ^= 1;
        assert!(decode_application_record(&bad_magic, key).is_err());

        let mut bad_version = encoded.clone();
        bad_version[APPLICATION_RECORD_MAGIC.len()] = 2;
        let error = decode_application_record(&bad_version, key)
            .expect_err("noncurrent application record version must fail closed");
        assert_eq!(
            error.class,
            crate::error::ErrorClass::IncompatiblePersistedFormat
        );

        let oversized_key = rewrite_application_payload(&encoded, |payload| {
            payload[32..36].copy_from_slice(
                &u32::try_from(MAX_SCHEMA_SUBMISSION_KEY_BYTES + 1)
                    .expect("submission-key bound should fit u32")
                    .to_be_bytes(),
            );
        });
        assert!(decode_application_record(&oversized_key, key).is_err());

        let unknown_outcome = rewrite_application_payload(&encoded, |payload| {
            let outcome_offset = 32 + 4 + "malformed".len() + 32 + 1;
            payload[outcome_offset] = u8::MAX;
        });
        assert!(decode_application_record(&unknown_outcome, key).is_err());

        let trailing = rewrite_application_payload(&encoded, |payload| payload.push(0));
        assert!(decode_application_record(&trailing, key).is_err());
    }

    #[test]
    fn marker_round_trip_binds_application_receipt_and_lineage_effect() {
        let record = pending_record("lineage-marker");
        let lineage =
            AcceptedEntitySourceLineageCatalog::try_new(std::collections::BTreeMap::from([(
                (
                    TargetStoreIdentity::from_bytes([0x44; 32]),
                    EntityTag::new(7),
                ),
                AcceptedEntitySourceLineage::unadopted(ExpectedAcceptedHead::Exact {
                    revision: 1,
                    fingerprint: ExpectedSchemaFingerprint::from_bytes([0x33; 32]),
                })
                .expect("lineage should admit"),
            )]))
            .expect("lineage catalog should admit");
        let application = SchemaApplicationRecordOp::insert(&record)
            .expect("application operation should prepare");
        let lineage = EntitySourceLineageCatalogOp::replace(None, &lineage)
            .expect("lineage operation should prepare");
        let marker = CommitMarker::from_parts_with_database_control(
            [0x55; 16],
            Vec::new(),
            vec![
                DatabaseControlOp::SchemaApplication(application.clone()),
                DatabaseControlOp::EntitySourceLineage(lineage.clone()),
            ],
        )
        .expect("marker should admit");
        let encoded = encode_commit_marker_payload(&marker).expect("marker should encode");
        let decoded = decode_commit_marker_payload(&encoded).expect("marker should decode");
        decoded
            .schema_application()
            .expect("application operation should remain");
        assert_eq!(
            decoded
                .entity_source_lineage()
                .expect("lineage effect should remain")
                .after_bytes(),
            lineage.after_bytes(),
        );
        assert!(
            CommitMarker::from_parts_with_database_control(
                [0x56; 16],
                Vec::new(),
                vec![
                    DatabaseControlOp::EntitySourceLineage(lineage),
                    DatabaseControlOp::SchemaApplication(application),
                ],
            )
            .is_err(),
            "database-control operations must remain in canonical target order",
        );
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
        let aborted = aborted_record(&pending);
        SchemaApplicationRecordOp::replace(&pending, &aborted)
            .expect("pending application should admit explicit abort");
        assert!(
            SchemaApplicationRecordOp::replace(&applied, &applied).is_err(),
            "terminal application records must be immutable",
        );
        assert!(
            SchemaApplicationRecordOp::replace(&aborted, &applied).is_err(),
            "an aborted application must remain terminal",
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

        let mut malformed_magic = encode_application_header();
        malformed_magic[0] ^= 1;
        let checksum = crc32c(&malformed_magic[..9]);
        malformed_magic[9..].copy_from_slice(&checksum.to_le_bytes());
        assert!(decode_application_header(&malformed_magic).is_err());
    }

    #[test]
    fn application_store_capacity_preserves_pending_evidence() {
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

    #[test]
    fn application_store_capacity_recycles_one_deterministic_terminal_receipt() {
        let mut store = empty_store(224);
        let mut terminal_keys = Vec::new();
        for ordinal in 0..MAX_SCHEMA_APPLICATION_RECORDS {
            let pending = pending_record(&format!("terminal-{ordinal}"));
            let terminal = if ordinal % 2 == 0 {
                applied_record(&pending)
            } else {
                aborted_record(&pending)
            };
            terminal_keys.push(
                ApplicationRecordKey::from_receipt(terminal.receipt())
                    .expect("terminal key should derive"),
            );
            let operation = SchemaApplicationRecordOp::insert(&terminal)
                .expect("terminal insert should prepare");
            store.apply(&operation).expect("terminal record should fit");
        }
        terminal_keys.sort_unstable();
        let retired = terminal_keys[0];
        let next = pending_record("capacity-recycled");
        let operation =
            SchemaApplicationRecordOp::insert(&next).expect("pending insert should prepare");

        store
            .apply(&operation)
            .expect("one terminal receipt should make bounded room");

        assert_eq!(
            store
                .record_count()
                .expect("record count should remain readable"),
            MAX_SCHEMA_APPLICATION_RECORDS,
        );
        assert!(
            store
                .load_key(retired)
                .expect("retired key should remain readable")
                .is_none(),
        );
        assert_eq!(
            store
                .load(
                    next.receipt().database_identity(),
                    next.receipt().submission_key(),
                )
                .expect("new pending record should load"),
            Some(next),
        );
    }
}
