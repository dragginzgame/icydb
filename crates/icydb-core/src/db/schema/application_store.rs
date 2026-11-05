//! Module: db::schema::application_store
//! Responsibility: persist bounded schema-application records in one database-control region.
//! Does not own: proposal lowering, accepted-schema publication, or activation advancement.
//! Boundary: marker-owned exact record replacement -> shared schema-control namespace.

use crate::db::codec::BoundedByteWriter;
use crate::{
    db::{
        database_format::crc32c,
        schema::{
            SchemaApplicationRecord, SchemaChangeActivation, SchemaChangeJob, SchemaChangeJobId,
            SchemaChangeOutcome, SchemaChangeReceipt,
            application_receipt::MAX_SCHEMA_CHANGE_ACTIVATIONS,
            control_store::{
                ControlMemory, ControlRecordFamily, SchemaControlBytes, SchemaControlMap,
                control_memory,
            },
            derive_schema_change_job_id,
            migration_lineage::{AcceptedEntitySourceVersion, PendingEntitySourceLineage},
            wire::SchemaWireReader,
        },
    },
    error::InternalError,
};
use icydb_schema::{
    EntitySourceDigest, ExpectedAcceptedHead, ExpectedSchemaFingerprint,
    MAX_SCHEMA_SUBMISSION_KEY_BYTES, SchemaProposalDigest, SchemaSubmissionKey,
    TargetDatabaseIdentity, TargetStoreIdentity,
};
use sha2::{Digest, Sha256};

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
pub(super) const MAX_SCHEMA_APPLICATION_RECORDS: u64 = 64;
const APPLICATION_RECORD_KEY_PROFILE: &[u8] = b"icydb.schema-application.record-key.v1";
type ApplicationRecordWriter = BoundedByteWriter<
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
        if key.0 == [0; 32] {
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

impl From<[u8; 32]> for ApplicationRecordKey {
    fn from(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }
}

impl From<ApplicationRecordKey> for [u8; 32] {
    fn from(key: ApplicationRecordKey) -> Self {
        key.0
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
        if self.key.0 == [0; 32] {
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
            SchemaChangeOutcome::Pending { .. },
            SchemaChangeOutcome::Applied { accepted_head }
            | SchemaChangeOutcome::Aborted { accepted_head },
        ) => accepted_head != before.receipt().prior_head(),
        _ => false,
    }
}

pub(in crate::db) struct SchemaApplicationStore {
    map: SchemaControlMap<ApplicationRecordKey>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum SchemaApplicationRecordPreflight {
    Ready {
        retired_terminal: Option<ApplicationRecordKey>,
    },
    AlreadyApplied,
}

impl SchemaApplicationStore {
    fn open(memory: ControlMemory) -> Result<Self, InternalError> {
        let store = Self {
            map: SchemaControlMap::open(memory, ControlRecordFamily::Application)?,
        };
        if store.record_count()? > MAX_SCHEMA_APPLICATION_RECORDS {
            return Err(InternalError::store_corruption());
        }
        Ok(store)
    }

    fn open_existing(memory: ControlMemory) -> Result<Option<Self>, InternalError> {
        let Some(map) = SchemaControlMap::open_existing(memory, ControlRecordFamily::Application)?
        else {
            return Ok(None);
        };
        let store = Self { map };
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
            .get(&key)?
            .map(|raw| decode_application_record(&raw.0, key))
            .transpose()
    }

    pub(in crate::db) fn load_job(
        &self,
        job_id: SchemaChangeJobId,
    ) -> Result<Option<SchemaApplicationRecord>, InternalError> {
        let mut found = None;
        for key in self.map.keys()? {
            let raw = self
                .map
                .get(&key)?
                .ok_or_else(InternalError::store_corruption)?;
            let record = decode_application_record(&raw.0, key)?;
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
                    && self.map.remove(&key)?.is_none()
                {
                    return Err(InternalError::store_corruption());
                }
            }
        }

        self.map.insert(
            operation.key(),
            SchemaControlBytes(operation.after_bytes().to_vec()),
        )?;
        Ok(())
    }

    pub(in crate::db) fn preflight(
        &self,
        operation: &SchemaApplicationRecordOp,
    ) -> Result<SchemaApplicationRecordPreflight, InternalError> {
        operation.validate()?;
        let current = self.map.get(&operation.key())?;
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
        let Some(raw) = self.map.get(&key)? else {
            return Ok(false);
        };
        let record = decode_application_record(&raw.0, key)?;
        Ok(ApplicationRecordKey::from_receipt(record.receipt())? == key && raw.0 == expected)
    }

    /// Return the bounded set of generated activations still owned by receipts.
    pub(in crate::db::schema) fn pending_activations(
        &self,
    ) -> Result<Vec<SchemaChangeActivation>, InternalError> {
        if self.record_count()? > MAX_SCHEMA_APPLICATION_RECORDS {
            return Err(InternalError::store_corruption());
        }
        let mut pending = Vec::new();
        for key in self.map.keys()? {
            let raw = self
                .map
                .get(&key)?
                .ok_or_else(InternalError::store_corruption)?;
            let record = decode_application_record(&raw.0, key)?;
            pending.extend_from_slice(record.activations());
        }
        Ok(pending)
    }

    fn record_count(&self) -> Result<u64, InternalError> {
        self.map.len()
    }

    fn terminal_retention_candidate(&self) -> Result<Option<ApplicationRecordKey>, InternalError> {
        let protected = crate::db::commit::current_schema_application_readiness_key()?
            .map(ApplicationRecordKey::from_bytes);
        for key in self.map.keys()? {
            let raw = self
                .map
                .get(&key)?
                .ok_or_else(InternalError::store_corruption)?;
            let record = decode_application_record(&raw.0, key)?;
            if Some(key) != protected
                && !matches!(
                    record.receipt().outcome(),
                    SchemaChangeOutcome::Pending { .. }
                )
            {
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
    match record.pending_source_lineage() {
        None => writer.push_u8(0),
        Some(pending) => {
            writer.push_u8(1);
            writer.push_u32(pending.version().get());
            writer.push_bytes(&pending.prior_digest().to_bytes());
            writer.push_bytes(&pending.proposed_digest().to_bytes());
        }
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
    let pending_source_lineage = match reader.read_u8()? {
        0 => None,
        1 => Some(PendingEntitySourceLineage::new(
            AcceptedEntitySourceVersion::try_new(reader.read_u32()?)
                .map_err(|_| InternalError::store_corruption())?,
            EntitySourceDigest::from_bytes(reader.read_array()?),
            EntitySourceDigest::from_bytes(reader.read_array()?),
        )?),
        _ => return Err(InternalError::store_corruption()),
    };
    reader.finish()?;

    let receipt = SchemaChangeReceipt::new(
        database_identity,
        submission_key,
        proposal_digest,
        prior_head,
        outcome,
    )?;
    SchemaApplicationRecord::new(receipt, activations)?
        .with_pending_source_lineage(pending_source_lineage)
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

// Registry preflight is read-only and uses the exact control allocation being
// reconciled, including before the shared map exists on a fresh installation.
pub(in crate::db) fn pending_application_blocks_registry_change(
    memory: ControlMemory,
) -> Result<bool, InternalError> {
    let Some(store) = SchemaApplicationStore::open_existing(memory)? else {
        return Ok(false);
    };
    store
        .pending_activations()
        .map(|pending| !pending.is_empty())
}

pub(in crate::db) fn with_schema_application_store<R>(
    f: impl FnOnce(&mut SchemaApplicationStore) -> Result<R, InternalError>,
) -> Result<R, InternalError> {
    let mut store = SchemaApplicationStore::open(control_memory()?)?;
    f(&mut store)
}

pub(in crate::db) fn load_schema_application_record_read_only(
    database_identity: TargetDatabaseIdentity,
    submission_key: &SchemaSubmissionKey,
) -> Result<Option<SchemaApplicationRecord>, InternalError> {
    let Some(store) = SchemaApplicationStore::open_existing(control_memory()?)? else {
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
mod tests;
