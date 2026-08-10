//! Module: db::commit::marker
//! Responsibility: define persisted commit-marker payloads and marker-shape validation.
//! Does not own: marker storage backend, commit-window lifecycle, or recovery orchestration.
//! Boundary: commit::{prepare,recovery,store} -> commit::marker (one-way).

use crate::{
    db::{
        data::{DecodedDataStoreKey, RawDataStoreKey},
        integrity::MutationProgressRecordOp,
        journal::{
            JournalBatch, JournalRecord, decode_journal_batch, encode_journal_batch,
            journal_batch_encoded_len,
        },
        schema::{ApplicationRecordKey, SchemaApplicationRecordOp},
    },
    error::InternalError,
    runtime::now_millis,
};
use ic_stable_structures::Storable;
#[cfg(test)]
use std::cell::Cell;
use std::{
    borrow::Cow,
    collections::BTreeSet,
    rc::Rc,
    sync::atomic::{AtomicU64, Ordering},
};

// Commit-marker durability invariant:
// - Persist one marker before any stable mutation.
// - After marker persistence, apply/recovery consume only marker payloads.
// - Recovery publishes marker-bound journal batches deterministically.
// This makes partial mutations deterministic without a WAL.

/// Stored commit-id byte width shared by marker and guard paths.
pub(in crate::db) const COMMIT_ID_BYTES: usize = 16;
const COMMIT_SCHEMA_FINGERPRINT_BYTES: usize = 16;
pub(in crate::db) const COMMIT_MARKER_FORMAT_VERSION_CURRENT: u8 = 3;
pub(in crate::db) const MAX_DATABASE_CONTROL_OPS_PER_MARKER: usize = 4;

pub(in crate::db) type CommitSchemaFingerprint = [u8; COMMIT_SCHEMA_FINGERPRINT_BYTES];

static COMMIT_ID_SEQUENCE: AtomicU64 = AtomicU64::new(1);
#[cfg(test)]
thread_local! {
    static TEST_JOURNAL_SEQUENCE: Cell<u64> = const { Cell::new(1) };
}

// Conservative upper bound to avoid rejecting valid commits when index entries
// are large; still small enough to fit typical canister constraints.
pub(crate) const MAX_COMMIT_BYTES: u32 = 16 * 1024 * 1024;

///
/// CommitRowOp
///
/// Row-level mutation recorded in a commit marker.
/// Store identity is derived from `entity_path` at apply/recovery time.
///

#[derive(Clone, Debug)]
pub(in crate::db) struct CommitRowOp {
    pub(crate) entity_path: Rc<str>,
    pub(crate) key: RawDataStoreKey,
    pub(crate) before: Option<Vec<u8>>,
    pub(crate) after: Option<Vec<u8>>,
    pub(crate) schema_fingerprint: CommitSchemaFingerprint,
    pub(crate) mutation_diagnostic_context: Option<crate::error::MutationDiagnosticContext>,
}

impl CommitRowOp {
    /// Construct a row-level commit operation.
    #[must_use]
    pub(crate) fn new(
        entity_path: impl Into<Rc<str>>,
        key: RawDataStoreKey,
        before: Option<Vec<u8>>,
        after: Option<Vec<u8>>,
        schema_fingerprint: CommitSchemaFingerprint,
    ) -> Self {
        Self {
            entity_path: entity_path.into(),
            key,
            before,
            after,
            schema_fingerprint,
            mutation_diagnostic_context: None,
        }
    }

    pub(crate) const fn with_mutation_diagnostic_context(
        mut self,
        context: crate::error::MutationDiagnosticContext,
    ) -> Self {
        self.mutation_diagnostic_context = Some(context);
        self
    }

    /// Construct one row-level commit operation from raw key bytes.
    ///
    /// This is the raw-key decode boundary for callers that still own opaque
    /// key bytes rather than a typed `RawDataStoreKey`.
    pub(crate) fn try_new_bytes(
        entity_path: impl Into<Rc<str>>,
        key: &[u8],
        before: Option<Vec<u8>>,
        after: Option<Vec<u8>>,
        schema_fingerprint: CommitSchemaFingerprint,
    ) -> Result<Self, InternalError> {
        let (raw_key, _) = decode_data_key(key)?;

        Ok(Self::new(
            entity_path,
            raw_key,
            before,
            after,
            schema_fingerprint,
        ))
    }
}

///
/// CommitMarker
///
/// Persisted mutation plan covering journal publication.
/// Recovery replays the marker exactly as stored.
/// Unknown fields are rejected as corruption inside one marker payload version.
/// Persisted format-version rejection is owned by the marker envelope in `commit::store`.
/// This is internal commit-protocol metadata, not a user-schema type.
///

#[derive(Clone, Debug)]
pub(in crate::db) enum DatabaseControlOp {
    SchemaApplication(SchemaApplicationRecordOp),
    #[cfg(any(test, feature = "migration"))]
    EntitySourceLineage(crate::db::schema::EntitySourceLineageCatalogOp),
    #[cfg(any(test, feature = "migration"))]
    SchemaMigration(crate::db::schema::SchemaMigrationRecordOp),
    MutationProgress(MutationProgressRecordOp),
}

#[derive(Clone, Debug)]
pub(crate) struct CommitMarker {
    pub(crate) id: [u8; COMMIT_ID_BYTES],
    pub(in crate::db) journal_batches: Vec<JournalBatch>,
    pub(in crate::db) database_control: Vec<DatabaseControlOp>,
}

impl CommitMarker {
    /// Construct one marker from already-derived durable payload parts.
    ///
    /// Journal batches are embedded in the marker so recovery can repair or
    /// verify marker-bound journal publication before replay.
    pub(in crate::db) fn from_parts(
        id: [u8; COMMIT_ID_BYTES],
        journal_batches: Vec<JournalBatch>,
    ) -> Result<Self, InternalError> {
        Self::from_parts_with_database_control(id, journal_batches, Vec::new())
    }

    /// Construct one marker that also owns an exact schema-application record
    /// replacement in the database-control region.
    pub(in crate::db) fn from_parts_with_schema_application(
        id: [u8; COMMIT_ID_BYTES],
        journal_batches: Vec<JournalBatch>,
        schema_application: Option<SchemaApplicationRecordOp>,
    ) -> Result<Self, InternalError> {
        let database_control = schema_application
            .map(DatabaseControlOp::SchemaApplication)
            .into_iter()
            .collect();
        Self::from_parts_with_database_control(id, journal_batches, database_control)
    }

    /// Construct one marker that atomically owns one mutation-progress replacement.
    pub(in crate::db) fn from_parts_with_mutation_progress(
        id: [u8; COMMIT_ID_BYTES],
        journal_batches: Vec<JournalBatch>,
        mutation_progress: MutationProgressRecordOp,
    ) -> Result<Self, InternalError> {
        Self::from_parts_with_database_control(
            id,
            journal_batches,
            vec![DatabaseControlOp::MutationProgress(mutation_progress)],
        )
    }

    /// Construct one marker with a bounded canonical database-control
    /// transaction applied atomically beside its journal batches.
    pub(in crate::db) fn from_parts_with_database_control(
        id: [u8; COMMIT_ID_BYTES],
        journal_batches: Vec<JournalBatch>,
        database_control: Vec<DatabaseControlOp>,
    ) -> Result<Self, InternalError> {
        let marker = Self {
            id,
            journal_batches,
            database_control,
        };
        validate_commit_marker_shape(&marker)?;

        Ok(marker)
    }

    /// Borrow marker-bound journal batches embedded in this commit marker.
    #[must_use]
    pub(in crate::db) fn journal_batches(&self) -> &[JournalBatch] {
        &self.journal_batches
    }

    /// Borrow the exact database-wide schema-application record effect.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db) fn schema_application(&self) -> Option<&SchemaApplicationRecordOp> {
        let mut index = 0;
        while index < self.database_control.len() {
            if let DatabaseControlOp::SchemaApplication(operation) = &self.database_control[index] {
                return Some(operation);
            }
            index += 1;
        }
        None
    }

    #[cfg(test)]
    pub(in crate::db) fn entity_source_lineage(
        &self,
    ) -> Option<&crate::db::schema::EntitySourceLineageCatalogOp> {
        self.database_control
            .iter()
            .find_map(|operation| match operation {
                DatabaseControlOp::EntitySourceLineage(operation) => Some(operation),
                DatabaseControlOp::SchemaApplication(_) | DatabaseControlOp::SchemaMigration(_) => {
                    None
                }
                DatabaseControlOp::MutationProgress(_) => None,
            })
    }

    #[must_use]
    pub(in crate::db) const fn database_control(&self) -> &[DatabaseControlOp] {
        self.database_control.as_slice()
    }

    // Build the canonical payload corruption for truncated variable-length fields.
    fn payload_truncated_length(_label: &'static str) -> InternalError {
        InternalError::commit_corruption()
    }

    // Build the canonical payload corruption for truncated byte payloads.
    fn payload_truncated_bytes(_label: &'static str) -> InternalError {
        InternalError::commit_corruption()
    }

    // Build the canonical payload corruption for invalid fixed-size payloads.
    fn payload_invalid_fixed_size(_label: &'static str) -> InternalError {
        InternalError::commit_corruption()
    }
}

const COMMIT_MARKER_ID_BYTES: usize = COMMIT_ID_BYTES;
const COMMIT_MARKER_JOURNAL_BATCH_COUNT_BYTES: usize = 4;
const COMMIT_MARKER_DATABASE_CONTROL_COUNT_BYTES: usize = 1;
const COMMIT_MARKER_DATABASE_CONTROL_TAG_BYTES: usize = 1;
const COMMIT_MARKER_SCHEMA_APPLICATION_KEY_BYTES: usize = 32;
const COMMIT_MARKER_CONTROL_BEFORE_TAG_BYTES: usize = 1;
const COMMIT_MARKER_MUTATION_PROGRESS_KEY_BYTES: usize = 32;
const COMMIT_MARKER_MUTATION_JOB_ID_BYTES: usize = 32;
const COMMIT_MARKER_MUTATION_SEQUENCE_BYTES: usize = 8;
const COMMIT_MARKER_MUTATION_DIGEST_BYTES: usize = 32;

/// Generate one deterministic commit id for marker persistence.
///
/// This id is persisted for marker identity and diagnostics; it is not a source
/// of user-visible randomness or durable commit ordering authority.
pub(in crate::db) fn generate_commit_id() -> Result<[u8; COMMIT_ID_BYTES], InternalError> {
    let sequence = COMMIT_ID_SEQUENCE
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
            current.checked_add(1)
        })
        .map_err(|_| InternalError::commit_id_generation_failed())?;

    let mut id = [0u8; COMMIT_ID_BYTES];
    id[..8].copy_from_slice(&now_millis().to_be_bytes());
    id[8..].copy_from_slice(&sequence.to_be_bytes());

    Ok(id)
}

/// Return the marker-local batch identity for one canonical batch ordinal.
///
/// The first batch retains the established single-store bytes. Every
/// additional batch consumes a fresh identity from the same generator used by
/// commit markers.
pub(in crate::db) fn generate_marker_batch_id(
    marker_id: [u8; COMMIT_ID_BYTES],
    ordinal: usize,
) -> Result<[u8; COMMIT_ID_BYTES], InternalError> {
    if ordinal == 0 {
        Ok(marker_id)
    } else {
        generate_commit_id()
    }
}

/// Encode one commit-marker payload in the canonical binary format.
#[cfg(test)]
pub(in crate::db) fn encode_commit_marker_payload(
    marker: &CommitMarker,
) -> Result<Vec<u8>, InternalError> {
    // Phase 1: size the output once so commit persistence writes one compact frame.
    let capacity = commit_marker_payload_capacity(marker);
    if capacity > u32::MAX as usize {
        return Err(InternalError::commit_marker_payload_exceeds_u32_length_limit());
    }

    // Phase 2: emit one length-delimited frame for deterministic recovery replay.
    let mut encoded = Vec::with_capacity(capacity);
    write_commit_marker_payload(&mut encoded, marker)?;

    Ok(encoded)
}

/// Return the canonical marker payload size without allocating it.
pub(in crate::db) fn commit_marker_payload_capacity(marker: &CommitMarker) -> usize {
    let mut capacity = COMMIT_MARKER_ID_BYTES + COMMIT_MARKER_JOURNAL_BATCH_COUNT_BYTES;
    for batch in &marker.journal_batches {
        capacity = capacity.saturating_add(4 + journal_batch_encoded_len(batch));
    }
    capacity = capacity.saturating_add(COMMIT_MARKER_DATABASE_CONTROL_COUNT_BYTES);
    for operation in marker.database_control() {
        capacity = capacity.saturating_add(COMMIT_MARKER_DATABASE_CONTROL_TAG_BYTES);
        match operation {
            DatabaseControlOp::SchemaApplication(operation) => {
                capacity = capacity
                    .saturating_add(COMMIT_MARKER_SCHEMA_APPLICATION_KEY_BYTES)
                    .saturating_add(encoded_replace_capacity(
                        operation.before_bytes(),
                        operation.after_bytes(),
                    ));
            }
            #[cfg(any(test, feature = "migration"))]
            DatabaseControlOp::EntitySourceLineage(operation) => {
                capacity = capacity.saturating_add(encoded_replace_capacity(
                    operation.before_bytes(),
                    operation.after_bytes(),
                ));
            }
            #[cfg(any(test, feature = "migration"))]
            DatabaseControlOp::SchemaMigration(operation) => {
                capacity = capacity.saturating_add(encoded_replace_capacity(
                    operation.before_bytes(),
                    operation.after_bytes(),
                ));
            }
            DatabaseControlOp::MutationProgress(operation) => {
                capacity = capacity
                    .saturating_add(COMMIT_MARKER_MUTATION_PROGRESS_KEY_BYTES)
                    .saturating_add(COMMIT_MARKER_MUTATION_JOB_ID_BYTES)
                    .saturating_add(COMMIT_MARKER_MUTATION_SEQUENCE_BYTES)
                    .saturating_add(COMMIT_MARKER_MUTATION_DIGEST_BYTES)
                    .saturating_add(size_of::<u32>() + operation.before_bytes().len())
                    .saturating_add(size_of::<u32>() + operation.after_bytes().len());
            }
        }
    }

    capacity
}

const fn encoded_replace_capacity(before: Option<&[u8]>, after: &[u8]) -> usize {
    let mut capacity =
        COMMIT_MARKER_CONTROL_BEFORE_TAG_BYTES.saturating_add(size_of::<u32>() + after.len());
    if let Some(before) = before {
        capacity = capacity.saturating_add(size_of::<u32>() + before.len());
    }
    capacity
}

// Write the canonical marker payload into an existing output buffer.
pub(in crate::db) fn write_commit_marker_payload(
    out: &mut Vec<u8>,
    marker: &CommitMarker,
) -> Result<(), InternalError> {
    out.extend_from_slice(&marker.id);
    write_len_u32(
        out,
        marker.journal_batches.len(),
        "commit marker journal batch count",
    )?;
    for batch in &marker.journal_batches {
        let encoded = encode_journal_batch(batch)?;
        write_len_prefixed_bytes(out, &encoded, "commit marker journal batch")?;
    }
    out.push(
        u8::try_from(marker.database_control().len())
            .map_err(|_| InternalError::commit_corruption())?,
    );
    for operation in marker.database_control() {
        match operation {
            DatabaseControlOp::SchemaApplication(operation) => {
                out.push(1);
                out.extend_from_slice(&operation.key().to_bytes());
                write_replace_bytes(
                    out,
                    operation.before_bytes(),
                    operation.after_bytes(),
                    "commit marker schema application",
                )?;
            }
            #[cfg(any(test, feature = "migration"))]
            DatabaseControlOp::EntitySourceLineage(operation) => {
                out.push(2);
                write_replace_bytes(
                    out,
                    operation.before_bytes(),
                    operation.after_bytes(),
                    "commit marker entity source lineage",
                )?;
            }
            #[cfg(any(test, feature = "migration"))]
            DatabaseControlOp::SchemaMigration(operation) => {
                out.push(3);
                write_replace_bytes(
                    out,
                    operation.before_bytes(),
                    operation.after_bytes(),
                    "commit marker schema migration",
                )?;
            }
            DatabaseControlOp::MutationProgress(operation) => {
                out.push(4);
                out.extend_from_slice(&operation.key());
                out.extend_from_slice(&operation.job_id().to_bytes());
                out.extend_from_slice(&operation.expected_sequence().to_le_bytes());
                out.extend_from_slice(&operation.expected_before_digest());
                write_len_prefixed_bytes(
                    out,
                    operation.before_bytes(),
                    "commit marker mutation progress before",
                )?;
                write_len_prefixed_bytes(
                    out,
                    operation.after_bytes(),
                    "commit marker mutation progress after",
                )?;
            }
        }
    }

    Ok(())
}

/// Decode one commit-marker payload from the canonical binary format.
pub(in crate::db) fn decode_commit_marker_payload(
    bytes: &[u8],
) -> Result<CommitMarker, InternalError> {
    // Phase 1: parse the fixed marker header before touching batch bytes.
    if bytes.len() < COMMIT_MARKER_ID_BYTES + COMMIT_MARKER_JOURNAL_BATCH_COUNT_BYTES {
        return Err(InternalError::commit_corruption());
    }

    let mut cursor = 0;
    let id = read_fixed_array::<COMMIT_MARKER_ID_BYTES>(bytes, &mut cursor, "commit marker id")?;
    let journal_batch_count =
        read_len_u32(bytes, &mut cursor, "commit marker journal batch count")? as usize;
    let mut journal_batches = Vec::new();
    for _ in 0..journal_batch_count {
        journal_batches
            .try_reserve(1)
            .map_err(|_| InternalError::commit_corruption())?;
        let encoded = read_len_prefixed_bytes(bytes, &mut cursor, "commit marker journal batch")?;
        journal_batches.push(decode_journal_batch(encoded)?);
    }
    let database_control_count = usize::from(read_tag_u8(
        bytes,
        &mut cursor,
        "commit marker database control count",
    )?);
    if database_control_count > MAX_DATABASE_CONTROL_OPS_PER_MARKER {
        return Err(InternalError::commit_corruption());
    }
    let mut database_control = Vec::new();
    database_control
        .try_reserve_exact(database_control_count)
        .map_err(|_| InternalError::commit_corruption())?;
    for _ in 0..database_control_count {
        database_control.push(decode_database_control_op(bytes, &mut cursor)?);
    }

    // Phase 3: reject trailing bytes so malformed payloads fail closed.
    if cursor != bytes.len() {
        return Err(InternalError::commit_corruption());
    }

    CommitMarker::from_parts_with_database_control(id, journal_batches, database_control)
        .map_err(|_| InternalError::commit_corruption())
}

fn decode_database_control_op(
    bytes: &[u8],
    cursor: &mut usize,
) -> Result<DatabaseControlOp, InternalError> {
    match read_tag_u8(bytes, cursor, "commit marker database control operation")? {
        1 => {
            let key = ApplicationRecordKey::from_bytes(read_fixed_array::<
                COMMIT_MARKER_SCHEMA_APPLICATION_KEY_BYTES,
            >(
                bytes,
                cursor,
                "commit marker schema application key",
            )?);
            let (before, after) =
                read_replace_bytes(bytes, cursor, "commit marker schema application")?;
            SchemaApplicationRecordOp::from_encoded(key, before, after)
                .map(DatabaseControlOp::SchemaApplication)
                .map_err(|_| InternalError::commit_corruption())
        }
        2 => decode_lineage_control_op(bytes, cursor),
        3 => decode_migration_control_op(bytes, cursor),
        4 => decode_mutation_progress_control_op(bytes, cursor),
        _ => Err(InternalError::commit_corruption()),
    }
}

fn decode_mutation_progress_control_op(
    bytes: &[u8],
    cursor: &mut usize,
) -> Result<DatabaseControlOp, InternalError> {
    let key = read_fixed_array::<COMMIT_MARKER_MUTATION_PROGRESS_KEY_BYTES>(
        bytes,
        cursor,
        "commit marker mutation progress key",
    )?;
    let job_id =
        crate::db::MutationJobId::try_from_bytes(read_fixed_array::<
            COMMIT_MARKER_MUTATION_JOB_ID_BYTES,
        >(
            bytes, cursor, "commit marker mutation job id"
        )?)
        .map_err(|_| InternalError::commit_corruption())?;
    let expected_sequence = u64::from_le_bytes(read_fixed_array::<
        COMMIT_MARKER_MUTATION_SEQUENCE_BYTES,
    >(
        bytes, cursor, "commit marker mutation sequence"
    )?);
    let expected_before_digest = read_fixed_array::<COMMIT_MARKER_MUTATION_DIGEST_BYTES>(
        bytes,
        cursor,
        "commit marker mutation before digest",
    )?;
    let before = read_len_prefixed_bytes(bytes, cursor, "commit marker mutation before")?;
    let after = read_len_prefixed_bytes(bytes, cursor, "commit marker mutation after")?;
    if before.len() > crate::db::MAX_MUTATION_JOB_RECORD_BYTES
        || after.len() > crate::db::MAX_MUTATION_JOB_RECORD_BYTES
    {
        return Err(InternalError::commit_corruption());
    }
    MutationProgressRecordOp::from_encoded(
        key,
        job_id,
        expected_sequence,
        expected_before_digest,
        before.to_vec(),
        after.to_vec(),
    )
    .map(DatabaseControlOp::MutationProgress)
    .map_err(|_| InternalError::commit_corruption())
}

#[cfg(any(test, feature = "migration"))]
fn decode_lineage_control_op(
    bytes: &[u8],
    cursor: &mut usize,
) -> Result<DatabaseControlOp, InternalError> {
    let (before, after) = read_replace_bytes(bytes, cursor, "commit marker entity source lineage")?;
    crate::db::schema::EntitySourceLineageCatalogOp::from_encoded(before, after)
        .map(DatabaseControlOp::EntitySourceLineage)
        .map_err(|_| InternalError::commit_corruption())
}

#[cfg(not(any(test, feature = "migration")))]
fn decode_lineage_control_op(
    _bytes: &[u8],
    _cursor: &mut usize,
) -> Result<DatabaseControlOp, InternalError> {
    Err(InternalError::commit_corruption())
}

#[cfg(any(test, feature = "migration"))]
fn decode_migration_control_op(
    bytes: &[u8],
    cursor: &mut usize,
) -> Result<DatabaseControlOp, InternalError> {
    let (before, after) = read_replace_bytes(bytes, cursor, "commit marker schema migration")?;
    crate::db::schema::SchemaMigrationRecordOp::from_encoded(before, after)
        .map(DatabaseControlOp::SchemaMigration)
        .map_err(|_| InternalError::commit_corruption())
}

#[cfg(not(any(test, feature = "migration")))]
fn decode_migration_control_op(
    _bytes: &[u8],
    _cursor: &mut usize,
) -> Result<DatabaseControlOp, InternalError> {
    Err(InternalError::commit_corruption())
}

fn write_replace_bytes(
    out: &mut Vec<u8>,
    before: Option<&[u8]>,
    after: &[u8],
    label: &'static str,
) -> Result<(), InternalError> {
    match before {
        None => out.push(0),
        Some(before) => {
            out.push(1);
            write_len_prefixed_bytes(out, before, label)?;
        }
    }
    write_len_prefixed_bytes(out, after, label)
}

fn read_replace_bytes(
    bytes: &[u8],
    cursor: &mut usize,
    label: &'static str,
) -> Result<(Option<Vec<u8>>, Vec<u8>), InternalError> {
    let before = match read_tag_u8(bytes, cursor, label)? {
        0 => None,
        1 => Some(read_len_prefixed_bytes(bytes, cursor, label)?.to_vec()),
        _ => return Err(InternalError::commit_corruption()),
    };
    let after = read_len_prefixed_bytes(bytes, cursor, label)?.to_vec();
    Ok((before, after))
}

fn read_tag_u8(
    bytes: &[u8],
    cursor: &mut usize,
    _label: &'static str,
) -> Result<u8, InternalError> {
    let tag = *bytes
        .get(*cursor)
        .ok_or_else(InternalError::commit_corruption)?;
    *cursor = cursor.saturating_add(1);
    Ok(tag)
}

// Write one bounded little-endian u32 length field.
fn write_len_u32(out: &mut Vec<u8>, len: usize, _label: &'static str) -> Result<(), InternalError> {
    let len = u32::try_from(len)
        .map_err(|_| InternalError::commit_marker_payload_exceeds_u32_length_limit())?;
    out.extend_from_slice(&len.to_le_bytes());

    Ok(())
}

// Write one length-delimited byte slice into the marker payload.
fn write_len_prefixed_bytes(
    out: &mut Vec<u8>,
    bytes: &[u8],
    label: &'static str,
) -> Result<(), InternalError> {
    write_len_u32(out, bytes.len(), label)?;
    out.extend_from_slice(bytes);

    Ok(())
}

// Read one little-endian u32 length from the marker payload.
fn read_len_u32(
    bytes: &[u8],
    cursor: &mut usize,
    label: &'static str,
) -> Result<u32, InternalError> {
    let payload = bytes
        .get(*cursor..cursor.saturating_add(4))
        .ok_or_else(|| CommitMarker::payload_truncated_length(label))?;
    *cursor = cursor.saturating_add(4);

    Ok(u32::from_le_bytes([
        payload[0], payload[1], payload[2], payload[3],
    ]))
}

// Read one fixed-size byte array from the marker payload.
fn read_fixed_array<const N: usize>(
    bytes: &[u8],
    cursor: &mut usize,
    label: &'static str,
) -> Result<[u8; N], InternalError> {
    let payload = bytes
        .get(*cursor..cursor.saturating_add(N))
        .ok_or_else(|| CommitMarker::payload_truncated_bytes(label))?;
    *cursor = cursor.saturating_add(N);

    payload
        .try_into()
        .map_err(|_| CommitMarker::payload_invalid_fixed_size(label))
}

// Read one length-delimited byte slice from the marker payload.
fn read_len_prefixed_bytes<'a>(
    bytes: &'a [u8],
    cursor: &mut usize,
    label: &'static str,
) -> Result<&'a [u8], InternalError> {
    let len = read_len_u32(bytes, cursor, label)? as usize;
    let payload = bytes
        .get(*cursor..cursor.saturating_add(len))
        .ok_or_else(|| CommitMarker::payload_truncated_bytes(label))?;
    *cursor = cursor.saturating_add(len);

    Ok(payload)
}

/// Decode a raw data-store key and validate its structural invariants.
pub(in crate::db) fn decode_data_key(
    bytes: &[u8],
) -> Result<(RawDataStoreKey, DecodedDataStoreKey), InternalError> {
    // Commit markers store the current data-key wire bytes length-prefixed.
    // The current data-key format is variable-width, so this gate is a bounded
    // maximum check; structural validation belongs to `DecodedDataStoreKey::try_from_raw`.
    let len = bytes.len();
    let max = RawDataStoreKey::MAX_STORED_SIZE_USIZE;
    if len > max {
        return Err(InternalError::commit_component_length_invalid(len, max));
    }

    let raw = <RawDataStoreKey as Storable>::from_bytes(Cow::Borrowed(bytes));
    let data_key = DecodedDataStoreKey::try_from_raw(&raw)
        .map_err(|_| InternalError::commit_component_corruption())?;

    Ok((raw, data_key))
}

/// Validate commit-marker row-op shape invariants.
///
/// Every row op must represent a concrete mutation:
/// - insert (`before=None`, `after=Some`)
/// - update (`before=Some`, `after=Some`)
/// - delete (`before=Some`, `after=None`)
///
/// The empty shape (`before=None`, `after=None`) is corruption.
pub(crate) fn validate_commit_marker_shape(marker: &CommitMarker) -> Result<(), InternalError> {
    // Validate every embedded journal batch is bound to this marker and has a
    // unique marker-local identity. Journal sequences are tail-local: two
    // stores participating in one commit may legitimately use the same next
    // sequence.
    let mut batch_ids = BTreeSet::new();
    let mut identity_owners = Vec::new();
    for batch in &marker.journal_batches {
        if batch.commit_marker_id() != marker.id {
            return Err(InternalError::commit_corruption());
        }
        if !batch_ids.insert(batch.batch_id()) {
            return Err(InternalError::commit_corruption());
        }
        for record in batch.records() {
            let JournalRecord::IdentityRangeAdvance { range } = record else {
                continue;
            };
            if identity_owners.contains(&range.owner()) {
                return Err(InternalError::commit_corruption());
            }
            identity_owners.push(range.owner());
        }
    }
    if marker.database_control().len() > MAX_DATABASE_CONTROL_OPS_PER_MARKER {
        return Err(InternalError::commit_corruption());
    }
    let mut application_keys = BTreeSet::new();
    let mut prior_application_key = None;
    let mut prior_rank = 0_u8;
    #[cfg(any(test, feature = "migration"))]
    let mut lineage_seen = false;
    #[cfg(any(test, feature = "migration"))]
    let mut migration_seen = false;
    let mut mutation_progress_seen = false;
    for operation in marker.database_control() {
        match operation {
            DatabaseControlOp::SchemaApplication(operation) => {
                let key = operation.key();
                if prior_rank > 1
                    || prior_application_key.is_some_and(|prior| prior >= key)
                    || !application_keys.insert(key)
                {
                    return Err(InternalError::commit_corruption());
                }
                prior_rank = 1;
                prior_application_key = Some(key);
                operation
                    .validate()
                    .map_err(|_| InternalError::commit_corruption())?;
            }
            #[cfg(any(test, feature = "migration"))]
            DatabaseControlOp::EntitySourceLineage(operation) => {
                if prior_rank > 2 || lineage_seen {
                    return Err(InternalError::commit_corruption());
                }
                prior_rank = 2;
                lineage_seen = true;
                operation
                    .validate()
                    .map_err(|_| InternalError::commit_corruption())?;
            }
            #[cfg(any(test, feature = "migration"))]
            DatabaseControlOp::SchemaMigration(operation) => {
                if prior_rank > 3 || migration_seen {
                    return Err(InternalError::commit_corruption());
                }
                prior_rank = 3;
                migration_seen = true;
                operation
                    .validate()
                    .map_err(|_| InternalError::commit_corruption())?;
            }
            DatabaseControlOp::MutationProgress(operation) => {
                if prior_rank > 4 || mutation_progress_seen {
                    return Err(InternalError::commit_corruption());
                }
                prior_rank = 4;
                mutation_progress_seen = true;
                operation
                    .validate()
                    .map_err(|_| InternalError::commit_corruption())?;
            }
        }
    }

    Ok(())
}
