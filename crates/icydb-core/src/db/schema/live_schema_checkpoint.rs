//! Module: db::schema::live_schema_checkpoint
//! Responsibility: durably retain current accepted candidates for live-only schema stores.
//! Does not own: proposal lowering, accepted reconciliation, or runtime schema interpretation.
//! Boundary: marker-owned accepted candidate -> bounded control-memory checkpoint -> recovery.

#[cfg(any(test, feature = "migration"))]
use crate::db::schema::migration_record::SchemaMigrationRecordOp;
use crate::{
    db::{
        commit::{commit_memory_handle, current_commit_memory_allocation},
        database_format::crc32c,
        integrity::DatabaseIncarnationId,
        schema::{
            AcceptedSchemaRevision, CandidateSchemaRevision,
            enum_catalog::{
                ACCEPTED_SCHEMA_ROOT_BYTES, MAX_ACCEPTED_SCHEMA_BUNDLE_BYTES,
                MAX_SCHEMA_STORE_PATH_BYTES,
            },
            identity_state::{
                IDENTITY_STATE_RECORD_BYTES, IdentityAdvanceId, IdentityRangeAdvance,
                IdentityStateInventory, IdentityStateLifecycle,
                MAX_IDENTITY_STATE_RECORDS_PER_DATABASE, decode_identity_state,
                encode_identity_state, identity_kind_maximum, prepare_identity_state_transition,
            },
            migration_record::{SchemaMigrationRecord, decode_schema_migration_record},
            wire::{SchemaWireReader, SchemaWireWriter},
        },
    },
    error::InternalError,
};
use ic_stable_structures::{
    BTreeMap as StableBTreeMap, DefaultMemoryImpl, RestrictedMemory, Storable,
    memory_manager::VirtualMemory, storable::Bound,
};
use sha2::{Digest, Sha256};
use std::{borrow::Cow, cell::Cell};

const MIGRATION_GATE_UNKNOWN: u8 = 0;
const MIGRATION_GATE_READY: u8 = 1;
const MIGRATION_GATE_BLOCKED: u8 = 2;

thread_local! {
    static MIGRATION_GATE_STATE: Cell<u8> = const { Cell::new(MIGRATION_GATE_UNKNOWN) };
}

#[cfg(any(test, feature = "migration"))]
use crate::db::schema::migration_lineage::{
    AcceptedEntitySourceLineageCatalog, EntitySourceLineageCatalogOp,
    decode_entity_source_lineage_catalog,
};

const CHECKPOINT_HEADER_KEY: LiveSchemaCheckpointKey = LiveSchemaCheckpointKey([0; 32]);
const CHECKPOINT_HEADER_MAGIC: &[u8; 8] = b"ICYSLVHD";
const CHECKPOINT_HEADER_VERSION: u8 = 1;
const CHECKPOINT_HEADER_BYTES: usize = 8 + 1 + 4;
const CHECKPOINT_MAGIC: &[u8; 8] = b"ICYSLIVE";
const CHECKPOINT_VERSION: u8 = 1;
const CHECKPOINT_FIXED_BYTES: usize = 8 + 1 + 4 + 4 + 4 + 4 + 4;
const CHECKPOINT_KEY_PROFILE: &[u8] = b"icydb.live-schema-checkpoint.key.v1";
const LINEAGE_KEY_PROFILE: &[u8] = b"icydb.schema-lineage.key.v1";
const MIGRATION_KEY_PROFILE: &[u8] = b"icydb.schema-migration.key.v1";
const MAX_LIVE_SCHEMA_CHECKPOINTS: u64 = icydb_schema::MAX_SCHEMA_ASSIGNMENTS as u64;
const MAX_LIVE_SCHEMA_CHECKPOINT_BYTES: usize = CHECKPOINT_FIXED_BYTES
    + MAX_SCHEMA_STORE_PATH_BYTES
    + MAX_ACCEPTED_SCHEMA_BUNDLE_BYTES
    + ACCEPTED_SCHEMA_ROOT_BYTES
    + MAX_IDENTITY_STATE_RECORDS_PER_DATABASE * IDENTITY_STATE_RECORD_BYTES;
const CHECKPOINT_MEMORY_START_PAGE: u64 = 4_096;
const CHECKPOINT_MEMORY_END_PAGE: u64 = 4_194_304;

type CheckpointMemory = RestrictedMemory<VirtualMemory<DefaultMemoryImpl>>;
type CheckpointWriter = SchemaWireWriter<MAX_LIVE_SCHEMA_CHECKPOINT_BYTES>;
type CheckpointReader<'a> = SchemaWireReader<'a>;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct LiveSchemaCheckpointKey([u8; 32]);

impl LiveSchemaCheckpointKey {
    fn reserved(profile: &[u8]) -> Self {
        Self(Sha256::digest(profile).into())
    }

    fn lineage() -> Self {
        Self::reserved(LINEAGE_KEY_PROFILE)
    }

    fn migration() -> Self {
        Self::reserved(MIGRATION_KEY_PROFILE)
    }

    fn for_store(store_path: &str) -> Result<Self, InternalError> {
        if store_path.is_empty() || store_path.len() > MAX_SCHEMA_STORE_PATH_BYTES {
            return Err(InternalError::store_invariant());
        }
        let path_len =
            u32::try_from(store_path.len()).map_err(|_| InternalError::store_invariant())?;
        let mut hasher = Sha256::new();
        hasher.update(CHECKPOINT_KEY_PROFILE);
        hasher.update(path_len.to_be_bytes());
        hasher.update(store_path.as_bytes());
        let key = Self(hasher.finalize().into());
        if key == CHECKPOINT_HEADER_KEY || key == Self::lineage() || key == Self::migration() {
            return Err(InternalError::store_invariant());
        }
        Ok(key)
    }
}

impl Storable for LiveSchemaCheckpointKey {
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
struct LiveSchemaCheckpointBytes(Vec<u8>);

impl Storable for LiveSchemaCheckpointBytes {
    fn to_bytes(&self) -> Cow<'_, [u8]> {
        Cow::Borrowed(self.0.as_slice())
    }

    fn from_bytes(bytes: Cow<'_, [u8]>) -> Self {
        Self(bytes.into_owned())
    }

    fn into_bytes(self) -> Vec<u8> {
        self.0
    }

    #[expect(
        clippy::cast_possible_truncation,
        reason = "the compile-time checkpoint ceiling is less than 24 MiB"
    )]
    const BOUND: Bound = Bound::Bounded {
        max_size: MAX_LIVE_SCHEMA_CHECKPOINT_BYTES as u32,
        is_fixed_size: false,
    };
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum LiveSchemaCheckpointPreflight {
    Ready,
    AlreadyApplied,
}

pub(in crate::db) struct LiveSchemaCheckpoint {
    candidate: CandidateSchemaRevision,
    identity_states: IdentityStateInventory,
}

impl LiveSchemaCheckpoint {
    #[must_use]
    pub(in crate::db) const fn candidate(&self) -> &CandidateSchemaRevision {
        &self.candidate
    }

    #[must_use]
    pub(in crate::db) const fn identity_states(&self) -> &IdentityStateInventory {
        &self.identity_states
    }
}

struct LiveSchemaCheckpointStore {
    map: StableBTreeMap<LiveSchemaCheckpointKey, LiveSchemaCheckpointBytes, CheckpointMemory>,
}

impl LiveSchemaCheckpointStore {
    fn open(memory: CheckpointMemory) -> Result<Self, InternalError> {
        let mut store = Self {
            map: StableBTreeMap::init(memory),
        };
        if store.map.is_empty() {
            store.map.insert(
                CHECKPOINT_HEADER_KEY,
                LiveSchemaCheckpointBytes(encode_checkpoint_header()),
            );
        } else {
            let header = store
                .map
                .get(&CHECKPOINT_HEADER_KEY)
                .ok_or_else(InternalError::store_corruption)?;
            decode_checkpoint_header(&header.0)?;
            if let Some(record) = store.map.get(&LiveSchemaCheckpointKey::migration()) {
                decode_schema_migration_record(&record.0)?;
            }
            if store.checkpoint_count()? > MAX_LIVE_SCHEMA_CHECKPOINTS {
                return Err(InternalError::store_corruption());
            }
        }
        Ok(store)
    }

    fn load(&self, store_path: &str) -> Result<Option<LiveSchemaCheckpoint>, InternalError> {
        let key = LiveSchemaCheckpointKey::for_store(store_path)?;
        self.map
            .get(&key)
            .map(|bytes| decode_checkpoint(&bytes.0, key))
            .transpose()
    }

    fn preflight(
        &self,
        incarnation: DatabaseIncarnationId,
        store_path: &str,
        expected_revision: AcceptedSchemaRevision,
        candidate: &CandidateSchemaRevision,
    ) -> Result<LiveSchemaCheckpointPreflight, InternalError> {
        let (_, preflight) = self.prepare(incarnation, store_path, expected_revision, candidate)?;
        Ok(preflight)
    }

    fn apply(
        &mut self,
        incarnation: DatabaseIncarnationId,
        store_path: &str,
        expected_revision: AcceptedSchemaRevision,
        candidate: &CandidateSchemaRevision,
    ) -> Result<(), InternalError> {
        let (checkpoint, preflight) =
            self.prepare(incarnation, store_path, expected_revision, candidate)?;
        match preflight {
            LiveSchemaCheckpointPreflight::AlreadyApplied => return Ok(()),
            LiveSchemaCheckpointPreflight::Ready => {}
        }
        let key = LiveSchemaCheckpointKey::for_store(store_path)?;
        self.map.insert(
            key,
            LiveSchemaCheckpointBytes(encode_checkpoint(store_path, &checkpoint)?),
        );
        Ok(())
    }

    fn prepare(
        &self,
        incarnation: DatabaseIncarnationId,
        store_path: &str,
        expected_revision: AcceptedSchemaRevision,
        candidate: &CandidateSchemaRevision,
    ) -> Result<(LiveSchemaCheckpoint, LiveSchemaCheckpointPreflight), InternalError> {
        validate_candidate_transition(store_path, expected_revision, candidate)?;
        let current = self.load(store_path)?;
        let candidate_is_current = current.as_ref().is_some_and(|checkpoint| {
            checkpoint.candidate().encoded_bundle() == candidate.encoded_bundle()
                && checkpoint.candidate().encoded_root() == candidate.encoded_root()
        });
        let current_revision = current
            .as_ref()
            .map(|checkpoint| checkpoint.candidate().revision());
        if !candidate_is_current
            && current_revision
                != (expected_revision != AcceptedSchemaRevision::NONE).then_some(expected_revision)
        {
            return Err(InternalError::store_corruption());
        }
        if current.is_none() && self.checkpoint_count()? >= MAX_LIVE_SCHEMA_CHECKPOINTS {
            return Err(InternalError::store_invariant());
        }
        let current_bundle = current
            .as_ref()
            .map(|checkpoint| checkpoint.candidate().bundle());
        let current_inventory = current
            .as_ref()
            .map(|checkpoint| checkpoint.identity_states().clone())
            .unwrap_or_default();
        let identity_states = prepare_identity_state_transition(
            incarnation,
            current_bundle,
            candidate.bundle(),
            current_inventory,
        )?
        .into_projected_inventory();
        let checkpoint = LiveSchemaCheckpoint {
            candidate: candidate.clone(),
            identity_states,
        };
        let preflight = if candidate_is_current
            && current
                .as_ref()
                .is_some_and(|current| current.identity_states() == checkpoint.identity_states())
        {
            LiveSchemaCheckpointPreflight::AlreadyApplied
        } else {
            LiveSchemaCheckpointPreflight::Ready
        };
        Ok((checkpoint, preflight))
    }

    fn checkpoint_count(&self) -> Result<u64, InternalError> {
        let non_header = self
            .map
            .len()
            .checked_sub(1)
            .ok_or_else(InternalError::store_corruption)?;
        non_header
            .checked_sub(u64::from(
                self.map.get(&LiveSchemaCheckpointKey::lineage()).is_some(),
            ))
            .and_then(|count| {
                count.checked_sub(u64::from(
                    self.map
                        .get(&LiveSchemaCheckpointKey::migration())
                        .is_some(),
                ))
            })
            .ok_or_else(InternalError::store_corruption)
    }

    fn load_migration(&self) -> Result<Option<SchemaMigrationRecord>, InternalError> {
        self.map
            .get(&LiveSchemaCheckpointKey::migration())
            .map(|bytes| decode_schema_migration_record(&bytes.0))
            .transpose()
    }

    #[cfg(any(test, feature = "migration"))]
    fn preflight_migration(
        &self,
        operation: &SchemaMigrationRecordOp,
    ) -> Result<LiveSchemaCheckpointPreflight, InternalError> {
        operation.validate()?;
        let current = self
            .map
            .get(&LiveSchemaCheckpointKey::migration())
            .map(|bytes| bytes.0);
        if current.as_deref() == Some(operation.after_bytes()) {
            return Ok(LiveSchemaCheckpointPreflight::AlreadyApplied);
        }
        if current.as_deref() != operation.before_bytes() {
            return Err(InternalError::schema_migration(
                icydb_diagnostic_code::SchemaMigrationCode::PublicationRaceLost,
            ));
        }
        Ok(LiveSchemaCheckpointPreflight::Ready)
    }

    #[cfg(any(test, feature = "migration"))]
    fn apply_migration(
        &mut self,
        operation: &SchemaMigrationRecordOp,
    ) -> Result<(), InternalError> {
        match self.preflight_migration(operation)? {
            LiveSchemaCheckpointPreflight::AlreadyApplied => return Ok(()),
            LiveSchemaCheckpointPreflight::Ready => {}
        }
        self.map.insert(
            LiveSchemaCheckpointKey::migration(),
            LiveSchemaCheckpointBytes(operation.after_bytes().to_vec()),
        );
        Ok(())
    }

    #[cfg(any(test, feature = "migration"))]
    fn load_lineage(&self) -> Result<Option<AcceptedEntitySourceLineageCatalog>, InternalError> {
        self.map
            .get(&LiveSchemaCheckpointKey::lineage())
            .map(|bytes| decode_entity_source_lineage_catalog(&bytes.0))
            .transpose()
    }

    #[cfg(any(test, feature = "migration"))]
    fn preflight_lineage(
        &self,
        operation: &EntitySourceLineageCatalogOp,
    ) -> Result<LiveSchemaCheckpointPreflight, InternalError> {
        operation.validate()?;
        let current = self
            .map
            .get(&LiveSchemaCheckpointKey::lineage())
            .map(|bytes| bytes.0);
        if current.as_deref() == Some(operation.after_bytes()) {
            return Ok(LiveSchemaCheckpointPreflight::AlreadyApplied);
        }
        if current.as_deref() != operation.before_bytes() {
            return Err(InternalError::schema_application_conflict());
        }
        Ok(LiveSchemaCheckpointPreflight::Ready)
    }

    #[cfg(any(test, feature = "migration"))]
    fn apply_lineage(
        &mut self,
        operation: &EntitySourceLineageCatalogOp,
    ) -> Result<(), InternalError> {
        match self.preflight_lineage(operation)? {
            LiveSchemaCheckpointPreflight::AlreadyApplied => return Ok(()),
            LiveSchemaCheckpointPreflight::Ready => {}
        }
        self.map.insert(
            LiveSchemaCheckpointKey::lineage(),
            LiveSchemaCheckpointBytes(operation.after_bytes().to_vec()),
        );
        Ok(())
    }

    fn preflight_identity_range(
        &self,
        store_path: &str,
        range: IdentityRangeAdvance,
    ) -> Result<(), InternalError> {
        let checkpoint = self
            .load(store_path)?
            .ok_or_else(InternalError::identity_state_corruption)?;
        let state = checkpoint
            .identity_states()
            .get(&(range.owner().entity_tag(), range.owner().field_id()))
            .ok_or_else(InternalError::identity_state_corruption)?;
        if state.owner() != range.owner()
            || state.lifecycle() != IdentityStateLifecycle::Active
            || range.new_high_water()
                > identity_kind_maximum(state.accepted_kind())
                    .ok_or_else(InternalError::identity_state_corruption)?
        {
            return Err(InternalError::identity_state_corruption());
        }
        if state.materialized_high_water() != range.expected_high_water() {
            return Err(InternalError::identity_state_conflict());
        }
        Ok(())
    }

    fn apply_identity_range(
        &mut self,
        store_path: &str,
        range: IdentityRangeAdvance,
        advance_id: IdentityAdvanceId,
    ) -> Result<(), InternalError> {
        let mut checkpoint = self
            .load(store_path)?
            .ok_or_else(InternalError::identity_state_corruption)?;
        let key = (range.owner().entity_tag(), range.owner().field_id());
        let state = checkpoint
            .identity_states
            .get(&key)
            .ok_or_else(InternalError::identity_state_corruption)?
            .apply_range_advance(range, advance_id)?;
        checkpoint.identity_states.insert(key, state);
        let checkpoint_key = LiveSchemaCheckpointKey::for_store(store_path)?;
        self.map.insert(
            checkpoint_key,
            LiveSchemaCheckpointBytes(encode_checkpoint(store_path, &checkpoint)?),
        );
        Ok(())
    }
}

fn validate_candidate_transition(
    store_path: &str,
    expected_revision: AcceptedSchemaRevision,
    candidate: &CandidateSchemaRevision,
) -> Result<(), InternalError> {
    if candidate.store_path() != store_path
        || expected_revision.checked_next() != Some(candidate.revision())
    {
        return Err(InternalError::store_invariant());
    }
    Ok(())
}

fn encode_checkpoint(
    store_path: &str,
    checkpoint: &LiveSchemaCheckpoint,
) -> Result<Vec<u8>, InternalError> {
    let candidate = checkpoint.candidate();
    if candidate.store_path() != store_path
        || store_path.is_empty()
        || store_path.len() > MAX_SCHEMA_STORE_PATH_BYTES
        || checkpoint.identity_states().len() > MAX_IDENTITY_STATE_RECORDS_PER_DATABASE
    {
        return Err(InternalError::store_invariant());
    }
    let mut writer = CheckpointWriter::new();
    writer.push_bytes(CHECKPOINT_MAGIC);
    writer.push_u8(CHECKPOINT_VERSION);
    writer.push_len_prefixed_bytes(store_path.as_bytes())?;
    writer.push_len_prefixed_bytes(candidate.encoded_bundle())?;
    writer.push_len_prefixed_bytes(candidate.encoded_root())?;
    writer.push_u32(
        u32::try_from(checkpoint.identity_states().len())
            .map_err(|_| InternalError::store_invariant())?,
    );
    for (key, state) in checkpoint.identity_states() {
        if *key != (state.owner().entity_tag(), state.owner().field_id()) {
            return Err(InternalError::store_invariant());
        }
        writer.push_bytes(&encode_identity_state(state)?);
    }
    let mut encoded = writer.finish()?;
    if encoded.len() > MAX_LIVE_SCHEMA_CHECKPOINT_BYTES.saturating_sub(size_of::<u32>()) {
        return Err(InternalError::store_invariant());
    }
    encoded.extend_from_slice(&crc32c(&encoded).to_be_bytes());
    Ok(encoded)
}

fn decode_checkpoint(
    bytes: &[u8],
    expected_key: LiveSchemaCheckpointKey,
) -> Result<LiveSchemaCheckpoint, InternalError> {
    if bytes.len() < CHECKPOINT_FIXED_BYTES || bytes.len() > MAX_LIVE_SCHEMA_CHECKPOINT_BYTES {
        return Err(InternalError::store_corruption());
    }
    let checksum_offset = bytes
        .len()
        .checked_sub(size_of::<u32>())
        .ok_or_else(InternalError::store_corruption)?;
    let (body, checksum_bytes) = bytes.split_at(checksum_offset);
    let expected_checksum = u32::from_be_bytes(
        checksum_bytes
            .try_into()
            .map_err(|_| InternalError::store_corruption())?,
    );
    if crc32c(body) != expected_checksum {
        return Err(InternalError::store_corruption());
    }
    let mut reader = CheckpointReader::new(body);
    if reader.read_array::<8>()? != *CHECKPOINT_MAGIC || reader.read_u8()? != CHECKPOINT_VERSION {
        return Err(InternalError::store_corruption());
    }
    let store_path_bytes = reader.read_len_prefixed_bytes()?;
    if store_path_bytes.is_empty() || store_path_bytes.len() > MAX_SCHEMA_STORE_PATH_BYTES {
        return Err(InternalError::store_corruption());
    }
    let store_path =
        std::str::from_utf8(store_path_bytes).map_err(|_| InternalError::store_corruption())?;
    if LiveSchemaCheckpointKey::for_store(store_path)
        .map_err(|_| InternalError::store_corruption())?
        != expected_key
    {
        return Err(InternalError::store_corruption());
    }
    let bundle_bytes = reader.read_len_prefixed_bytes()?;
    if bundle_bytes.len() > MAX_ACCEPTED_SCHEMA_BUNDLE_BYTES {
        return Err(InternalError::store_corruption());
    }
    let root_bytes = reader.read_len_prefixed_bytes()?;
    if root_bytes.len() != ACCEPTED_SCHEMA_ROOT_BYTES {
        return Err(InternalError::store_corruption());
    }
    let identity_state_count =
        usize::try_from(reader.read_u32()?).map_err(|_| InternalError::store_corruption())?;
    if identity_state_count > MAX_IDENTITY_STATE_RECORDS_PER_DATABASE {
        return Err(InternalError::store_corruption());
    }
    let mut identity_states = IdentityStateInventory::new();
    let mut prior_key = None;
    for _ in 0..identity_state_count {
        let encoded_state = reader.read_array::<IDENTITY_STATE_RECORD_BYTES>()?;
        let state = decode_identity_state(&encoded_state)?;
        let key = (state.owner().entity_tag(), state.owner().field_id());
        if prior_key.is_some_and(|prior| prior >= key)
            || identity_states.insert(key, state).is_some()
        {
            return Err(InternalError::identity_state_corruption());
        }
        prior_key = Some(key);
    }
    reader.finish()?;
    let candidate =
        CandidateSchemaRevision::from_encoded(bundle_bytes.to_vec(), root_bytes.to_vec())?;
    let checkpoint = LiveSchemaCheckpoint {
        candidate,
        identity_states,
    };
    if checkpoint.candidate().store_path() != store_path
        || encode_checkpoint(store_path, &checkpoint)? != bytes
    {
        return Err(InternalError::store_corruption());
    }
    Ok(checkpoint)
}

fn encode_checkpoint_header() -> Vec<u8> {
    let mut encoded = Vec::with_capacity(CHECKPOINT_HEADER_BYTES);
    encoded.extend_from_slice(CHECKPOINT_HEADER_MAGIC);
    encoded.push(CHECKPOINT_HEADER_VERSION);
    encoded.extend_from_slice(&crc32c(&encoded).to_be_bytes());
    encoded
}

fn decode_checkpoint_header(bytes: &[u8]) -> Result<(), InternalError> {
    if bytes.len() != CHECKPOINT_HEADER_BYTES
        || &bytes[..8] != CHECKPOINT_HEADER_MAGIC
        || bytes[8] != CHECKPOINT_HEADER_VERSION
        || crc32c(&bytes[..9])
            != u32::from_be_bytes(
                bytes[9..13]
                    .try_into()
                    .map_err(|_| InternalError::store_corruption())?,
            )
    {
        return Err(InternalError::store_corruption());
    }
    Ok(())
}

fn checkpoint_memory() -> Result<CheckpointMemory, InternalError> {
    let memory = commit_memory_handle(current_commit_memory_allocation()?)?;
    Ok(RestrictedMemory::new(
        memory,
        CHECKPOINT_MEMORY_START_PAGE..CHECKPOINT_MEMORY_END_PAGE,
    ))
}

pub(in crate::db) fn load_live_schema_checkpoint(
    store_path: &str,
) -> Result<Option<LiveSchemaCheckpoint>, InternalError> {
    LiveSchemaCheckpointStore::open(checkpoint_memory()?)?.load(store_path)
}

pub(in crate::db) fn preflight_live_schema_checkpoint(
    incarnation: DatabaseIncarnationId,
    store_path: &str,
    expected_revision: AcceptedSchemaRevision,
    candidate: &CandidateSchemaRevision,
) -> Result<LiveSchemaCheckpointPreflight, InternalError> {
    LiveSchemaCheckpointStore::open(checkpoint_memory()?)?.preflight(
        incarnation,
        store_path,
        expected_revision,
        candidate,
    )
}

pub(in crate::db) fn apply_live_schema_checkpoint(
    incarnation: DatabaseIncarnationId,
    store_path: &str,
    expected_revision: AcceptedSchemaRevision,
    candidate: &CandidateSchemaRevision,
) -> Result<(), InternalError> {
    LiveSchemaCheckpointStore::open(checkpoint_memory()?)?.apply(
        incarnation,
        store_path,
        expected_revision,
        candidate,
    )
}

pub(in crate::db) fn verify_live_schema_checkpoint(
    store_path: &str,
    candidate: &CandidateSchemaRevision,
) -> Result<(), InternalError> {
    let current = load_live_schema_checkpoint(store_path)?
        .ok_or_else(InternalError::recovery_effect_verification_failed)?;
    if current.candidate().encoded_bundle() != candidate.encoded_bundle()
        || current.candidate().encoded_root() != candidate.encoded_root()
    {
        return Err(InternalError::recovery_effect_verification_failed());
    }
    Ok(())
}

pub(in crate::db) fn preflight_live_identity_range_checkpoint(
    store_path: &str,
    range: IdentityRangeAdvance,
) -> Result<(), InternalError> {
    LiveSchemaCheckpointStore::open(checkpoint_memory()?)?
        .preflight_identity_range(store_path, range)
}

pub(in crate::db) fn apply_live_identity_range_checkpoint(
    store_path: &str,
    range: IdentityRangeAdvance,
    advance_id: IdentityAdvanceId,
) -> Result<(), InternalError> {
    LiveSchemaCheckpointStore::open(checkpoint_memory()?)?
        .apply_identity_range(store_path, range, advance_id)
}

pub(in crate::db) fn verify_live_identity_range_checkpoint(
    store_path: &str,
    range: IdentityRangeAdvance,
    advance_id: IdentityAdvanceId,
) -> Result<(), InternalError> {
    let checkpoint = load_live_schema_checkpoint(store_path)?
        .ok_or_else(InternalError::recovery_effect_verification_failed)?;
    let state = checkpoint
        .identity_states()
        .get(&(range.owner().entity_tag(), range.owner().field_id()))
        .ok_or_else(InternalError::recovery_effect_verification_failed)?;
    if state.materialized_high_water() != range.new_high_water()
        || state.last_applied_advance() != Some(advance_id)
    {
        return Err(InternalError::recovery_effect_verification_failed());
    }
    Ok(())
}

pub(in crate::db) fn load_schema_migration_record()
-> Result<Option<SchemaMigrationRecord>, InternalError> {
    LiveSchemaCheckpointStore::open(checkpoint_memory()?)?.load_migration()
}

/// Reject ordinary database work while one durable offline migration owns the
/// database-wide gate. A Wasm without the migration capability cannot safely
/// interpret any nonterminal record, including `Prepared`.
pub(in crate::db) fn ensure_schema_migration_ready_for_ordinary_operations()
-> Result<(), InternalError> {
    match MIGRATION_GATE_STATE.with(Cell::get) {
        MIGRATION_GATE_READY => return Ok(()),
        MIGRATION_GATE_BLOCKED => {
            return Err(InternalError::schema_migration(
                icydb_diagnostic_code::SchemaMigrationCode::MigrationInProgress,
            ));
        }
        MIGRATION_GATE_UNKNOWN => {}
        _ => return Err(InternalError::store_invariant()),
    }
    let Some(record) = load_schema_migration_record()? else {
        MIGRATION_GATE_STATE.with(|state| state.set(MIGRATION_GATE_READY));
        return Ok(());
    };
    if schema_migration_record_blocks_ordinary_operations(&record, cfg!(feature = "migration")) {
        MIGRATION_GATE_STATE.with(|state| state.set(MIGRATION_GATE_BLOCKED));
        return Err(InternalError::schema_migration(
            icydb_diagnostic_code::SchemaMigrationCode::MigrationInProgress,
        ));
    }
    MIGRATION_GATE_STATE.with(|state| state.set(MIGRATION_GATE_READY));
    Ok(())
}

const fn schema_migration_record_blocks_ordinary_operations(
    record: &SchemaMigrationRecord,
    migration_capability_compiled: bool,
) -> bool {
    !record.phase().terminal()
        && (!migration_capability_compiled || record.phase().blocks_ordinary_operations())
}

#[cfg(any(test, feature = "migration"))]
pub(in crate::db) fn preflight_schema_migration_record_op(
    operation: &SchemaMigrationRecordOp,
) -> Result<LiveSchemaCheckpointPreflight, InternalError> {
    LiveSchemaCheckpointStore::open(checkpoint_memory()?)?.preflight_migration(operation)
}

#[cfg(any(test, feature = "migration"))]
pub(in crate::db) fn apply_schema_migration_record_op(
    operation: &SchemaMigrationRecordOp,
) -> Result<(), InternalError> {
    LiveSchemaCheckpointStore::open(checkpoint_memory()?)?.apply_migration(operation)?;
    let after = decode_schema_migration_record(operation.after_bytes())?;
    let state = if schema_migration_record_blocks_ordinary_operations(
        &after,
        cfg!(feature = "migration"),
    ) {
        MIGRATION_GATE_BLOCKED
    } else {
        MIGRATION_GATE_READY
    };
    MIGRATION_GATE_STATE.with(|gate| gate.set(state));
    Ok(())
}

#[cfg(any(test, feature = "migration"))]
pub(in crate::db) fn verify_schema_migration_record_op(
    operation: &SchemaMigrationRecordOp,
) -> Result<(), InternalError> {
    let current = LiveSchemaCheckpointStore::open(checkpoint_memory()?)?
        .map
        .get(&LiveSchemaCheckpointKey::migration())
        .ok_or_else(InternalError::recovery_effect_verification_failed)?;
    if current.0 != operation.after_bytes() {
        return Err(InternalError::recovery_effect_verification_failed());
    }
    operation.validate()
}

#[cfg(any(test, feature = "migration"))]
pub(in crate::db::schema) fn load_entity_source_lineage_catalog()
-> Result<Option<AcceptedEntitySourceLineageCatalog>, InternalError> {
    LiveSchemaCheckpointStore::open(checkpoint_memory()?)?.load_lineage()
}

#[cfg(test)]
pub(in crate::db) fn entity_source_lineage_matches_for_tests(
    operation: &EntitySourceLineageCatalogOp,
) -> Result<bool, InternalError> {
    let Some(catalog) = load_entity_source_lineage_catalog()? else {
        return Ok(false);
    };
    Ok(
        crate::db::schema::migration_lineage::encode_entity_source_lineage_catalog(&catalog)?
            == operation.after_bytes(),
    )
}

#[cfg(test)]
pub(in crate::db) fn schema_migration_record_matches_for_tests(
    operation: &SchemaMigrationRecordOp,
) -> Result<bool, InternalError> {
    let Some(record) = load_schema_migration_record()? else {
        return Ok(false);
    };
    Ok(
        crate::db::schema::migration_record::encode_schema_migration_record(&record)?
            == operation.after_bytes(),
    )
}

#[cfg(any(test, feature = "migration"))]
pub(in crate::db) fn preflight_entity_source_lineage_catalog_op(
    operation: &EntitySourceLineageCatalogOp,
) -> Result<LiveSchemaCheckpointPreflight, InternalError> {
    LiveSchemaCheckpointStore::open(checkpoint_memory()?)?.preflight_lineage(operation)
}

#[cfg(any(test, feature = "migration"))]
pub(in crate::db) fn apply_entity_source_lineage_catalog_op(
    operation: &EntitySourceLineageCatalogOp,
) -> Result<(), InternalError> {
    LiveSchemaCheckpointStore::open(checkpoint_memory()?)?.apply_lineage(operation)
}

#[cfg(any(test, feature = "migration"))]
pub(in crate::db) fn verify_entity_source_lineage_catalog_op(
    operation: &EntitySourceLineageCatalogOp,
) -> Result<(), InternalError> {
    let current = LiveSchemaCheckpointStore::open(checkpoint_memory()?)?
        .map
        .get(&LiveSchemaCheckpointKey::lineage())
        .ok_or_else(InternalError::recovery_effect_verification_failed)?;
    if current.0 != operation.after_bytes() {
        return Err(InternalError::recovery_effect_verification_failed());
    }
    operation.validate()
}

#[cfg(test)]
mod tests {
    use super::{
        IdentityStateInventory, LiveSchemaCheckpointKey, LiveSchemaCheckpointPreflight,
        LiveSchemaCheckpointStore, decode_checkpoint, encode_checkpoint,
        schema_migration_record_blocks_ordinary_operations,
    };
    use crate::{
        db::{
            integrity::DatabaseIncarnationId,
            schema::{
                AcceptedSchemaRevision, empty_accepted_schema_candidate_for_tests,
                live_schema_checkpoint::LiveSchemaCheckpoint,
                migration_lineage::{
                    AcceptedEntitySourceLineage, AcceptedEntitySourceLineageCatalog,
                    EntitySourceLineageCatalogOp,
                },
                migration_record::{
                    PersistedSchemaMigrationEntity, PersistedSchemaMigrationPhase,
                    PersistedSchemaMigrationProgress, PersistedSchemaMigrationTransition,
                    SchemaMigrationRecord, SchemaMigrationRecordOp,
                },
            },
        },
        testing::test_memory,
    };
    use ic_stable_structures::RestrictedMemory;
    use icydb_schema::{
        EntitySourceDigest, EntitySourceKey, ExpectedAcceptedHead, ExpectedSchemaFingerprint,
        SchemaMigrationPlanDigest, SchemaProposalDigest, TargetDatabaseIdentity,
        TargetStoreIdentity,
    };

    fn prepared_migration_record() -> SchemaMigrationRecord {
        SchemaMigrationRecord::prepared(
            TargetDatabaseIdentity::from_bytes([1; 32]),
            ExpectedAcceptedHead::Exact {
                revision: 1,
                fingerprint: ExpectedSchemaFingerprint::from_bytes([2; 32]),
            },
            ExpectedAcceptedHead::Exact {
                revision: 2,
                fingerprint: ExpectedSchemaFingerprint::from_bytes([3; 32]),
            },
            SchemaProposalDigest::from_bytes([4; 32]),
            SchemaMigrationPlanDigest::from_bytes([5; 32]),
            vec![
                PersistedSchemaMigrationTransition::try_new(
                    EntitySourceKey::try_new("User").expect("source key should admit"),
                    1,
                    2,
                )
                .expect("transition should admit"),
            ],
            vec![
                PersistedSchemaMigrationEntity::try_new(
                    TargetStoreIdentity::from_bytes([6; 32]),
                    crate::types::EntityTag::new(7),
                    EntitySourceDigest::from_bytes([8; 32]),
                )
                .expect("entity should admit"),
            ],
            Vec::new(),
        )
        .expect("prepared migration should admit")
    }

    #[test]
    fn checkpoint_codec_is_canonical_and_checksum_bound() {
        let candidate =
            empty_accepted_schema_candidate_for_tests("test::Live", AcceptedSchemaRevision::new(1));
        let checkpoint = LiveSchemaCheckpoint {
            candidate: candidate.clone(),
            identity_states: IdentityStateInventory::default(),
        };
        let encoded = encode_checkpoint("test::Live", &checkpoint)
            .expect("accepted candidate should checkpoint");
        let key =
            LiveSchemaCheckpointKey::for_store("test::Live").expect("checkpoint key should derive");
        let decoded = decode_checkpoint(&encoded, key).expect("checkpoint should decode");
        assert_eq!(
            decoded.candidate().encoded_bundle(),
            candidate.encoded_bundle()
        );
        assert_eq!(decoded.candidate().encoded_root(), candidate.encoded_root());

        let mut corrupted = encoded;
        let last = corrupted
            .last_mut()
            .expect("checkpoint should include a checksum");
        *last ^= 0x80;
        assert!(decode_checkpoint(&corrupted, key).is_err());
    }

    #[test]
    fn checkpoint_compare_and_replace_is_idempotent_and_reopen_safe() {
        let first =
            empty_accepted_schema_candidate_for_tests("test::Live", AcceptedSchemaRevision::new(1));
        let second =
            empty_accepted_schema_candidate_for_tests("test::Live", AcceptedSchemaRevision::new(2));
        let memory = RestrictedMemory::new(test_memory(244), 0..4_096);
        let mut store =
            LiveSchemaCheckpointStore::open(memory.clone()).expect("store should initialize");
        let incarnation = DatabaseIncarnationId::for_tests(0x63);

        assert_eq!(
            store
                .preflight(
                    incarnation,
                    "test::Live",
                    AcceptedSchemaRevision::NONE,
                    &first,
                )
                .expect("initial checkpoint should preflight"),
            LiveSchemaCheckpointPreflight::Ready,
        );
        store
            .apply(
                incarnation,
                "test::Live",
                AcceptedSchemaRevision::NONE,
                &first,
            )
            .expect("initial checkpoint should apply");
        store
            .apply(
                incarnation,
                "test::Live",
                AcceptedSchemaRevision::NONE,
                &first,
            )
            .expect("initial checkpoint replay should be idempotent");
        store
            .apply(
                incarnation,
                "test::Live",
                AcceptedSchemaRevision::new(1),
                &second,
            )
            .expect("next checkpoint should apply");

        let reopened =
            LiveSchemaCheckpointStore::open(memory).expect("checkpoint store should reopen");
        let loaded = reopened
            .load("test::Live")
            .expect("checkpoint should remain readable")
            .expect("checkpoint should exist");
        assert_eq!(loaded.candidate().encoded_bundle(), second.encoded_bundle());
        assert_eq!(loaded.candidate().encoded_root(), second.encoded_root());
    }

    #[test]
    fn lineage_uses_one_reserved_record_without_changing_checkpoint_count() {
        let memory = RestrictedMemory::new(test_memory(245), 0..4_096);
        let mut store =
            LiveSchemaCheckpointStore::open(memory.clone()).expect("store should initialize");
        let mut lineage = AcceptedEntitySourceLineageCatalog::default();
        lineage
            .insert(
                TargetStoreIdentity::from_bytes([4; 32]),
                crate::types::EntityTag::new(9),
                AcceptedEntitySourceLineage::unadopted(ExpectedAcceptedHead::Exact {
                    revision: 3,
                    fingerprint: ExpectedSchemaFingerprint::from_bytes([3; 32]),
                })
                .expect("lineage should admit"),
            )
            .expect("lineage entry should insert");

        let operation = EntitySourceLineageCatalogOp::replace(None, &lineage)
            .expect("lineage operation should prepare");
        store
            .apply_lineage(&operation)
            .expect("lineage should persist");
        assert_eq!(store.checkpoint_count().expect("count should close"), 0);
        assert_eq!(
            store.load_lineage().expect("lineage should load"),
            Some(lineage.clone()),
        );
        assert_eq!(
            store
                .preflight_lineage(&operation)
                .expect("replay should preflight"),
            LiveSchemaCheckpointPreflight::AlreadyApplied,
        );

        let reopened = LiveSchemaCheckpointStore::open(memory).expect("store should reopen");
        assert_eq!(
            reopened.load_lineage().expect("lineage should load"),
            Some(lineage),
        );
        assert_ne!(
            LiveSchemaCheckpointKey::lineage(),
            LiveSchemaCheckpointKey::migration()
        );
    }

    #[test]
    fn migration_uses_one_reserved_record_and_exact_compare_replace() {
        let memory = RestrictedMemory::new(test_memory(246), 0..4_096);
        let mut store =
            LiveSchemaCheckpointStore::open(memory.clone()).expect("store should initialize");
        let prepared = prepared_migration_record();
        let validating = prepared
            .transition(
                PersistedSchemaMigrationPhase::Validating,
                PersistedSchemaMigrationProgress::default(),
            )
            .expect("validation should start");
        let insert = SchemaMigrationRecordOp::insert(&prepared).expect("insert should prepare");
        let replace = SchemaMigrationRecordOp::replace(&prepared, &validating)
            .expect("replace should prepare");

        store.apply_migration(&insert).expect("insert should apply");
        assert_eq!(store.checkpoint_count().expect("count should close"), 0);
        assert_eq!(
            store
                .preflight_migration(&insert)
                .expect("replay should preflight"),
            LiveSchemaCheckpointPreflight::AlreadyApplied,
        );
        store
            .apply_migration(&replace)
            .expect("replacement should apply");
        assert_eq!(
            store.load_migration().expect("migration should load"),
            Some(validating.clone()),
        );

        let reopened = LiveSchemaCheckpointStore::open(memory).expect("store should reopen");
        assert_eq!(
            reopened.load_migration().expect("migration should load"),
            Some(validating),
        );
    }

    #[test]
    fn migration_gate_is_phase_and_capability_exact() {
        let prepared = prepared_migration_record();
        assert!(!schema_migration_record_blocks_ordinary_operations(
            &prepared, true,
        ));
        assert!(schema_migration_record_blocks_ordinary_operations(
            &prepared, false,
        ));
        let validating = prepared
            .transition(
                PersistedSchemaMigrationPhase::Validating,
                PersistedSchemaMigrationProgress::default(),
            )
            .expect("validation should start");
        assert!(schema_migration_record_blocks_ordinary_operations(
            &validating,
            true,
        ));
        let aborted = validating
            .transition(
                PersistedSchemaMigrationPhase::Aborted,
                PersistedSchemaMigrationProgress::default(),
            )
            .expect("pre-rewrite abort should admit");
        assert!(!schema_migration_record_blocks_ordinary_operations(
            &aborted, false,
        ));
    }
}
