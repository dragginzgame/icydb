//! Module: db::schema::live_schema_checkpoint
//! Responsibility: durably retain current accepted candidates for live-only schema stores.
//! Does not own: proposal lowering, accepted reconciliation, or runtime schema interpretation.
//! Boundary: marker-owned accepted candidate -> bounded control-memory checkpoint -> recovery.

// Test counters stay at the checkpoint read boundary; counter state and helpers live in helper_tests.

#[cfg(test)]
mod codec_tests;

use crate::db::codec::BoundedByteWriter;
#[cfg(any(test, feature = "migration"))]
use crate::db::schema::migration_record::SchemaMigrationRecordOp;
use crate::{
    db::{
        database_format::{crc32c, verify_crc32c_trailer},
        integrity::DatabaseIncarnationId,
        resource::construction::ConstructionBudget,
        schema::{
            AcceptedSchemaRevision, CandidateSchemaRevision,
            control_store::{
                ControlMemory, ControlRecordFamily, SchemaControlBytes, SchemaControlMap,
                control_memory,
            },
            enum_catalog::{
                ACCEPTED_SCHEMA_ROOT_BYTES, AcceptedSchemaRoot, MAX_ACCEPTED_SCHEMA_BUNDLE_BYTES,
                MAX_SCHEMA_STORE_PATH_BYTES, decode_accepted_schema_root,
                encode_accepted_schema_root,
            },
            identity_state::{
                IDENTITY_STATE_RECORD_BYTES, IdentityAdvanceId, IdentityRangeAdvance,
                IdentityStateInventory, MAX_IDENTITY_STATE_RECORDS_PER_DATABASE,
                decode_identity_state, encode_identity_state, prepare_identity_state_transition,
            },
            migration_record::{SchemaMigrationRecord, decode_schema_migration_record},
            wire::SchemaWireReader,
        },
    },
    error::InternalError,
};
use sha2::{Digest, Sha256};
use std::cell::Cell;

const MIGRATION_GATE_UNKNOWN: u8 = 0;
const MIGRATION_GATE_READY: u8 = 1;
const MIGRATION_GATE_BLOCKED: u8 = 2;

thread_local! {
    static MIGRATION_GATE_STATE: Cell<u8> = const { Cell::new(MIGRATION_GATE_UNKNOWN) };
}

use crate::db::schema::migration_lineage::{
    AcceptedEntitySourceLineageCatalog, EntitySourceLineageCatalogOp,
    decode_entity_source_lineage_catalog,
};

const CHECKPOINT_MAGIC: &[u8; 8] = b"ICYSCAND";
const IDENTITY_CHECKPOINT_MAGIC: &[u8; 8] = b"ICYIDINV";
const CHECKPOINT_VERSION: u8 = 1;
const CHECKPOINT_FIXED_BYTES: usize = 8 + 1 + 4 + 4 + 4 + 4;
const IDENTITY_CHECKPOINT_FIXED_BYTES: usize = 8 + 1 + 4 + 4 + 4 + 4;
const CHECKPOINT_KEY_PROFILE: &[u8] = b"icydb.live-schema-checkpoint.key.v1";
const LINEAGE_KEY_PROFILE: &[u8] = b"icydb.schema-lineage.key.v1";
const MIGRATION_KEY_PROFILE: &[u8] = b"icydb.schema-migration.key.v1";
const MAX_LIVE_SCHEMA_CHECKPOINTS: u64 = icydb_schema::MAX_SCHEMA_ASSIGNMENTS as u64;
const MAX_LIVE_SCHEMA_CHECKPOINT_BYTES: usize = CHECKPOINT_FIXED_BYTES
    + MAX_SCHEMA_STORE_PATH_BYTES
    + MAX_ACCEPTED_SCHEMA_BUNDLE_BYTES
    + ACCEPTED_SCHEMA_ROOT_BYTES;
const MAX_IDENTITY_CHECKPOINT_BYTES: usize = IDENTITY_CHECKPOINT_FIXED_BYTES
    + MAX_SCHEMA_STORE_PATH_BYTES
    + ACCEPTED_SCHEMA_ROOT_BYTES
    + MAX_IDENTITY_STATE_RECORDS_PER_DATABASE * IDENTITY_STATE_RECORD_BYTES;
type CheckpointWriter = BoundedByteWriter<MAX_LIVE_SCHEMA_CHECKPOINT_BYTES>;
type CheckpointReader<'a> = SchemaWireReader<'a>;
type IdentityCheckpointWriter = BoundedByteWriter<MAX_IDENTITY_CHECKPOINT_BYTES>;

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
        if key.0 == [0; 32] || key == Self::lineage() || key == Self::migration() {
            return Err(InternalError::store_invariant());
        }
        Ok(key)
    }
}

impl From<[u8; 32]> for LiveSchemaCheckpointKey {
    fn from(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }
}

impl From<LiveSchemaCheckpointKey> for [u8; 32] {
    fn from(key: LiveSchemaCheckpointKey) -> Self {
        key.0
    }
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
    map: SchemaControlMap<LiveSchemaCheckpointKey>,
    identities: SchemaControlMap<LiveSchemaCheckpointKey>,
}

#[derive(Debug)]
struct LiveIdentityCheckpoint {
    root: AcceptedSchemaRoot,
    states: IdentityStateInventory,
}

/// Encoded identity-only record for one synchronous commit window.
/// Recovery continues to derive its updates from the durable marker.
#[derive(Debug)]
pub(in crate::db) struct PreparedLiveIdentityCheckpoint {
    key: LiveSchemaCheckpointKey,
    bytes: Vec<u8>,
}

impl LiveSchemaCheckpointStore {
    fn open(memory: ControlMemory) -> Result<Self, InternalError> {
        let identities = SchemaControlMap::open(memory.clone(), ControlRecordFamily::Identity)?;
        Self::from_maps(
            SchemaControlMap::open(memory, ControlRecordFamily::Checkpoint)?,
            identities,
        )
    }

    fn open_existing(memory: ControlMemory) -> Result<Option<Self>, InternalError> {
        let Some(map) =
            SchemaControlMap::open_existing(memory.clone(), ControlRecordFamily::Checkpoint)?
        else {
            return Ok(None);
        };
        let identities = SchemaControlMap::open_existing(memory, ControlRecordFamily::Identity)?
            .ok_or_else(InternalError::store_corruption)?;
        Self::from_maps(map, identities).map(Some)
    }

    fn from_maps(
        map: SchemaControlMap<LiveSchemaCheckpointKey>,
        identities: SchemaControlMap<LiveSchemaCheckpointKey>,
    ) -> Result<Self, InternalError> {
        let store = Self { map, identities };
        if let Some(record) = store.map.get(&LiveSchemaCheckpointKey::migration())? {
            decode_schema_migration_record(&record.0)?;
        }
        if store.checkpoint_count()? > MAX_LIVE_SCHEMA_CHECKPOINTS {
            return Err(InternalError::store_corruption());
        }
        Ok(store)
    }

    fn load(&self, store_path: &str) -> Result<Option<LiveSchemaCheckpoint>, InternalError> {
        let key = LiveSchemaCheckpointKey::for_store(store_path)?;
        let Some(bytes) = self.map.get(&key)? else {
            if self.identities.get(&key)?.is_some() {
                return Err(InternalError::identity_state_corruption());
            }
            return Ok(None);
        };
        let candidate = decode_checkpoint(&bytes.0, key)?;
        let identity = self.load_identity(store_path, candidate.root())?;
        Ok(Some(LiveSchemaCheckpoint {
            candidate,
            identity_states: identity.states,
        }))
    }

    // Accepted runtime root authority binds the small mutable record without
    // reopening or decoding the immutable candidate during identity work.
    fn load_identity(
        &self,
        store_path: &str,
        root: AcceptedSchemaRoot,
    ) -> Result<LiveIdentityCheckpoint, InternalError> {
        let key = LiveSchemaCheckpointKey::for_store(store_path)?;
        let bytes = self
            .identities
            .get(&key)?
            .ok_or_else(InternalError::identity_state_corruption)?;
        let identity = decode_identity_checkpoint(&bytes.0, key)?;
        if identity.root != root {
            return Err(InternalError::identity_state_corruption());
        }
        Ok(identity)
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
        // Encode both before publication. The existing marker apply owner keeps
        // this synchronous pair in one IC message and traps on contradiction.
        let candidate_bytes = encode_checkpoint(store_path, checkpoint.candidate())?;
        let identity_bytes = encode_identity_checkpoint(
            store_path,
            &LiveIdentityCheckpoint {
                root: checkpoint.candidate().root(),
                states: checkpoint.identity_states,
            },
        )?;
        self.map.insert(key, SchemaControlBytes(candidate_bytes))?;
        self.identities
            .insert(key, SchemaControlBytes(identity_bytes))?;
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
        let lineage = self.map.get(&LiveSchemaCheckpointKey::lineage())?.is_some();
        let migration = self
            .map
            .get(&LiveSchemaCheckpointKey::migration())?
            .is_some();
        self.map
            .len()?
            .checked_sub(u64::from(lineage) + u64::from(migration))
            .ok_or_else(InternalError::store_corruption)
    }

    fn load_migration(&self) -> Result<Option<SchemaMigrationRecord>, InternalError> {
        self.map
            .get(&LiveSchemaCheckpointKey::migration())?
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
            .get(&LiveSchemaCheckpointKey::migration())?
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
            SchemaControlBytes(operation.after_bytes().to_vec()),
        )?;
        Ok(())
    }

    fn load_lineage(&self) -> Result<Option<AcceptedEntitySourceLineageCatalog>, InternalError> {
        self.map
            .get(&LiveSchemaCheckpointKey::lineage())?
            .map(|bytes| decode_entity_source_lineage_catalog(&bytes.0))
            .transpose()
    }

    fn preflight_lineage(
        &self,
        operation: &EntitySourceLineageCatalogOp,
    ) -> Result<LiveSchemaCheckpointPreflight, InternalError> {
        operation.validate()?;
        let current = self
            .map
            .get(&LiveSchemaCheckpointKey::lineage())?
            .map(|bytes| bytes.0);
        if current.as_deref() == Some(operation.after_bytes()) {
            return Ok(LiveSchemaCheckpointPreflight::AlreadyApplied);
        }
        if current.as_deref() != operation.before_bytes() {
            return Err(InternalError::schema_application_conflict());
        }
        Ok(LiveSchemaCheckpointPreflight::Ready)
    }

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
            SchemaControlBytes(operation.after_bytes().to_vec()),
        )?;
        Ok(())
    }

    fn prepare_identity_ranges(
        &self,
        store_path: &str,
        root: AcceptedSchemaRoot,
        ranges: &[(IdentityRangeAdvance, IdentityAdvanceId)],
        budget: &dyn ConstructionBudget,
    ) -> Result<PreparedLiveIdentityCheckpoint, InternalError> {
        let mut identity = self.load_identity(store_path, root)?;
        // Coalesce every owner before encoding the one bounded mutable record.
        for (range, advance_id) in ranges {
            let key = (range.owner().entity_tag(), range.owner().field_id());
            let state = identity
                .states
                .get(&key)
                .ok_or_else(InternalError::identity_state_corruption)?;
            state.preflight_range_advance(*range)?;
            let next = state.apply_range_advance(*range, *advance_id)?;
            identity.states.insert(key, next);
        }
        let bytes = IDENTITY_CHECKPOINT_FIXED_BYTES
            + store_path.len()
            + ACCEPTED_SCHEMA_ROOT_BYTES
            + identity.states.len() * IDENTITY_STATE_RECORD_BYTES;
        budget.charge(
            icydb_diagnostic_code::DiagnosticExecutionBudgetResource::TemporaryBytes,
            bytes as u64,
        )?;
        Ok(PreparedLiveIdentityCheckpoint {
            key: LiveSchemaCheckpointKey::for_store(store_path)?,
            bytes: encode_identity_checkpoint(store_path, &identity)?,
        })
    }

    fn apply_prepared_identity_checkpoint(
        &mut self,
        prepared: PreparedLiveIdentityCheckpoint,
    ) -> Result<(), InternalError> {
        self.identities
            .insert(prepared.key, SchemaControlBytes(prepared.bytes))?;
        Ok(())
    }

    fn apply_identity_range(
        &mut self,
        store_path: &str,
        root: AcceptedSchemaRoot,
        range: IdentityRangeAdvance,
        advance_id: IdentityAdvanceId,
    ) -> Result<(), InternalError> {
        let mut identity = self.load_identity(store_path, root)?;
        let key = (range.owner().entity_tag(), range.owner().field_id());
        let current = identity
            .states
            .get(&key)
            .ok_or_else(InternalError::identity_state_corruption)?;
        let state = current.apply_range_advance(range, advance_id)?;
        if state == *current {
            return Ok(());
        }
        identity.states.insert(key, state);
        self.identities.insert(
            LiveSchemaCheckpointKey::for_store(store_path)?,
            SchemaControlBytes(encode_identity_checkpoint(store_path, &identity)?),
        )?;
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
    candidate: &CandidateSchemaRevision,
) -> Result<Vec<u8>, InternalError> {
    if candidate.store_path() != store_path
        || store_path.is_empty()
        || store_path.len() > MAX_SCHEMA_STORE_PATH_BYTES
    {
        return Err(InternalError::store_invariant());
    }
    let mut writer = CheckpointWriter::new();
    writer.push_bytes(CHECKPOINT_MAGIC);
    writer.push_u8(CHECKPOINT_VERSION);
    writer.push_len_prefixed_bytes(store_path.as_bytes())?;
    writer.push_len_prefixed_bytes(candidate.encoded_bundle())?;
    writer.push_len_prefixed_bytes(candidate.encoded_root())?;
    let mut bytes = writer.finish()?;
    if bytes.len() > MAX_LIVE_SCHEMA_CHECKPOINT_BYTES.saturating_sub(size_of::<u32>()) {
        return Err(InternalError::store_invariant());
    }
    bytes.extend_from_slice(&crc32c(&bytes).to_be_bytes());
    Ok(bytes)
}

fn decode_checkpoint(
    bytes: &[u8],
    expected_key: LiveSchemaCheckpointKey,
) -> Result<CandidateSchemaRevision, InternalError> {
    if bytes.len() < CHECKPOINT_FIXED_BYTES || bytes.len() > MAX_LIVE_SCHEMA_CHECKPOINT_BYTES {
        return Err(InternalError::store_corruption());
    }
    let body = verify_crc32c_trailer(bytes).ok_or_else(InternalError::store_corruption)?;
    let mut reader = CheckpointReader::new(body);
    if reader.read_array::<8>()? != *CHECKPOINT_MAGIC || reader.read_u8()? != CHECKPOINT_VERSION {
        return Err(InternalError::store_corruption());
    }
    let path = reader.read_len_prefixed_bytes()?;
    let path = std::str::from_utf8(path).map_err(|_| InternalError::store_corruption())?;
    if LiveSchemaCheckpointKey::for_store(path).map_err(|_| InternalError::store_corruption())?
        != expected_key
    {
        return Err(InternalError::store_corruption());
    }
    let bundle = reader.read_len_prefixed_bytes()?;
    let root = reader.read_len_prefixed_bytes()?;
    if bundle.len() > MAX_ACCEPTED_SCHEMA_BUNDLE_BYTES || root.len() != ACCEPTED_SCHEMA_ROOT_BYTES {
        return Err(InternalError::store_corruption());
    }
    reader.finish()?;
    let candidate = CandidateSchemaRevision::from_encoded(bundle.to_vec(), root.to_vec())?;
    if candidate.store_path() != path {
        return Err(InternalError::store_corruption());
    }
    Ok(candidate)
}

fn encode_identity_checkpoint(
    store_path: &str,
    identity: &LiveIdentityCheckpoint,
) -> Result<Vec<u8>, InternalError> {
    LiveSchemaCheckpointKey::for_store(store_path)?;
    if identity.states.len() > MAX_IDENTITY_STATE_RECORDS_PER_DATABASE {
        return Err(InternalError::identity_state_corruption());
    }
    let mut writer = IdentityCheckpointWriter::new();
    writer.push_bytes(IDENTITY_CHECKPOINT_MAGIC);
    writer.push_u8(CHECKPOINT_VERSION);
    writer.push_len_prefixed_bytes(store_path.as_bytes())?;
    writer.push_len_prefixed_bytes(&encode_accepted_schema_root(identity.root)?)?;
    writer.push_u32(
        u32::try_from(identity.states.len()).map_err(|_| InternalError::store_invariant())?,
    );
    for (key, state) in &identity.states {
        if *key != (state.owner().entity_tag(), state.owner().field_id()) {
            return Err(InternalError::identity_state_corruption());
        }
        writer.push_bytes(&encode_identity_state(state)?);
    }
    let mut bytes = writer.finish()?;
    if bytes.len() > MAX_IDENTITY_CHECKPOINT_BYTES.saturating_sub(size_of::<u32>()) {
        return Err(InternalError::store_invariant());
    }
    bytes.extend_from_slice(&crc32c(&bytes).to_be_bytes());
    Ok(bytes)
}

fn decode_identity_checkpoint(
    bytes: &[u8],
    expected_key: LiveSchemaCheckpointKey,
) -> Result<LiveIdentityCheckpoint, InternalError> {
    if bytes.len() < IDENTITY_CHECKPOINT_FIXED_BYTES || bytes.len() > MAX_IDENTITY_CHECKPOINT_BYTES
    {
        return Err(InternalError::identity_state_corruption());
    }
    let body = verify_crc32c_trailer(bytes).ok_or_else(InternalError::identity_state_corruption)?;
    let mut reader = CheckpointReader::new(body);
    if reader.read_array::<8>()? != *IDENTITY_CHECKPOINT_MAGIC
        || reader.read_u8()? != CHECKPOINT_VERSION
    {
        return Err(InternalError::identity_state_corruption());
    }
    let path = reader.read_len_prefixed_bytes()?;
    let path = std::str::from_utf8(path).map_err(|_| InternalError::identity_state_corruption())?;
    if LiveSchemaCheckpointKey::for_store(path)
        .map_err(|_| InternalError::identity_state_corruption())?
        != expected_key
    {
        return Err(InternalError::identity_state_corruption());
    }
    let root = decode_accepted_schema_root(reader.read_len_prefixed_bytes()?)?;
    let count = usize::try_from(reader.read_u32()?)
        .map_err(|_| InternalError::identity_state_corruption())?;
    if count > MAX_IDENTITY_STATE_RECORDS_PER_DATABASE {
        return Err(InternalError::identity_state_corruption());
    }
    let mut states = IdentityStateInventory::new();
    let mut prior = None;
    for _ in 0..count {
        let state = decode_identity_state(&reader.read_array::<IDENTITY_STATE_RECORD_BYTES>()?)?;
        let key = (state.owner().entity_tag(), state.owner().field_id());
        if prior.is_some_and(|prior| prior >= key) || states.insert(key, state).is_some() {
            return Err(InternalError::identity_state_corruption());
        }
        prior = Some(key);
    }
    reader.finish()?;
    Ok(LiveIdentityCheckpoint { root, states })
}

pub(in crate::db) fn load_live_schema_checkpoint(
    store_path: &str,
) -> Result<Option<LiveSchemaCheckpoint>, InternalError> {
    let Some(store) = LiveSchemaCheckpointStore::open_existing(control_memory()?)? else {
        // Preserve path admission even when there is no checkpoint allocation yet.
        LiveSchemaCheckpointKey::for_store(store_path)?;
        return Ok(None);
    };
    store.load(store_path)
}

pub(in crate::db) fn preflight_live_schema_checkpoint(
    incarnation: DatabaseIncarnationId,
    store_path: &str,
    expected_revision: AcceptedSchemaRevision,
    candidate: &CandidateSchemaRevision,
) -> Result<LiveSchemaCheckpointPreflight, InternalError> {
    LiveSchemaCheckpointStore::open(control_memory()?)?.preflight(
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
    LiveSchemaCheckpointStore::open(control_memory()?)?.apply(
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

pub(in crate::db) fn prepare_live_identity_ranges_checkpoint(
    store_path: &str,
    root: AcceptedSchemaRoot,
    ranges: &[(IdentityRangeAdvance, IdentityAdvanceId)],
    budget: &dyn ConstructionBudget,
) -> Result<PreparedLiveIdentityCheckpoint, InternalError> {
    LiveSchemaCheckpointStore::open(control_memory()?)?
        .prepare_identity_ranges(store_path, root, ranges, budget)
}

/// Publish images prepared before this synchronous window's marker. No schema
/// publication may intervene; interrupted windows recover from marker records.
pub(in crate::db) fn apply_prepared_live_identity_checkpoints(
    checkpoints: Vec<PreparedLiveIdentityCheckpoint>,
) -> Result<(), InternalError> {
    if checkpoints.is_empty() {
        return Ok(());
    }
    let mut store = LiveSchemaCheckpointStore::open(control_memory()?)?;
    for checkpoint in checkpoints {
        store.apply_prepared_identity_checkpoint(checkpoint)?;
    }
    Ok(())
}

pub(in crate::db) fn apply_live_identity_range_checkpoint(
    store_path: &str,
    root: AcceptedSchemaRoot,
    range: IdentityRangeAdvance,
    advance_id: IdentityAdvanceId,
) -> Result<(), InternalError> {
    LiveSchemaCheckpointStore::open(control_memory()?)?
        .apply_identity_range(store_path, root, range, advance_id)
}

pub(in crate::db) fn verify_live_identity_range_checkpoint(
    store_path: &str,
    root: AcceptedSchemaRoot,
    range: IdentityRangeAdvance,
    advance_id: IdentityAdvanceId,
) -> Result<(), InternalError> {
    let identity =
        LiveSchemaCheckpointStore::open(control_memory()?)?.load_identity(store_path, root)?;
    let state = identity
        .states
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
    #[cfg(test)]
    MIGRATION_RECORD_LOADS.with(|loads| loads.set(loads.get() + 1));
    let Some(store) = LiveSchemaCheckpointStore::open_existing(control_memory()?)? else {
        return Ok(None);
    };
    store.load_migration()
}

// Observe real durable authority loads without substituting the decoder.

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

/// Preserve the exact predecessor head for every nonterminal migration,
/// including `Prepared`, which still admits ordinary row reads and writes.
pub(in crate::db) fn ensure_schema_migration_ready_for_schema_changes() -> Result<(), InternalError>
{
    if load_schema_migration_record()?.is_some_and(|record| !record.phase().terminal()) {
        return Err(InternalError::schema_migration(
            icydb_diagnostic_code::SchemaMigrationCode::MigrationInProgress,
        ));
    }
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
    LiveSchemaCheckpointStore::open(control_memory()?)?.preflight_migration(operation)
}

#[cfg(any(test, feature = "migration"))]
pub(in crate::db) fn apply_schema_migration_record_op(
    operation: &SchemaMigrationRecordOp,
) -> Result<(), InternalError> {
    LiveSchemaCheckpointStore::open(control_memory()?)?.apply_migration(operation)?;
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
    let current = LiveSchemaCheckpointStore::open(control_memory()?)?
        .map
        .get(&LiveSchemaCheckpointKey::migration())?
        .ok_or_else(InternalError::recovery_effect_verification_failed)?;
    if current.0 != operation.after_bytes() {
        return Err(InternalError::recovery_effect_verification_failed());
    }
    operation.validate()
}

pub(in crate::db::schema) fn load_entity_source_lineage_catalog()
-> Result<Option<AcceptedEntitySourceLineageCatalog>, InternalError> {
    let Some(store) = LiveSchemaCheckpointStore::open_existing(control_memory()?)? else {
        return Ok(None);
    };
    store.load_lineage()
}

pub(in crate::db) fn preflight_entity_source_lineage_catalog_op(
    operation: &EntitySourceLineageCatalogOp,
) -> Result<LiveSchemaCheckpointPreflight, InternalError> {
    LiveSchemaCheckpointStore::open(control_memory()?)?.preflight_lineage(operation)
}

pub(in crate::db) fn apply_entity_source_lineage_catalog_op(
    operation: &EntitySourceLineageCatalogOp,
) -> Result<(), InternalError> {
    LiveSchemaCheckpointStore::open(control_memory()?)?.apply_lineage(operation)
}

pub(in crate::db) fn verify_entity_source_lineage_catalog_op(
    operation: &EntitySourceLineageCatalogOp,
) -> Result<(), InternalError> {
    let current = LiveSchemaCheckpointStore::open(control_memory()?)?
        .map
        .get(&LiveSchemaCheckpointKey::lineage())?
        .ok_or_else(InternalError::recovery_effect_verification_failed)?;
    if current.0 != operation.after_bytes() {
        return Err(InternalError::recovery_effect_verification_failed());
    }
    operation.validate()
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod helper_tests;

#[cfg(test)]
pub(in crate::db) use helper_tests::entity_source_lineage_matches_for_tests;
#[cfg(test)]
pub(in crate::db) use helper_tests::schema_migration_record_matches_for_tests;
#[cfg(test)]
pub(in crate::db) use helper_tests::take_migration_record_load_count_for_tests;

#[cfg(test)]
use helper_tests::MIGRATION_RECORD_LOADS;
