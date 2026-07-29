//! Module: db::schema::live_schema_checkpoint
//! Responsibility: durably retain current accepted candidates for live-only schema stores.
//! Does not own: proposal lowering, accepted reconciliation, or runtime schema interpretation.
//! Boundary: marker-owned accepted candidate -> bounded control-memory checkpoint -> recovery.

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
use std::borrow::Cow;

const CHECKPOINT_HEADER_KEY: LiveSchemaCheckpointKey = LiveSchemaCheckpointKey([0; 32]);
const CHECKPOINT_HEADER_MAGIC: &[u8; 8] = b"ICYSLVHD";
const CHECKPOINT_HEADER_VERSION: u8 = 1;
const CHECKPOINT_HEADER_BYTES: usize = 8 + 1 + 4;
const CHECKPOINT_MAGIC: &[u8; 8] = b"ICYSLIVE";
const CHECKPOINT_VERSION: u8 = 1;
const CHECKPOINT_FIXED_BYTES: usize = 8 + 1 + 4 + 4 + 4 + 4 + 4;
const CHECKPOINT_KEY_PROFILE: &[u8] = b"icydb.live-schema-checkpoint.key.v1";
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
        if key == CHECKPOINT_HEADER_KEY {
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
        self.map
            .len()
            .checked_sub(1)
            .ok_or_else(InternalError::store_corruption)
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

#[cfg(test)]
mod tests {
    use super::{
        IdentityStateInventory, LiveSchemaCheckpointPreflight, LiveSchemaCheckpointStore,
        decode_checkpoint, encode_checkpoint,
    };
    use crate::{
        db::{
            integrity::DatabaseIncarnationId,
            schema::{
                AcceptedSchemaRevision, empty_accepted_schema_candidate_for_tests,
                live_schema_checkpoint::{LiveSchemaCheckpoint, LiveSchemaCheckpointKey},
            },
        },
        testing::test_memory,
    };
    use ic_stable_structures::RestrictedMemory;

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
}
