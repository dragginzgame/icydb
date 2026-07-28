//! Module: db::schema::live_schema_checkpoint
//! Responsibility: durably retain current accepted candidates for live-only schema stores.
//! Does not own: proposal lowering, accepted reconciliation, or runtime schema interpretation.
//! Boundary: marker-owned accepted candidate -> bounded control-memory checkpoint -> recovery.

use crate::{
    db::{
        commit::{commit_memory_handle, current_commit_memory_allocation},
        database_format::crc32c,
        schema::{
            AcceptedSchemaRevision, CandidateSchemaRevision,
            enum_catalog::{
                ACCEPTED_SCHEMA_ROOT_BYTES, MAX_ACCEPTED_SCHEMA_BUNDLE_BYTES,
                MAX_SCHEMA_STORE_PATH_BYTES,
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
const CHECKPOINT_FIXED_BYTES: usize = 8 + 1 + 4 + 4 + 4 + 4;
const CHECKPOINT_KEY_PROFILE: &[u8] = b"icydb.live-schema-checkpoint.key.v1";
const MAX_LIVE_SCHEMA_CHECKPOINTS: u64 = icydb_schema::MAX_SCHEMA_ASSIGNMENTS as u64;
const MAX_LIVE_SCHEMA_CHECKPOINT_BYTES: usize = CHECKPOINT_FIXED_BYTES
    + MAX_SCHEMA_STORE_PATH_BYTES
    + MAX_ACCEPTED_SCHEMA_BUNDLE_BYTES
    + ACCEPTED_SCHEMA_ROOT_BYTES;
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
        reason = "the compile-time checkpoint ceiling is less than 17 MiB"
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

    fn load(&self, store_path: &str) -> Result<Option<CandidateSchemaRevision>, InternalError> {
        let key = LiveSchemaCheckpointKey::for_store(store_path)?;
        self.map
            .get(&key)
            .map(|bytes| decode_checkpoint(&bytes.0, key))
            .transpose()
    }

    fn preflight(
        &self,
        store_path: &str,
        expected_revision: AcceptedSchemaRevision,
        candidate: &CandidateSchemaRevision,
    ) -> Result<LiveSchemaCheckpointPreflight, InternalError> {
        validate_candidate_transition(store_path, expected_revision, candidate)?;
        let current = self.load(store_path)?;
        if current.as_ref().is_some_and(|current| {
            current.encoded_bundle() == candidate.encoded_bundle()
                && current.encoded_root() == candidate.encoded_root()
        }) {
            return Ok(LiveSchemaCheckpointPreflight::AlreadyApplied);
        }
        if current.as_ref().map(CandidateSchemaRevision::revision)
            != (expected_revision != AcceptedSchemaRevision::NONE).then_some(expected_revision)
        {
            return Err(InternalError::store_corruption());
        }
        if current.is_none() && self.checkpoint_count()? >= MAX_LIVE_SCHEMA_CHECKPOINTS {
            return Err(InternalError::store_invariant());
        }
        Ok(LiveSchemaCheckpointPreflight::Ready)
    }

    fn apply(
        &mut self,
        store_path: &str,
        expected_revision: AcceptedSchemaRevision,
        candidate: &CandidateSchemaRevision,
    ) -> Result<(), InternalError> {
        match self.preflight(store_path, expected_revision, candidate)? {
            LiveSchemaCheckpointPreflight::AlreadyApplied => return Ok(()),
            LiveSchemaCheckpointPreflight::Ready => {}
        }
        let key = LiveSchemaCheckpointKey::for_store(store_path)?;
        self.map.insert(
            key,
            LiveSchemaCheckpointBytes(encode_checkpoint(store_path, candidate)?),
        );
        Ok(())
    }

    fn checkpoint_count(&self) -> Result<u64, InternalError> {
        self.map
            .len()
            .checked_sub(1)
            .ok_or_else(InternalError::store_corruption)
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
) -> Result<CandidateSchemaRevision, InternalError> {
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
    reader.finish()?;
    let candidate =
        CandidateSchemaRevision::from_encoded(bundle_bytes.to_vec(), root_bytes.to_vec())?;
    if candidate.store_path() != store_path || encode_checkpoint(store_path, &candidate)? != bytes {
        return Err(InternalError::store_corruption());
    }
    Ok(candidate)
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
) -> Result<Option<CandidateSchemaRevision>, InternalError> {
    LiveSchemaCheckpointStore::open(checkpoint_memory()?)?.load(store_path)
}

pub(in crate::db) fn preflight_live_schema_checkpoint(
    store_path: &str,
    expected_revision: AcceptedSchemaRevision,
    candidate: &CandidateSchemaRevision,
) -> Result<LiveSchemaCheckpointPreflight, InternalError> {
    LiveSchemaCheckpointStore::open(checkpoint_memory()?)?.preflight(
        store_path,
        expected_revision,
        candidate,
    )
}

pub(in crate::db) fn apply_live_schema_checkpoint(
    store_path: &str,
    expected_revision: AcceptedSchemaRevision,
    candidate: &CandidateSchemaRevision,
) -> Result<(), InternalError> {
    LiveSchemaCheckpointStore::open(checkpoint_memory()?)?.apply(
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
    if current.encoded_bundle() != candidate.encoded_bundle()
        || current.encoded_root() != candidate.encoded_root()
    {
        return Err(InternalError::recovery_effect_verification_failed());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        LiveSchemaCheckpointPreflight, LiveSchemaCheckpointStore, decode_checkpoint,
        encode_checkpoint,
    };
    use crate::{
        db::schema::{
            AcceptedSchemaRevision, empty_accepted_schema_candidate_for_tests,
            live_schema_checkpoint::LiveSchemaCheckpointKey,
        },
        testing::test_memory,
    };
    use ic_stable_structures::RestrictedMemory;

    #[test]
    fn checkpoint_codec_is_canonical_and_checksum_bound() {
        let candidate =
            empty_accepted_schema_candidate_for_tests("test::Live", AcceptedSchemaRevision::new(1));
        let encoded = encode_checkpoint("test::Live", &candidate)
            .expect("accepted candidate should checkpoint");
        let key =
            LiveSchemaCheckpointKey::for_store("test::Live").expect("checkpoint key should derive");
        let decoded = decode_checkpoint(&encoded, key).expect("checkpoint should decode");
        assert_eq!(decoded.encoded_bundle(), candidate.encoded_bundle());
        assert_eq!(decoded.encoded_root(), candidate.encoded_root());

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

        assert_eq!(
            store
                .preflight("test::Live", AcceptedSchemaRevision::NONE, &first)
                .expect("initial checkpoint should preflight"),
            LiveSchemaCheckpointPreflight::Ready,
        );
        store
            .apply("test::Live", AcceptedSchemaRevision::NONE, &first)
            .expect("initial checkpoint should apply");
        store
            .apply("test::Live", AcceptedSchemaRevision::NONE, &first)
            .expect("initial checkpoint replay should be idempotent");
        store
            .apply("test::Live", AcceptedSchemaRevision::new(1), &second)
            .expect("next checkpoint should apply");

        let reopened =
            LiveSchemaCheckpointStore::open(memory).expect("checkpoint store should reopen");
        let loaded = reopened
            .load("test::Live")
            .expect("checkpoint should remain readable")
            .expect("checkpoint should exist");
        assert_eq!(loaded.encoded_bundle(), second.encoded_bundle());
        assert_eq!(loaded.encoded_root(), second.encoded_root());
    }
}
