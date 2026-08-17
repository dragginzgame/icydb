//! Module: db::schema::cardinality_generation
//! Responsibility: current durable exact-cardinality identity and record codecs.
//! Does not own: populated construction, publication, incremental maintenance, or planning.
//! Boundary: accepted source identities -> bounded version-1 schema-allocation records.

#![cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "Patch 2 freezes codecs that the bounded Patch 3 builder activates"
    )
)]

use crate::{
    MAX_INDEX_FIELDS,
    db::{
        data::RawDataStoreKey,
        index::{IndexId, IndexKey, RawIndexStoreKey, UserIndexPrefixCardinalityKey},
        integrity::DatabaseIncarnationId,
        journal::FoldWatermark,
        registry::StoreAllocationIdentities,
        schema::enum_catalog::{AcceptedSchemaFingerprint, AcceptedSchemaRevision},
    },
    error::InternalError,
    types::EntityTag,
};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;

const CARDINALITY_FORMAT_VERSION_CURRENT: u8 = 1;
const CARDINALITY_GENERATION_HEADER_MAGIC: &[u8; 8] = b"ICYDBCGH";
const CARDINALITY_BUILD_CURSOR_MAGIC: &[u8; 8] = b"ICYDBCGC";
const CARDINALITY_COUNT_RECORD_MAGIC: &[u8; 8] = b"ICYDBCNT";
const CARDINALITY_GENERATION_HEADER_FINGERPRINT_DOMAIN: &[u8] =
    b"icydb.cardinality-generation-header.v1";
const CARDINALITY_BUILD_CURSOR_FINGERPRINT_DOMAIN: &[u8] = b"icydb.cardinality-build-cursor.v1";
const CARDINALITY_STORE_ALLOCATION_FINGERPRINT_DOMAIN: &[u8] =
    b"icydb.cardinality-store-allocation.v1";
const CARDINALITY_ACCEPTED_INDEX_SET_FINGERPRINT_DOMAIN: &[u8] =
    b"icydb.cardinality-accepted-index-set.v1";
const CARDINALITY_COUNT_KEY_FINGERPRINT_DOMAIN: &[u8] = b"icydb.cardinality-count-key.v1";
const CARDINALITY_FINGERPRINT_BYTES: usize = 32;
const CARDINALITY_SOURCE_IDENTITY_BYTES: usize = 141;
const CARDINALITY_GENERATION_HEADER_BODY_BYTES: usize = 19 + CARDINALITY_SOURCE_IDENTITY_BYTES;
const CARDINALITY_BUILD_CURSOR_FIXED_BODY_BYTES: usize = 56 + CARDINALITY_SOURCE_IDENTITY_BYTES;
pub(in crate::db) const CARDINALITY_GENERATION_HEADER_BYTES: usize =
    CARDINALITY_GENERATION_HEADER_BODY_BYTES + CARDINALITY_FINGERPRINT_BYTES;
pub(in crate::db) const CARDINALITY_COUNT_RECORD_BYTES: usize = 57;
pub(in crate::db) const MAX_CARDINALITY_BUILD_CURSOR_BYTES: usize = 20 * 1024;

/// Monotonic identity for one store-local cardinality generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct CardinalityGenerationId(u64);

impl CardinalityGenerationId {
    pub(in crate::db) const INITIAL: Self = Self(1);

    /// Construct a nonzero current generation identity.
    pub(in crate::db) fn try_new(value: u64) -> Result<Self, InternalError> {
        if value == 0 {
            return Err(InternalError::store_corruption());
        }
        Ok(Self(value))
    }

    /// Return the next monotonic generation identity.
    pub(in crate::db) fn checked_next(self) -> Result<Self, InternalError> {
        self.0
            .checked_add(1)
            .map(Self)
            .ok_or_else(InternalError::store_unsupported)
    }

    #[must_use]
    pub(in crate::db) const fn get(self) -> u64 {
        self.0
    }
}

/// Physical count namespace selected for one Building or Ready generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum CardinalityCountSlot {
    A,
    B,
}

impl CardinalityCountSlot {
    const fn to_tag(self) -> u8 {
        match self {
            Self::A => 1,
            Self::B => 2,
        }
    }

    fn from_tag(tag: u8) -> Result<Self, InternalError> {
        match tag {
            1 => Ok(Self::A),
            2 => Ok(Self::B),
            _ => Err(InternalError::store_corruption()),
        }
    }
}

/// Persisted lifecycle state for one complete cardinality generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum CardinalityGenerationState {
    Building,
    Ready,
}

impl CardinalityGenerationState {
    const fn to_tag(self) -> u8 {
        match self {
            Self::Building => 1,
            Self::Ready => 2,
        }
    }

    fn from_tag(tag: u8) -> Result<Self, InternalError> {
        match tag {
            1 => Ok(Self::Building),
            2 => Ok(Self::Ready),
            _ => Err(InternalError::store_corruption()),
        }
    }
}

/// Accepted store-root identity bound by one cardinality generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct CardinalityAcceptedRootIdentity {
    revision: AcceptedSchemaRevision,
    fingerprint: AcceptedSchemaFingerprint,
}

impl CardinalityAcceptedRootIdentity {
    /// Construct one present accepted-root identity.
    pub(in crate::db) fn new(
        revision: AcceptedSchemaRevision,
        fingerprint: AcceptedSchemaFingerprint,
    ) -> Result<Self, InternalError> {
        if revision == AcceptedSchemaRevision::NONE {
            return Err(InternalError::store_invariant());
        }
        Ok(Self {
            revision,
            fingerprint,
        })
    }
}

/// Complete canonical source identity described by one generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct CardinalitySourceIdentity {
    database_incarnation: DatabaseIncarnationId,
    store_allocation_fingerprint: [u8; 32],
    accepted_root: Option<CardinalityAcceptedRootIdentity>,
    accepted_index_count: u32,
    accepted_index_set_fingerprint: [u8; 32],
    fold_watermark: FoldWatermark,
}

impl CardinalitySourceIdentity {
    /// Derive one source identity from current runtime authorities.
    pub(in crate::db) fn derive(
        database_incarnation: DatabaseIncarnationId,
        allocations: StoreAllocationIdentities,
        accepted_root: Option<CardinalityAcceptedRootIdentity>,
        accepted_indexes: impl IntoIterator<Item = IndexId>,
        fold_watermark: FoldWatermark,
    ) -> Result<Self, InternalError> {
        let store_allocation_fingerprint = store_allocation_fingerprint(allocations)?;
        let (accepted_index_count, accepted_index_set_fingerprint) =
            accepted_index_set_fingerprint(accepted_indexes)?;
        if accepted_root.is_none() && accepted_index_count != 0 {
            return Err(InternalError::store_invariant());
        }
        Ok(Self {
            database_incarnation,
            store_allocation_fingerprint,
            accepted_root,
            accepted_index_count,
            accepted_index_set_fingerprint,
            fold_watermark,
        })
    }
}

/// Exact reason persisted generation evidence does not describe current source authority.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum CardinalitySourceMismatch {
    DatabaseIncarnation,
    StoreAllocation,
    AcceptedRoot,
    AcceptedIndexSet,
    FoldWatermark,
}

/// Checksummed current generation header stored in schema namespace `0x05`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct CardinalityGenerationHeader {
    generation: CardinalityGenerationId,
    state: CardinalityGenerationState,
    slot: CardinalityCountSlot,
    source: CardinalitySourceIdentity,
}

impl CardinalityGenerationHeader {
    #[must_use]
    pub(in crate::db) const fn new(
        generation: CardinalityGenerationId,
        state: CardinalityGenerationState,
        slot: CardinalityCountSlot,
        source: CardinalitySourceIdentity,
    ) -> Self {
        Self {
            generation,
            state,
            slot,
            source,
        }
    }

    /// Encode the sole current fixed-size header form.
    pub(in crate::db) fn encode(self) -> Vec<u8> {
        let mut out = Vec::with_capacity(CARDINALITY_GENERATION_HEADER_BYTES);
        out.extend_from_slice(CARDINALITY_GENERATION_HEADER_MAGIC);
        out.push(CARDINALITY_FORMAT_VERSION_CURRENT);
        out.push(self.state.to_tag());
        out.push(self.slot.to_tag());
        out.extend_from_slice(&self.generation.get().to_be_bytes());
        encode_source_identity(&mut out, self.source);
        debug_assert_eq!(out.len(), CARDINALITY_GENERATION_HEADER_BODY_BYTES);
        let fingerprint = fingerprint(CARDINALITY_GENERATION_HEADER_FINGERPRINT_DOMAIN, &out);
        out.extend_from_slice(&fingerprint);
        out
    }

    /// Decode one bounded current header or fail closed.
    pub(in crate::db) fn decode(bytes: &[u8]) -> Result<Self, InternalError> {
        if bytes.len() != CARDINALITY_GENERATION_HEADER_BYTES {
            return Err(InternalError::store_corruption());
        }
        validate_magic_and_version(bytes, *CARDINALITY_GENERATION_HEADER_MAGIC)?;
        validate_record_fingerprint(
            bytes,
            CARDINALITY_GENERATION_HEADER_BODY_BYTES,
            CARDINALITY_GENERATION_HEADER_FINGERPRINT_DOMAIN,
        )?;
        let mut reader = CardinalityReader::new(&bytes[..CARDINALITY_GENERATION_HEADER_BODY_BYTES]);
        reader.read_exact::<8>()?;
        reader.read_u8()?;
        let state = CardinalityGenerationState::from_tag(reader.read_u8()?)?;
        let slot = CardinalityCountSlot::from_tag(reader.read_u8()?)?;
        let generation = CardinalityGenerationId::try_new(reader.read_u64()?)?;
        let source = decode_source_identity(&mut reader)?;
        reader.finish()?;
        Ok(Self::new(generation, state, slot, source))
    }

    /// Compare every source dimension without guessing or repairing stale evidence.
    pub(in crate::db) fn validate_source(
        self,
        expected: CardinalitySourceIdentity,
    ) -> Result<(), CardinalitySourceMismatch> {
        if self.source.database_incarnation != expected.database_incarnation {
            return Err(CardinalitySourceMismatch::DatabaseIncarnation);
        }
        if self.source.store_allocation_fingerprint != expected.store_allocation_fingerprint {
            return Err(CardinalitySourceMismatch::StoreAllocation);
        }
        if self.source.accepted_root != expected.accepted_root {
            return Err(CardinalitySourceMismatch::AcceptedRoot);
        }
        if self.source.accepted_index_count != expected.accepted_index_count
            || self.source.accepted_index_set_fingerprint != expected.accepted_index_set_fingerprint
        {
            return Err(CardinalitySourceMismatch::AcceptedIndexSet);
        }
        if self.source.fold_watermark != expected.fold_watermark {
            return Err(CardinalitySourceMismatch::FoldWatermark);
        }
        Ok(())
    }
}

/// Bounded physical scan phase for an isolated Building generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum CardinalityBuildPhase {
    Rows,
    Indexes,
}

impl CardinalityBuildPhase {
    const fn to_tag(self) -> u8 {
        match self {
            Self::Rows => 1,
            Self::Indexes => 2,
        }
    }

    fn from_tag(tag: u8) -> Result<Self, InternalError> {
        match tag {
            1 => Ok(Self::Rows),
            2 => Ok(Self::Indexes),
            _ => Err(InternalError::store_corruption()),
        }
    }
}

/// Last completely processed physical key for one bounded build phase.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) enum CardinalityBuildCheckpoint {
    Row(RawDataStoreKey),
    Index(RawIndexStoreKey),
}

/// Cumulative work facts persisted with a build cursor.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(in crate::db) struct CardinalityBuildTotals {
    source_entries: u64,
    source_bytes: u64,
    prefix_updates: u64,
    distinct_count_keys: u64,
}

impl CardinalityBuildTotals {
    #[must_use]
    pub(in crate::db) const fn new(
        source_entries: u64,
        source_bytes: u64,
        prefix_updates: u64,
        distinct_count_keys: u64,
    ) -> Self {
        Self {
            source_entries,
            source_bytes,
            prefix_updates,
            distinct_count_keys,
        }
    }
}

/// Checksummed bounded checkpoint for optional populated-store construction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct CardinalityBuildCursor {
    generation: CardinalityGenerationId,
    slot: CardinalityCountSlot,
    source: CardinalitySourceIdentity,
    phase: CardinalityBuildPhase,
    checkpoint: Option<CardinalityBuildCheckpoint>,
    totals: CardinalityBuildTotals,
}

impl CardinalityBuildCursor {
    /// Construct a cursor whose checkpoint belongs to its declared phase.
    pub(in crate::db) fn new(
        generation: CardinalityGenerationId,
        slot: CardinalityCountSlot,
        source: CardinalitySourceIdentity,
        phase: CardinalityBuildPhase,
        checkpoint: Option<CardinalityBuildCheckpoint>,
        totals: CardinalityBuildTotals,
    ) -> Result<Self, InternalError> {
        if !checkpoint_matches_phase(checkpoint.as_ref(), phase) {
            return Err(InternalError::store_invariant());
        }
        Ok(Self {
            generation,
            slot,
            source,
            phase,
            checkpoint,
            totals,
        })
    }

    /// Encode one cursor while enforcing the frozen 20-KiB bound.
    pub(in crate::db) fn encode(&self) -> Result<Vec<u8>, InternalError> {
        let checkpoint_bytes = match self.checkpoint.as_ref() {
            None => &[][..],
            Some(CardinalityBuildCheckpoint::Row(key)) => key.as_bytes(),
            Some(CardinalityBuildCheckpoint::Index(key)) => key.as_bytes(),
        };
        let checkpoint_len = u32::try_from(checkpoint_bytes.len())
            .map_err(|_| InternalError::store_unsupported())?;
        let capacity = CARDINALITY_BUILD_CURSOR_FIXED_BODY_BYTES
            .checked_add(checkpoint_bytes.len())
            .and_then(|value| value.checked_add(CARDINALITY_FINGERPRINT_BYTES))
            .ok_or_else(InternalError::store_unsupported)?;
        if capacity > MAX_CARDINALITY_BUILD_CURSOR_BYTES {
            return Err(InternalError::store_unsupported());
        }
        let mut out = Vec::with_capacity(capacity);
        out.extend_from_slice(CARDINALITY_BUILD_CURSOR_MAGIC);
        out.push(CARDINALITY_FORMAT_VERSION_CURRENT);
        out.push(self.slot.to_tag());
        out.extend_from_slice(&self.generation.get().to_be_bytes());
        encode_source_identity(&mut out, self.source);
        out.push(self.phase.to_tag());
        out.extend_from_slice(&self.totals.source_entries.to_be_bytes());
        out.extend_from_slice(&self.totals.source_bytes.to_be_bytes());
        out.extend_from_slice(&self.totals.prefix_updates.to_be_bytes());
        out.extend_from_slice(&self.totals.distinct_count_keys.to_be_bytes());
        out.push(match self.checkpoint {
            None => 0,
            Some(CardinalityBuildCheckpoint::Row(_)) => 1,
            Some(CardinalityBuildCheckpoint::Index(_)) => 2,
        });
        out.extend_from_slice(&checkpoint_len.to_be_bytes());
        out.extend_from_slice(checkpoint_bytes);
        let fingerprint = fingerprint(CARDINALITY_BUILD_CURSOR_FINGERPRINT_DOMAIN, &out);
        out.extend_from_slice(&fingerprint);
        Ok(out)
    }

    /// Decode one current bounded cursor without accepting predecessor forms.
    pub(in crate::db) fn decode(bytes: &[u8]) -> Result<Self, InternalError> {
        if bytes.len() < CARDINALITY_BUILD_CURSOR_FIXED_BODY_BYTES + CARDINALITY_FINGERPRINT_BYTES
            || bytes.len() > MAX_CARDINALITY_BUILD_CURSOR_BYTES
        {
            return Err(InternalError::store_corruption());
        }
        validate_magic_and_version(bytes, *CARDINALITY_BUILD_CURSOR_MAGIC)?;
        let body_len = bytes
            .len()
            .checked_sub(CARDINALITY_FINGERPRINT_BYTES)
            .ok_or_else(InternalError::store_corruption)?;
        validate_record_fingerprint(bytes, body_len, CARDINALITY_BUILD_CURSOR_FINGERPRINT_DOMAIN)?;
        let mut reader = CardinalityReader::new(&bytes[..body_len]);
        reader.read_exact::<8>()?;
        reader.read_u8()?;
        let slot = CardinalityCountSlot::from_tag(reader.read_u8()?)?;
        let generation = CardinalityGenerationId::try_new(reader.read_u64()?)?;
        let source = decode_source_identity(&mut reader)?;
        let phase = CardinalityBuildPhase::from_tag(reader.read_u8()?)?;
        let totals = CardinalityBuildTotals::new(
            reader.read_u64()?,
            reader.read_u64()?,
            reader.read_u64()?,
            reader.read_u64()?,
        );
        let checkpoint_tag = reader.read_u8()?;
        let checkpoint_len =
            usize::try_from(reader.read_u32()?).map_err(|_| InternalError::store_corruption())?;
        let checkpoint_bytes = reader.read_bytes(checkpoint_len)?;
        let checkpoint = decode_checkpoint(checkpoint_tag, checkpoint_bytes)?;
        reader.finish()?;
        Self::new(generation, slot, source, phase, checkpoint, totals)
            .map_err(|_| InternalError::store_corruption())
    }
}

/// Full digest identifying one entity or accepted user-index prefix count.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct CardinalityCountDigest([u8; 32]);

impl CardinalityCountDigest {
    #[must_use]
    pub(in crate::db) const fn as_bytes(self) -> [u8; 32] {
        self.0
    }
}

/// Logical count target from which the durable truncated key is derived.
pub(in crate::db) enum CardinalityLogicalCountKey<'a> {
    Entity(EntityTag),
    UserIndexPrefix(&'a UserIndexPrefixCardinalityKey),
}

impl CardinalityLogicalCountKey<'_> {
    /// Hash the exact frozen logical-key preimage.
    pub(in crate::db) fn digest(&self) -> Result<CardinalityCountDigest, InternalError> {
        let mut hasher = Sha256::new();
        hasher.update(CARDINALITY_COUNT_KEY_FINGERPRINT_DOMAIN);
        match self {
            Self::Entity(entity) => {
                hasher.update([0]);
                hasher.update(entity.value().to_be_bytes());
            }
            Self::UserIndexPrefix(prefix) => {
                let components = prefix.prefix_components();
                if components.is_empty() || components.len() > MAX_INDEX_FIELDS {
                    return Err(InternalError::store_invariant());
                }
                hasher.update([1]);
                hasher.update(prefix.index_id().entity_tag().value().to_be_bytes());
                hasher.update(prefix.index_id().to_bytes());
                hasher
                    .update([u8::try_from(components.len())
                        .map_err(|_| InternalError::store_invariant())?]);
                for component in components {
                    if component.is_empty() || component.len() > IndexKey::MAX_COMPONENT_SIZE {
                        return Err(InternalError::store_invariant());
                    }
                    let len = u32::try_from(component.len())
                        .map_err(|_| InternalError::store_invariant())?;
                    hasher.update(len.to_be_bytes());
                    hasher.update(component);
                }
            }
        }
        Ok(CardinalityCountDigest(hasher.finalize().into()))
    }
}

/// Fixed-size durable nonzero count record stored in slot namespace `0x06` or `0x07`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct CardinalityCountRecord {
    generation: CardinalityGenerationId,
    digest: CardinalityCountDigest,
    count: u64,
}

impl CardinalityCountRecord {
    /// Construct one stored nonzero count. Exact zero is represented by absence.
    pub(in crate::db) fn new(
        generation: CardinalityGenerationId,
        digest: CardinalityCountDigest,
        count: u64,
    ) -> Result<Self, InternalError> {
        if count == 0 {
            return Err(InternalError::store_invariant());
        }
        Ok(Self {
            generation,
            digest,
            count,
        })
    }

    #[must_use]
    pub(in crate::db) fn encode(self) -> [u8; CARDINALITY_COUNT_RECORD_BYTES] {
        let mut out = [0_u8; CARDINALITY_COUNT_RECORD_BYTES];
        out[..8].copy_from_slice(CARDINALITY_COUNT_RECORD_MAGIC);
        out[8] = CARDINALITY_FORMAT_VERSION_CURRENT;
        out[9..17].copy_from_slice(&self.generation.get().to_be_bytes());
        out[17..49].copy_from_slice(&self.digest.as_bytes());
        out[49..].copy_from_slice(&self.count.to_be_bytes());
        out
    }

    /// Decode the sole fixed current count form.
    pub(in crate::db) fn decode(bytes: &[u8]) -> Result<Self, InternalError> {
        if bytes.len() != CARDINALITY_COUNT_RECORD_BYTES {
            return Err(InternalError::store_corruption());
        }
        validate_magic_and_version(bytes, *CARDINALITY_COUNT_RECORD_MAGIC)?;
        let mut generation = [0_u8; 8];
        generation.copy_from_slice(&bytes[9..17]);
        let generation = CardinalityGenerationId::try_new(u64::from_be_bytes(generation))?;
        let mut digest = [0_u8; 32];
        digest.copy_from_slice(&bytes[17..49]);
        let mut count = [0_u8; 8];
        count.copy_from_slice(&bytes[49..]);
        Self::new(
            generation,
            CardinalityCountDigest(digest),
            u64::from_be_bytes(count),
        )
        .map_err(|_| InternalError::store_corruption())
    }

    /// Prove that a truncated stable key resolved to the requested generation and digest.
    pub(in crate::db) fn validate_identity(
        self,
        generation: CardinalityGenerationId,
        digest: CardinalityCountDigest,
    ) -> Result<u64, CardinalityCountRecordMismatch> {
        if self.generation != generation {
            return Err(CardinalityCountRecordMismatch::Generation);
        }
        if self.digest != digest {
            return Err(CardinalityCountRecordMismatch::DigestCollision);
        }
        Ok(self.count)
    }
}

/// Typed count-record mismatch used to make truncated-key collisions unavailable.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum CardinalityCountRecordMismatch {
    Generation,
    DigestCollision,
}

fn encode_source_identity(out: &mut Vec<u8>, source: CardinalitySourceIdentity) {
    out.extend_from_slice(&source.database_incarnation.to_bytes());
    out.extend_from_slice(&source.store_allocation_fingerprint);
    if let Some(root) = source.accepted_root {
        out.push(1);
        out.extend_from_slice(&root.revision.get().to_be_bytes());
        out.extend_from_slice(&root.fingerprint.as_bytes());
    } else {
        out.push(0);
        out.extend_from_slice(&0_u64.to_be_bytes());
        out.extend_from_slice(&[0; 32]);
    }
    out.extend_from_slice(&source.accepted_index_count.to_be_bytes());
    out.extend_from_slice(&source.accepted_index_set_fingerprint);
    out.extend_from_slice(
        &source
            .fold_watermark
            .highest_folded_journal_sequence()
            .get()
            .to_be_bytes(),
    );
    out.extend_from_slice(&source.fold_watermark.fold_epoch().to_be_bytes());
}

fn decode_source_identity(
    reader: &mut CardinalityReader<'_>,
) -> Result<CardinalitySourceIdentity, InternalError> {
    let database_incarnation = DatabaseIncarnationId::try_from_bytes(reader.read_exact::<16>()?)?;
    let store_allocation_fingerprint = reader.read_exact::<32>()?;
    let root_present = reader.read_u8()?;
    let root_revision = AcceptedSchemaRevision::new(reader.read_u64()?);
    let root_fingerprint = AcceptedSchemaFingerprint::new(reader.read_exact::<32>()?);
    let accepted_root = match root_present {
        0 if root_revision == AcceptedSchemaRevision::NONE
            && root_fingerprint.as_bytes() == [0; 32] =>
        {
            None
        }
        1 => Some(
            CardinalityAcceptedRootIdentity::new(root_revision, root_fingerprint)
                .map_err(|_| InternalError::store_corruption())?,
        ),
        _ => return Err(InternalError::store_corruption()),
    };
    let accepted_index_count = reader.read_u32()?;
    let accepted_index_set_fingerprint = reader.read_exact::<32>()?;
    if accepted_root.is_none() && accepted_index_count != 0 {
        return Err(InternalError::store_corruption());
    }
    let highest_folded = crate::db::journal::JournalSequence::new(reader.read_u64()?);
    let fold_epoch = reader.read_u64()?;
    Ok(CardinalitySourceIdentity {
        database_incarnation,
        store_allocation_fingerprint,
        accepted_root,
        accepted_index_count,
        accepted_index_set_fingerprint,
        fold_watermark: FoldWatermark::new(highest_folded, fold_epoch),
    })
}

fn store_allocation_fingerprint(
    allocations: StoreAllocationIdentities,
) -> Result<[u8; 32], InternalError> {
    let identities = [
        allocations.data(),
        allocations.index(),
        allocations.schema(),
        allocations.journal(),
    ];
    let mut hasher = Sha256::new();
    hasher.update(CARDINALITY_STORE_ALLOCATION_FINGERPRINT_DOMAIN);
    for (role, identity) in identities.into_iter().enumerate() {
        let identity = identity.ok_or_else(InternalError::store_invariant)?;
        hasher.update([
            u8::try_from(role).map_err(|_| InternalError::store_invariant())?,
            identity.memory_id(),
        ]);
        let stable_key = identity.stable_key().as_bytes();
        let stable_key_len =
            u32::try_from(stable_key.len()).map_err(|_| InternalError::store_unsupported())?;
        hasher.update(stable_key_len.to_be_bytes());
        hasher.update(stable_key);
    }
    Ok(hasher.finalize().into())
}

fn accepted_index_set_fingerprint(
    accepted_indexes: impl IntoIterator<Item = IndexId>,
) -> Result<(u32, [u8; 32]), InternalError> {
    let mut indexes = BTreeSet::new();
    for index in accepted_indexes {
        if !indexes.insert(index) {
            return Err(InternalError::store_invariant());
        }
    }
    let count = u32::try_from(indexes.len()).map_err(|_| InternalError::store_unsupported())?;
    let mut hasher = Sha256::new();
    hasher.update(CARDINALITY_ACCEPTED_INDEX_SET_FINGERPRINT_DOMAIN);
    hasher.update(count.to_be_bytes());
    for index in indexes {
        hasher.update(index.to_bytes());
    }
    Ok((count, hasher.finalize().into()))
}

fn decode_checkpoint(
    tag: u8,
    bytes: &[u8],
) -> Result<Option<CardinalityBuildCheckpoint>, InternalError> {
    match tag {
        0 if bytes.is_empty() => Ok(None),
        1 if !bytes.is_empty() && bytes.len() <= RawDataStoreKey::MAX_STORED_SIZE_USIZE => {
            Ok(Some(CardinalityBuildCheckpoint::Row(
                RawDataStoreKey::from_persisted_bytes(bytes.to_vec()),
            )))
        }
        2 if bytes.len() >= IndexKey::MIN_STORED_SIZE_USIZE
            && bytes.len() <= IndexKey::MAX_STORED_SIZE_USIZE =>
        {
            Ok(Some(CardinalityBuildCheckpoint::Index(
                RawIndexStoreKey::from_persisted_bytes(bytes.to_vec()),
            )))
        }
        _ => Err(InternalError::store_corruption()),
    }
}

const fn checkpoint_matches_phase(
    checkpoint: Option<&CardinalityBuildCheckpoint>,
    phase: CardinalityBuildPhase,
) -> bool {
    matches!(
        (checkpoint, phase),
        (None, _)
            | (
                Some(CardinalityBuildCheckpoint::Row(_)),
                CardinalityBuildPhase::Rows
            )
            | (
                Some(CardinalityBuildCheckpoint::Index(_)),
                CardinalityBuildPhase::Indexes
            )
    )
}

fn validate_magic_and_version(bytes: &[u8], magic: [u8; 8]) -> Result<(), InternalError> {
    let encoded_magic = bytes
        .get(..magic.len())
        .ok_or_else(InternalError::store_corruption)?;
    if encoded_magic != magic {
        return Err(InternalError::store_corruption());
    }
    let version = *bytes
        .get(magic.len())
        .ok_or_else(InternalError::store_corruption)?;
    if version != CARDINALITY_FORMAT_VERSION_CURRENT {
        return Err(InternalError::serialize_incompatible_persisted_format());
    }
    Ok(())
}

fn validate_record_fingerprint(
    bytes: &[u8],
    body_len: usize,
    domain: &[u8],
) -> Result<(), InternalError> {
    let body = bytes
        .get(..body_len)
        .ok_or_else(InternalError::store_corruption)?;
    let stored = bytes
        .get(body_len..)
        .ok_or_else(InternalError::store_corruption)?;
    if stored.len() != CARDINALITY_FINGERPRINT_BYTES || stored != fingerprint(domain, body) {
        return Err(InternalError::store_corruption());
    }
    Ok(())
}

fn fingerprint(domain: &[u8], bytes: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(domain);
    hasher.update(bytes);
    hasher.finalize().into()
}

struct CardinalityReader<'a> {
    bytes: &'a [u8],
    cursor: usize,
}

impl<'a> CardinalityReader<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, cursor: 0 }
    }

    fn read_u8(&mut self) -> Result<u8, InternalError> {
        let value = *self
            .bytes
            .get(self.cursor)
            .ok_or_else(InternalError::store_corruption)?;
        self.cursor = self
            .cursor
            .checked_add(1)
            .ok_or_else(InternalError::store_corruption)?;
        Ok(value)
    }

    fn read_u32(&mut self) -> Result<u32, InternalError> {
        Ok(u32::from_be_bytes(self.read_exact::<4>()?))
    }

    fn read_u64(&mut self) -> Result<u64, InternalError> {
        Ok(u64::from_be_bytes(self.read_exact::<8>()?))
    }

    fn read_exact<const N: usize>(&mut self) -> Result<[u8; N], InternalError> {
        let bytes = self.read_bytes(N)?;
        let mut out = [0_u8; N];
        out.copy_from_slice(bytes);
        Ok(out)
    }

    fn read_bytes(&mut self, len: usize) -> Result<&'a [u8], InternalError> {
        let end = self
            .cursor
            .checked_add(len)
            .ok_or_else(InternalError::store_corruption)?;
        let bytes = self
            .bytes
            .get(self.cursor..end)
            .ok_or_else(InternalError::store_corruption)?;
        self.cursor = end;
        Ok(bytes)
    }

    fn finish(self) -> Result<(), InternalError> {
        if self.cursor != self.bytes.len() {
            return Err(InternalError::store_corruption());
        }
        Ok(())
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        db::{
            index::IndexId,
            registry::StoreAllocationIdentity,
            schema::enum_catalog::{AcceptedSchemaFingerprint, AcceptedSchemaRevision},
        },
        error::ErrorClass,
    };

    fn allocations(offset: u8) -> StoreAllocationIdentities {
        StoreAllocationIdentities::new_journaled(
            StoreAllocationIdentity::new(100 + offset, "test.cardinality.data.v1"),
            StoreAllocationIdentity::new(101 + offset, "test.cardinality.index.v1"),
            StoreAllocationIdentity::new(102 + offset, "test.cardinality.schema.v1"),
            StoreAllocationIdentity::new(103 + offset, "test.cardinality.journal.v1"),
        )
    }

    fn source(seed: u8) -> CardinalitySourceIdentity {
        CardinalitySourceIdentity::derive(
            DatabaseIncarnationId::for_tests(seed),
            allocations(seed),
            Some(
                CardinalityAcceptedRootIdentity::new(
                    AcceptedSchemaRevision::new(u64::from(seed) + 1),
                    AcceptedSchemaFingerprint::new([seed; 32]),
                )
                .unwrap(),
            ),
            [
                IndexId::new_with_generation(EntityTag::new(7), 2, 9),
                IndexId::new_with_generation(EntityTag::new(7), 1, 9),
            ],
            FoldWatermark::new(
                crate::db::journal::JournalSequence::new(u64::from(seed) + 3),
                u64::from(seed) + 4,
            ),
        )
        .unwrap()
    }

    #[test]
    fn generation_header_round_trips_the_complete_current_identity() {
        let header = CardinalityGenerationHeader::new(
            CardinalityGenerationId::INITIAL,
            CardinalityGenerationState::Building,
            CardinalityCountSlot::B,
            source(1),
        );
        let encoded = header.encode();
        assert_eq!(encoded.len(), 192);
        assert!(encoded.len() <= 256);
        assert_eq!(
            CardinalityGenerationHeader::decode(&encoded).unwrap(),
            header
        );
        assert_eq!(header.validate_source(source(1)), Ok(()));
    }

    #[test]
    fn generation_header_rejects_noncurrent_and_malformed_forms() {
        let encoded = CardinalityGenerationHeader::new(
            CardinalityGenerationId::INITIAL,
            CardinalityGenerationState::Ready,
            CardinalityCountSlot::A,
            source(2),
        )
        .encode();
        assert!(CardinalityGenerationHeader::decode(&encoded[..encoded.len() - 1]).is_err());
        let mut trailing = encoded.clone();
        trailing.push(0);
        assert!(CardinalityGenerationHeader::decode(&trailing).is_err());
        let mut future = encoded.clone();
        future[8] = 2;
        assert_eq!(
            CardinalityGenerationHeader::decode(&future)
                .unwrap_err()
                .class(),
            ErrorClass::IncompatiblePersistedFormat,
        );
        let mut corrupt = encoded;
        corrupt[40] ^= 0x80;
        assert!(CardinalityGenerationHeader::decode(&corrupt).is_err());
    }

    #[test]
    fn source_validation_reports_each_stale_identity_dimension() {
        let header = CardinalityGenerationHeader::new(
            CardinalityGenerationId::INITIAL,
            CardinalityGenerationState::Ready,
            CardinalityCountSlot::A,
            source(3),
        );
        let mut expected = source(3);
        expected.database_incarnation = DatabaseIncarnationId::for_tests(99);
        assert_eq!(
            header.validate_source(expected),
            Err(CardinalitySourceMismatch::DatabaseIncarnation)
        );
        let mut expected = source(3);
        expected.store_allocation_fingerprint[0] ^= 1;
        assert_eq!(
            header.validate_source(expected),
            Err(CardinalitySourceMismatch::StoreAllocation)
        );
        let mut expected = source(3);
        expected.accepted_root = None;
        assert_eq!(
            header.validate_source(expected),
            Err(CardinalitySourceMismatch::AcceptedRoot)
        );
        let mut expected = source(3);
        expected.accepted_index_count += 1;
        assert_eq!(
            header.validate_source(expected),
            Err(CardinalitySourceMismatch::AcceptedIndexSet)
        );
        let mut expected = source(3);
        expected.fold_watermark = FoldWatermark::initial();
        assert_eq!(
            header.validate_source(expected),
            Err(CardinalitySourceMismatch::FoldWatermark)
        );
    }

    #[test]
    fn allocation_and_index_set_fingerprints_are_canonical_and_strict() {
        assert_ne!(
            store_allocation_fingerprint(allocations(0)).unwrap(),
            store_allocation_fingerprint(allocations(1)).unwrap()
        );
        let first = IndexId::new_with_generation(EntityTag::new(1), 2, 3);
        let second = IndexId::new_with_generation(EntityTag::new(1), 1, 3);
        assert_eq!(
            accepted_index_set_fingerprint([first, second]).unwrap(),
            accepted_index_set_fingerprint([second, first]).unwrap()
        );
        assert!(accepted_index_set_fingerprint([first, first]).is_err());
        assert!(
            CardinalitySourceIdentity::derive(
                DatabaseIncarnationId::for_tests(1),
                allocations(0),
                None,
                [first],
                FoldWatermark::initial(),
            )
            .is_err()
        );
    }

    #[test]
    fn count_key_and_value_forms_are_exact_and_collision_checked() {
        let entity_digest = CardinalityLogicalCountKey::Entity(EntityTag::new(42))
            .digest()
            .unwrap();
        assert_eq!(
            entity_digest.as_bytes(),
            [
                0x41, 0x00, 0x22, 0x00, 0x2d, 0xea, 0x85, 0xb1, 0xac, 0x3d, 0x8a, 0x83, 0x2b, 0x54,
                0x68, 0x37, 0x7c, 0x6e, 0xdf, 0x17, 0x2d, 0xc2, 0x9a, 0x37, 0x10, 0x64, 0x85, 0x85,
                0xcf, 0x6d, 0x2a, 0x23,
            ],
            "entity count-key preimage is persisted format",
        );
        let prefix = UserIndexPrefixCardinalityKey::new(
            IndexId::new_with_generation(EntityTag::new(42), 3, 7),
            vec![vec![1, 2], vec![3]],
        );
        let prefix_digest = CardinalityLogicalCountKey::UserIndexPrefix(&prefix)
            .digest()
            .unwrap();
        assert_eq!(
            prefix_digest.as_bytes(),
            [
                0xe1, 0xb8, 0x75, 0xac, 0x03, 0x55, 0x4c, 0x2f, 0x1c, 0xce, 0xdb, 0xc0, 0x78, 0x09,
                0x42, 0x42, 0x94, 0x9a, 0xae, 0xa3, 0x59, 0x47, 0x71, 0x41, 0xcd, 0x66, 0xe9, 0xe9,
                0xa6, 0xfc, 0xf9, 0xd7,
            ],
            "user-index prefix count-key preimage is persisted format",
        );
        assert_ne!(entity_digest, prefix_digest);
        let record =
            CardinalityCountRecord::new(CardinalityGenerationId::INITIAL, prefix_digest, u64::MAX)
                .unwrap();
        let encoded = record.encode();
        assert_eq!(encoded.len(), 57);
        assert!(encoded.len() <= 64);
        assert_eq!(CardinalityCountRecord::decode(&encoded).unwrap(), record);
        assert!(CardinalityCountRecord::decode(&encoded[..encoded.len() - 1]).is_err());
        let mut trailing = encoded.to_vec();
        trailing.push(0);
        assert!(CardinalityCountRecord::decode(&trailing).is_err());
        let mut future = encoded;
        future[8] = 2;
        assert_eq!(
            CardinalityCountRecord::decode(&future).unwrap_err().class(),
            ErrorClass::IncompatiblePersistedFormat,
        );
        assert_eq!(
            record.validate_identity(CardinalityGenerationId::INITIAL, prefix_digest),
            Ok(u64::MAX)
        );
        assert_eq!(
            record.validate_identity(CardinalityGenerationId::try_new(2).unwrap(), prefix_digest),
            Err(CardinalityCountRecordMismatch::Generation)
        );
        assert_eq!(
            record.validate_identity(CardinalityGenerationId::INITIAL, entity_digest),
            Err(CardinalityCountRecordMismatch::DigestCollision)
        );
        assert!(
            CardinalityCountRecord::new(CardinalityGenerationId::INITIAL, entity_digest, 0)
                .is_err()
        );
    }

    #[test]
    fn count_key_rejects_empty_oversized_and_overwide_prefixes() {
        let index = IndexId::new_with_generation(EntityTag::new(1), 1, 1);
        for components in [
            Vec::new(),
            vec![Vec::new()],
            vec![vec![0; IndexKey::MAX_COMPONENT_SIZE + 1]],
            vec![vec![1], vec![2], vec![3], vec![4], vec![5]],
        ] {
            let prefix = UserIndexPrefixCardinalityKey::new(index, components);
            assert!(
                CardinalityLogicalCountKey::UserIndexPrefix(&prefix)
                    .digest()
                    .is_err()
            );
        }
    }

    #[test]
    fn build_cursor_round_trips_maximum_row_and_index_checkpoints() {
        let row = CardinalityBuildCursor::new(
            CardinalityGenerationId::INITIAL,
            CardinalityCountSlot::A,
            source(4),
            CardinalityBuildPhase::Rows,
            Some(CardinalityBuildCheckpoint::Row(
                RawDataStoreKey::from_persisted_bytes(vec![
                    1;
                    RawDataStoreKey::MAX_STORED_SIZE_USIZE
                ]),
            )),
            CardinalityBuildTotals::new(1, 2, 3, 4),
        )
        .unwrap();
        let row_encoded = row.encode().unwrap();
        assert_eq!(CardinalityBuildCursor::decode(&row_encoded).unwrap(), row);

        let index = CardinalityBuildCursor::new(
            CardinalityGenerationId::try_new(2).unwrap(),
            CardinalityCountSlot::B,
            source(5),
            CardinalityBuildPhase::Indexes,
            Some(CardinalityBuildCheckpoint::Index(
                RawIndexStoreKey::from_persisted_bytes(vec![1; IndexKey::MAX_STORED_SIZE_USIZE]),
            )),
            CardinalityBuildTotals::new(u64::MAX, u64::MAX, u64::MAX, u64::MAX),
        )
        .unwrap();
        let index_encoded = index.encode().unwrap();
        assert!(index_encoded.len() <= MAX_CARDINALITY_BUILD_CURSOR_BYTES);
        assert_eq!(
            CardinalityBuildCursor::decode(&index_encoded).unwrap(),
            index
        );
    }

    #[test]
    fn build_cursor_rejects_phase_mismatch_corruption_and_noncurrent_version() {
        assert!(
            CardinalityBuildCursor::new(
                CardinalityGenerationId::INITIAL,
                CardinalityCountSlot::A,
                source(6),
                CardinalityBuildPhase::Rows,
                Some(CardinalityBuildCheckpoint::Index(
                    RawIndexStoreKey::from_persisted_bytes(vec![
                        1;
                        IndexKey::MIN_STORED_SIZE_USIZE
                    ]),
                )),
                CardinalityBuildTotals::default(),
            )
            .is_err()
        );
        let cursor = CardinalityBuildCursor::new(
            CardinalityGenerationId::INITIAL,
            CardinalityCountSlot::A,
            source(6),
            CardinalityBuildPhase::Rows,
            None,
            CardinalityBuildTotals::default(),
        )
        .unwrap();
        let encoded = cursor.encode().unwrap();
        assert!(CardinalityBuildCursor::decode(&encoded[..encoded.len() - 1]).is_err());
        let mut future = encoded.clone();
        future[8] = 2;
        assert_eq!(
            CardinalityBuildCursor::decode(&future).unwrap_err().class(),
            ErrorClass::IncompatiblePersistedFormat,
        );
        let mut corrupt = encoded;
        corrupt[50] ^= 1;
        assert!(CardinalityBuildCursor::decode(&corrupt).is_err());
    }

    #[test]
    fn generation_identity_and_count_arithmetic_are_checked() {
        assert!(CardinalityGenerationId::try_new(0).is_err());
        assert_eq!(
            CardinalityGenerationId::INITIAL
                .checked_next()
                .unwrap()
                .get(),
            2
        );
        assert!(
            CardinalityGenerationId::try_new(u64::MAX)
                .unwrap()
                .checked_next()
                .is_err()
        );
    }
}
