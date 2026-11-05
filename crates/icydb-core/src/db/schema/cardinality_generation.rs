//! Module: db::schema::cardinality_generation
//! Responsibility: current durable exact-cardinality identity and record codecs.
//! Does not own: populated construction, publication, incremental maintenance, or planning.
//! Boundary: accepted source identities -> bounded version-1 schema-allocation records.

use crate::{
    MAX_INDEX_FIELDS,
    db::{
        data::RawDataStoreKey,
        index::{IndexId, IndexKey, RawIndexStoreKey},
        integrity::DatabaseIncarnationId,
        journal::FoldWatermark,
        registry::StoreAllocationIdentities,
        schema::{
            enum_catalog::{AcceptedSchemaFingerprint, AcceptedSchemaRevision, AcceptedSchemaRoot},
            wire::SchemaWireReader,
        },
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
    /// Select the other reusable isolated count namespace.
    #[must_use]
    pub(in crate::db) const fn alternate(self) -> Self {
        match self {
            Self::A => Self::B,
            Self::B => Self::A,
        }
    }

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

    #[must_use]
    pub(in crate::db) fn matches(self, root: AcceptedSchemaRoot) -> bool {
        self.revision == root.revision() && self.fingerprint == root.fingerprint()
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

/// Canonical identity of the complete accepted physical user-index set.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct CardinalityAcceptedIndexSetIdentity {
    count: u32,
    fingerprint: [u8; 32],
}

/// Canonical identity of one journaled store's complete allocation bundle.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct CardinalityStoreAllocationIdentity([u8; 32]);

impl CardinalityStoreAllocationIdentity {
    pub(in crate::db) fn derive(
        allocations: StoreAllocationIdentities,
    ) -> Result<Self, InternalError> {
        store_allocation_fingerprint(allocations).map(Self)
    }
}

impl CardinalityAcceptedIndexSetIdentity {
    pub(in crate::db) fn derive(
        accepted_indexes: impl IntoIterator<Item = IndexId>,
    ) -> Result<Self, InternalError> {
        let (count, fingerprint) = accepted_index_set_fingerprint(accepted_indexes)?;
        Ok(Self { count, fingerprint })
    }
}

impl CardinalitySourceIdentity {
    /// Bind one already-validated immutable accepted-index-set identity.
    pub(in crate::db) fn derive_with_bound_identities(
        database_incarnation: DatabaseIncarnationId,
        store_allocation: CardinalityStoreAllocationIdentity,
        accepted_root: Option<CardinalityAcceptedRootIdentity>,
        accepted_index_set: CardinalityAcceptedIndexSetIdentity,
        fold_watermark: FoldWatermark,
    ) -> Result<Self, InternalError> {
        if accepted_root.is_none() && accepted_index_set.count != 0 {
            return Err(InternalError::store_invariant());
        }
        Ok(Self {
            database_incarnation,
            store_allocation_fingerprint: store_allocation.0,
            accepted_root,
            accepted_index_count: accepted_index_set.count,
            accepted_index_set_fingerprint: accepted_index_set.fingerprint,
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

    #[must_use]
    pub(in crate::db) const fn generation(self) -> CardinalityGenerationId {
        self.generation
    }

    #[must_use]
    pub(in crate::db) const fn state(self) -> CardinalityGenerationState {
        self.state
    }

    #[must_use]
    pub(in crate::db) const fn slot(self) -> CardinalityCountSlot {
        self.slot
    }

    #[must_use]
    pub(in crate::db) const fn source(self) -> CardinalitySourceIdentity {
        self.source
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
        let mut reader = SchemaWireReader::new(&bytes[..CARDINALITY_GENERATION_HEADER_BODY_BYTES]);
        reader.read_array::<8>()?;
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

    /// Add one fully validated page and its newly materialized logical keys.
    pub(in crate::db) fn checked_add_page(
        self,
        source_entries: u64,
        source_bytes: u64,
        prefix_updates: u64,
        distinct_count_keys: u64,
    ) -> Result<Self, InternalError> {
        Ok(Self::new(
            self.source_entries
                .checked_add(source_entries)
                .ok_or_else(InternalError::store_unsupported)?,
            self.source_bytes
                .checked_add(source_bytes)
                .ok_or_else(InternalError::store_unsupported)?,
            self.prefix_updates
                .checked_add(prefix_updates)
                .ok_or_else(InternalError::store_unsupported)?,
            self.distinct_count_keys
                .checked_add(distinct_count_keys)
                .ok_or_else(InternalError::store_unsupported)?,
        ))
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

    #[must_use]
    pub(in crate::db) const fn phase(&self) -> CardinalityBuildPhase {
        self.phase
    }

    #[must_use]
    pub(in crate::db) const fn checkpoint(&self) -> Option<&CardinalityBuildCheckpoint> {
        self.checkpoint.as_ref()
    }

    #[must_use]
    pub(in crate::db) const fn totals(&self) -> CardinalityBuildTotals {
        self.totals
    }

    /// Prove that this cursor belongs to the exact current Building header.
    pub(in crate::db) fn validate_header(
        &self,
        header: CardinalityGenerationHeader,
    ) -> Result<(), InternalError> {
        if header.state() != CardinalityGenerationState::Building
            || self.generation != header.generation()
            || self.slot != header.slot()
            || self.source != header.source()
        {
            return Err(InternalError::store_corruption());
        }
        Ok(())
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
        let mut reader = SchemaWireReader::new(&bytes[..body_len]);
        reader.read_array::<8>()?;
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
        let checkpoint_bytes = reader.read_len_prefixed_bytes()?;
        let checkpoint = decode_checkpoint(checkpoint_tag, checkpoint_bytes)?;
        reader.finish()?;
        Self::new(generation, slot, source, phase, checkpoint, totals)
            .map_err(|_| InternalError::store_corruption())
    }
}

/// Full digest identifying one entity or accepted user-index prefix count.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(in crate::db) struct CardinalityCountDigest([u8; 32]);

impl CardinalityCountDigest {
    #[must_use]
    pub(in crate::db) const fn as_bytes(self) -> [u8; 32] {
        self.0
    }

    /// Hash one entity count key from its frozen logical preimage.
    #[must_use]
    pub(in crate::db) fn for_entity(entity: EntityTag) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(CARDINALITY_COUNT_KEY_FINGERPRINT_DOMAIN);
        hasher.update([0]);
        hasher.update(entity.value().to_be_bytes());
        Self(hasher.finalize().into())
    }

    /// Hash one accepted user-index prefix without allocating a second key owner.
    pub(in crate::db) fn for_user_index_prefix(
        index_id: IndexId,
        components: &[impl AsRef<[u8]>],
    ) -> Result<Self, InternalError> {
        if components.is_empty() || components.len() > MAX_INDEX_FIELDS {
            return Err(InternalError::store_invariant());
        }
        let mut hasher = Sha256::new();
        hasher.update(CARDINALITY_COUNT_KEY_FINGERPRINT_DOMAIN);
        hasher.update([1]);
        hasher.update(index_id.entity_tag().value().to_be_bytes());
        hasher.update(index_id.to_bytes());
        hasher.update([
            u8::try_from(components.len()).map_err(|_| InternalError::store_invariant())?
        ]);
        for component in components {
            let component = component.as_ref();
            if component.is_empty() || component.len() > IndexKey::MAX_COMPONENT_SIZE {
                return Err(InternalError::store_invariant());
            }
            let len =
                u32::try_from(component.len()).map_err(|_| InternalError::store_invariant())?;
            hasher.update(len.to_be_bytes());
            hasher.update(component);
        }
        Ok(Self(hasher.finalize().into()))
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
    reader: &mut SchemaWireReader<'_>,
) -> Result<CardinalitySourceIdentity, InternalError> {
    let database_incarnation = DatabaseIncarnationId::try_from_bytes(reader.read_array::<16>()?)?;
    let store_allocation_fingerprint = reader.read_array::<32>()?;
    let root_present = reader.read_u8()?;
    let root_revision = AcceptedSchemaRevision::new(reader.read_u64()?);
    let root_fingerprint = AcceptedSchemaFingerprint::new(reader.read_array::<32>()?);
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
    let accepted_index_set_fingerprint = reader.read_array::<32>()?;
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
        hasher.update([u8::try_from(role).map_err(|_| InternalError::store_invariant())?]);
        hasher.update(identity.memory_id().to_le_bytes());
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

///
/// TESTS
///

#[cfg(test)]
mod tests;

#[cfg(test)]
mod helper_tests;
