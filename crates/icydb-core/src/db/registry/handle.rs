//! Module: db::registry::handle
//! Responsibility: stable store handles and runtime storage capability descriptors.
//! Does not own: registry path lookup or store mutation semantics.
//! Boundary: exposes registered storage roles without exposing registry internals.

use crate::db::{
    commit::database_incarnation_id,
    data::DataStore,
    index::{IndexId, IndexKeyKind, IndexState, IndexStore, UserIndexPrefixCardinalityKey},
    integrity::DatabaseIncarnationId,
    journal::{FoldWatermark, JournalTailStore},
    schema::{
        SchemaStore,
        cardinality_build::CardinalityBuildAuthority,
        cardinality_generation::{
            CardinalityAcceptedRootIdentity, CardinalityCountDigest, CardinalityGenerationState,
            CardinalityStoreAllocationIdentity,
        },
    },
};
use crate::{error::InternalError, types::EntityTag};
use candid::CandidType;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::{cell::RefCell, thread::LocalKey};

///
/// StoreHandle
///
/// StoreHandle binds the row, index, and schema stores for one generated schema
/// `Store` path.
/// It is the stable access token passed across commit, recovery, executor, and
/// diagnostics boundaries instead of exposing registry internals directly.
///

#[derive(Clone, Copy, Debug)]
pub struct StoreHandle {
    data: &'static LocalKey<RefCell<DataStore>>,
    index: &'static LocalKey<RefCell<IndexStore>>,
    schema: &'static LocalKey<RefCell<SchemaStore>>,
    journal: Option<&'static LocalKey<RefCell<JournalTailStore>>>,
    allocations: StoreAllocationIdentities,
    cardinality_allocation: Option<CardinalityStoreAllocationIdentity>,
    capabilities: StoreRuntimeStorageCapabilities,
}

enum ReadyCardinalityCountTargets<'a> {
    Digests(&'a [CardinalityCountDigest]),
    UserIndexPrefixes(&'a [UserIndexPrefixCardinalityKey]),
}

enum ReadyCardinalitySource {
    Current {
        database_incarnation: DatabaseIncarnationId,
    },
    Admitted {
        database_incarnation: DatabaseIncarnationId,
        accepted_root: CardinalityAcceptedRootIdentity,
        fold_watermark: FoldWatermark,
    },
}

/// Opaque comparable lifecycle identity for optional exact-prefix evidence.
///
/// The fields remain private to the evidence owner. Consumers may only retain
/// and compare this value; they cannot reconstruct generation policy from it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct ExactPrefixCardinalityLifecycleStamp(
    ExactPrefixCardinalityLifecycleIdentity,
);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ExactPrefixCardinalityLifecycleIdentity {
    Volatile,
    MissingDurableAuthority,
    Corrupt,
    Journaled {
        header_digest: Option<[u8; 32]>,
        cursor_present: bool,
        delta_watermark: Option<FoldWatermark>,
    },
}

/// One complete exact-prefix evidence attempt through current store authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) enum ExactUserIndexPrefixEvidence {
    Exact(Vec<u64>),
    Unavailable(ExactPrefixCardinalityLifecycleStamp),
}

impl ReadyCardinalityCountTargets<'_> {
    const fn len(&self) -> usize {
        match self {
            Self::Digests(digests) => digests.len(),
            Self::UserIndexPrefixes(keys) => keys.len(),
        }
    }
}

/// Diagnostic storage mode carried by a runtime storage capability descriptor.
///
/// Policy code should branch on capability axes instead of this display value.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
pub enum StoreRuntimeStorageMode {
    /// Volatile in-process heap storage.
    #[default]
    Heap,
    /// Journaled cached-stable durable storage.
    Journaled,
}

impl StoreRuntimeStorageMode {
    /// Return the user-facing storage mode label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Heap => "heap",
            Self::Journaled => "journaled",
        }
    }
}

/// Whether a store owns durable allocation identity.
#[derive(CandidType, Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
pub enum StoreAllocationIdentityCapability {
    /// Stable allocation identity is present.
    #[default]
    Present,
    /// Stable allocation identity is absent.
    Absent,
}

impl StoreAllocationIdentityCapability {
    /// Return the user-facing capability label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Present => "present",
            Self::Absent => "absent",
        }
    }
}

/// Store durability class.
#[derive(CandidType, Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
pub enum StoreDurability {
    /// Store contents participate in durable storage semantics.
    #[default]
    Durable,
    /// Store contents are live-only and volatile.
    Volatile,
}

impl StoreDurability {
    /// Return the user-facing durability label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Durable => "durable",
            Self::Volatile => "volatile",
        }
    }
}

/// Store recovery capability.
#[derive(CandidType, Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
pub enum StoreRecoveryCapability {
    /// Store contents can be recovered from canonical stable BTrees plus a
    /// committed journal tail.
    #[default]
    StableBasePlusJournalReplay,
    /// Store contents are not recovered.
    None,
}

impl StoreRecoveryCapability {
    /// Return the user-facing recovery label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::StableBasePlusJournalReplay => "stable-base-plus-journal-replay",
            Self::None => "none",
        }
    }
}

/// Store commit participation class.
#[derive(CandidType, Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
pub enum StoreCommitParticipation {
    /// Store mutations participate in the durable commit path.
    #[default]
    Durable,
    /// Store mutations are live-only side effects.
    LiveOnly,
}

impl StoreCommitParticipation {
    /// Return the user-facing commit-participation label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Durable => "durable",
            Self::LiveOnly => "live-only",
        }
    }
}

/// Store schema metadata persistence class.
#[derive(CandidType, Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
pub enum StoreSchemaMetadataCapability {
    /// The store-local projection is rebuilt from a durable accepted checkpoint
    /// and does not retain its own schema history.
    LiveRebuiltMetadata,
    /// Schema metadata is canonical stable history plus committed journal tail.
    #[default]
    CanonicalStableHistoryPlusJournalTail,
}

impl StoreSchemaMetadataCapability {
    /// Return the user-facing schema-metadata capability label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LiveRebuiltMetadata => "live-rebuilt-metadata",
            Self::CanonicalStableHistoryPlusJournalTail => {
                "canonical-stable-history-plus-journal-tail"
            }
        }
    }
}

/// Relation source capability for a store.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
pub enum StoreRelationSourceCapability {
    /// Source rows can own durable relation integrity.
    #[default]
    DurableSource,
    /// Source rows can participate in live relation validation.
    LiveSource,
}

/// Relation target capability for a store.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
pub enum StoreRelationTargetCapability {
    /// Target rows can be referenced by durable source rows.
    #[default]
    DurableTarget,
    /// Target rows are volatile and cannot satisfy durable source integrity.
    VolatileTarget,
}

/// Runtime storage capability descriptor carried by one registered store.
///
/// Capabilities describe storage policy. They are not allocation identity.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
pub struct StoreRuntimeStorageCapabilities {
    storage_mode: StoreRuntimeStorageMode,
    allocation_identity: StoreAllocationIdentityCapability,
    durability: StoreDurability,
    recovery: StoreRecoveryCapability,
    commit_participation: StoreCommitParticipation,
    schema_metadata: StoreSchemaMetadataCapability,
    relation_source: StoreRelationSourceCapability,
    relation_target: StoreRelationTargetCapability,
}

impl StoreRuntimeStorageCapabilities {
    /// Capability descriptor for heap stores.
    #[must_use]
    pub const fn heap() -> Self {
        Self {
            storage_mode: StoreRuntimeStorageMode::Heap,
            allocation_identity: StoreAllocationIdentityCapability::Absent,
            durability: StoreDurability::Volatile,
            recovery: StoreRecoveryCapability::None,
            commit_participation: StoreCommitParticipation::LiveOnly,
            schema_metadata: StoreSchemaMetadataCapability::LiveRebuiltMetadata,
            relation_source: StoreRelationSourceCapability::LiveSource,
            relation_target: StoreRelationTargetCapability::VolatileTarget,
        }
    }

    /// Capability descriptor for journaled cached-stable stores.
    #[must_use]
    pub const fn journaled() -> Self {
        Self {
            storage_mode: StoreRuntimeStorageMode::Journaled,
            allocation_identity: StoreAllocationIdentityCapability::Present,
            durability: StoreDurability::Durable,
            recovery: StoreRecoveryCapability::StableBasePlusJournalReplay,
            commit_participation: StoreCommitParticipation::Durable,
            schema_metadata: StoreSchemaMetadataCapability::CanonicalStableHistoryPlusJournalTail,
            relation_source: StoreRelationSourceCapability::DurableSource,
            relation_target: StoreRelationTargetCapability::DurableTarget,
        }
    }

    /// Diagnostic storage mode. Policy code should use the capability axes.
    #[must_use]
    pub const fn storage_mode(self) -> StoreRuntimeStorageMode {
        self.storage_mode
    }

    /// Allocation identity capability.
    #[must_use]
    pub const fn allocation_identity(self) -> StoreAllocationIdentityCapability {
        self.allocation_identity
    }

    /// Durability capability.
    #[must_use]
    pub const fn durability(self) -> StoreDurability {
        self.durability
    }

    /// Recovery capability.
    #[must_use]
    pub const fn recovery(self) -> StoreRecoveryCapability {
        self.recovery
    }

    /// Commit participation capability.
    #[must_use]
    pub const fn commit_participation(self) -> StoreCommitParticipation {
        self.commit_participation
    }

    /// Schema metadata persistence capability.
    #[must_use]
    pub const fn schema_metadata(self) -> StoreSchemaMetadataCapability {
        self.schema_metadata
    }

    /// Relation source capability.
    #[must_use]
    pub const fn relation_source(self) -> StoreRelationSourceCapability {
        self.relation_source
    }

    /// Relation target capability.
    #[must_use]
    pub const fn relation_target(self) -> StoreRelationTargetCapability {
        self.relation_target
    }
}

///
/// StoreAllocationIdentity
///
/// Durable allocation identity for one physical stable-memory role.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StoreAllocationIdentity {
    memory_id: u8,
    stable_key: &'static str,
}

impl StoreAllocationIdentity {
    /// Build one stable allocation identity descriptor.
    #[must_use]
    pub const fn new(memory_id: u8, stable_key: &'static str) -> Self {
        Self {
            memory_id,
            stable_key,
        }
    }

    /// Stable-memory manager ID.
    #[must_use]
    pub const fn memory_id(self) -> u8 {
        self.memory_id
    }

    /// Durable stable-memory key.
    #[must_use]
    pub const fn stable_key(self) -> &'static str {
        self.stable_key
    }
}

///
/// StoreAllocationIdentities
///
/// Durable allocation identities for one logical store's data, index, and
/// schema memories.
///

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct StoreAllocationIdentities {
    data: Option<StoreAllocationIdentity>,
    index: Option<StoreAllocationIdentity>,
    schema: Option<StoreAllocationIdentity>,
    journal: Option<StoreAllocationIdentity>,
}

impl StoreAllocationIdentities {
    /// Build an absent allocation identity bundle.
    #[must_use]
    pub const fn absent() -> Self {
        Self {
            data: None,
            index: None,
            schema: None,
            journal: None,
        }
    }

    /// Build one journaled cached-stable allocation identity bundle.
    #[must_use]
    pub const fn new_journaled(
        data: StoreAllocationIdentity,
        index: StoreAllocationIdentity,
        schema: StoreAllocationIdentity,
        journal: StoreAllocationIdentity,
    ) -> Self {
        Self {
            data: Some(data),
            index: Some(index),
            schema: Some(schema),
            journal: Some(journal),
        }
    }

    /// Return data-memory allocation identity.
    #[must_use]
    pub const fn data(self) -> Option<StoreAllocationIdentity> {
        self.data
    }

    /// Return index-memory allocation identity.
    #[must_use]
    pub const fn index(self) -> Option<StoreAllocationIdentity> {
        self.index
    }

    /// Return schema-memory allocation identity.
    #[must_use]
    pub const fn schema(self) -> Option<StoreAllocationIdentity> {
        self.schema
    }

    /// Return journal-tail allocation identity.
    #[must_use]
    pub const fn journal(self) -> Option<StoreAllocationIdentity> {
        self.journal
    }

    /// Return the allocation capability represented by this triplet, or
    /// `None` if the triplet is partially populated and therefore invalid.
    #[must_use]
    pub const fn allocation_identity_capability(self) -> Option<StoreAllocationIdentityCapability> {
        match (self.data, self.index, self.schema) {
            (Some(_), Some(_), Some(_)) => Some(StoreAllocationIdentityCapability::Present),
            (None, None, None) if self.journal.is_none() => {
                Some(StoreAllocationIdentityCapability::Absent)
            }
            _ => None,
        }
    }

    /// Return whether this allocation shape matches the concrete storage
    /// capability descriptor.
    #[must_use]
    pub const fn matches_storage_capabilities(
        self,
        capabilities: StoreRuntimeStorageCapabilities,
    ) -> bool {
        match capabilities.storage_mode() {
            StoreRuntimeStorageMode::Heap => {
                self.data.is_none()
                    && self.index.is_none()
                    && self.schema.is_none()
                    && self.journal.is_none()
            }
            StoreRuntimeStorageMode::Journaled => {
                self.data.is_some()
                    && self.index.is_some()
                    && self.schema.is_some()
                    && self.journal.is_some()
            }
        }
    }
}

impl StoreHandle {
    /// Build a store handle with an explicit allocation identity decision.
    #[must_use]
    pub const fn new(
        data: &'static LocalKey<RefCell<DataStore>>,
        index: &'static LocalKey<RefCell<IndexStore>>,
        schema: &'static LocalKey<RefCell<SchemaStore>>,
        allocations: StoreAllocationIdentities,
        capabilities: StoreRuntimeStorageCapabilities,
    ) -> Self {
        Self {
            data,
            index,
            schema,
            journal: None,
            allocations,
            cardinality_allocation: None,
            capabilities,
        }
    }

    /// Build a journaled store handle with an explicit journal-tail store.
    #[must_use]
    pub fn new_journaled(
        data: &'static LocalKey<RefCell<DataStore>>,
        index: &'static LocalKey<RefCell<IndexStore>>,
        schema: &'static LocalKey<RefCell<SchemaStore>>,
        journal: &'static LocalKey<RefCell<JournalTailStore>>,
        allocations: StoreAllocationIdentities,
        capabilities: StoreRuntimeStorageCapabilities,
    ) -> Self {
        let cardinality_allocation = CardinalityStoreAllocationIdentity::derive(allocations).ok();
        Self {
            data,
            index,
            schema,
            journal: Some(journal),
            allocations,
            cardinality_allocation,
            capabilities,
        }
    }

    /// Borrow the row store immutably.
    pub fn with_data<R>(&self, f: impl FnOnce(&DataStore) -> R) -> R {
        self.data.with_borrow(f)
    }

    /// Borrow the row store mutably.
    pub fn with_data_mut<R>(&self, f: impl FnOnce(&mut DataStore) -> R) -> R {
        self.data.with_borrow_mut(f)
    }

    /// Borrow the index store immutably.
    pub fn with_index<R>(&self, f: impl FnOnce(&IndexStore) -> R) -> R {
        self.index.with_borrow(f)
    }

    /// Borrow the index store mutably.
    pub fn with_index_mut<R>(&self, f: impl FnOnce(&mut IndexStore) -> R) -> R {
        self.index.with_borrow_mut(f)
    }

    /// Borrow the schema store immutably.
    pub fn with_schema<R>(&self, f: impl FnOnce(&SchemaStore) -> R) -> R {
        self.schema.with_borrow(f)
    }

    /// Borrow the schema store mutably.
    pub fn with_schema_mut<R>(&self, f: impl FnOnce(&mut SchemaStore) -> R) -> R {
        self.schema.with_borrow_mut(f)
    }

    /// Return exact visible entity cardinality through the store's canonical proof boundary.
    #[must_use]
    pub(in crate::db) fn exact_entity_count(&self, entity: EntityTag) -> Option<u64> {
        if self.journal.is_none() {
            return self.with_data(|store| store.exact_entity_count(entity));
        }
        let delta = self.with_data(|store| store.exact_entity_cardinality_delta(entity))?;
        let digest = CardinalityCountDigest::for_entity(entity);
        let base = self
            .ready_cardinality_counts(&[digest], |authority| authority.accepts_entity(entity))
            .ok()
            .flatten()?
            .into_iter()
            .next()?;
        apply_visible_cardinality_delta(base, delta)
    }

    /// Return exact visible cardinality for one accepted user-index prefix.
    #[must_use]
    pub(in crate::db) fn exact_user_index_prefix_count(
        &self,
        data_generation: u64,
        key_kind: IndexKeyKind,
        index_id: IndexId,
        components: &[Vec<u8>],
    ) -> Option<u64> {
        self.exact_user_index_prefix_counts(data_generation, key_kind, index_id, [components])?
            .into_iter()
            .next()
    }

    /// Return exact visible counts for prefixes on one accepted user index.
    #[must_use]
    pub(in crate::db) fn exact_user_index_prefix_counts<'a>(
        &self,
        data_generation: u64,
        key_kind: IndexKeyKind,
        index_id: IndexId,
        component_prefixes: impl IntoIterator<Item = &'a [Vec<u8>]>,
    ) -> Option<Vec<u64>> {
        let component_prefixes = component_prefixes.into_iter().collect::<Vec<_>>();
        if key_kind != IndexKeyKind::User {
            return None;
        }
        if self.journal.is_none() {
            return self.with_index(|store| {
                component_prefixes
                    .iter()
                    .map(|components| {
                        store.exact_prefix_cardinality(
                            data_generation,
                            key_kind,
                            index_id,
                            components,
                        )
                    })
                    .collect()
            });
        }
        let deltas = self.with_index(|store| {
            component_prefixes
                .iter()
                .map(|components| {
                    store.exact_prefix_cardinality_delta(key_kind, index_id, components)
                })
                .collect::<Option<Vec<_>>>()
        })?;
        let digests = component_prefixes
            .iter()
            .map(|components| {
                CardinalityCountDigest::for_user_index_prefix(index_id, components).ok()
            })
            .collect::<Option<Vec<_>>>()?;
        let bases = self
            .ready_cardinality_counts(&digests, |authority| {
                component_prefixes.iter().all(|components| {
                    authority.accepts_user_index_prefix(index_id, components.len())
                })
            })
            .ok()
            .flatten()?;
        bases
            .into_iter()
            .zip(deltas)
            .map(|(base, delta)| apply_visible_cardinality_delta(base, delta))
            .collect()
    }

    /// Return exact visible counts for accepted prefix keys across user indexes.
    #[must_use]
    pub(in crate::db) fn exact_user_index_prefix_key_counts(
        &self,
        data_generation: u64,
        keys: &[UserIndexPrefixCardinalityKey],
    ) -> Option<Vec<u64>> {
        self.exact_user_index_prefix_key_counts_with_authority(data_generation, keys, None)
    }

    /// Return exact prefix counts through an accepted root admitted in this request.
    #[must_use]
    pub(in crate::db) fn exact_user_index_prefix_key_counts_for_admitted_root(
        &self,
        database_incarnation: DatabaseIncarnationId,
        accepted_root: CardinalityAcceptedRootIdentity,
        data_generation: u64,
        keys: &[UserIndexPrefixCardinalityKey],
    ) -> Option<Vec<u64>> {
        self.exact_user_index_prefix_key_counts_with_authority(
            data_generation,
            keys,
            Some((database_incarnation, accepted_root)),
        )
    }

    /// Return complete exact counts or one opaque current availability stamp.
    ///
    /// This boundary knows nothing about plans, caches, bindings, cursors, or
    /// selected winners. It only proves current accepted-prefix evidence.
    #[must_use]
    pub(in crate::db) fn exact_user_index_prefix_evidence_for_admitted_root(
        &self,
        database_incarnation: DatabaseIncarnationId,
        accepted_root: CardinalityAcceptedRootIdentity,
        keys: &[UserIndexPrefixCardinalityKey],
    ) -> ExactUserIndexPrefixEvidence {
        let data_generation = self.with_data(DataStore::generation);
        if let Some(counts) = self.exact_user_index_prefix_key_counts_for_admitted_root(
            database_incarnation,
            accepted_root,
            data_generation,
            keys,
        ) {
            return ExactUserIndexPrefixEvidence::Exact(counts);
        }

        ExactUserIndexPrefixEvidence::Unavailable(
            self.exact_user_index_prefix_evidence_lifecycle_stamp(),
        )
    }

    /// Return the cheap availability identity without reading any prefix count.
    #[must_use]
    pub(in crate::db) fn exact_user_index_prefix_evidence_lifecycle_stamp(
        &self,
    ) -> ExactPrefixCardinalityLifecycleStamp {
        if self.journal.is_none() {
            return ExactPrefixCardinalityLifecycleStamp(
                ExactPrefixCardinalityLifecycleIdentity::Volatile,
            );
        }
        if self.cardinality_allocation.is_none() {
            return ExactPrefixCardinalityLifecycleStamp(
                ExactPrefixCardinalityLifecycleIdentity::MissingDurableAuthority,
            );
        }
        let delta_watermark = self.with_index(IndexStore::exact_prefix_cardinality_delta_watermark);
        match self.with_schema(SchemaStore::cardinality_generation_lifecycle_control) {
            Ok((header, cursor_present)) => ExactPrefixCardinalityLifecycleStamp(
                ExactPrefixCardinalityLifecycleIdentity::Journaled {
                    header_digest: header.map(|header| {
                        let mut hasher = Sha256::new();
                        hasher.update(b"icydb.cardinality-lifecycle-stamp.v1");
                        hasher.update(header.encode());
                        hasher.finalize().into()
                    }),
                    cursor_present,
                    delta_watermark,
                },
            ),
            Err(_) => ExactPrefixCardinalityLifecycleStamp(
                ExactPrefixCardinalityLifecycleIdentity::Corrupt,
            ),
        }
    }

    fn exact_user_index_prefix_key_counts_with_authority(
        &self,
        data_generation: u64,
        keys: &[UserIndexPrefixCardinalityKey],
        admitted: Option<(DatabaseIncarnationId, CardinalityAcceptedRootIdentity)>,
    ) -> Option<Vec<u64>> {
        if keys.is_empty() {
            return None;
        }
        if self.journal.is_none() {
            return self.with_index(|store| {
                keys.iter()
                    .map(|key| {
                        store.exact_prefix_cardinality(
                            data_generation,
                            IndexKeyKind::User,
                            key.index_id(),
                            key.prefix_components(),
                        )
                    })
                    .collect()
            });
        }
        let (delta_watermark, deltas) = self.with_index(|store| {
            let watermark = store.exact_prefix_cardinality_delta_watermark()?;
            keys.iter()
                .map(|key| {
                    store.exact_prefix_cardinality_delta(
                        IndexKeyKind::User,
                        key.index_id(),
                        key.prefix_components(),
                    )
                })
                .collect::<Option<Vec<_>>>()
                .map(|deltas| (watermark, deltas))
        })?;
        let accepts = |authority: &CardinalityBuildAuthority| {
            keys.iter().all(|key| {
                authority.accepts_user_index_prefix(key.index_id(), key.prefix_components().len())
            })
        };
        let bases = match admitted {
            Some((database_incarnation, accepted_root)) => self
                .ready_cardinality_counts_for_source(
                    ReadyCardinalitySource::Admitted {
                        database_incarnation,
                        accepted_root,
                        fold_watermark: delta_watermark,
                    },
                    ReadyCardinalityCountTargets::UserIndexPrefixes(keys),
                    accepts,
                ),
            None => self.ready_cardinality_counts_for_targets(
                ReadyCardinalityCountTargets::UserIndexPrefixes(keys),
                accepts,
            ),
        }
        .ok()
        .flatten()?;
        bases
            .into_iter()
            .zip(deltas)
            .map(|(base, delta)| apply_visible_cardinality_delta(base, delta))
            .collect()
    }

    /// Prove that one accepted user-index prefix family has a synchronized Ready generation.
    ///
    /// This does not expose or infer count values. Callers may retain every
    /// branch conservatively, but must use the exact count methods above before
    /// pruning any branch as empty.
    #[must_use]
    pub(in crate::db) fn user_index_prefix_family_has_ready_generation<'a, I>(
        &self,
        database_incarnation: DatabaseIncarnationId,
        accepted_root: CardinalityAcceptedRootIdentity,
        data_generation: u64,
        key_kind: IndexKeyKind,
        index_id: IndexId,
        component_prefixes: I,
    ) -> bool
    where
        I: Clone + IntoIterator<Item = &'a [Vec<u8>]>,
    {
        if key_kind != IndexKeyKind::User || component_prefixes.clone().into_iter().next().is_none()
        {
            return false;
        }
        if self.journal.is_none() {
            return self.with_index(|store| {
                component_prefixes.clone().into_iter().all(|components| {
                    store
                        .exact_prefix_cardinality(data_generation, key_kind, index_id, components)
                        .is_some()
                })
            });
        }
        let delta_watermark = self.with_index(|store| {
            let watermark = store.exact_prefix_cardinality_delta_watermark()?;
            component_prefixes
                .clone()
                .into_iter()
                .all(|components| {
                    store
                        .exact_prefix_cardinality_delta(key_kind, index_id, components)
                        .is_some()
                })
                .then_some(watermark)
        });
        delta_watermark.is_some_and(|watermark| {
            self.ready_cardinality_counts_for_source(
                ReadyCardinalitySource::Admitted {
                    database_incarnation,
                    accepted_root,
                    fold_watermark: watermark,
                },
                ReadyCardinalityCountTargets::Digests(&[]),
                |authority| {
                    component_prefixes.into_iter().all(|components| {
                        authority.accepts_user_index_prefix(index_id, components.len())
                    })
                },
            )
            .is_ok_and(|counts| counts.is_some())
        })
    }

    /// Sum exact visible counts for prefixes on one accepted user index.
    #[must_use]
    pub(in crate::db) fn exact_user_index_prefix_count_sum<'a>(
        &self,
        data_generation: u64,
        key_kind: IndexKeyKind,
        index_id: IndexId,
        component_prefixes: impl IntoIterator<Item = &'a [Vec<u8>]>,
        stop_after: Option<u64>,
    ) -> Option<u64> {
        let component_prefixes = component_prefixes.into_iter().collect::<Vec<_>>();
        if self.journal.is_none() {
            return self.with_index(|store| {
                store.exact_prefix_cardinality_sum(
                    data_generation,
                    key_kind,
                    index_id,
                    component_prefixes.iter().copied(),
                    stop_after,
                )
            });
        }
        let counts = self.exact_user_index_prefix_counts(
            data_generation,
            key_kind,
            index_id,
            component_prefixes.iter().copied(),
        )?;
        let mut total = 0_u64;
        for count in counts {
            total = total.checked_add(count)?;
            if stop_after.is_some_and(|required| total >= required) {
                break;
            }
        }
        Some(total)
    }

    /// Enumerate one bounded child-prefix family proven complete by exact parent/child totals.
    #[must_use]
    pub(in crate::db) fn exact_user_index_child_prefixes_for_parent_set<'a>(
        &self,
        data_generation: u64,
        index_id: IndexId,
        parent_prefixes: impl IntoIterator<Item = &'a [Vec<u8>]>,
        total_cap: usize,
    ) -> Option<Vec<Vec<Vec<u8>>>> {
        let mut parent_prefixes = parent_prefixes
            .into_iter()
            .map(<[Vec<u8>]>::to_vec)
            .collect::<Vec<_>>();
        if parent_prefixes.iter().any(Vec::is_empty) {
            return None;
        }
        icydb_schema::compact_sort_unstable_by(&mut parent_prefixes, Ord::cmp);
        parent_prefixes.dedup();
        let child_prefixes = self.with_index(|store| {
            store.exact_child_prefixes_for_parent_set(
                data_generation,
                IndexKeyKind::User,
                index_id,
                parent_prefixes.iter().map(Vec::as_slice),
                total_cap,
            )
        })?;
        if self.journal.is_none() {
            return Some(child_prefixes);
        }
        let parent_count = parent_prefixes.len();
        let counts = self.exact_user_index_prefix_counts(
            data_generation,
            IndexKeyKind::User,
            index_id,
            parent_prefixes
                .iter()
                .chain(&child_prefixes)
                .map(Vec::as_slice),
        )?;
        let (parent_counts, child_counts) = counts.split_at(parent_count);
        let parent_total = checked_cardinality_sum(parent_counts)?;
        let child_total = checked_cardinality_sum(child_counts)?;
        (parent_total == child_total).then_some(child_prefixes)
    }

    fn ready_cardinality_counts(
        &self,
        digests: &[CardinalityCountDigest],
        accepts: impl FnOnce(&CardinalityBuildAuthority) -> bool,
    ) -> Result<Option<Vec<u64>>, InternalError> {
        self.ready_cardinality_counts_for_targets(
            ReadyCardinalityCountTargets::Digests(digests),
            accepts,
        )
    }

    fn ready_cardinality_counts_for_targets(
        &self,
        targets: ReadyCardinalityCountTargets<'_>,
        accepts: impl FnOnce(&CardinalityBuildAuthority) -> bool,
    ) -> Result<Option<Vec<u64>>, InternalError> {
        let incarnation = database_incarnation_id()?;
        self.ready_cardinality_counts_for_source(
            ReadyCardinalitySource::Current {
                database_incarnation: incarnation,
            },
            targets,
            accepts,
        )
    }

    fn ready_cardinality_counts_for_source(
        &self,
        source: ReadyCardinalitySource,
        targets: ReadyCardinalityCountTargets<'_>,
        accepts: impl FnOnce(&CardinalityBuildAuthority) -> bool,
    ) -> Result<Option<Vec<u64>>, InternalError> {
        let Some(journal) = self.journal else {
            return Ok(None);
        };
        let Some(allocation) = self.cardinality_allocation else {
            return Ok(None);
        };
        let (incarnation, accepted_root, watermark) = match source {
            ReadyCardinalitySource::Current {
                database_incarnation,
            } => (
                database_incarnation,
                None,
                journal.with_borrow(JournalTailStore::fold_watermark)?,
            ),
            ReadyCardinalitySource::Admitted {
                database_incarnation,
                accepted_root,
                fold_watermark,
            } => (database_incarnation, Some(accepted_root), fold_watermark),
        };
        self.with_schema(|schema| {
            let (header, cursor) = schema.cardinality_generation_control()?;
            let Some(header) = header else {
                return Ok(None);
            };
            if header.state() != CardinalityGenerationState::Ready || cursor.is_some() {
                return Ok(None);
            }
            let authority = match accepted_root {
                Some(root) => CardinalityBuildAuthority::derive_for_admitted_consumer_root(
                    schema,
                    incarnation,
                    allocation,
                    root,
                    watermark,
                )?,
                None => CardinalityBuildAuthority::derive_for_current_consumer(
                    schema,
                    incarnation,
                    allocation,
                    watermark,
                )?,
            };
            let Some(authority) = authority else {
                return Ok(None);
            };
            if !accepts(&authority) {
                return Ok(None);
            }
            if header.validate_source(authority.source()).is_err() {
                return Ok(None);
            }
            if targets.len() != 0 && schema.cardinality_count_slot_is_empty(header.slot())? {
                return Ok(Some(vec![0; targets.len()]));
            }
            let counts = match targets {
                ReadyCardinalityCountTargets::Digests(digests) => digests
                    .iter()
                    .map(|digest| {
                        schema
                            .cardinality_count(header.slot(), header.generation(), *digest)
                            .map(|count| count.unwrap_or(0))
                    })
                    .collect::<Result<Vec<_>, _>>()?,
                ReadyCardinalityCountTargets::UserIndexPrefixes(keys) => keys
                    .iter()
                    .map(|key| {
                        let digest = CardinalityCountDigest::for_user_index_prefix(
                            key.index_id(),
                            key.prefix_components(),
                        )?;
                        schema
                            .cardinality_count(header.slot(), header.generation(), digest)
                            .map(|count| count.unwrap_or(0))
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            };
            Ok(Some(counts))
        })
    }

    /// Return the explicit lifecycle state of the bound index store.
    #[must_use]
    pub(in crate::db) fn index_state(&self) -> IndexState {
        self.with_index(IndexStore::state)
    }

    /// Return the monotonic physical access-readiness revision.
    pub(in crate::db) fn access_state_revision(&self) -> Result<u64, crate::error::InternalError> {
        self.journal.map_or_else(
            || Ok(self.with_index(IndexStore::access_state_revision)),
            |journal| journal.with_borrow(JournalTailStore::access_state_revision),
        )
    }

    /// Mark the bound index store as Building.
    pub(in crate::db) fn mark_index_building(&self) -> Result<(), crate::error::InternalError> {
        self.set_index_state(IndexState::Building)
    }

    /// Mark the bound index store as Ready.
    pub(in crate::db) fn mark_index_ready(&self) -> Result<(), crate::error::InternalError> {
        self.set_index_state(IndexState::Ready)
    }

    fn set_index_state(&self, state: IndexState) -> Result<(), crate::error::InternalError> {
        if self.index_state() == state {
            return Ok(());
        }
        let revision = self.journal.map_or_else(
            || {
                self.with_index(IndexStore::access_state_revision)
                    .checked_add(1)
                    .ok_or_else(crate::error::InternalError::store_invariant)
            },
            |journal| journal.with_borrow_mut(JournalTailStore::advance_access_state_revision),
        )?;
        self.with_index_mut(|index| index.set_access_state(state, revision));
        Ok(())
    }

    /// Return the raw row-store accessor.
    #[must_use]
    pub const fn data_store(&self) -> &'static LocalKey<RefCell<DataStore>> {
        self.data
    }

    /// Return the raw index-store accessor.
    #[must_use]
    pub const fn index_store(&self) -> &'static LocalKey<RefCell<IndexStore>> {
        self.index
    }

    /// Return the raw schema-store accessor.
    #[must_use]
    pub const fn schema_store(&self) -> &'static LocalKey<RefCell<SchemaStore>> {
        self.schema
    }

    /// Return the raw journal-tail store accessor when this store is journaled.
    #[must_use]
    pub const fn journal_tail_store(&self) -> Option<&'static LocalKey<RefCell<JournalTailStore>>> {
        self.journal
    }

    /// Return the data-memory allocation identity when generated wiring
    /// supplied it.
    #[must_use]
    pub const fn data_allocation(&self) -> Option<StoreAllocationIdentity> {
        self.allocations.data()
    }

    /// Return the index-memory allocation identity when generated wiring
    /// supplied it.
    #[must_use]
    pub const fn index_allocation(&self) -> Option<StoreAllocationIdentity> {
        self.allocations.index()
    }

    /// Return the schema-memory allocation identity when generated wiring
    /// supplied it.
    #[must_use]
    pub const fn schema_allocation(&self) -> Option<StoreAllocationIdentity> {
        self.allocations.schema()
    }

    /// Return the journal-tail allocation identity when generated wiring
    /// supplied it.
    #[must_use]
    pub const fn journal_allocation(&self) -> Option<StoreAllocationIdentity> {
        self.allocations.journal()
    }

    /// Return this store's complete allocation identity bundle.
    #[must_use]
    pub(in crate::db) const fn allocation_identities(&self) -> StoreAllocationIdentities {
        self.allocations
    }

    /// Return this store's explicit runtime storage capabilities.
    #[must_use]
    pub const fn storage_capabilities(&self) -> StoreRuntimeStorageCapabilities {
        self.capabilities
    }
}

fn apply_visible_cardinality_delta(base: u64, delta: i64) -> Option<u64> {
    if delta >= 0 {
        base.checked_add(u64::try_from(delta).ok()?)
    } else {
        base.checked_sub(delta.unsigned_abs())
    }
}

fn checked_cardinality_sum(counts: &[u64]) -> Option<u64> {
    counts
        .iter()
        .try_fold(0_u64, |total, count| total.checked_add(*count))
}
