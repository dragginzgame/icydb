//! Module: db::registry::handle
//! Responsibility: stable store handles and runtime storage capability descriptors.
//! Does not own: registry path lookup or store mutation semantics.
//! Boundary: exposes registered storage roles without exposing registry internals.

use crate::db::{
    data::DataStore,
    index::{IndexState, IndexStore},
    journal::JournalTailStore,
    schema::SchemaStore,
};
use candid::CandidType;
use serde::Deserialize;
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
    capabilities: StoreRuntimeStorageCapabilities,
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
    /// Schema metadata is rebuilt live and is not durable history.
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
            capabilities,
        }
    }

    /// Build a journaled store handle with an explicit journal-tail store.
    #[must_use]
    pub const fn new_journaled(
        data: &'static LocalKey<RefCell<DataStore>>,
        index: &'static LocalKey<RefCell<IndexStore>>,
        schema: &'static LocalKey<RefCell<SchemaStore>>,
        journal: &'static LocalKey<RefCell<JournalTailStore>>,
        allocations: StoreAllocationIdentities,
        capabilities: StoreRuntimeStorageCapabilities,
    ) -> Self {
        Self {
            data,
            index,
            schema,
            journal: Some(journal),
            allocations,
            capabilities,
        }
    }

    /// Borrow the row store immutably.
    pub fn with_data<R>(&self, f: impl FnOnce(&DataStore) -> R) -> R {
        #[cfg(feature = "diagnostics")]
        {
            crate::db::physical_access::measure_physical_access_operation(|| {
                self.data.with_borrow(f)
            })
        }

        #[cfg(not(feature = "diagnostics"))]
        {
            self.data.with_borrow(f)
        }
    }

    /// Borrow the row store mutably.
    pub fn with_data_mut<R>(&self, f: impl FnOnce(&mut DataStore) -> R) -> R {
        self.data.with_borrow_mut(f)
    }

    /// Borrow the index store immutably.
    pub fn with_index<R>(&self, f: impl FnOnce(&IndexStore) -> R) -> R {
        #[cfg(feature = "diagnostics")]
        {
            crate::db::physical_access::measure_physical_access_operation(|| {
                self.index.with_borrow(f)
            })
        }

        #[cfg(not(feature = "diagnostics"))]
        {
            self.index.with_borrow(f)
        }
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

    /// Return the explicit lifecycle state of the bound index store.
    #[must_use]
    pub(in crate::db) fn index_state(&self) -> IndexState {
        self.with_index(IndexStore::state)
    }

    /// Mark the bound index store as Building.
    pub(in crate::db) fn mark_index_building(&self) {
        self.with_index_mut(IndexStore::mark_building);
    }

    /// Mark the bound index store as Ready.
    pub(in crate::db) fn mark_index_ready(&self) {
        self.with_index_mut(IndexStore::mark_ready);
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
