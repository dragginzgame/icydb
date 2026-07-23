//! Module: diagnostics::model
//! Responsibility: storage diagnostics DTO contracts and simple accessors.
//! Does not own: store traversal or execution trace policy.
//! Boundary: report assembly modules construct these DTOs; public callers read them.

use crate::db::{
    index::IndexState,
    registry::{
        StoreAllocationIdentityCapability, StoreCommitParticipation, StoreDurability,
        StoreRecoveryCapability, StoreRuntimeStorageCapabilities, StoreSchemaMetadataCapability,
    },
};
use candid::CandidType;
use serde::Deserialize;

#[cfg_attr(doc, doc = "StorageReport\n\nLive storage snapshot payload.")]
#[derive(CandidType, Clone, Debug, Default, Deserialize)]
pub struct StorageReport {
    pub(crate) storage_data: Vec<DataStoreSnapshot>,
    pub(crate) storage_index: Vec<IndexStoreSnapshot>,
    pub(crate) schema_storage: Vec<SchemaStoreSnapshot>,
    pub(crate) entity_storage: Vec<EntitySnapshot>,
    pub(crate) corrupted_keys: u64,
    pub(crate) corrupted_entries: u64,
}

impl StorageReport {
    /// Construct one storage report payload.
    #[must_use]
    pub(super) const fn new(
        storage_data: Vec<DataStoreSnapshot>,
        storage_index: Vec<IndexStoreSnapshot>,
        schema_storage: Vec<SchemaStoreSnapshot>,
        entity_storage: Vec<EntitySnapshot>,
        corrupted_keys: u64,
        corrupted_entries: u64,
    ) -> Self {
        Self {
            storage_data,
            storage_index,
            schema_storage,
            entity_storage,
            corrupted_keys,
            corrupted_entries,
        }
    }

    /// Borrow data-store snapshots.
    #[must_use]
    pub const fn storage_data(&self) -> &[DataStoreSnapshot] {
        self.storage_data.as_slice()
    }

    /// Borrow index-store snapshots.
    #[must_use]
    pub const fn storage_index(&self) -> &[IndexStoreSnapshot] {
        self.storage_index.as_slice()
    }

    /// Borrow schema-store snapshots.
    #[must_use]
    pub const fn schema_storage(&self) -> &[SchemaStoreSnapshot] {
        self.schema_storage.as_slice()
    }

    /// Borrow entity-level storage snapshots.
    #[must_use]
    pub const fn entity_storage(&self) -> &[EntitySnapshot] {
        self.entity_storage.as_slice()
    }

    /// Return count of corrupted decoded data keys.
    #[must_use]
    pub const fn corrupted_keys(&self) -> u64 {
        self.corrupted_keys
    }

    /// Return count of corrupted index entries.
    #[must_use]
    pub const fn corrupted_entries(&self) -> u64 {
        self.corrupted_entries
    }
}

#[cfg_attr(doc, doc = "SchemaStoreSnapshot\n\nSchema-store diagnostic row.")]
#[derive(CandidType, Clone, Debug, Default, Deserialize)]
pub struct SchemaStoreSnapshot {
    pub(crate) path: String,
    pub(crate) storage: StoreSnapshotStorageMode,
    pub(crate) allocation: StoreAllocationIdentityCapability,
    pub(crate) durability: StoreDurability,
    pub(crate) commit: StoreCommitParticipation,
    pub(crate) recovery: StoreRecoveryCapability,
    pub(crate) schema_metadata: StoreSchemaMetadataCapability,
    pub(crate) memory_id: Option<u8>,
    pub(crate) stable_key: Option<String>,
    pub(crate) schema_version: Option<u32>,
    pub(crate) schema_fingerprint_method_version: Option<u8>,
    pub(crate) schema_fingerprint: Option<String>,
    pub(crate) entity_count: u64,
}

/// Diagnostic storage mode reported for one store-role snapshot.
///
/// This is observability metadata only. It does not participate in allocation
/// identity, stable-key generation, or durable row/index/schema storage ABI.
#[derive(CandidType, Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
pub enum StoreSnapshotStorageMode {
    Heap,
    #[default]
    Journaled,
}

impl StoreSnapshotStorageMode {
    /// Return the user-facing storage mode label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Heap => "heap",
            Self::Journaled => "journaled",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct StoreSnapshotAllocationIdentity {
    memory_id: u8,
    stable_key: String,
}

impl StoreSnapshotAllocationIdentity {
    pub(super) const fn new(memory_id: u8, stable_key: String) -> Self {
        Self {
            memory_id,
            stable_key,
        }
    }

    const fn memory_id(&self) -> u8 {
        self.memory_id
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(super) struct StoreSnapshotSchemaMetadata {
    version: Option<u32>,
    fingerprint_method_version: Option<u8>,
    fingerprint: Option<String>,
}

impl StoreSnapshotSchemaMetadata {
    pub(super) const fn absent() -> Self {
        Self {
            version: None,
            fingerprint_method_version: None,
            fingerprint: None,
        }
    }

    pub(super) const fn new(
        schema_version: u32,
        schema_fingerprint_method_version: u8,
        schema_fingerprint: String,
    ) -> Self {
        Self {
            version: Some(schema_version),
            fingerprint_method_version: Some(schema_fingerprint_method_version),
            fingerprint: Some(schema_fingerprint),
        }
    }

    const fn schema_version(&self) -> Option<u32> {
        self.version
    }

    const fn schema_fingerprint_method_version(&self) -> Option<u8> {
        self.fingerprint_method_version
    }

    fn schema_fingerprint(&self) -> Option<String> {
        self.fingerprint.clone()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct StoreRoleSnapshotFields {
    path: String,
    storage: StoreSnapshotStorageMode,
    allocation: StoreAllocationIdentityCapability,
    durability: StoreDurability,
    commit: StoreCommitParticipation,
    recovery: StoreRecoveryCapability,
    schema_metadata: StoreSchemaMetadataCapability,
    memory_id: Option<u8>,
    stable_key: Option<String>,
    schema_version: Option<u32>,
    schema_fingerprint_method_version: Option<u8>,
    schema_fingerprint: Option<String>,
}

impl StoreRoleSnapshotFields {
    fn new(
        path: String,
        storage: StoreSnapshotStorageMode,
        capabilities: StoreRuntimeStorageCapabilities,
        allocation: Option<StoreSnapshotAllocationIdentity>,
        schema_metadata: StoreSnapshotSchemaMetadata,
    ) -> Self {
        let (memory_id, stable_key) = match allocation {
            Some(allocation) => (Some(allocation.memory_id()), Some(allocation.stable_key)),
            None => (None, None),
        };
        Self {
            path,
            storage,
            allocation: capabilities.allocation_identity(),
            durability: capabilities.durability(),
            commit: capabilities.commit_participation(),
            recovery: capabilities.recovery(),
            schema_metadata: capabilities.schema_metadata(),
            memory_id,
            stable_key,
            schema_version: schema_metadata.schema_version(),
            schema_fingerprint_method_version: schema_metadata.schema_fingerprint_method_version(),
            schema_fingerprint: schema_metadata.schema_fingerprint(),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct IndexStoreSnapshotStats {
    entries: u64,
    user_entries: u64,
    system_entries: u64,
    memory_bytes: u64,
    state: IndexState,
}

impl IndexStoreSnapshotStats {
    pub(super) const fn new(
        entries: u64,
        user_entries: u64,
        system_entries: u64,
        memory_bytes: u64,
        state: IndexState,
    ) -> Self {
        Self {
            entries,
            user_entries,
            system_entries,
            memory_bytes,
            state,
        }
    }
}

impl SchemaStoreSnapshot {
    /// Construct one schema-store diagnostic row.
    #[must_use]
    pub(super) fn new(
        path: String,
        storage: StoreSnapshotStorageMode,
        capabilities: StoreRuntimeStorageCapabilities,
        allocation: Option<StoreSnapshotAllocationIdentity>,
        schema_metadata: StoreSnapshotSchemaMetadata,
        entity_count: u64,
    ) -> Self {
        let fields =
            StoreRoleSnapshotFields::new(path, storage, capabilities, allocation, schema_metadata);
        Self {
            path: fields.path,
            storage: fields.storage,
            allocation: fields.allocation,
            durability: fields.durability,
            commit: fields.commit,
            recovery: fields.recovery,
            schema_metadata: fields.schema_metadata,
            memory_id: fields.memory_id,
            stable_key: fields.stable_key,
            schema_version: fields.schema_version,
            schema_fingerprint_method_version: fields.schema_fingerprint_method_version,
            schema_fingerprint: fields.schema_fingerprint,
            entity_count,
        }
    }

    /// Borrow store path.
    #[must_use]
    pub const fn path(&self) -> &str {
        self.path.as_str()
    }

    /// Return diagnostic storage mode.
    #[must_use]
    pub const fn storage(&self) -> StoreSnapshotStorageMode {
        self.storage
    }

    /// Return allocation-identity capability metadata.
    #[must_use]
    pub const fn allocation(&self) -> StoreAllocationIdentityCapability {
        self.allocation
    }

    /// Return durability capability metadata.
    #[must_use]
    pub const fn durability(&self) -> StoreDurability {
        self.durability
    }

    /// Return commit participation capability metadata.
    #[must_use]
    pub const fn commit(&self) -> StoreCommitParticipation {
        self.commit
    }

    /// Return recovery capability metadata.
    #[must_use]
    pub const fn recovery(&self) -> StoreRecoveryCapability {
        self.recovery
    }

    /// Return schema-metadata persistence capability metadata.
    #[must_use]
    pub const fn schema_metadata(&self) -> StoreSchemaMetadataCapability {
        self.schema_metadata
    }

    /// Return stable-memory manager ID, when generated wiring supplied it.
    #[must_use]
    pub const fn memory_id(&self) -> Option<u8> {
        self.memory_id
    }

    /// Return durable stable-memory key, when generated wiring supplied it.
    #[must_use]
    pub const fn stable_key(&self) -> Option<&str> {
        match &self.stable_key {
            Some(value) => Some(value.as_str()),
            None => None,
        }
    }

    /// Return accepted schema/catalog version, when known.
    #[must_use]
    pub const fn schema_version(&self) -> Option<u32> {
        self.schema_version
    }

    /// Return accepted schema/catalog fingerprint method version, when known.
    #[must_use]
    pub const fn schema_fingerprint_method_version(&self) -> Option<u8> {
        self.schema_fingerprint_method_version
    }

    /// Return accepted schema/catalog fingerprint, when known.
    #[must_use]
    pub const fn schema_fingerprint(&self) -> Option<&str> {
        match &self.schema_fingerprint {
            Some(value) => Some(value.as_str()),
            None => None,
        }
    }

    /// Return number of entity schemas represented in this schema catalog.
    #[must_use]
    pub const fn entity_count(&self) -> u64 {
        self.entity_count
    }
}

#[cfg_attr(doc, doc = "DataStoreSnapshot\n\nData-store snapshot row.")]
#[derive(CandidType, Clone, Debug, Default, Deserialize)]
pub struct DataStoreSnapshot {
    pub(crate) path: String,
    pub(crate) storage: StoreSnapshotStorageMode,
    pub(crate) allocation: StoreAllocationIdentityCapability,
    pub(crate) durability: StoreDurability,
    pub(crate) commit: StoreCommitParticipation,
    pub(crate) recovery: StoreRecoveryCapability,
    pub(crate) schema_metadata: StoreSchemaMetadataCapability,
    pub(crate) memory_id: Option<u8>,
    pub(crate) stable_key: Option<String>,
    pub(crate) schema_version: Option<u32>,
    pub(crate) schema_fingerprint_method_version: Option<u8>,
    pub(crate) schema_fingerprint: Option<String>,
    pub(crate) entries: u64,
    pub(crate) memory_bytes: u64,
}

impl DataStoreSnapshot {
    /// Construct one data-store snapshot row.
    #[must_use]
    pub(super) fn new(
        path: String,
        storage: StoreSnapshotStorageMode,
        capabilities: StoreRuntimeStorageCapabilities,
        allocation: Option<StoreSnapshotAllocationIdentity>,
        schema_metadata: StoreSnapshotSchemaMetadata,
        entries: u64,
        memory_bytes: u64,
    ) -> Self {
        let fields =
            StoreRoleSnapshotFields::new(path, storage, capabilities, allocation, schema_metadata);
        Self {
            path: fields.path,
            storage: fields.storage,
            allocation: fields.allocation,
            durability: fields.durability,
            commit: fields.commit,
            recovery: fields.recovery,
            schema_metadata: fields.schema_metadata,
            memory_id: fields.memory_id,
            stable_key: fields.stable_key,
            schema_version: fields.schema_version,
            schema_fingerprint_method_version: fields.schema_fingerprint_method_version,
            schema_fingerprint: fields.schema_fingerprint,
            entries,
            memory_bytes,
        }
    }

    /// Borrow store path.
    #[must_use]
    pub const fn path(&self) -> &str {
        self.path.as_str()
    }

    /// Return diagnostic storage mode.
    #[must_use]
    pub const fn storage(&self) -> StoreSnapshotStorageMode {
        self.storage
    }

    /// Return allocation-identity capability metadata.
    #[must_use]
    pub const fn allocation(&self) -> StoreAllocationIdentityCapability {
        self.allocation
    }

    /// Return durability capability metadata.
    #[must_use]
    pub const fn durability(&self) -> StoreDurability {
        self.durability
    }

    /// Return commit participation capability metadata.
    #[must_use]
    pub const fn commit(&self) -> StoreCommitParticipation {
        self.commit
    }

    /// Return recovery capability metadata.
    #[must_use]
    pub const fn recovery(&self) -> StoreRecoveryCapability {
        self.recovery
    }

    /// Return schema-metadata persistence capability metadata.
    #[must_use]
    pub const fn schema_metadata(&self) -> StoreSchemaMetadataCapability {
        self.schema_metadata
    }

    /// Return stable-memory manager ID, when generated wiring supplied it.
    #[must_use]
    pub const fn memory_id(&self) -> Option<u8> {
        self.memory_id
    }

    /// Return durable stable-memory key, when generated wiring supplied it.
    #[must_use]
    pub const fn stable_key(&self) -> Option<&str> {
        match &self.stable_key {
            Some(value) => Some(value.as_str()),
            None => None,
        }
    }

    /// Return accepted schema/catalog version, when known.
    #[must_use]
    pub const fn schema_version(&self) -> Option<u32> {
        self.schema_version
    }

    /// Return accepted schema/catalog fingerprint method version, when known.
    #[must_use]
    pub const fn schema_fingerprint_method_version(&self) -> Option<u8> {
        self.schema_fingerprint_method_version
    }

    /// Return accepted schema/catalog fingerprint, when known.
    #[must_use]
    pub const fn schema_fingerprint(&self) -> Option<&str> {
        match &self.schema_fingerprint {
            Some(value) => Some(value.as_str()),
            None => None,
        }
    }

    /// Return row count.
    #[must_use]
    pub const fn entries(&self) -> u64 {
        self.entries
    }

    /// Return memory usage in bytes.
    #[must_use]
    pub const fn memory_bytes(&self) -> u64 {
        self.memory_bytes
    }
}

#[cfg_attr(doc, doc = "IndexStoreSnapshot\n\nIndex-store snapshot row.")]
#[derive(CandidType, Clone, Debug, Default, Deserialize)]
pub struct IndexStoreSnapshot {
    pub(crate) path: String,
    pub(crate) storage: StoreSnapshotStorageMode,
    pub(crate) allocation: StoreAllocationIdentityCapability,
    pub(crate) durability: StoreDurability,
    pub(crate) commit: StoreCommitParticipation,
    pub(crate) recovery: StoreRecoveryCapability,
    pub(crate) schema_metadata: StoreSchemaMetadataCapability,
    pub(crate) memory_id: Option<u8>,
    pub(crate) stable_key: Option<String>,
    pub(crate) schema_version: Option<u32>,
    pub(crate) schema_fingerprint_method_version: Option<u8>,
    pub(crate) schema_fingerprint: Option<String>,
    pub(crate) entries: u64,
    pub(crate) user_entries: u64,
    pub(crate) system_entries: u64,
    pub(crate) memory_bytes: u64,
    pub(crate) state: IndexState,
}

impl IndexStoreSnapshot {
    /// Construct one index-store snapshot row.
    #[must_use]
    pub(super) fn new(
        path: String,
        storage: StoreSnapshotStorageMode,
        capabilities: StoreRuntimeStorageCapabilities,
        allocation: Option<StoreSnapshotAllocationIdentity>,
        schema_metadata: StoreSnapshotSchemaMetadata,
        stats: IndexStoreSnapshotStats,
    ) -> Self {
        let fields =
            StoreRoleSnapshotFields::new(path, storage, capabilities, allocation, schema_metadata);
        Self {
            path: fields.path,
            storage: fields.storage,
            allocation: fields.allocation,
            durability: fields.durability,
            commit: fields.commit,
            recovery: fields.recovery,
            schema_metadata: fields.schema_metadata,
            memory_id: fields.memory_id,
            stable_key: fields.stable_key,
            schema_version: fields.schema_version,
            schema_fingerprint_method_version: fields.schema_fingerprint_method_version,
            schema_fingerprint: fields.schema_fingerprint,
            entries: stats.entries,
            user_entries: stats.user_entries,
            system_entries: stats.system_entries,
            memory_bytes: stats.memory_bytes,
            state: stats.state,
        }
    }

    /// Borrow store path.
    #[must_use]
    pub const fn path(&self) -> &str {
        self.path.as_str()
    }

    /// Return diagnostic storage mode.
    #[must_use]
    pub const fn storage(&self) -> StoreSnapshotStorageMode {
        self.storage
    }

    /// Return allocation-identity capability metadata.
    #[must_use]
    pub const fn allocation(&self) -> StoreAllocationIdentityCapability {
        self.allocation
    }

    /// Return durability capability metadata.
    #[must_use]
    pub const fn durability(&self) -> StoreDurability {
        self.durability
    }

    /// Return commit participation capability metadata.
    #[must_use]
    pub const fn commit(&self) -> StoreCommitParticipation {
        self.commit
    }

    /// Return recovery capability metadata.
    #[must_use]
    pub const fn recovery(&self) -> StoreRecoveryCapability {
        self.recovery
    }

    /// Return schema-metadata persistence capability metadata.
    #[must_use]
    pub const fn schema_metadata(&self) -> StoreSchemaMetadataCapability {
        self.schema_metadata
    }

    /// Return stable-memory manager ID, when generated wiring supplied it.
    #[must_use]
    pub const fn memory_id(&self) -> Option<u8> {
        self.memory_id
    }

    /// Return durable stable-memory key, when generated wiring supplied it.
    #[must_use]
    pub const fn stable_key(&self) -> Option<&str> {
        match &self.stable_key {
            Some(value) => Some(value.as_str()),
            None => None,
        }
    }

    /// Return accepted schema/catalog version, when known.
    #[must_use]
    pub const fn schema_version(&self) -> Option<u32> {
        self.schema_version
    }

    /// Return accepted schema/catalog fingerprint method version, when known.
    #[must_use]
    pub const fn schema_fingerprint_method_version(&self) -> Option<u8> {
        self.schema_fingerprint_method_version
    }

    /// Return accepted schema/catalog fingerprint, when known.
    #[must_use]
    pub const fn schema_fingerprint(&self) -> Option<&str> {
        match &self.schema_fingerprint {
            Some(value) => Some(value.as_str()),
            None => None,
        }
    }

    /// Return total entry count.
    #[must_use]
    pub const fn entries(&self) -> u64 {
        self.entries
    }

    /// Return user-namespace entry count.
    #[must_use]
    pub const fn user_entries(&self) -> u64 {
        self.user_entries
    }

    /// Return system-namespace entry count.
    #[must_use]
    pub const fn system_entries(&self) -> u64 {
        self.system_entries
    }

    /// Return memory usage in bytes.
    #[must_use]
    pub const fn memory_bytes(&self) -> u64 {
        self.memory_bytes
    }

    /// Return the current explicit runtime lifecycle state for this index
    /// store snapshot.
    #[must_use]
    pub const fn state(&self) -> IndexState {
        self.state
    }
}

#[cfg_attr(doc, doc = "EntitySnapshot\n\nPer-entity storage snapshot row.")]
#[derive(CandidType, Clone, Debug, Default, Deserialize)]
pub struct EntitySnapshot {
    pub(crate) store: String,

    pub(crate) path: String,

    pub(crate) entries: u64,

    pub(crate) memory_bytes: u64,
}

impl EntitySnapshot {
    /// Construct one entity-storage snapshot row.
    #[must_use]
    pub(super) const fn new(store: String, path: String, entries: u64, memory_bytes: u64) -> Self {
        Self {
            store,
            path,
            entries,
            memory_bytes,
        }
    }

    /// Borrow store path.
    #[must_use]
    pub const fn store(&self) -> &str {
        self.store.as_str()
    }

    /// Borrow entity path.
    #[must_use]
    pub const fn path(&self) -> &str {
        self.path.as_str()
    }

    /// Return row count.
    #[must_use]
    pub const fn entries(&self) -> u64 {
        self.entries
    }

    /// Return memory usage in bytes.
    #[must_use]
    pub const fn memory_bytes(&self) -> u64 {
        self.memory_bytes
    }
}
