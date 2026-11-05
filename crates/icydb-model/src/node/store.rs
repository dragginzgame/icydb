use crate::node::{validate_stable_key, validate_stable_key_segment};
use crate::prelude::*;

///
/// Store
///
/// Schema node describing the storage mode for:
/// - primary entity data
/// - all index data for that entity
/// - schema metadata for that store
///

#[derive(Clone, Debug, Serialize)]
pub struct Store {
    def: Def,
    canister: &'static str,
    storage: StoreStorage,
}

/// Storage configuration owned by one schema store declaration.
///
/// Store storage has two public modes: volatile heap storage and journaled
/// cached-stable durable storage.
///
/// Use `Journaled` for user data that must survive upgrade/reinitialization.
/// `Heap` is live-only process state: it has no stable-memory allocation
/// identity, no commit-marker or journal-tail participation, and no recovery
/// path.
#[derive(Clone, Debug, Serialize)]
pub enum StoreStorage {
    /// Volatile heap store with no stable allocation identity or recovery path.
    Heap(StoreHeapConfig),
    /// Journaled cached-stable store using canonical stable data/index/schema
    /// memories plus a durable journal-tail memory.
    Journaled(StoreJournaledMemoryConfig),
}

impl StoreStorage {
    /// Borrow the journaled cached-stable configuration.
    #[must_use]
    pub const fn journaled_memory_config(&self) -> Option<&StoreJournaledMemoryConfig> {
        match self {
            Self::Journaled(config) => Some(config),
            Self::Heap(_) => None,
        }
    }

    /// Return the capability descriptor derived from this storage mode.
    #[must_use]
    pub const fn storage_capabilities(&self) -> StoreStorageCapabilities {
        match self {
            Self::Heap(_) => StoreStorageCapabilities::heap(),
            Self::Journaled(_) => StoreStorageCapabilities::journaled(),
        }
    }
}

/// Diagnostic storage mode carried by a storage capability descriptor.
///
/// Policy code should branch on capability axes instead of this display value.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum StoreStorageMode {
    /// Volatile in-process heap storage.
    Heap,
    /// Journaled cached-stable durable storage.
    Journaled,
}

/// Whether a store storage mode owns durable stable-memory allocation identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum AllocationIdentityCapability {
    /// Stable allocation identity is present.
    Present,
    /// Stable allocation identity is absent.
    Absent,
}

/// Store durability class.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum StoreDurability {
    /// Store contents participate in durable storage semantics.
    Durable,
    /// Store contents are live-only and volatile.
    Volatile,
}

/// Store recovery capability.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum StoreRecoveryCapability {
    /// Store contents recover from canonical stable BTrees plus committed
    /// journal tail replay.
    StableBasePlusJournalReplay,
    /// Store contents are not recovered.
    None,
}

/// Store commit participation class.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum CommitParticipation {
    /// Store mutations participate in the durable commit path.
    Durable,
    /// Store mutations are live-only side effects.
    LiveOnly,
}

/// Store schema metadata persistence class.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum SchemaMetadataCapability {
    /// The store-local projection is rebuilt from a durable accepted checkpoint
    /// and does not retain its own schema history.
    LiveRebuiltMetadata,
    /// Schema metadata is canonical stable history plus committed journal tail.
    CanonicalStableHistoryPlusJournalTail,
}

/// Relation source capability for a store.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum RelationSourceCapability {
    /// Source rows can own durable relation integrity.
    DurableSource,
    /// Source rows can participate in live relation validation.
    LiveSource,
}

/// Relation target capability for a store.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum RelationTargetCapability {
    /// Target rows can be referenced by durable source rows.
    DurableTarget,
    /// Target rows are volatile and cannot satisfy durable source integrity.
    VolatileTarget,
}

/// Whether the store can participate in live validation.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum LiveValidationCapability {
    /// Live validation is supported.
    Supported,
}

/// Storage capability descriptor derived from a store storage mode.
///
/// Capabilities describe storage policy. They are not allocation identity.
/// Stable allocation identity is the permanent key; heap allocation
/// identity remains absent.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct StoreStorageCapabilities {
    storage_mode: StoreStorageMode,
    allocation_identity: AllocationIdentityCapability,
    durability: StoreDurability,
    recovery: StoreRecoveryCapability,
    commit_participation: CommitParticipation,
    schema_metadata: SchemaMetadataCapability,
    relation_source: RelationSourceCapability,
    relation_target: RelationTargetCapability,
    live_validation: LiveValidationCapability,
}

impl StoreStorageCapabilities {
    /// Capability descriptor for heap stores.
    #[must_use]
    pub const fn heap() -> Self {
        Self {
            storage_mode: StoreStorageMode::Heap,
            allocation_identity: AllocationIdentityCapability::Absent,
            durability: StoreDurability::Volatile,
            recovery: StoreRecoveryCapability::None,
            commit_participation: CommitParticipation::LiveOnly,
            schema_metadata: SchemaMetadataCapability::LiveRebuiltMetadata,
            relation_source: RelationSourceCapability::LiveSource,
            relation_target: RelationTargetCapability::VolatileTarget,
            live_validation: LiveValidationCapability::Supported,
        }
    }

    /// Capability descriptor for journaled cached-stable stores.
    #[must_use]
    pub const fn journaled() -> Self {
        Self {
            storage_mode: StoreStorageMode::Journaled,
            allocation_identity: AllocationIdentityCapability::Present,
            durability: StoreDurability::Durable,
            recovery: StoreRecoveryCapability::StableBasePlusJournalReplay,
            commit_participation: CommitParticipation::Durable,
            schema_metadata: SchemaMetadataCapability::CanonicalStableHistoryPlusJournalTail,
            relation_source: RelationSourceCapability::DurableSource,
            relation_target: RelationTargetCapability::DurableTarget,
            live_validation: LiveValidationCapability::Supported,
        }
    }

    /// Diagnostic storage mode. Policy code should use the capability axes.
    #[must_use]
    pub const fn storage_mode(self) -> StoreStorageMode {
        self.storage_mode
    }

    /// Stable allocation identity capability.
    #[must_use]
    pub const fn allocation_identity(self) -> AllocationIdentityCapability {
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
    pub const fn commit_participation(self) -> CommitParticipation {
        self.commit_participation
    }

    /// Schema metadata persistence capability.
    #[must_use]
    pub const fn schema_metadata(self) -> SchemaMetadataCapability {
        self.schema_metadata
    }

    /// Relation source capability.
    #[must_use]
    pub const fn relation_source(self) -> RelationSourceCapability {
        self.relation_source
    }

    /// Relation target capability.
    #[must_use]
    pub const fn relation_target(self) -> RelationTargetCapability {
        self.relation_target
    }

    /// Live validation capability.
    #[must_use]
    pub const fn live_validation(self) -> LiveValidationCapability {
        self.live_validation
    }

    /// Return whether stable allocation identity is present.
    #[must_use]
    pub const fn has_allocation_identity(self) -> bool {
        matches!(
            self.allocation_identity,
            AllocationIdentityCapability::Present
        )
    }

    /// Return whether mutations participate in durable commit.
    #[must_use]
    pub const fn participates_in_durable_commit(self) -> bool {
        matches!(self.commit_participation, CommitParticipation::Durable)
    }

    /// Return whether the store is volatile.
    #[must_use]
    pub const fn is_volatile(self) -> bool {
        matches!(self.durability, StoreDurability::Volatile)
    }
}

/// Heap storage configuration for one volatile store.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
pub struct StoreHeapConfig;

impl StoreHeapConfig {
    /// Build an empty heap storage configuration.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

/// Permanent store key for the four durable roles owned by one journaled
/// cached-stable store.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct StoreJournaledMemoryConfig {
    key: &'static str,
}

impl StoreJournaledMemoryConfig {
    /// Name this store permanently within its canister namespace.
    #[must_use]
    pub const fn new(key: &'static str) -> Self {
        Self { key }
    }

    /// Durable logical identity, independent of Rust names and physical placement.
    #[must_use]
    pub const fn key(self) -> &'static str {
        self.key
    }
}

impl Store {
    /// Build a heap-backed volatile store declaration.
    #[must_use]
    pub const fn new_heap(def: Def, canister: &'static str, heap: StoreHeapConfig) -> Self {
        Self {
            def,
            canister,
            storage: StoreStorage::Heap(heap),
        }
    }

    /// Build a journaled cached-stable store declaration.
    #[must_use]
    pub const fn new_journaled(
        def: Def,
        canister: &'static str,
        journaled: StoreJournaledMemoryConfig,
    ) -> Self {
        Self {
            def,
            canister,
            storage: StoreStorage::Journaled(journaled),
        }
    }

    #[must_use]
    pub const fn def(&self) -> &Def {
        &self.def
    }

    #[must_use]
    pub const fn canister(&self) -> &'static str {
        self.canister
    }

    /// Borrow this store's storage configuration.
    #[must_use]
    pub const fn storage(&self) -> &StoreStorage {
        &self.storage
    }

    /// Return whether this store is heap-backed and volatile.
    #[must_use]
    pub const fn is_heap_storage(&self) -> bool {
        matches!(self.storage, StoreStorage::Heap(_))
    }

    /// Return whether this store is journaled cached-stable.
    #[must_use]
    pub const fn is_journaled_storage(&self) -> bool {
        matches!(self.storage, StoreStorage::Journaled(_))
    }

    /// Borrow the journaled store's logical allocation configuration when it uses
    /// journaled storage.
    #[must_use]
    pub const fn journaled_memory_config(&self) -> Option<&StoreJournaledMemoryConfig> {
        self.storage.journaled_memory_config()
    }

    /// Return the capability descriptor derived from this store's storage mode.
    #[must_use]
    pub const fn storage_capabilities(&self) -> StoreStorageCapabilities {
        self.storage.storage_capabilities()
    }

    /// Return the permanent key for journaled storage.
    ///
    /// # Panics
    ///
    /// Panics when this store uses heap storage.
    #[must_use]
    pub const fn stable_store_key(&self) -> &'static str {
        match self.storage {
            StoreStorage::Journaled(config) => config.key(),
            StoreStorage::Heap(_) => panic!("heap stores do not have a stable store key"),
        }
    }

    /// Build the data-memory allocation descriptor for journaled storage.
    ///
    /// # Panics
    ///
    /// Panics when this store uses heap storage.
    #[must_use]
    pub fn stable_data_allocation(&self, memory_namespace: &str) -> StableMemoryAllocation {
        self.stable_allocation(memory_namespace, StoreMemoryRole::Data)
    }

    /// Build the data-memory allocation descriptor with accepted row-layout
    /// schema metadata attached for diagnostics.
    ///
    /// # Panics
    ///
    /// Panics when this store uses heap storage.
    #[must_use]
    pub fn stable_data_allocation_with_schema_metadata(
        &self,
        memory_namespace: &str,
        schema_metadata: StableMemoryAllocationMetadata,
    ) -> StableMemoryAllocation {
        self.stable_allocation_with_schema_metadata(
            memory_namespace,
            StoreMemoryRole::Data,
            schema_metadata,
        )
    }

    /// Build the index-memory allocation descriptor for journaled storage.
    ///
    /// # Panics
    ///
    /// Panics when this store uses heap storage.
    #[must_use]
    pub fn stable_index_allocation(&self, memory_namespace: &str) -> StableMemoryAllocation {
        self.stable_allocation(memory_namespace, StoreMemoryRole::Index)
    }

    /// Build the index-memory allocation descriptor with accepted index-catalog
    /// schema metadata attached for diagnostics.
    ///
    /// # Panics
    ///
    /// Panics when this store uses heap storage.
    #[must_use]
    pub fn stable_index_allocation_with_schema_metadata(
        &self,
        memory_namespace: &str,
        schema_metadata: StableMemoryAllocationMetadata,
    ) -> StableMemoryAllocation {
        self.stable_allocation_with_schema_metadata(
            memory_namespace,
            StoreMemoryRole::Index,
            schema_metadata,
        )
    }

    /// Build the schema-memory allocation descriptor for journaled storage.
    ///
    /// # Panics
    ///
    /// Panics when this store uses heap storage.
    #[must_use]
    pub fn stable_schema_allocation(&self, memory_namespace: &str) -> StableMemoryAllocation {
        self.stable_allocation(memory_namespace, StoreMemoryRole::Schema)
    }

    /// Build the journal-tail allocation descriptor for journaled stores.
    ///
    /// # Panics
    ///
    /// Panics when this store uses heap storage.
    #[must_use]
    pub fn journal_allocation(&self, memory_namespace: &str) -> StableMemoryAllocation {
        StableMemoryAllocation::without_schema_metadata(stable_store_memory_key(
            memory_namespace,
            self.stable_store_key(),
            "journal",
        ))
    }

    /// Build the schema-memory allocation descriptor with accepted catalog
    /// schema metadata attached for diagnostics.
    ///
    /// # Panics
    ///
    /// Panics when this store uses heap storage.
    #[must_use]
    pub fn stable_schema_allocation_with_schema_metadata(
        &self,
        memory_namespace: &str,
        schema_metadata: StableMemoryAllocationMetadata,
    ) -> StableMemoryAllocation {
        self.stable_allocation_with_schema_metadata(
            memory_namespace,
            StoreMemoryRole::Schema,
            schema_metadata,
        )
    }

    /// Build an allocation descriptor for one role in journaled storage.
    ///
    /// Use [`Self::is_journaled_storage`] or [`Self::journaled_memory_config`]
    /// to check the storage mode before requesting a stable allocation.
    ///
    /// # Panics
    ///
    /// Panics when this store uses heap storage.
    #[must_use]
    pub fn stable_allocation(
        &self,
        memory_namespace: &str,
        role: StoreMemoryRole,
    ) -> StableMemoryAllocation {
        StableMemoryAllocation::without_schema_metadata(stable_store_memory_key(
            memory_namespace,
            self.stable_store_key(),
            role.as_str(),
        ))
    }

    fn stable_allocation_with_schema_metadata(
        &self,
        memory_namespace: &str,
        role: StoreMemoryRole,
        schema_metadata: StableMemoryAllocationMetadata,
    ) -> StableMemoryAllocation {
        StableMemoryAllocation::with_schema_metadata(
            stable_store_memory_key(memory_namespace, self.stable_store_key(), role.as_str()),
            schema_metadata,
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StoreMemoryRole {
    Data,
    Index,
    Schema,
}

impl StoreMemoryRole {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Data => "data",
            Self::Index => "index",
            Self::Schema => "schema",
        }
    }
}

/// Diagnostic schema metadata associated with a stable-memory allocation.
///
/// This metadata does not participate in durable allocation identity. The
/// durable identity remains the permanent stable key.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StableMemoryAllocationMetadata {
    version: Option<u32>,
    fingerprint_method_version: Option<u8>,
    fingerprint: Option<String>,
}

impl StableMemoryAllocationMetadata {
    const fn new(
        schema_version: Option<u32>,
        schema_fingerprint_method_version: Option<u8>,
        schema_fingerprint: Option<String>,
    ) -> Self {
        Self {
            version: schema_version,
            fingerprint_method_version: schema_fingerprint_method_version,
            fingerprint: schema_fingerprint,
        }
    }

    /// Build allocation metadata from an accepted schema/catalog authority.
    #[must_use]
    pub const fn from_accepted_schema_contract(
        schema_version: u32,
        schema_fingerprint_method_version: u8,
        schema_fingerprint: String,
    ) -> Self {
        Self::new(
            Some(schema_version),
            Some(schema_fingerprint_method_version),
            Some(schema_fingerprint),
        )
    }

    /// Build absent allocation metadata for allocations with no accepted
    /// schema/catalog authority.
    #[must_use]
    pub const fn absent() -> Self {
        Self::new(None, None, None)
    }

    /// Accepted schema/catalog version, when known.
    #[must_use]
    pub const fn schema_version(&self) -> Option<u32> {
        self.version
    }

    /// Accepted schema/catalog fingerprint method version, when known.
    #[must_use]
    pub const fn schema_fingerprint_method_version(&self) -> Option<u8> {
        self.fingerprint_method_version
    }

    /// Accepted schema/catalog fingerprint, when known.
    #[must_use]
    pub const fn schema_fingerprint(&self) -> Option<&str> {
        match &self.fingerprint {
            Some(value) => Some(value.as_str()),
            None => None,
        }
    }
}

/// Stable-memory allocation descriptor.
///
/// The stable key is the durable allocation identity; physical placement is
/// resolved by the runtime, not the schema model.
/// `schema_version + schema_fingerprint_method_version + schema_fingerprint`
/// is diagnostic metadata only.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StableMemoryAllocation {
    stable_key: String,
    schema_metadata: StableMemoryAllocationMetadata,
}

impl StableMemoryAllocation {
    /// Build an allocation descriptor without schema metadata.
    #[must_use]
    pub const fn without_schema_metadata(stable_key: String) -> Self {
        Self::with_schema_metadata(stable_key, StableMemoryAllocationMetadata::absent())
    }

    /// Build an allocation descriptor with diagnostic schema metadata.
    ///
    /// The metadata must come from accepted schema/catalog authority. Generated
    /// model fallback metadata is not an allocation metadata authority.
    #[must_use]
    pub const fn with_schema_metadata(
        stable_key: String,
        schema_metadata: StableMemoryAllocationMetadata,
    ) -> Self {
        Self {
            stable_key,
            schema_metadata,
        }
    }

    /// Durable stable-memory key.
    #[must_use]
    pub const fn stable_key(&self) -> &str {
        self.stable_key.as_str()
    }

    /// Diagnostic schema/catalog metadata.
    #[must_use]
    pub const fn schema_metadata(&self) -> &StableMemoryAllocationMetadata {
        &self.schema_metadata
    }

    /// Accepted schema/catalog version, when known.
    #[must_use]
    pub const fn schema_version(&self) -> Option<u32> {
        self.schema_metadata.schema_version()
    }

    /// Accepted schema/catalog fingerprint method version, when known.
    #[must_use]
    pub const fn schema_fingerprint_method_version(&self) -> Option<u8> {
        self.schema_metadata.schema_fingerprint_method_version()
    }

    /// Accepted schema/catalog fingerprint, when known.
    #[must_use]
    pub const fn schema_fingerprint(&self) -> Option<&str> {
        self.schema_metadata.schema_fingerprint()
    }

    /// Compare durable allocation identity only.
    ///
    /// Schema metadata is intentionally ignored because metadata changes are
    /// diagnostics, not memory replacement.
    #[must_use]
    pub fn same_identity_as(&self, other: &Self) -> bool {
        self.stable_key == other.stable_key
    }
}

#[must_use]
pub fn stable_memory_key(memory_namespace: &str, allocation: &str, role: &str) -> String {
    format!("icydb.{memory_namespace}.{allocation}.{role}.v1")
}

#[must_use]
fn stable_store_memory_key(memory_namespace: &str, store_key: &str, role: &str) -> String {
    // Explicit logical identity survives source-level renames and declaration order.
    format!("icydb.{memory_namespace}.store.{store_key}.{role}.v1")
}

impl MacroNode for Store {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl ValidateNode for Store {
    fn validate(&self) -> Result<(), ErrorTree> {
        let mut errs = ErrorTree::new();

        {
            let schema = schema_read();

            match schema.cast_node::<Canister>(self.canister()) {
                Ok(canister) => match self.storage() {
                    StoreStorage::Heap(_) => {}
                    StoreStorage::Journaled(config) => {
                        validate_journaled_memory_config(&mut errs, self, *config, canister);
                    }
                },
                Err(e) => errs.add(e),
            }
        }

        errs.result()
    }
}

fn validate_journaled_memory_config(
    errs: &mut ErrorTree,
    store: &Store,
    config: StoreJournaledMemoryConfig,
    canister: &Canister,
) {
    validate_stable_key_segment(errs, "store key", config.key());
    for allocation in [
        store.stable_data_allocation(canister.memory_namespace()),
        store.stable_index_allocation(canister.memory_namespace()),
        store.stable_schema_allocation(canister.memory_namespace()),
        store.journal_allocation(canister.memory_namespace()),
    ] {
        validate_stable_key(errs, "store allocation key", allocation.stable_key());
    }
}

impl VisitableNode for Store {
    fn route_key(&self) -> String {
        self.def().path()
    }

    fn drive<V: Visitor>(&self, v: &mut V) {
        self.def().accept(v);
    }
}

#[cfg(test)]
mod tests;
