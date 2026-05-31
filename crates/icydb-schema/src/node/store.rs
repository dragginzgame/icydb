use crate::node::{
    validate_app_memory_id, validate_memory_id_in_range, validate_memory_id_not_reserved,
    validate_stable_key, validate_stable_key_segment,
};
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
    ident: &'static str,
    name: &'static str,
    canister: &'static str,
    storage: StoreStorage,
}

/// Storage configuration owned by one schema store declaration.
///
/// 0.174 admits stable, heap, and journaled cached-stable storage as distinct
/// store storage modes. Stable-only memory ID fields must not acquire heap
/// meaning.
#[derive(Clone, Debug, Serialize)]
pub enum StoreStorage {
    /// Durable stable-memory store using one memory for data, one for indexes,
    /// and one for accepted schema metadata.
    Stable(StoreStableMemoryConfig),
    /// Volatile heap store with no stable allocation identity.
    Heap(StoreHeapConfig),
    /// Journaled cached-stable store using canonical stable data/index/schema
    /// memories plus a durable journal-tail memory.
    Journaled(StoreJournaledMemoryConfig),
}

impl StoreStorage {
    /// Borrow the stable-memory configuration.
    ///
    /// Future storage forms return `None`; callers that require stable memory
    /// must fail closed or remain explicitly stable-only.
    #[must_use]
    pub const fn stable_memory_config(&self) -> Option<&StoreStableMemoryConfig> {
        match self {
            Self::Stable(config) => Some(config),
            Self::Heap(_) | Self::Journaled(_) => None,
        }
    }

    /// Borrow the journaled cached-stable configuration.
    #[must_use]
    pub const fn journaled_memory_config(&self) -> Option<&StoreJournaledMemoryConfig> {
        match self {
            Self::Journaled(config) => Some(config),
            Self::Stable(_) | Self::Heap(_) => None,
        }
    }

    /// Return the capability descriptor derived from this storage mode.
    #[must_use]
    pub const fn storage_capabilities(&self) -> StoreStorageCapabilities {
        match self {
            Self::Stable(_) => StoreStorageCapabilities::stable(),
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
    /// Durable stable-memory storage.
    Stable,
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
    /// Store contents can be recovered through stable commit replay.
    StableCommitReplay,
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
    /// Schema metadata has durable accepted-history semantics.
    DurableAcceptedHistory,
    /// Schema metadata is rebuilt live and is not durable history.
    LiveRebuiltMetadata,
    /// Schema metadata is canonical stable history plus committed journal tail.
    CanonicalStableHistoryPlusJournalTail,
}

/// Strong relation source capability for a store.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum RelationSourceCapability {
    /// Source rows can own durable relation integrity.
    DurableSource,
    /// Source rows can participate in live relation validation.
    LiveSource,
}

/// Strong relation target capability for a store.
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
/// Stable allocation identity remains `memory_id + stable_key`; heap allocation
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
    /// Capability descriptor for stable-memory stores.
    #[must_use]
    pub const fn stable() -> Self {
        Self {
            storage_mode: StoreStorageMode::Stable,
            allocation_identity: AllocationIdentityCapability::Present,
            durability: StoreDurability::Durable,
            recovery: StoreRecoveryCapability::StableCommitReplay,
            commit_participation: CommitParticipation::Durable,
            schema_metadata: SchemaMetadataCapability::DurableAcceptedHistory,
            relation_source: RelationSourceCapability::DurableSource,
            relation_target: RelationTargetCapability::DurableTarget,
            live_validation: LiveValidationCapability::Supported,
        }
    }

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

/// Stable-memory IDs for the three durable roles owned by one store.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct StoreStableMemoryConfig {
    data: u8,
    index: u8,
    schema: u8,
}

/// Stable-memory IDs for the four durable roles owned by one journaled
/// cached-stable store.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct StoreJournaledMemoryConfig {
    data: u8,
    index: u8,
    schema: u8,
    journal: u8,
}

impl StoreJournaledMemoryConfig {
    /// Build a journaled memory configuration from canonical data, index,
    /// schema, and journal-tail memory IDs.
    #[must_use]
    pub const fn new(
        data_memory_id: u8,
        index_memory_id: u8,
        schema_memory_id: u8,
        journal_memory_id: u8,
    ) -> Self {
        Self {
            data: data_memory_id,
            index: index_memory_id,
            schema: schema_memory_id,
            journal: journal_memory_id,
        }
    }

    /// Canonical data-store stable memory ID.
    #[must_use]
    pub const fn data_memory_id(self) -> u8 {
        self.data
    }

    /// Canonical index-store stable memory ID.
    #[must_use]
    pub const fn index_memory_id(self) -> u8 {
        self.index
    }

    /// Canonical schema-store stable memory ID.
    #[must_use]
    pub const fn schema_memory_id(self) -> u8 {
        self.schema
    }

    /// Durable journal-tail stable memory ID.
    #[must_use]
    pub const fn journal_memory_id(self) -> u8 {
        self.journal
    }
}

impl StoreStableMemoryConfig {
    /// Build a stable-memory configuration from data, index, and schema memory
    /// IDs.
    #[must_use]
    pub const fn new(data_memory_id: u8, index_memory_id: u8, schema_memory_id: u8) -> Self {
        Self {
            data: data_memory_id,
            index: index_memory_id,
            schema: schema_memory_id,
        }
    }

    /// Data-store stable memory ID.
    #[must_use]
    pub const fn data_memory_id(self) -> u8 {
        self.data
    }

    /// Index-store stable memory ID.
    #[must_use]
    pub const fn index_memory_id(self) -> u8 {
        self.index
    }

    /// Schema-store stable memory ID.
    #[must_use]
    pub const fn schema_memory_id(self) -> u8 {
        self.schema
    }
}

impl Store {
    /// Build a stable-memory-backed store declaration.
    ///
    /// This is the durable store constructor.
    #[must_use]
    pub const fn new_stable(
        def: Def,
        ident: &'static str,
        store_name: &'static str,
        canister: &'static str,
        stable: StoreStableMemoryConfig,
    ) -> Self {
        Self {
            def,
            ident,
            name: store_name,
            canister,
            storage: StoreStorage::Stable(stable),
        }
    }

    /// Build a heap-backed volatile store declaration.
    #[must_use]
    pub const fn new_heap(
        def: Def,
        ident: &'static str,
        store_name: &'static str,
        canister: &'static str,
        heap: StoreHeapConfig,
    ) -> Self {
        Self {
            def,
            ident,
            name: store_name,
            canister,
            storage: StoreStorage::Heap(heap),
        }
    }

    /// Build a journaled cached-stable store declaration.
    #[must_use]
    pub const fn new_journaled(
        def: Def,
        ident: &'static str,
        store_name: &'static str,
        canister: &'static str,
        journaled: StoreJournaledMemoryConfig,
    ) -> Self {
        Self {
            def,
            ident,
            name: store_name,
            canister,
            storage: StoreStorage::Journaled(journaled),
        }
    }

    #[must_use]
    pub const fn def(&self) -> &Def {
        &self.def
    }

    #[must_use]
    pub const fn ident(&self) -> &'static str {
        self.ident
    }

    #[must_use]
    pub const fn store_name(&self) -> &'static str {
        self.name
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

    /// Return whether this store is stable-memory-backed.
    #[must_use]
    pub const fn is_stable_storage(&self) -> bool {
        matches!(self.storage, StoreStorage::Stable(_))
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

    /// Borrow stable-memory IDs when this store uses stable storage.
    #[must_use]
    pub const fn stable_memory_config(&self) -> Option<&StoreStableMemoryConfig> {
        self.storage.stable_memory_config()
    }

    /// Borrow journaled cached-stable memory IDs when this store uses
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

    #[must_use]
    pub const fn stable_data_memory_id(&self) -> u8 {
        match self.storage {
            StoreStorage::Stable(config) => config.data_memory_id(),
            StoreStorage::Journaled(config) => config.data_memory_id(),
            StoreStorage::Heap(_) => panic!("heap stores do not have a stable data memory id"),
        }
    }

    #[must_use]
    pub const fn stable_index_memory_id(&self) -> u8 {
        match self.storage {
            StoreStorage::Stable(config) => config.index_memory_id(),
            StoreStorage::Journaled(config) => config.index_memory_id(),
            StoreStorage::Heap(_) => panic!("heap stores do not have a stable index memory id"),
        }
    }

    #[must_use]
    pub const fn stable_schema_memory_id(&self) -> u8 {
        match self.storage {
            StoreStorage::Stable(config) => config.schema_memory_id(),
            StoreStorage::Journaled(config) => config.schema_memory_id(),
            StoreStorage::Heap(_) => panic!("heap stores do not have a stable schema memory id"),
        }
    }

    #[must_use]
    pub const fn journal_memory_id(&self) -> u8 {
        match self.storage {
            StoreStorage::Journaled(config) => config.journal_memory_id(),
            StoreStorage::Stable(_) => {
                panic!("stable stores do not have a journal memory id")
            }
            StoreStorage::Heap(_) => panic!("heap stores do not have a journal memory id"),
        }
    }

    #[must_use]
    pub fn stable_data_allocation(&self, memory_namespace: &str) -> StableMemoryAllocation {
        self.stable_allocation(memory_namespace, StoreMemoryRole::Data)
    }

    /// Build the data-memory allocation descriptor with accepted row-layout
    /// schema metadata attached for diagnostics.
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

    #[must_use]
    pub fn stable_index_allocation(&self, memory_namespace: &str) -> StableMemoryAllocation {
        self.stable_allocation(memory_namespace, StoreMemoryRole::Index)
    }

    /// Build the index-memory allocation descriptor with accepted index-catalog
    /// schema metadata attached for diagnostics.
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

    #[must_use]
    pub fn stable_schema_allocation(&self, memory_namespace: &str) -> StableMemoryAllocation {
        self.stable_allocation(memory_namespace, StoreMemoryRole::Schema)
    }

    /// Build the journal-tail allocation descriptor for journaled stores.
    #[must_use]
    pub fn journal_allocation(&self, memory_namespace: &str) -> StableMemoryAllocation {
        StableMemoryAllocation::without_schema_metadata(
            self.journal_memory_id(),
            stable_memory_key(memory_namespace, self.store_name(), "journal"),
        )
    }

    /// Build the schema-memory allocation descriptor with accepted catalog
    /// schema metadata attached for diagnostics.
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

    #[must_use]
    pub fn stable_allocation(
        &self,
        memory_namespace: &str,
        role: StoreMemoryRole,
    ) -> StableMemoryAllocation {
        let memory_id = match role {
            StoreMemoryRole::Data => self.stable_data_memory_id(),
            StoreMemoryRole::Index => self.stable_index_memory_id(),
            StoreMemoryRole::Schema => self.stable_schema_memory_id(),
        };

        StableMemoryAllocation::without_schema_metadata(
            memory_id,
            stable_memory_key(memory_namespace, self.store_name(), role.as_str()),
        )
    }

    fn stable_allocation_with_schema_metadata(
        &self,
        memory_namespace: &str,
        role: StoreMemoryRole,
        schema_metadata: StableMemoryAllocationMetadata,
    ) -> StableMemoryAllocation {
        let memory_id = match role {
            StoreMemoryRole::Data => self.stable_data_memory_id(),
            StoreMemoryRole::Index => self.stable_index_memory_id(),
            StoreMemoryRole::Schema => self.stable_schema_memory_id(),
        };

        StableMemoryAllocation::with_schema_metadata(
            memory_id,
            stable_memory_key(memory_namespace, self.store_name(), role.as_str()),
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
/// durable identity remains `memory_id + stable_key`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StableMemoryAllocationMetadata {
    schema_version: Option<u32>,
    schema_fingerprint: Option<String>,
}

impl StableMemoryAllocationMetadata {
    const fn new(schema_version: Option<u32>, schema_fingerprint: Option<String>) -> Self {
        Self {
            schema_version,
            schema_fingerprint,
        }
    }

    /// Build allocation metadata from an accepted schema/catalog authority.
    #[must_use]
    pub const fn from_accepted_schema_contract(
        schema_version: u32,
        schema_fingerprint: String,
    ) -> Self {
        Self::new(Some(schema_version), Some(schema_fingerprint))
    }

    /// Build absent allocation metadata for allocations with no accepted
    /// schema/catalog authority.
    #[must_use]
    pub const fn absent() -> Self {
        Self::new(None, None)
    }

    /// Accepted schema/catalog version, when known.
    #[must_use]
    pub const fn schema_version(&self) -> Option<u32> {
        self.schema_version
    }

    /// Accepted schema/catalog fingerprint, when known.
    #[must_use]
    pub const fn schema_fingerprint(&self) -> Option<&str> {
        match &self.schema_fingerprint {
            Some(value) => Some(value.as_str()),
            None => None,
        }
    }
}

/// Stable-memory allocation descriptor.
///
/// `memory_id + stable_key` is the durable allocation identity.
/// `schema_version + schema_fingerprint` is diagnostic metadata only.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StableMemoryAllocation {
    memory_id: u8,
    stable_key: String,
    schema_metadata: StableMemoryAllocationMetadata,
}

impl StableMemoryAllocation {
    /// Build an allocation descriptor without schema metadata.
    #[must_use]
    pub const fn without_schema_metadata(memory_id: u8, stable_key: String) -> Self {
        Self::with_schema_metadata(
            memory_id,
            stable_key,
            StableMemoryAllocationMetadata::absent(),
        )
    }

    /// Build an allocation descriptor with diagnostic schema metadata.
    ///
    /// The metadata must come from accepted schema/catalog authority. Generated
    /// model fallback metadata is not an allocation metadata authority.
    #[must_use]
    pub const fn with_schema_metadata(
        memory_id: u8,
        stable_key: String,
        schema_metadata: StableMemoryAllocationMetadata,
    ) -> Self {
        Self {
            memory_id,
            stable_key,
            schema_metadata,
        }
    }

    /// Stable-memory manager ID.
    #[must_use]
    pub const fn memory_id(&self) -> u8 {
        self.memory_id
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
        self.memory_id == other.memory_id && self.stable_key == other.stable_key
    }
}

#[must_use]
pub fn stable_memory_key(memory_namespace: &str, store_name: &str, role: &str) -> String {
    format!("icydb.{memory_namespace}.{store_name}.{role}.v1")
}

impl MacroNode for Store {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl ValidateNode for Store {
    fn validate(&self) -> Result<(), ErrorTree> {
        let mut errs = ErrorTree::new();
        let schema = schema_read();

        match schema.cast_node::<Canister>(self.canister()) {
            Ok(canister) => {
                validate_stable_key_segment(&mut errs, "store store_name", self.store_name());
                match self.storage() {
                    StoreStorage::Stable(config) => {
                        validate_stable_memory_config(&mut errs, self, *config, canister);
                    }
                    StoreStorage::Heap(_) => {}
                    StoreStorage::Journaled(config) => {
                        validate_journaled_memory_config(&mut errs, self, *config, canister);
                    }
                }
            }
            Err(e) => errs.add(e),
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
    validate_stable_memory_role(
        errs,
        "data_memory_id",
        "data stable key",
        config.data_memory_id(),
        store
            .stable_data_allocation(canister.memory_namespace())
            .stable_key(),
        canister,
    );
    validate_stable_memory_role(
        errs,
        "index_memory_id",
        "index stable key",
        config.index_memory_id(),
        store
            .stable_index_allocation(canister.memory_namespace())
            .stable_key(),
        canister,
    );
    validate_stable_memory_role(
        errs,
        "schema_memory_id",
        "schema stable key",
        config.schema_memory_id(),
        store
            .stable_schema_allocation(canister.memory_namespace())
            .stable_key(),
        canister,
    );
    validate_stable_memory_role(
        errs,
        "journal_memory_id",
        "journal stable key",
        config.journal_memory_id(),
        store
            .journal_allocation(canister.memory_namespace())
            .stable_key(),
        canister,
    );

    validate_distinct_journaled_memory_ids(errs, config);
}

fn validate_distinct_journaled_memory_ids(
    errs: &mut ErrorTree,
    config: StoreJournaledMemoryConfig,
) {
    let roles = [
        ("data_memory_id", config.data_memory_id()),
        ("index_memory_id", config.index_memory_id()),
        ("schema_memory_id", config.schema_memory_id()),
        ("journal_memory_id", config.journal_memory_id()),
    ];

    for (idx, (left_label, left_id)) in roles.iter().enumerate() {
        for (right_label, right_id) in roles.iter().skip(idx + 1) {
            if left_id == right_id {
                err!(
                    errs,
                    "{} and {} must differ (both are {})",
                    left_label,
                    right_label,
                    left_id,
                );
            }
        }
    }
}

fn validate_stable_memory_config(
    errs: &mut ErrorTree,
    store: &Store,
    config: StoreStableMemoryConfig,
    canister: &Canister,
) {
    validate_stable_memory_role(
        errs,
        "data_memory_id",
        "data stable key",
        config.data_memory_id(),
        store
            .stable_data_allocation(canister.memory_namespace())
            .stable_key(),
        canister,
    );
    validate_stable_memory_role(
        errs,
        "index_memory_id",
        "index stable key",
        config.index_memory_id(),
        store
            .stable_index_allocation(canister.memory_namespace())
            .stable_key(),
        canister,
    );
    validate_stable_memory_role(
        errs,
        "schema_memory_id",
        "schema stable key",
        config.schema_memory_id(),
        store
            .stable_schema_allocation(canister.memory_namespace())
            .stable_key(),
        canister,
    );

    if config.data_memory_id() == config.index_memory_id() {
        err!(
            errs,
            "data_memory_id and index_memory_id must differ (both are {})",
            config.data_memory_id(),
        );
    }
    if config.data_memory_id() == config.schema_memory_id() {
        err!(
            errs,
            "data_memory_id and schema_memory_id must differ (both are {})",
            config.data_memory_id(),
        );
    }
    if config.index_memory_id() == config.schema_memory_id() {
        err!(
            errs,
            "index_memory_id and schema_memory_id must differ (both are {})",
            config.index_memory_id(),
        );
    }
}

fn validate_stable_memory_role(
    errs: &mut ErrorTree,
    memory_label: &str,
    stable_key_label: &str,
    memory_id: u8,
    stable_key: &str,
    canister: &Canister,
) {
    validate_memory_id_in_range(
        errs,
        memory_label,
        memory_id,
        canister.memory_min(),
        canister.memory_max(),
    );
    validate_app_memory_id(errs, memory_label, memory_id);
    validate_memory_id_not_reserved(errs, memory_label, memory_id);
    validate_stable_key(errs, stable_key_label, stable_key);
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
mod tests {
    use crate::{
        build::schema_write,
        node::{Canister, SchemaNode},
    };

    use super::*;

    fn insert_canister(path_module: &'static str, ident: &'static str) {
        schema_write().insert_node(SchemaNode::Canister(Canister::new(
            Def::new(path_module, ident),
            "test_db",
            100,
            254,
            254,
        )));
    }

    #[test]
    fn store_stable_keys_use_durable_icydb_shape() {
        let store = Store::new_stable(
            Def::new("demo::rpg", "CharacterStore"),
            "CHARACTER_STORE",
            "characters",
            "demo::rpg::Canister",
            StoreStableMemoryConfig::new(110, 111, 112),
        );

        assert_eq!(
            store.stable_data_allocation("demo_rpg").stable_key(),
            "icydb.demo_rpg.characters.data.v1",
        );
        assert_eq!(
            store.stable_index_allocation("demo_rpg").stable_key(),
            "icydb.demo_rpg.characters.index.v1",
        );
        assert_eq!(
            store.stable_schema_allocation("demo_rpg").stable_key(),
            "icydb.demo_rpg.characters.schema.v1",
        );
    }

    #[test]
    fn store_allocations_default_to_absent_schema_metadata() {
        let store = Store::new_stable(
            Def::new("demo::rpg", "CharacterStore"),
            "CHARACTER_STORE",
            "characters",
            "demo::rpg::Canister",
            StoreStableMemoryConfig::new(110, 111, 112),
        );

        for allocation in [
            store.stable_data_allocation("demo_rpg"),
            store.stable_index_allocation("demo_rpg"),
            store.stable_schema_allocation("demo_rpg"),
        ] {
            assert_eq!(allocation.schema_version(), None);
            assert_eq!(allocation.schema_fingerprint(), None);
            assert_eq!(
                allocation.schema_metadata(),
                &StableMemoryAllocationMetadata::absent()
            );
        }
    }

    #[test]
    fn allocation_metadata_is_role_specific_and_diagnostic_only() {
        let store = Store::new_stable(
            Def::new("demo::rpg", "CharacterStore"),
            "CHARACTER_STORE",
            "characters",
            "demo::rpg::Canister",
            StoreStableMemoryConfig::new(110, 111, 112),
        );
        let data = store.stable_data_allocation_with_schema_metadata(
            "demo_rpg",
            StableMemoryAllocationMetadata::from_accepted_schema_contract(
                7,
                "data-row-layout".to_string(),
            ),
        );
        let index = store.stable_index_allocation_with_schema_metadata(
            "demo_rpg",
            StableMemoryAllocationMetadata::from_accepted_schema_contract(
                8,
                "index-catalog".to_string(),
            ),
        );
        let schema = store.stable_schema_allocation_with_schema_metadata(
            "demo_rpg",
            StableMemoryAllocationMetadata::from_accepted_schema_contract(
                10,
                "schema-catalog".to_string(),
            ),
        );
        let data_after_reconcile = store.stable_data_allocation_with_schema_metadata(
            "demo_rpg",
            StableMemoryAllocationMetadata::from_accepted_schema_contract(
                9,
                "data-row-layout-v2".to_string(),
            ),
        );

        assert_eq!(data.schema_version(), Some(7));
        assert_eq!(data.schema_fingerprint(), Some("data-row-layout"));
        assert_eq!(index.schema_version(), Some(8));
        assert_eq!(index.schema_fingerprint(), Some("index-catalog"));
        assert_eq!(schema.schema_version(), Some(10));
        assert_eq!(schema.schema_fingerprint(), Some("schema-catalog"));
        assert!(data.same_identity_as(&data_after_reconcile));
        assert!(!data.same_identity_as(&index));
        assert!(!data.same_identity_as(&schema));
    }

    #[test]
    fn store_owns_explicit_stable_storage_config() {
        let store = Store::new_stable(
            Def::new("demo::rpg", "CharacterStore"),
            "CHARACTER_STORE",
            "characters",
            "demo::rpg::Canister",
            StoreStableMemoryConfig::new(110, 111, 112),
        );

        assert!(store.is_stable_storage());
        assert!(store.storage().stable_memory_config().is_some());
        let stable = store
            .stable_memory_config()
            .expect("0.167 model stores stable config explicitly");

        assert_eq!(stable.data_memory_id(), 110);
        assert_eq!(stable.index_memory_id(), 111);
        assert_eq!(stable.schema_memory_id(), 112);
        assert_eq!(store.stable_data_memory_id(), 110);
        assert_eq!(store.stable_index_memory_id(), 111);
        assert_eq!(store.stable_schema_memory_id(), 112);
    }

    #[test]
    fn stable_store_storage_capabilities_describe_durable_contract() {
        let store = Store::new_stable(
            Def::new("demo::rpg", "CharacterStore"),
            "CHARACTER_STORE",
            "characters",
            "demo::rpg::Canister",
            StoreStableMemoryConfig::new(110, 111, 112),
        );
        let capabilities = store.storage_capabilities();

        assert_eq!(capabilities.storage_mode(), StoreStorageMode::Stable);
        assert_eq!(
            capabilities.allocation_identity(),
            AllocationIdentityCapability::Present,
        );
        assert_eq!(capabilities.durability(), StoreDurability::Durable);
        assert_eq!(
            capabilities.recovery(),
            StoreRecoveryCapability::StableCommitReplay,
        );
        assert_eq!(
            capabilities.commit_participation(),
            CommitParticipation::Durable,
        );
        assert_eq!(
            capabilities.schema_metadata(),
            SchemaMetadataCapability::DurableAcceptedHistory,
        );
        assert_eq!(
            capabilities.relation_source(),
            RelationSourceCapability::DurableSource,
        );
        assert_eq!(
            capabilities.relation_target(),
            RelationTargetCapability::DurableTarget,
        );
        assert_eq!(
            capabilities.live_validation(),
            LiveValidationCapability::Supported,
        );
        assert!(capabilities.has_allocation_identity());
        assert!(capabilities.participates_in_durable_commit());
        assert!(!capabilities.is_volatile());
    }

    #[test]
    fn store_owns_explicit_heap_storage_config() {
        insert_canister("store_heap_config", "Canister");
        let store = Store::new_heap(
            Def::new("store_heap_config", "Store"),
            "STORE",
            "heap_store",
            "store_heap_config::Canister",
            StoreHeapConfig::new(),
        );

        assert!(store.is_heap_storage());
        assert!(!store.is_stable_storage());
        assert!(store.stable_memory_config().is_none());
        assert!(store.validate().is_ok());
    }

    #[test]
    fn heap_store_storage_capabilities_describe_volatile_contract() {
        let store = Store::new_heap(
            Def::new("store_heap_capabilities", "Store"),
            "STORE",
            "heap_store",
            "store_heap_capabilities::Canister",
            StoreHeapConfig::new(),
        );
        let capabilities = store.storage_capabilities();

        assert_eq!(capabilities.storage_mode(), StoreStorageMode::Heap);
        assert_eq!(
            capabilities.allocation_identity(),
            AllocationIdentityCapability::Absent,
        );
        assert_eq!(capabilities.durability(), StoreDurability::Volatile);
        assert_eq!(capabilities.recovery(), StoreRecoveryCapability::None);
        assert_eq!(
            capabilities.commit_participation(),
            CommitParticipation::LiveOnly,
        );
        assert_eq!(
            capabilities.schema_metadata(),
            SchemaMetadataCapability::LiveRebuiltMetadata,
        );
        assert_eq!(
            capabilities.relation_source(),
            RelationSourceCapability::LiveSource,
        );
        assert_eq!(
            capabilities.relation_target(),
            RelationTargetCapability::VolatileTarget,
        );
        assert_eq!(
            capabilities.live_validation(),
            LiveValidationCapability::Supported,
        );
        assert!(!capabilities.has_allocation_identity());
        assert!(!capabilities.participates_in_durable_commit());
        assert!(capabilities.is_volatile());
    }

    #[test]
    fn store_owns_explicit_journaled_storage_config() {
        insert_canister("store_journaled_config", "Canister");
        let store = Store::new_journaled(
            Def::new("store_journaled_config", "Store"),
            "STORE",
            "journaled_store",
            "store_journaled_config::Canister",
            StoreJournaledMemoryConfig::new(110, 111, 112, 113),
        );

        assert!(store.is_journaled_storage());
        assert!(!store.is_stable_storage());
        assert!(!store.is_heap_storage());
        let journaled = store
            .journaled_memory_config()
            .expect("journaled model stores four-role config explicitly");

        assert_eq!(journaled.data_memory_id(), 110);
        assert_eq!(journaled.index_memory_id(), 111);
        assert_eq!(journaled.schema_memory_id(), 112);
        assert_eq!(journaled.journal_memory_id(), 113);
        assert_eq!(store.stable_data_memory_id(), 110);
        assert_eq!(store.stable_index_memory_id(), 111);
        assert_eq!(store.stable_schema_memory_id(), 112);
        assert_eq!(store.journal_memory_id(), 113);
        assert!(store.validate().is_ok());
    }

    #[test]
    fn journaled_store_storage_capabilities_describe_cached_stable_contract() {
        let store = Store::new_journaled(
            Def::new("store_journaled_capabilities", "Store"),
            "STORE",
            "journaled_store",
            "store_journaled_capabilities::Canister",
            StoreJournaledMemoryConfig::new(110, 111, 112, 113),
        );
        let capabilities = store.storage_capabilities();

        assert_eq!(capabilities.storage_mode(), StoreStorageMode::Journaled);
        assert_eq!(
            capabilities.allocation_identity(),
            AllocationIdentityCapability::Present,
        );
        assert_eq!(capabilities.durability(), StoreDurability::Durable);
        assert_eq!(
            capabilities.recovery(),
            StoreRecoveryCapability::StableBasePlusJournalReplay,
        );
        assert_eq!(
            capabilities.commit_participation(),
            CommitParticipation::Durable,
        );
        assert_eq!(
            capabilities.schema_metadata(),
            SchemaMetadataCapability::CanonicalStableHistoryPlusJournalTail,
        );
        assert_eq!(
            capabilities.relation_source(),
            RelationSourceCapability::DurableSource,
        );
        assert_eq!(
            capabilities.relation_target(),
            RelationTargetCapability::DurableTarget,
        );
        assert_eq!(
            capabilities.live_validation(),
            LiveValidationCapability::Supported,
        );
        assert!(capabilities.has_allocation_identity());
        assert!(capabilities.participates_in_durable_commit());
        assert!(!capabilities.is_volatile());
    }

    #[test]
    fn journaled_store_allocations_use_role_named_stable_keys() {
        let store = Store::new_journaled(
            Def::new("demo::rpg", "CharacterStore"),
            "CHARACTER_STORE",
            "characters",
            "demo::rpg::Canister",
            StoreJournaledMemoryConfig::new(110, 111, 112, 113),
        );

        assert_eq!(
            store.stable_data_allocation("demo_rpg").stable_key(),
            "icydb.demo_rpg.characters.data.v1",
        );
        assert_eq!(
            store.stable_index_allocation("demo_rpg").stable_key(),
            "icydb.demo_rpg.characters.index.v1",
        );
        assert_eq!(
            store.stable_schema_allocation("demo_rpg").stable_key(),
            "icydb.demo_rpg.characters.schema.v1",
        );
        assert_eq!(
            store.journal_allocation("demo_rpg").stable_key(),
            "icydb.demo_rpg.characters.journal.v1",
        );
    }

    #[test]
    fn storage_capabilities_are_not_allocation_identity() {
        let store_a = Store::new_stable(
            Def::new("demo::rpg", "CharacterStore"),
            "CHARACTER_STORE",
            "characters",
            "demo::rpg::Canister",
            StoreStableMemoryConfig::new(110, 111, 112),
        );
        let store_b = Store::new_stable(
            Def::new("demo::rpg", "InventoryStore"),
            "INVENTORY_STORE",
            "inventory",
            "demo::rpg::Canister",
            StoreStableMemoryConfig::new(120, 121, 122),
        );

        assert_eq!(
            store_a.storage_capabilities(),
            store_b.storage_capabilities()
        );
        assert_ne!(
            store_a.stable_data_allocation("demo_rpg"),
            store_b.stable_data_allocation("demo_rpg"),
            "stable allocation identity must remain separate from capabilities",
        );
    }

    #[test]
    fn capability_consumers_use_axes_not_storage_mode() {
        const fn commit_label(capabilities: StoreStorageCapabilities) -> &'static str {
            match capabilities.commit_participation() {
                CommitParticipation::Durable => "durable",
                CommitParticipation::LiveOnly => "live-only",
            }
        }

        let future_durable_heap_mode = StoreStorageCapabilities {
            storage_mode: StoreStorageMode::Heap,
            allocation_identity: AllocationIdentityCapability::Present,
            durability: StoreDurability::Durable,
            recovery: StoreRecoveryCapability::StableCommitReplay,
            commit_participation: CommitParticipation::Durable,
            schema_metadata: SchemaMetadataCapability::DurableAcceptedHistory,
            relation_source: RelationSourceCapability::DurableSource,
            relation_target: RelationTargetCapability::DurableTarget,
            live_validation: LiveValidationCapability::Supported,
        };

        assert_eq!(commit_label(future_durable_heap_mode), "durable");
        assert!(future_durable_heap_mode.participates_in_durable_commit());
        assert_eq!(
            future_durable_heap_mode.storage_mode(),
            StoreStorageMode::Heap,
            "the diagnostic storage mode must not drive commit policy",
        );
    }

    #[test]
    fn store_stable_storage_config_rejects_duplicate_role_memory_ids() {
        insert_canister("store_duplicate_role_memory_ids", "Canister");
        let store = Store::new_stable(
            Def::new("store_duplicate_role_memory_ids", "Store"),
            "STORE",
            "duplicate_role_memory_ids",
            "store_duplicate_role_memory_ids::Canister",
            StoreStableMemoryConfig::new(110, 110, 112),
        );

        let err = store
            .validate()
            .expect_err("duplicate store role memory IDs must fail validation");
        let rendered = err.to_string();

        assert!(
            rendered.contains("data_memory_id and index_memory_id must differ"),
            "expected duplicate role memory-id error, got: {rendered}"
        );
    }

    #[test]
    fn store_journaled_storage_config_rejects_duplicate_role_memory_ids() {
        insert_canister("store_duplicate_journaled_role_memory_ids", "Canister");
        let store = Store::new_journaled(
            Def::new("store_duplicate_journaled_role_memory_ids", "Store"),
            "STORE",
            "duplicate_journaled_role_memory_ids",
            "store_duplicate_journaled_role_memory_ids::Canister",
            StoreJournaledMemoryConfig::new(110, 111, 112, 112),
        );

        let err = store
            .validate()
            .expect_err("duplicate journaled role memory IDs must fail validation");
        let rendered = err.to_string();

        assert!(
            rendered.contains("schema_memory_id and journal_memory_id must differ"),
            "expected duplicate journaled role memory-id error, got: {rendered}"
        );
    }
}
