use crate::node::{
    validate_app_memory_id, validate_memory_id_in_range, validate_memory_id_not_reserved,
    validate_stable_key, validate_stable_key_segment,
};
use crate::prelude::*;

///
/// Store
///
/// Schema node describing stable IC BTreeMap memories that store:
/// - primary entity data
/// - all index data for that entity
/// - persisted schema metadata for that store
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
/// 0.167 implements only stable storage. Future storage forms should add new
/// variants here rather than teaching stable-only memory ID fields new meaning.
#[derive(Clone, Debug, Serialize)]
pub enum StoreStorage {
    /// Durable stable-memory store using one memory for data, one for indexes,
    /// and one for accepted schema metadata.
    Stable(StoreStableMemoryConfig),
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
        }
    }
}

/// Stable-memory IDs for the three durable roles owned by one store.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct StoreStableMemoryConfig {
    data: u8,
    index: u8,
    schema: u8,
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
    /// This is the only implemented store constructor in 0.167.
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

    /// Borrow stable-memory IDs when this store uses stable storage.
    #[must_use]
    pub const fn stable_memory_config(&self) -> Option<&StoreStableMemoryConfig> {
        self.storage.stable_memory_config()
    }

    #[must_use]
    pub const fn data_memory_id(&self) -> u8 {
        match self.storage {
            StoreStorage::Stable(config) => config.data_memory_id(),
        }
    }

    #[must_use]
    pub const fn index_memory_id(&self) -> u8 {
        match self.storage {
            StoreStorage::Stable(config) => config.index_memory_id(),
        }
    }

    #[must_use]
    pub const fn schema_memory_id(&self) -> u8 {
        match self.storage {
            StoreStorage::Stable(config) => config.schema_memory_id(),
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
            StoreMemoryRole::Data => self.data_memory_id(),
            StoreMemoryRole::Index => self.index_memory_id(),
            StoreMemoryRole::Schema => self.schema_memory_id(),
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
            StoreMemoryRole::Data => self.data_memory_id(),
            StoreMemoryRole::Index => self.index_memory_id(),
            StoreMemoryRole::Schema => self.schema_memory_id(),
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
                }
            }
            Err(e) => errs.add(e),
        }

        errs.result()
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
        assert_eq!(store.data_memory_id(), 110);
        assert_eq!(store.index_memory_id(), 111);
        assert_eq!(store.schema_memory_id(), 112);
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
}
