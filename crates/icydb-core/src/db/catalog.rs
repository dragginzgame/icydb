//! Module: db::catalog
//! Responsibility: catalog-level metadata DTOs for SHOW-style introspection.
//! Does not own: schema validation, query planning, or runtime store policy.
//! Boundary: projects runtime entity/store registration metadata for callers.

use candid::CandidType;
use serde::Deserialize;

///
/// EntityCatalogDescription
///
/// One runtime-registered entity entry for `SHOW ENTITIES`.
///

#[cfg_attr(
    doc,
    doc = "EntityCatalogDescription\n\nRuntime catalog entry for one registered entity."
)]
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct EntityCatalogDescription {
    entity_name: String,
    entity_path: String,
    store_path: String,
    storage: String,
    columns: u32,
    indexes: u32,
    relations: u32,
    schema_version: u32,
}

impl EntityCatalogDescription {
    /// Construct one entity catalog entry.
    #[must_use]
    pub const fn new(
        entity_name: String,
        entity_path: String,
        store_path: String,
        storage: String,
        columns: u32,
        indexes: u32,
        relations: u32,
        schema_version: u32,
    ) -> Self {
        Self {
            entity_name,
            entity_path,
            store_path,
            storage,
            columns,
            indexes,
            relations,
            schema_version,
        }
    }

    /// Stable external entity name.
    #[must_use]
    pub const fn entity_name(&self) -> &str {
        self.entity_name.as_str()
    }

    /// Runtime entity path.
    #[must_use]
    pub const fn entity_path(&self) -> &str {
        self.entity_path.as_str()
    }

    /// Owning store path.
    #[must_use]
    pub const fn store_path(&self) -> &str {
        self.store_path.as_str()
    }

    /// User-facing storage mode for the owning store.
    #[must_use]
    pub const fn storage(&self) -> &str {
        self.storage.as_str()
    }

    /// Number of top-level columns registered for this entity.
    #[must_use]
    pub const fn columns(&self) -> u32 {
        self.columns
    }

    /// Number of accepted secondary indexes registered for this entity.
    #[must_use]
    pub const fn indexes(&self) -> u32 {
        self.indexes
    }

    /// Number of relation fields registered for this entity.
    #[must_use]
    pub const fn relations(&self) -> u32 {
        self.relations
    }

    /// Accepted schema version for this entity.
    #[must_use]
    pub const fn schema_version(&self) -> u32 {
        self.schema_version
    }
}

///
/// StoreCatalogDescription
///
/// One runtime-registered store entry for `SHOW STORES`.
///

#[cfg_attr(
    doc,
    doc = "StoreCatalogDescription\n\nRuntime catalog entry for one registered store."
)]
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct StoreCatalogDescription {
    store_path: String,
    storage: String,
}

impl StoreCatalogDescription {
    /// Construct one store catalog entry.
    #[must_use]
    pub const fn new(store_path: String, storage: String) -> Self {
        Self {
            store_path,
            storage,
        }
    }

    /// Store path.
    #[must_use]
    pub const fn store_path(&self) -> &str {
        self.store_path.as_str()
    }

    /// User-facing storage mode.
    #[must_use]
    pub const fn storage(&self) -> &str {
        self.storage.as_str()
    }
}

///
/// MemoryCatalogDescription
///
/// One runtime-registered stable-memory allocation entry for `SHOW MEMORY`.
///

#[cfg_attr(
    doc,
    doc = "MemoryCatalogDescription\n\nRuntime catalog entry for one stable-memory allocation."
)]
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct MemoryCatalogDescription {
    tag: String,
    memory_id: u8,
    store_path: String,
}

impl MemoryCatalogDescription {
    /// Construct one memory catalog entry.
    #[must_use]
    pub const fn new(tag: String, memory_id: u8, store_path: String) -> Self {
        Self {
            tag,
            memory_id,
            store_path,
        }
    }

    /// Durable stable-memory key used as the memory tag.
    #[must_use]
    pub const fn tag(&self) -> &str {
        self.tag.as_str()
    }

    /// Stable-memory manager ID.
    #[must_use]
    pub const fn memory_id(&self) -> u8 {
        self.memory_id
    }

    /// Owning store path.
    #[must_use]
    pub const fn store_path(&self) -> &str {
        self.store_path.as_str()
    }
}
