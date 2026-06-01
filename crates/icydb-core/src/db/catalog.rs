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
}

impl EntityCatalogDescription {
    /// Construct one entity catalog entry.
    #[must_use]
    pub const fn new(entity_name: String, entity_path: String, store_path: String) -> Self {
        Self {
            entity_name,
            entity_path,
            store_path,
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
