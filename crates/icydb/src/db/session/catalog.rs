//! Module: db::session::catalog
//!
//! Responsibility: public `DbSession` catalog, schema, and storage-report
//! facade methods.
//! Does not own: catalog construction, storage diagnostics collection, or SQL
//! rendering.
//! Boundary: delegates to core accepted-schema/catalog authority and maps core
//! errors onto the public facade error type.

use crate::{
    db::{
        EntityCatalogDescription, EntityFieldDescription, EntitySchemaDescription,
        MemoryCatalogDescription, StorageReport, StoreCatalogDescription, session::DbSession,
    },
    error::Error,
    traits::CanisterKind,
};

use icydb_core as core;

impl<C: CanisterKind> DbSession<C> {
    /// Return one stable, human-readable index listing for the entity schema.
    #[must_use]
    pub fn show_indexes<E>(&self) -> Vec<String>
    where
        E: crate::traits::EntityFor<C>,
    {
        self.inner.show_indexes::<E>()
    }

    /// Return one stable list of field descriptors for the entity schema.
    #[must_use]
    pub fn show_columns<E>(&self) -> Vec<EntityFieldDescription>
    where
        E: crate::traits::EntityFor<C>,
    {
        self.inner.show_columns::<E>()
    }

    /// Return one stable list of runtime-registered entity catalog entries.
    #[must_use]
    pub fn show_entities(&self) -> Vec<EntityCatalogDescription> {
        self.inner.show_entities()
    }

    /// Return one stable list of runtime-registered entity catalog entries.
    pub fn try_show_entities(
        &self,
    ) -> Result<Vec<EntityCatalogDescription>, core::error::InternalError> {
        self.inner.try_show_entities()
    }

    /// Return one stable list of runtime-registered store catalog entries.
    #[must_use]
    pub fn show_stores(&self) -> Vec<StoreCatalogDescription> {
        self.inner.show_stores()
    }

    /// Return one stable list of runtime-registered stable-memory allocations.
    #[must_use]
    pub fn show_memory(&self) -> Vec<MemoryCatalogDescription> {
        self.inner.show_memory()
    }

    /// Return one structured schema description for the entity.
    #[must_use]
    pub fn describe_entity<E>(&self) -> EntitySchemaDescription
    where
        E: crate::traits::EntityFor<C>,
    {
        self.inner.describe_entity::<E>()
    }

    /// Return one accepted live-schema description for the entity.
    ///
    /// Generated schema endpoints use this accepted-schema path so DDL-published
    /// index metadata and recovered schema authority are reflected in tooling
    /// payloads instead of only the compiled model proposal.
    pub fn try_describe_entity<E>(&self) -> Result<EntitySchemaDescription, Error>
    where
        E: crate::traits::EntityFor<C>,
    {
        Ok(self.inner.try_describe_entity::<E>()?)
    }

    /// Build one point-in-time storage report for observability endpoints.
    pub fn storage_report(
        &self,
        name_to_path: &[(&'static str, &'static str)],
    ) -> Result<StorageReport, Error> {
        Ok(self.inner.storage_report(name_to_path)?)
    }
}
