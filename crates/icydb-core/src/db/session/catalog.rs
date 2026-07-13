//! Module: db::session::catalog
//! Responsibility: session-owned catalog, schema-description, and storage
//! observability surfaces.
//! Does not own: schema reconciliation policy, query planning, or storage
//! mutation.
//! Boundary: converts accepted/generated schema authority into stable
//! introspection DTOs at the session facade.

#[cfg(any(test, feature = "sql-explain"))]
use crate::db::{IndexState, QueryError, query::plan::VisibleIndexes};
use crate::{
    db::{
        DbSession, EntityCatalogCounts, EntityCatalogDescription, EntityFieldDescription,
        EntitySchemaDescription, IntegrityReport, StorageReport, StoreCatalogDescription,
        schema::{
            AcceptedFieldKind, PersistedFieldSnapshot, SchemaInfo, describe_entity_fields,
            describe_entity_fields_with_persisted_schema, describe_entity_model,
            describe_entity_model_with_persisted_schema, show_indexes_for_model,
            show_indexes_for_model_with_runtime_state,
            show_indexes_for_schema_info_with_runtime_state,
        },
    },
    entity::EntityKind,
    error::InternalError,
    model::entity::EntityModel,
    traits::{CanisterKind, Path},
};

fn relation_field_count(fields: &[PersistedFieldSnapshot]) -> usize {
    fields
        .iter()
        .filter(|field| persisted_kind_is_relation_field(field.kind()))
        .count()
}

fn persisted_kind_is_relation_field(kind: &AcceptedFieldKind) -> bool {
    match kind {
        AcceptedFieldKind::Relation { .. } => true,
        AcceptedFieldKind::List(inner) | AcceptedFieldKind::Set(inner) => {
            matches!(inner.as_ref(), AcceptedFieldKind::Relation { .. })
        }
        _ => false,
    }
}

impl<C: CanisterKind> DbSession<C> {
    /// Return one stable, human-readable index listing for the entity schema.
    ///
    /// Output format mirrors SQL-style introspection:
    /// - `PRIMARY KEY (field) [state=ready] [origin=generated]`
    /// - `INDEX name (field_a, field_b) [state=ready] [origin=generated]`
    /// - `UNIQUE INDEX name (field_a, field_b) [state=ready] [origin=generated]`
    #[must_use]
    pub fn show_indexes<E>(&self) -> Vec<String>
    where
        E: EntityKind<Canister = C>,
    {
        self.show_indexes_for_store_model(E::Store::PATH, E::MODEL)
    }

    /// Return one stable, human-readable index listing for one schema model.
    ///
    /// This model-only helper is schema-owned and intentionally does not
    /// attach runtime lifecycle state because it does not carry store
    /// placement context.
    #[must_use]
    pub fn show_indexes_for_model(&self, model: &'static EntityModel) -> Vec<String> {
        show_indexes_for_model(model)
    }

    /// Return one stable, human-readable index listing for the accepted schema.
    ///
    /// Unlike `show_indexes`, this fallible live-schema helper reflects
    /// accepted DDL-created indexes as well as compiled schema indexes.
    pub fn try_show_indexes<E>(&self) -> Result<Vec<String>, InternalError>
    where
        E: EntityKind<Canister = C>,
    {
        let schema = self.accepted_schema_info_for_entity::<E>()?;

        Ok(self.show_indexes_for_store_schema_info(E::Store::PATH, &schema))
    }

    // Return one stable, human-readable index listing for one resolved
    // store/model pair, attaching the current runtime lifecycle state when the
    // registry can resolve the backing store handle.
    pub(in crate::db) fn show_indexes_for_store_model(
        &self,
        store_path: &str,
        model: &'static EntityModel,
    ) -> Vec<String> {
        let runtime_state = self
            .db
            .with_store_registry(|registry| registry.try_get_store(store_path).ok())
            .map(|store| store.index_state());

        show_indexes_for_model_with_runtime_state(model, runtime_state)
    }

    // Return one stable, human-readable index listing for one resolved
    // store/accepted-schema pair, attaching the current runtime lifecycle state
    // when the registry can resolve the backing store handle.
    pub(in crate::db) fn show_indexes_for_store_schema_info(
        &self,
        store_path: &str,
        schema: &SchemaInfo,
    ) -> Vec<String> {
        let runtime_state = self
            .db
            .with_store_registry(|registry| registry.try_get_store(store_path).ok())
            .map(|store| store.index_state());

        show_indexes_for_schema_info_with_runtime_state(schema, runtime_state)
    }

    /// Return one stable generated-model list of field descriptors.
    ///
    /// This infallible Rust metadata helper intentionally reports the compiled
    /// schema model. Use `try_show_columns` for the accepted persisted-schema
    /// view used by SQL and diagnostics surfaces.
    #[must_use]
    pub fn show_columns<E>(&self) -> Vec<EntityFieldDescription>
    where
        E: EntityKind<Canister = C>,
    {
        self.show_columns_for_model(E::MODEL)
    }

    /// Return one stable generated-model list of field descriptors.
    #[must_use]
    pub fn show_columns_for_model(
        &self,
        model: &'static EntityModel,
    ) -> Vec<EntityFieldDescription> {
        describe_entity_fields(model)
    }

    /// Return field descriptors using the accepted persisted schema snapshot.
    ///
    /// This fallible variant is intended for SQL and diagnostics surfaces that
    /// can report schema reconciliation failures. The infallible
    /// `show_columns` helper remains generated-model based.
    pub fn try_show_columns<E>(&self) -> Result<Vec<EntityFieldDescription>, InternalError>
    where
        E: EntityKind<Canister = C>,
    {
        let snapshot = self.ensure_accepted_schema_snapshot::<E>()?;

        Ok(describe_entity_fields_with_persisted_schema(&snapshot))
    }

    /// Return one stable list of runtime-registered entity catalog entries.
    ///
    /// # Panics
    ///
    /// Panics if the runtime session cannot read its registered entity catalog.
    /// Use `try_show_entities` when the caller can report the internal error.
    #[must_use]
    pub fn show_entities(&self) -> Vec<crate::db::EntityCatalogDescription> {
        self.try_show_entities().expect("session invariant")
    }

    /// Return one stable list of runtime-registered entity catalog entries.
    pub fn try_show_entities(&self) -> Result<Vec<EntityCatalogDescription>, InternalError> {
        let mut entities = Vec::with_capacity(self.db.entity_runtime_hooks.len());

        for hooks in self.db.entity_runtime_hooks {
            let store = self.db.recovered_store(hooks.store_path)?;
            let storage = store
                .storage_capabilities()
                .storage_mode()
                .as_str()
                .to_string();
            let accepted = self.accepted_schema_catalog_context_for_runtime_hook(hooks, store)?;
            let snapshot = accepted.snapshot().persisted_snapshot();

            entities.push(EntityCatalogDescription::new(
                snapshot.entity_name().to_string(),
                snapshot.entity_path().to_string(),
                hooks.store_path.to_string(),
                storage,
                EntityCatalogCounts::new(
                    u32::try_from(snapshot.fields().len()).unwrap_or(u32::MAX),
                    u32::try_from(snapshot.indexes().len()).unwrap_or(u32::MAX),
                    u32::try_from(relation_field_count(snapshot.fields())).unwrap_or(u32::MAX),
                    snapshot.version().get(),
                ),
            ));
        }

        Ok(entities)
    }

    /// Return one stable list of runtime-registered stores.
    #[must_use]
    pub fn show_stores(&self) -> Vec<StoreCatalogDescription> {
        self.db.runtime_store_catalog()
    }

    /// Return one stable list of runtime-registered stable-memory allocations.
    #[must_use]
    pub fn show_memory(&self) -> Vec<crate::db::MemoryCatalogDescription> {
        self.db.runtime_memory_catalog()
    }

    // Resolve the exact secondary-index set that is visible to planner-owned
    // query planning for one recovered store and accepted schema pair.
    #[cfg(any(test, feature = "sql-explain"))]
    pub(in crate::db::session) fn visible_indexes_for_store_accepted_schema(
        &self,
        store_path: &str,
        schema_info: &SchemaInfo,
    ) -> Result<VisibleIndexes<'static>, QueryError> {
        // Phase 1: resolve the recovered store state once at the session
        // boundary so query/executor planning does not reopen lifecycle checks.
        let store = self
            .db
            .recovered_store(store_path)
            .map_err(QueryError::execute)?;
        let state = store.index_state();
        if state != IndexState::Ready {
            return Ok(VisibleIndexes::none());
        }
        debug_assert_eq!(state, IndexState::Ready);

        // Phase 2: planner-visible indexes are accepted schema contracts once
        // the recovered store is query-visible.
        let visible_indexes = VisibleIndexes::accepted_schema_visible(schema_info);
        debug_assert!(visible_indexes.accepted_field_path_contracts_are_consistent());
        debug_assert!(visible_indexes.accepted_expression_contracts_are_consistent());
        debug_assert_eq!(
            visible_indexes.accepted_expression_index_count(),
            Some(visible_indexes.accepted_expression_indexes().len()),
        );

        Ok(visible_indexes)
    }

    /// Return one generated-model schema description for the entity.
    ///
    /// This is a typed `DESCRIBE`-style introspection surface consumed by
    /// developer tooling and pre-EXPLAIN debugging when a non-failing compiled
    /// schema view is required.
    #[must_use]
    pub fn describe_entity<E>(&self) -> EntitySchemaDescription
    where
        E: EntityKind<Canister = C>,
    {
        self.describe_entity_model(E::MODEL)
    }

    /// Return one generated-model schema description for one schema model.
    #[must_use]
    pub fn describe_entity_model(&self, model: &'static EntityModel) -> EntitySchemaDescription {
        describe_entity_model(model)
    }

    /// Return a schema description using the accepted persisted schema snapshot.
    ///
    /// This is the live-schema counterpart to `describe_entity`. It is fallible
    /// because loading accepted schema authority can fail if startup
    /// reconciliation rejects the stored metadata.
    pub fn try_describe_entity<E>(&self) -> Result<EntitySchemaDescription, InternalError>
    where
        E: EntityKind<Canister = C>,
    {
        let snapshot = self.ensure_accepted_schema_snapshot::<E>()?;

        Ok(describe_entity_model_with_persisted_schema(
            E::MODEL,
            &snapshot,
        ))
    }

    /// Build one point-in-time storage report for observability endpoints.
    pub fn storage_report(
        &self,
        name_to_path: &[(&'static str, &'static str)],
    ) -> Result<StorageReport, InternalError> {
        self.db.storage_report(name_to_path)
    }

    /// Build one point-in-time storage report using default entity-path labels.
    pub fn storage_report_default(&self) -> Result<StorageReport, InternalError> {
        self.db.storage_report_default()
    }

    /// Build one point-in-time integrity scan report for observability endpoints.
    pub fn integrity_report(&self) -> Result<IntegrityReport, InternalError> {
        self.db.integrity_report()
    }
}
