//! Module: db::session::catalog
//! Responsibility: session-owned catalog, schema-description, and storage
//! observability surfaces.
//! Does not own: schema reconciliation policy, query planning, or storage
//! mutation.
//! Boundary: converts accepted/generated schema authority into stable
//! introspection DTOs at the session facade.

#[cfg(feature = "sql")]
use crate::db::schema::show_indexes_for_schema_info_with_runtime_state;
use crate::{
    db::{
        DbSession, EntityCatalogCounts, EntityCatalogDescription, EntityIdentityDescription,
        EntitySchemaDescription, IndexState, QueryError, SchemaApplicationTarget,
        SchemaChangeJobId, SchemaChangeProgress, SchemaChangeReceipt, StorageReport,
        StoreCatalogDescription,
        commit::database_incarnation_id,
        query::plan::VisibleIndexes,
        schema::{
            AcceptedEntityDescriptionMetadata, ConstraintValidationJob, SchemaInfo,
            describe_accepted_entity_with_persisted_schema, describe_accepted_identity,
        },
    },
    error::InternalError,
    traits::CanisterKind,
};
use icydb_schema::{SchemaProposal, SchemaSubmissionKey, TargetDatabaseIdentity};

#[cfg(feature = "migration")]
use crate::db::{SchemaMigrationCommand, SchemaMigrationStatusPage, SchemaMigrationStatusRequest};

impl<C: CanisterKind> DbSession<C> {
    /// Return whether exact prepared migration authority deliberately defers
    /// generated schema application while predecessor runtime state stays live.
    #[cfg(feature = "migration")]
    pub fn defer_generated_schema_application_for_prepared_migration(
        &self,
        proposal: &SchemaProposal,
    ) -> Result<bool, InternalError> {
        crate::db::schema::defer_generated_schema_application_for_prepared_migration(
            &self.db, proposal,
        )
    }

    /// Execute one explicit metadata source-migration operation.
    #[cfg(feature = "migration")]
    pub fn migrate_schema(
        &self,
        proposal: &SchemaProposal,
        command: SchemaMigrationCommand,
    ) -> Result<SchemaMigrationStatusPage, InternalError> {
        crate::db::schema::migrate_schema(&self.db, proposal, command)
    }

    /// Return one bounded source-migration status page.
    #[cfg(feature = "migration")]
    pub fn schema_migration_status(
        &self,
        proposal: &SchemaProposal,
        request: &SchemaMigrationStatusRequest,
    ) -> Result<SchemaMigrationStatusPage, InternalError> {
        crate::db::schema::schema_migration_status(&self.db, proposal, request)
    }

    /// Apply one canonical generated proposal after proving it contains no
    /// explicit removals.
    #[doc(hidden)]
    pub fn apply_generated_schema(
        &self,
        proposal: &SchemaProposal,
    ) -> Result<SchemaChangeReceipt, InternalError> {
        crate::db::schema::apply_generated_schema(&self.db, proposal)
    }

    /// Apply one exact source-keyed schema proposal through accepted catalog
    /// authority and return its durable idempotent receipt.
    pub fn apply_schema(
        &self,
        proposal: &SchemaProposal,
    ) -> Result<SchemaChangeReceipt, InternalError> {
        crate::db::schema::apply_schema(&self.db, proposal)
    }

    /// Issue the opaque database/store identities and exact accepted head used
    /// to compose one optimistic schema proposal.
    pub fn schema_application_target(&self) -> Result<SchemaApplicationTarget, InternalError> {
        crate::db::schema::schema_application_target(&self.db)
    }

    /// Load one durable schema-application receipt by exact target and
    /// submission identity.
    pub fn schema_application_receipt(
        &self,
        database_identity: TargetDatabaseIdentity,
        submission_key: &SchemaSubmissionKey,
    ) -> Result<Option<SchemaChangeReceipt>, InternalError> {
        crate::db::schema::schema_application_receipt(&self.db, database_identity, submission_key)
    }

    /// Advance one pending schema application by at most one bounded
    /// activation step.
    pub fn continue_schema_application(
        &self,
        job_id: SchemaChangeJobId,
        acknowledged_receipt: Option<u64>,
    ) -> Result<SchemaChangeProgress, InternalError> {
        crate::db::schema::continue_schema_application(&self.db, job_id, acknowledged_receipt)
    }

    /// Abort one pending schema application after acknowledging any retained
    /// finding page by exact sequence.
    pub fn abort_schema_application(
        &self,
        job_id: SchemaChangeJobId,
        acknowledged_receipt: Option<u64>,
    ) -> Result<SchemaChangeProgress, InternalError> {
        crate::db::schema::abort_schema_application(&self.db, job_id, acknowledged_receipt)
    }

    // Return one stable, human-readable index listing for one resolved
    // store/accepted-schema pair, attaching the current runtime lifecycle state
    // when the registry can resolve the backing store handle.
    #[cfg(feature = "sql")]
    pub(in crate::db) fn show_indexes_for_store_schema_info(
        &self,
        store_path: &str,
        schema: &SchemaInfo,
        snapshot: &crate::db::schema::PersistedSchemaSnapshot,
    ) -> Vec<String> {
        let runtime_state = self
            .db
            .with_store_registry(|registry| registry.try_get_store(store_path).ok())
            .map(|store| store.index_state());

        show_indexes_for_schema_info_with_runtime_state(schema, snapshot, runtime_state)
    }

    /// Return one stable list of accepted runtime entity catalog entries.
    pub fn show_entities(&self) -> Result<Vec<EntityCatalogDescription>, InternalError> {
        let runtime_entities = self.db.accepted_runtime_entities()?;
        let mut entities = Vec::with_capacity(runtime_entities.len());

        for runtime_entity in runtime_entities {
            let store = self.db.recovered_store(runtime_entity.store_path())?;
            let storage = store
                .storage_capabilities()
                .storage_mode()
                .as_str()
                .to_string();
            let accepted = self.accepted_schema_catalog_context_for_runtime_entity(
                runtime_entity.clone(),
                store,
            )?;
            let snapshot = accepted.snapshot().persisted_snapshot();

            entities.push(EntityCatalogDescription::new(
                snapshot.entity_name().to_string(),
                snapshot.entity_path().to_string(),
                runtime_entity.store_path().to_string(),
                storage,
                EntityCatalogCounts::new(
                    u32::try_from(snapshot.fields().len()).unwrap_or(u32::MAX),
                    u32::try_from(snapshot.indexes().len()).unwrap_or(u32::MAX),
                    u32::try_from(snapshot.relations().len()).unwrap_or(u32::MAX),
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
    pub(in crate::db::session) fn visible_indexes_for_store_accepted_schema(
        &self,
        store_path: &str,
        schema_info: &SchemaInfo,
    ) -> Result<VisibleIndexes, QueryError> {
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

    /// Return one schema description selected by an immutable authored source key.
    pub fn try_describe_entity_by_source_key(
        &self,
        entity_source: &str,
    ) -> Result<EntitySchemaDescription, InternalError> {
        let catalog = self.accepted_schema_catalog_context_for_entity_source_key(entity_source)?;
        self.describe_accepted_catalog(&catalog)
    }

    /// Return one schema description selected by its accepted display name.
    pub fn try_describe_entity_by_name(
        &self,
        entity: &str,
    ) -> Result<EntitySchemaDescription, InternalError> {
        let catalog = self.accepted_schema_catalog_context_for_entity_name(Some(entity))?;
        self.describe_accepted_catalog(&catalog)
    }

    fn describe_accepted_catalog(
        &self,
        catalog: &crate::db::session::AcceptedSchemaCatalogContext,
    ) -> Result<EntitySchemaDescription, InternalError> {
        let validation_jobs = self.constraint_validation_jobs_for_accepted_catalog(catalog)?;
        let identity = self.identity_description_for_accepted_catalog(catalog)?;

        describe_accepted_entity_with_persisted_schema(
            catalog.snapshot(),
            catalog.value_catalog_handle(),
            validation_jobs.as_slice(),
            AcceptedEntityDescriptionMetadata::new(
                identity,
                catalog.identity().entity_tag().value(),
                catalog.fingerprint_method_version(),
                catalog.fingerprint(),
            ),
            |target_path| catalog.relation_target_description(target_path),
        )
    }

    pub(in crate::db::session) fn identity_description_for_accepted_catalog(
        &self,
        catalog: &crate::db::session::AcceptedSchemaCatalogContext,
    ) -> Result<Option<EntityIdentityDescription>, InternalError> {
        let Some(identity) = catalog.inspection_plan().identity_inspection() else {
            return Ok(None);
        };
        let catalog_identity = catalog.identity();
        let store = self.db.recovered_store(catalog_identity.store_path())?;
        let incarnation = database_incarnation_id()?;
        let high_water = store.with_schema(|schema_store| {
            schema_store.identity_high_water_for_integrity(
                incarnation,
                catalog_identity.entity_tag(),
                identity.field_id(),
                identity.accepted_kind(),
            )
        })?;
        describe_accepted_identity(identity, high_water).map(Some)
    }

    pub(in crate::db::session) fn constraint_validation_jobs_for_accepted_catalog(
        &self,
        catalog: &crate::db::session::AcceptedSchemaCatalogContext,
    ) -> Result<Vec<ConstraintValidationJob>, InternalError> {
        let identity = catalog.inspection_plan().identity();
        let store = self.db.recovered_store(identity.store_path())?;
        store.with_schema(|schema_store| {
            let jobs = catalog
                .snapshot()
                .persisted_snapshot()
                .constraint_activations()
                .iter()
                .map(|activation| {
                    schema_store.constraint_validation_job(identity.entity_tag(), activation.id())
                })
                .collect::<Result<Vec<_>, InternalError>>()?;
            jobs.into_iter()
                .flatten()
                .map(|job| {
                    if job.entity_tag() != identity.entity_tag()
                        || job.entity_path() != catalog.snapshot().entity_path()
                    {
                        return Err(InternalError::store_invariant());
                    }
                    Ok(job)
                })
                .collect()
        })
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
}
