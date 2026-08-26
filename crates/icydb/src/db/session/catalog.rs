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
        EntityCatalogDescription, EntitySchemaDescription, MemoryCatalogDescription,
        SchemaApplicationTarget, SchemaChangeJobId, SchemaChangeProgress, SchemaChangeReceipt,
        StorageReport, StoreCatalogDescription, session::DbSession,
    },
    error::Error,
    traits::CanisterKind,
};

use icydb_schema::{
    EntitySourceKey, EntityStoreAssignment, FieldInsertPolicy, SchemaCapability, SchemaFragment,
    SchemaMigrationPlan, SchemaProposal, SchemaSubmissionKey, TargetDatabaseIdentity,
    decode_schema_fragment, decode_schema_migration_plan,
};

#[cfg(feature = "migration")]
use crate::db::{SchemaMigrationCommand, SchemaMigrationStatusPage, SchemaMigrationStatusRequest};

impl<C: CanisterKind> DbSession<C> {
    /// Validate generated deployment identity and defer ordinary schema
    /// application only while one exact migration is durably `Prepared`.
    #[doc(hidden)]
    pub fn ensure_generated_schema_fragment(
        &self,
        fragment_bytes: &[u8],
        migration_plan_bytes: Option<&[u8]>,
        submission_key: &str,
        entity_stores: &[(&str, &str)],
    ) -> Result<(), Error> {
        let fragment =
            decode_schema_fragment(fragment_bytes).map_err(|_| generated_schema_input_error())?;
        let migration_plan = migration_plan_bytes
            .map(decode_schema_migration_plan)
            .transpose()
            .map_err(|_| generated_schema_input_error())?;
        let submission_key = SchemaSubmissionKey::try_new(submission_key)
            .map_err(|_| generated_schema_input_error())?;
        let target = self.schema_application_target()?;
        let expected_head = if let Some(receipt) =
            self.schema_application_receipt(target.database_identity(), &submission_key)?
        {
            receipt.prior_head().clone()
        } else {
            target.accepted_head().clone()
        };
        let proposal = generated_schema_proposal(
            &fragment,
            &target,
            submission_key,
            expected_head,
            entity_stores,
            migration_plan,
        )?;
        #[cfg(feature = "migration")]
        {
            if self
                .inner
                .defer_generated_schema_application_for_prepared_migration(&proposal)?
            {
                return Ok(());
            }
        }
        self.inner.apply_generated_schema(&proposal)?;
        Ok(())
    }

    /// Execute one explicit migration operation for this deployed generated schema.
    #[cfg(feature = "migration")]
    #[doc(hidden)]
    pub fn migrate_generated_schema(
        &self,
        fragment_bytes: &[u8],
        migration_plan_bytes: Option<&[u8]>,
        submission_key: &str,
        entity_stores: &[(&str, &str)],
        command: SchemaMigrationCommand,
    ) -> Result<SchemaMigrationStatusPage, Error> {
        let expected_head = migration_command_head(&command).clone();
        let proposal = self.generated_schema_proposal(
            fragment_bytes,
            migration_plan_bytes,
            submission_key,
            entity_stores,
            expected_head,
        )?;
        Ok(self.inner.migrate_schema(&proposal, command)?)
    }

    /// Return migration status for this deployed generated schema.
    #[cfg(feature = "migration")]
    #[doc(hidden)]
    pub fn generated_schema_migration_status(
        &self,
        fragment_bytes: &[u8],
        migration_plan_bytes: Option<&[u8]>,
        submission_key: &str,
        entity_stores: &[(&str, &str)],
        request: &SchemaMigrationStatusRequest,
    ) -> Result<SchemaMigrationStatusPage, Error> {
        let target = self.schema_application_target()?;
        let proposal = self.generated_schema_proposal(
            fragment_bytes,
            migration_plan_bytes,
            submission_key,
            entity_stores,
            target.accepted_head().clone(),
        )?;
        Ok(self.inner.schema_migration_status(&proposal, request)?)
    }

    #[cfg(feature = "migration")]
    fn generated_schema_proposal(
        &self,
        fragment_bytes: &[u8],
        migration_plan_bytes: Option<&[u8]>,
        submission_key: &str,
        entity_stores: &[(&str, &str)],
        expected_head: icydb_schema::ExpectedAcceptedHead,
    ) -> Result<SchemaProposal, Error> {
        let fragment =
            decode_schema_fragment(fragment_bytes).map_err(|_| generated_schema_input_error())?;
        let migration_plan = migration_plan_bytes
            .map(decode_schema_migration_plan)
            .transpose()
            .map_err(|_| generated_schema_input_error())?;
        let submission_key = SchemaSubmissionKey::try_new(submission_key)
            .map_err(|_| generated_schema_input_error())?;
        let target = self.schema_application_target()?;
        generated_schema_proposal(
            &fragment,
            &target,
            submission_key,
            expected_head,
            entity_stores,
            migration_plan,
        )
    }

    /// Compose and apply one sealed generated schema fragment against this
    /// database's current accepted head.
    ///
    /// This hidden actor-wiring boundary owns no schema semantics: it decodes
    /// the public fragment, resolves opaque store identities issued by the
    /// runtime, then submits the ordinary public proposal.
    #[doc(hidden)]
    pub fn apply_generated_schema_fragment(
        &self,
        fragment_bytes: &[u8],
        migration_plan_bytes: Option<&[u8]>,
        submission_key: &str,
        entity_stores: &[(&str, &str)],
    ) -> Result<SchemaChangeReceipt, Error> {
        let fragment =
            decode_schema_fragment(fragment_bytes).map_err(|_| generated_schema_input_error())?;
        let migration_plan = migration_plan_bytes
            .map(decode_schema_migration_plan)
            .transpose()
            .map_err(|_| generated_schema_input_error())?;
        let submission_key = SchemaSubmissionKey::try_new(submission_key)
            .map_err(|_| generated_schema_input_error())?;
        let target = self.schema_application_target()?;
        let expected_head = if let Some(receipt) =
            self.schema_application_receipt(target.database_identity(), &submission_key)?
        {
            receipt.prior_head().clone()
        } else {
            target.accepted_head().clone()
        };
        let proposal = generated_schema_proposal(
            &fragment,
            &target,
            submission_key,
            expected_head,
            entity_stores,
            migration_plan,
        )?;

        Ok(self.inner.apply_generated_schema(&proposal)?)
    }

    /// Apply one exact source-keyed schema proposal and return its durable
    /// idempotent receipt.
    pub fn apply_schema(&self, proposal: &SchemaProposal) -> Result<SchemaChangeReceipt, Error> {
        Ok(self.inner.apply_schema(proposal)?)
    }

    /// Execute one explicit source-migration operation for an exact proposal.
    ///
    /// This is the Rust capability used by generated and handwritten
    /// controller endpoints; calling it does not export a Candid method.
    #[cfg(feature = "migration")]
    pub fn migrate_schema(
        &self,
        proposal: &SchemaProposal,
        command: SchemaMigrationCommand,
    ) -> Result<SchemaMigrationStatusPage, Error> {
        Ok(self.inner.migrate_schema(proposal, command)?)
    }

    /// Return one bounded source-migration status page for an exact proposal.
    ///
    /// Calling this Rust capability does not export a Candid method.
    #[cfg(feature = "migration")]
    pub fn schema_migration_status(
        &self,
        proposal: &SchemaProposal,
        request: &SchemaMigrationStatusRequest,
    ) -> Result<SchemaMigrationStatusPage, Error> {
        Ok(self.inner.schema_migration_status(proposal, request)?)
    }

    /// Issue the opaque database/store identities and exact accepted head used
    /// to compose one optimistic schema proposal.
    pub fn schema_application_target(&self) -> Result<SchemaApplicationTarget, Error> {
        Ok(self.inner.schema_application_target()?)
    }

    /// Load one durable schema-application receipt by exact target and
    /// submission identity.
    pub fn schema_application_receipt(
        &self,
        database_identity: TargetDatabaseIdentity,
        submission_key: &SchemaSubmissionKey,
    ) -> Result<Option<SchemaChangeReceipt>, Error> {
        Ok(self
            .inner
            .schema_application_receipt(database_identity, submission_key)?)
    }

    /// Advance one pending schema application by at most one bounded
    /// activation step.
    pub fn continue_schema_application(
        &self,
        job_id: SchemaChangeJobId,
        acknowledged_receipt: Option<u64>,
    ) -> Result<SchemaChangeProgress, Error> {
        Ok(self
            .inner
            .continue_schema_application(job_id, acknowledged_receipt)?)
    }

    /// Abort one pending schema application after acknowledging any retained
    /// finding page by exact sequence.
    pub fn abort_schema_application(
        &self,
        job_id: SchemaChangeJobId,
        acknowledged_receipt: Option<u64>,
    ) -> Result<SchemaChangeProgress, Error> {
        Ok(self
            .inner
            .abort_schema_application(job_id, acknowledged_receipt)?)
    }

    /// Return one stable list of accepted runtime entity catalog entries.
    pub fn show_entities(&self) -> Result<Vec<EntityCatalogDescription>, Error> {
        Ok(self.inner.show_entities()?)
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

    /// Return one accepted live-schema description selected by immutable
    /// authored source identity.
    pub fn try_describe_entity_by_source_key(
        &self,
        entity_source: &str,
    ) -> Result<EntitySchemaDescription, Error> {
        Ok(self
            .inner
            .try_describe_entity_by_source_key(entity_source)?)
    }

    /// Return one accepted live-schema description selected by accepted
    /// display name.
    pub fn try_describe_entity_by_name(
        &self,
        entity: &str,
    ) -> Result<EntitySchemaDescription, Error> {
        Ok(self.inner.try_describe_entity_by_name(entity)?)
    }

    /// Build one point-in-time storage report for observability endpoints.
    pub fn storage_report(
        &self,
        name_to_path: &[(&'static str, &'static str)],
    ) -> Result<StorageReport, Error> {
        Ok(self.inner.storage_report(name_to_path)?)
    }
}

#[cfg(feature = "migration")]
const fn migration_command_head(
    command: &SchemaMigrationCommand,
) -> &icydb_schema::ExpectedAcceptedHead {
    match command {
        SchemaMigrationCommand::Adopt { expected_head, .. }
        | SchemaMigrationCommand::Advance { expected_head, .. }
        | SchemaMigrationCommand::Abort { expected_head, .. } => expected_head,
    }
}

fn generated_schema_proposal(
    fragment: &SchemaFragment,
    target: &SchemaApplicationTarget,
    submission_key: SchemaSubmissionKey,
    expected_head: icydb_schema::ExpectedAcceptedHead,
    entity_stores: &[(&str, &str)],
    migration_plan: Option<SchemaMigrationPlan>,
) -> Result<SchemaProposal, Error> {
    let assignments = entity_stores
        .iter()
        .map(|(entity_source, store_path)| {
            let entity = EntitySourceKey::try_new(*entity_source)
                .map_err(|_| generated_schema_input_error())?;
            let store = target
                .stores()
                .iter()
                .find(|store| store.path() == *store_path)
                .ok_or_else(generated_schema_input_error)?;
            Ok(EntityStoreAssignment::new(entity, store.identity()))
        })
        .collect::<Result<Vec<_>, Error>>()?;
    SchemaProposal::try_compose(
        generated_fragment_capabilities(fragment, migration_plan.is_some()),
        target.database_identity(),
        submission_key,
        expected_head,
        vec![fragment.clone()],
        assignments,
        Vec::new(),
        migration_plan,
    )
    .map_err(|_| generated_schema_input_error())
}

fn generated_fragment_capabilities(
    fragment: &SchemaFragment,
    has_migration_plan: bool,
) -> Vec<SchemaCapability> {
    let mut exact_composite_types = !fragment.types().is_empty();
    let mut accepted_checks = false;
    let mut secondary_indexes = false;
    let mut restrictive_relations = false;
    let mut insert_defaults = false;
    let mut generated_values = false;
    let mut managed_timestamps = false;

    for entity in fragment.entities() {
        accepted_checks |= !entity.constraints().is_empty();
        secondary_indexes |= !entity.indexes().is_empty();
        restrictive_relations |= !entity.relations().is_empty();
        for field in entity.fields() {
            exact_composite_types |= matches!(
                field.field_type(),
                icydb_schema::FieldType::Named(_) | icydb_schema::FieldType::List(_)
            );
            insert_defaults |= matches!(field.insert_policy(), FieldInsertPolicy::Default(_));
            generated_values |= matches!(field.insert_policy(), FieldInsertPolicy::Generated);
            managed_timestamps |= field.management().is_some();
        }
    }

    [
        (
            exact_composite_types,
            SchemaCapability::EXACT_COMPOSITE_TYPES,
        ),
        (accepted_checks, SchemaCapability::ACCEPTED_CHECKS),
        (secondary_indexes, SchemaCapability::SECONDARY_INDEXES),
        (
            restrictive_relations,
            SchemaCapability::RESTRICTIVE_RELATIONS,
        ),
        (insert_defaults, SchemaCapability::INSERT_DEFAULTS),
        (generated_values, SchemaCapability::GENERATED_VALUES),
        (managed_timestamps, SchemaCapability::MANAGED_TIMESTAMPS),
        (has_migration_plan, SchemaCapability::VERSIONED_MIGRATIONS),
    ]
    .into_iter()
    .filter_map(|(required, capability)| required.then_some(capability))
    .collect()
}

const fn generated_schema_input_error() -> Error {
    Error::from_kind(
        crate::ErrorKind::Runtime(crate::RuntimeErrorKind::Internal),
        crate::ErrorOrigin::Runtime,
    )
}
