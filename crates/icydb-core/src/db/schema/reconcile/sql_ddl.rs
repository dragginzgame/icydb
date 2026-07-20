mod field_metadata;

use super::{
    publish_accepted_entity_snapshot_revision,
    publish_accepted_entity_snapshot_revision_with_row_puts,
    publish_accepted_entity_snapshot_revision_with_user_index_domain,
    schema_publication_error_allows_physical_rollback,
    user_index_domain::stage_sql_ddl_user_index_domain_replacement,
    validate_publishable_transition_plan,
};
use crate::{
    db::{
        registry::StoreHandle,
        schema::{
            AcceptedCatalogIdentity, AcceptedSchemaSnapshot, PersistedSchemaSnapshot,
            SchemaDdlAcceptedSnapshotDerivation, SchemaTransitionDecision,
            SchemaTransitionPlanKind, StagedUserIndexDomainReplacement, decide_schema_transition,
            transition::SchemaTransitionPlan,
        },
    },
    error::InternalError,
    types::EntityTag,
};

pub(in crate::db) use field_metadata::{
    execute_admin_sql_ddl_field_default_change, execute_admin_sql_ddl_field_drop,
    execute_admin_sql_ddl_field_nullability_change, execute_admin_sql_ddl_field_rename,
};

pub(in crate::db) fn execute_admin_sql_ddl_field_path_index_addition(
    store: StoreHandle,
    entity_tag: EntityTag,
    entity_path: &'static str,
    accepted_before: &AcceptedSchemaSnapshot,
    accepted_before_identity: AcceptedCatalogIdentity,
    derivation: &SchemaDdlAcceptedSnapshotDerivation,
) -> Result<(usize, usize), InternalError> {
    let envelope = SqlDdlPublicationEnvelope::new(
        store,
        entity_tag,
        entity_path,
        accepted_before,
        accepted_before_identity,
        derivation,
    );
    let plan = envelope.require_transition_plan(
        "field-path index",
        SchemaTransitionPlanKind::AddFieldPathIndex,
        "add_field_path_index",
    )?;
    let target = plan
        .field_path_index_target()
        .ok_or_else(InternalError::store_unsupported)?;
    if Some(target) != derivation.admission().field_path_target() {
        return Err(InternalError::store_unsupported());
    }

    let replacement = stage_sql_ddl_user_index_domain_replacement(
        envelope.store(),
        accepted_before_identity,
        envelope.before(),
        envelope.after(),
    )?;
    let rows_scanned = replacement.usage().source_rows();
    let index_keys_written = staged_added_entry_count(&replacement)?;
    publish_accepted_entity_snapshot_revision_with_user_index_domain(
        envelope.store(),
        accepted_before_identity,
        envelope.after(),
        replacement,
    )?;

    Ok((rows_scanned, index_keys_written))
}

/// Execute one supported SQL DDL expression index addition through the schema
/// mutation staging and publication boundary.
pub(in crate::db) fn execute_admin_sql_ddl_expression_index_addition(
    store: StoreHandle,
    entity_tag: EntityTag,
    entity_path: &'static str,
    accepted_before: &AcceptedSchemaSnapshot,
    accepted_before_identity: AcceptedCatalogIdentity,
    derivation: &SchemaDdlAcceptedSnapshotDerivation,
) -> Result<(usize, usize), InternalError> {
    let envelope = SqlDdlPublicationEnvelope::new(
        store,
        entity_tag,
        entity_path,
        accepted_before,
        accepted_before_identity,
        derivation,
    );
    let plan = envelope.require_transition_plan(
        "expression-index",
        SchemaTransitionPlanKind::AddExpressionIndex,
        "add_expression_index",
    )?;
    let Some(target) = derivation.admission().expression_target() else {
        return Err(InternalError::store_unsupported());
    };
    if plan.expression_index_target() != Some(target) {
        return Err(InternalError::store_unsupported());
    }
    let replacement = stage_sql_ddl_user_index_domain_replacement(
        envelope.store(),
        accepted_before_identity,
        envelope.before(),
        envelope.after(),
    )?;
    let rows_scanned = replacement.usage().source_rows();
    let index_keys_written = staged_added_entry_count(&replacement)?;
    publish_accepted_entity_snapshot_revision_with_user_index_domain(
        envelope.store(),
        accepted_before_identity,
        envelope.after(),
        replacement,
    )?;

    Ok((rows_scanned, index_keys_written))
}

fn staged_added_entry_count(
    replacement: &StagedUserIndexDomainReplacement,
) -> Result<usize, InternalError> {
    let usage = replacement.usage();
    usage
        .accepted_after_entries()
        .checked_sub(usage.accepted_before_entries())
        .ok_or_else(InternalError::store_invariant)
}

/// Execute one metadata-only SQL DDL additive-field publication.
pub(in crate::db) fn execute_admin_sql_ddl_field_addition(
    store: StoreHandle,
    entity_tag: EntityTag,
    entity_path: &'static str,
    accepted_before: &AcceptedSchemaSnapshot,
    accepted_before_identity: AcceptedCatalogIdentity,
    derivation: &SchemaDdlAcceptedSnapshotDerivation,
) -> Result<(), InternalError> {
    let envelope = SqlDdlPublicationEnvelope::new(
        store,
        entity_tag,
        entity_path,
        accepted_before,
        accepted_before_identity,
        derivation,
    );
    let Some(target) = derivation.admission().field_addition_target() else {
        return Err(InternalError::store_unsupported());
    };
    let plan = envelope.require_transition_plan(
        "field-addition",
        SchemaTransitionPlanKind::AppendOnlyNullableFields,
        "append-only nullable fields",
    )?;
    validate_publishable_transition_plan(entity_path, &plan)?;

    let added_field = envelope
        .after()
        .fields()
        .iter()
        .find(|field| field.id() == target.field_id())
        .ok_or_else(InternalError::store_unsupported)?;
    if added_field.name() != target.name() || added_field.slot() != target.slot() {
        return Err(InternalError::store_unsupported());
    }

    envelope.publish()
}

pub(super) struct SqlDdlPublicationEnvelope<'a> {
    store: StoreHandle,
    entity_tag: EntityTag,
    entity_path: &'static str,
    accepted_before_identity: AcceptedCatalogIdentity,
    before: &'a PersistedSchemaSnapshot,
    after: &'a PersistedSchemaSnapshot,
}

impl<'a> SqlDdlPublicationEnvelope<'a> {
    pub(super) const fn new(
        store: StoreHandle,
        entity_tag: EntityTag,
        entity_path: &'static str,
        accepted_before: &'a AcceptedSchemaSnapshot,
        accepted_before_identity: AcceptedCatalogIdentity,
        derivation: &'a SchemaDdlAcceptedSnapshotDerivation,
    ) -> Self {
        Self {
            store,
            entity_tag,
            entity_path,
            accepted_before_identity,
            before: accepted_before.persisted_snapshot(),
            after: derivation.accepted_after().persisted_snapshot(),
        }
    }

    pub(super) const fn store(&self) -> StoreHandle {
        self.store
    }

    pub(super) const fn before(&self) -> &'a PersistedSchemaSnapshot {
        self.before
    }

    pub(super) const fn after(&self) -> &'a PersistedSchemaSnapshot {
        self.after
    }

    pub(super) const fn entity_path(&self) -> &'static str {
        self.entity_path
    }

    pub(super) fn require_transition_plan(
        &self,
        operation: &'static str,
        expected_kind: SchemaTransitionPlanKind,
        expected_label: &'static str,
    ) -> Result<SchemaTransitionPlan, InternalError> {
        require_sql_ddl_transition_plan(
            self.entity_path,
            operation,
            self.before,
            self.after,
            expected_kind,
            expected_label,
        )
    }

    pub(super) fn publish(&self) -> Result<(), InternalError> {
        publish_sql_ddl_accepted_snapshot(
            self.store,
            self.entity_tag,
            self.accepted_before_identity,
            self.after,
        )
    }

    pub(super) fn publish_with_row_puts(
        &self,
        row_puts: Vec<crate::db::journal::JournalRecord>,
    ) -> Result<(), InternalError> {
        debug_assert_eq!(self.entity_tag, self.accepted_before_identity.entity_tag());
        publish_accepted_entity_snapshot_revision_with_row_puts(
            self.store,
            self.accepted_before_identity,
            self.after,
            row_puts,
        )
    }

    pub(super) fn publication_error_allows_physical_rollback(&self) -> bool {
        schema_publication_error_allows_physical_rollback(self.store, self.entity_tag, self.before)
    }
}

fn require_sql_ddl_transition_plan(
    entity_path: &'static str,
    operation: &'static str,
    before: &PersistedSchemaSnapshot,
    after: &PersistedSchemaSnapshot,
    expected_kind: SchemaTransitionPlanKind,
    expected_label: &'static str,
) -> Result<SchemaTransitionPlan, InternalError> {
    let plan = match decide_schema_transition(before, after) {
        SchemaTransitionDecision::Accepted(plan) => plan,
        SchemaTransitionDecision::Rejected(rejection) => {
            let _ = (operation, entity_path, rejection);
            return Err(InternalError::store_unsupported());
        }
    };
    if plan.kind() != expected_kind {
        let _ = (operation, expected_label, entity_path);
        return Err(InternalError::store_unsupported());
    }

    Ok(plan)
}

fn publish_sql_ddl_accepted_snapshot(
    store: StoreHandle,
    entity_tag: EntityTag,
    accepted_before_identity: AcceptedCatalogIdentity,
    after: &PersistedSchemaSnapshot,
) -> Result<(), InternalError> {
    debug_assert_eq!(entity_tag, accepted_before_identity.entity_tag());
    publish_accepted_entity_snapshot_revision(store, accepted_before_identity, after)
}

/// Execute one supported SQL DDL secondary-index drop through marker-first
/// complete-domain replacement.
pub(in crate::db) fn execute_admin_sql_ddl_secondary_index_drop(
    store: StoreHandle,
    entity_tag: EntityTag,
    entity_path: &'static str,
    accepted_before: &AcceptedSchemaSnapshot,
    accepted_before_identity: AcceptedCatalogIdentity,
    derivation: &SchemaDdlAcceptedSnapshotDerivation,
) -> Result<(), InternalError> {
    let envelope = SqlDdlPublicationEnvelope::new(
        store,
        entity_tag,
        entity_path,
        accepted_before,
        accepted_before_identity,
        derivation,
    );
    if !derivation.admission().is_secondary_drop() {
        return Err(InternalError::store_unsupported());
    }
    let replacement = stage_sql_ddl_user_index_domain_replacement(
        envelope.store(),
        accepted_before_identity,
        envelope.before(),
        envelope.after(),
    )?;
    publish_accepted_entity_snapshot_revision_with_user_index_domain(
        envelope.store(),
        accepted_before_identity,
        envelope.after(),
        replacement,
    )?;

    Ok(())
}

fn validate_sql_ddl_drop_schema_gate(
    store: StoreHandle,
    entity_tag: EntityTag,
    entity_path: &'static str,
    accepted_before: &PersistedSchemaSnapshot,
    boundary: &'static str,
) -> Result<(), InternalError> {
    let latest = store.with_schema_mut(|schema_store| {
        schema_store.current_accepted_persisted_snapshot(entity_tag)
    })?;
    if latest.as_ref() == Some(accepted_before) {
        return Ok(());
    }

    let _ = (entity_path, boundary);
    Err(InternalError::store_unsupported())
}
