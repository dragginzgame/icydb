mod field_metadata;

use crate::{
    db::{
        index::{
            IndexEntryValue, IndexId, IndexKey, IndexState, IndexStore, IndexStoreVisit,
            RawIndexStoreKey,
        },
        registry::StoreHandle,
        schema::{
            AcceptedCatalogIdentity, AcceptedSchemaSnapshot, PersistedSchemaSnapshot,
            SchemaDdlAcceptedSnapshotDerivation, SchemaSecondaryIndexDropCleanupTarget,
            SchemaTransitionDecision, SchemaTransitionPlanKind, decide_schema_transition,
            transition::SchemaTransitionPlan,
        },
    },
    error::InternalError,
    types::EntityTag,
};
use std::collections::HashSet;

use super::{
    publish_accepted_entity_snapshot_revision,
    publish_accepted_entity_snapshot_revision_with_row_puts,
    schema_publication_error_allows_physical_rollback,
    startup_expression::execute_supported_expression_index_addition,
    startup_field_path::{SchemaMutationCatalogScope, execute_supported_field_path_index_addition},
    validate_publishable_transition_plan,
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

    let report = execute_supported_field_path_index_addition(
        envelope.store(),
        envelope.publication_gate(),
        entity_path,
        envelope.before(),
        envelope.after(),
        &plan,
    )?;

    Ok((
        report.metrics().rows_scanned(),
        report.metrics().index_keys_written(),
    ))
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

    execute_supported_expression_index_addition(
        envelope.store(),
        envelope.publication_gate(),
        entity_path,
        envelope.before(),
        envelope.after(),
        &plan,
        target,
    )
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

    pub(super) const fn publication_gate(&self) -> SchemaMutationCatalogScope {
        SchemaMutationCatalogScope::sql_ddl(self.entity_tag, self.accepted_before_identity)
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

/// Execute one supported SQL DDL secondary-index drop by cleaning the target
/// physical index namespace before publishing the accepted-after schema.
pub(in crate::db) fn execute_admin_sql_ddl_secondary_index_drop(
    store: StoreHandle,
    entity_tag: EntityTag,
    entity_path: &'static str,
    accepted_before: &AcceptedSchemaSnapshot,
    accepted_before_identity: AcceptedCatalogIdentity,
    derivation: &SchemaDdlAcceptedSnapshotDerivation,
) -> Result<usize, InternalError> {
    let envelope = SqlDdlPublicationEnvelope::new(
        store,
        entity_tag,
        entity_path,
        accepted_before,
        accepted_before_identity,
        derivation,
    );
    let Some(target) = derivation.admission().drop_target() else {
        return Err(InternalError::store_unsupported());
    };

    validate_sql_ddl_drop_schema_gate(
        envelope.store(),
        entity_tag,
        entity_path,
        envelope.before(),
        "before cleanup",
    )?;
    let affected = envelope.store().with_index_mut(|index_store| {
        validate_sql_ddl_drop_ready_index_state(entity_path, target, index_store.state())?;
        let mut affected = Vec::new();
        index_store.visit_entries(|raw_key, value| {
            let index_key =
                IndexKey::try_from_raw(raw_key).map_err(|_| InternalError::store_corruption())?;
            if index_key.index_id().entity_tag() == entity_tag
                && index_key.index_id().ordinal() >= target.ordinal()
            {
                let remapped = if index_key.index_id().ordinal() == target.ordinal() {
                    None
                } else {
                    let ordinal = index_key
                        .index_id()
                        .ordinal()
                        .checked_sub(1)
                        .ok_or_else(InternalError::store_corruption)?;
                    let key = index_key
                        .clone_with_index_id(IndexId::new(entity_tag, ordinal))
                        .to_raw()
                        .map_err(|_| InternalError::store_corruption())?;
                    Some(key)
                };
                affected.push(SqlDdlSecondaryIndexDropEntry {
                    original_key: raw_key.clone(),
                    remapped_key: remapped,
                    value: value.clone(),
                });
            }
            Ok::<IndexStoreVisit, InternalError>(IndexStoreVisit::Continue)
        })?;
        validate_sql_ddl_secondary_index_drop_entries(index_store, &affected)?;
        if let Err(error) = apply_sql_ddl_secondary_index_drop(index_store, &affected) {
            rollback_sql_ddl_secondary_index_drop(index_store, &affected);
            return Err(error);
        }

        Ok::<_, InternalError>(affected)
    })?;
    let removed = affected
        .iter()
        .filter(|entry| entry.remapped_key.is_none())
        .count();
    let publication_result = validate_sql_ddl_drop_schema_gate(
        envelope.store(),
        entity_tag,
        entity_path,
        envelope.before(),
        "before publication",
    )
    .and_then(|()| envelope.publish());
    if let Err(error) = publication_result {
        if envelope.publication_error_allows_physical_rollback() {
            store.with_index_mut(|index_store| {
                rollback_sql_ddl_secondary_index_drop(index_store, &affected);
            });
        }
        return Err(error);
    }

    Ok(removed)
}

/// One affected physical entry. The original key/value is the rollback image;
/// `remapped_key` is the accepted-after location for a surviving higher index.
struct SqlDdlSecondaryIndexDropEntry {
    original_key: RawIndexStoreKey,
    remapped_key: Option<RawIndexStoreKey>,
    value: IndexEntryValue,
}

fn validate_sql_ddl_secondary_index_drop_entries(
    index_store: &IndexStore,
    affected: &[SqlDdlSecondaryIndexDropEntry],
) -> Result<(), InternalError> {
    // Reject collisions before removing anything so the mutation has one
    // reversible old-key to new-key mapping.
    let original_keys = affected
        .iter()
        .map(|entry| entry.original_key.clone())
        .collect::<HashSet<_>>();
    let mut remapped_keys = HashSet::new();

    for remapped_key in affected
        .iter()
        .filter_map(|entry| entry.remapped_key.as_ref())
    {
        if !remapped_keys.insert(remapped_key.clone())
            || (index_store.get(remapped_key).is_some() && !original_keys.contains(remapped_key))
        {
            return Err(InternalError::store_corruption());
        }
    }

    Ok(())
}

fn apply_sql_ddl_secondary_index_drop(
    index_store: &mut IndexStore,
    affected: &[SqlDdlSecondaryIndexDropEntry],
) -> Result<(), InternalError> {
    for entry in affected {
        if index_store.remove(&entry.original_key) != Some(entry.value.clone()) {
            return Err(InternalError::store_corruption());
        }
    }
    for entry in affected {
        if let Some(remapped_key) = &entry.remapped_key
            && index_store
                .insert(remapped_key.clone(), entry.value.clone())
                .is_some()
        {
            return Err(InternalError::store_corruption());
        }
    }

    Ok(())
}

fn rollback_sql_ddl_secondary_index_drop(
    index_store: &mut IndexStore,
    affected: &[SqlDdlSecondaryIndexDropEntry],
) {
    for remapped_key in affected
        .iter()
        .filter_map(|entry| entry.remapped_key.as_ref())
    {
        index_store.remove(remapped_key);
    }
    for entry in affected {
        index_store.remove(&entry.original_key);
    }
    for entry in affected {
        index_store.insert(entry.original_key.clone(), entry.value.clone());
    }
}

fn validate_sql_ddl_drop_ready_index_state(
    entity_path: &'static str,
    target: &SchemaSecondaryIndexDropCleanupTarget,
    state: IndexState,
) -> Result<(), InternalError> {
    if state == IndexState::Ready {
        return Ok(());
    }

    let _ = (entity_path, target, state);
    Err(InternalError::store_unsupported())
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
