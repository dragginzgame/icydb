mod field_metadata;

use crate::{
    db::{
        index::{IndexId, IndexKey, IndexState, IndexStoreVisit},
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

use super::{
    publish_accepted_entity_snapshot_revision,
    startup_expression::execute_supported_expression_index_addition,
    startup_field_path::{SchemaPublicationGate, execute_supported_field_path_index_addition},
    validate_publishable_transition_plan,
};

pub(in crate::db) use field_metadata::{
    execute_sql_ddl_field_default_change, execute_sql_ddl_field_drop,
    execute_sql_ddl_field_nullability_change, execute_sql_ddl_field_rename,
};

pub(in crate::db) fn execute_sql_ddl_field_path_index_addition(
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
    let supported = plan
        .supported_developer_physical_path()
        .map_err(|rejection| {
            let _ = rejection;
            InternalError::store_unsupported()
        })?;
    if supported.target() != derivation.admission().target() {
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

    Ok((report.rows_scanned(), report.index_keys_written()))
}

/// Execute one supported SQL DDL expression index addition through the schema
/// mutation staging and publication boundary.
pub(in crate::db) fn execute_sql_ddl_expression_index_addition(
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
pub(in crate::db) fn execute_sql_ddl_field_addition(
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

    pub(super) const fn publication_gate(&self) -> SchemaPublicationGate {
        SchemaPublicationGate::sql_ddl(self.entity_tag, self.accepted_before_identity)
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

    pub(super) fn publish(self) -> Result<(), InternalError> {
        publish_sql_ddl_accepted_snapshot(
            self.store,
            self.entity_tag,
            self.accepted_before_identity,
            self.after,
        )
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
pub(in crate::db) fn execute_sql_ddl_secondary_index_drop(
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
    let affected = envelope.store().with_index(|index_store| {
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
                affected.push((raw_key.clone(), remapped, value.clone()));
            }
            Ok::<IndexStoreVisit, InternalError>(IndexStoreVisit::Continue)
        })?;

        Ok::<_, InternalError>(affected)
    })?;
    let removed = affected
        .iter()
        .filter(|(_, remapped, _)| remapped.is_none())
        .count();
    envelope.store().with_index_mut(|index_store| {
        validate_sql_ddl_drop_ready_index_state(entity_path, target, index_store.state())?;
        for (raw_key, _, _) in &affected {
            if index_store.remove(raw_key).is_none() {
                return Err(InternalError::store_corruption());
            }
        }
        for (_, remapped, value) in affected {
            if let Some(remapped) = remapped
                && index_store.insert(remapped, value).is_some()
            {
                return Err(InternalError::store_corruption());
            }
        }

        Ok::<_, InternalError>(())
    })?;
    validate_sql_ddl_drop_schema_gate(
        envelope.store(),
        entity_tag,
        entity_path,
        envelope.before(),
        "before publication",
    )?;
    envelope.publish()?;

    Ok(removed)
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
