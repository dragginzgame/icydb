mod field_metadata;

use crate::{
    db::{
        index::{IndexId, IndexKey, IndexState, IndexStoreVisit, RawIndexStoreKey},
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
    let supported = plan.supported_developer_physical_path().map_err(|rejection| {
        InternalError::store_unsupported(format!(
            "SQL DDL schema mutation physical execution rejected for entity '{entity_path}': supported_path_rejection={rejection:?}",
        ))
    })?;
    if supported.target() != derivation.admission().target() {
        return Err(InternalError::store_unsupported(format!(
            "SQL DDL schema mutation target drifted before physical execution for entity '{entity_path}': prepared='{}' actual='{}'",
            derivation.admission().target().name(),
            supported.target().name(),
        )));
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
        return Err(InternalError::store_unsupported(format!(
            "SQL DDL expression-index execution requires an expression target for entity '{entity_path}'",
        )));
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
        return Err(InternalError::store_unsupported(format!(
            "SQL DDL field-addition execution requires a field target for entity '{entity_path}'",
        )));
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
        .ok_or_else(|| {
            InternalError::store_unsupported(format!(
                "SQL DDL field-addition target is absent from accepted-after schema for entity '{entity_path}': field='{}'",
                target.name(),
            ))
        })?;
    if added_field.name() != target.name() || added_field.slot() != target.slot() {
        return Err(InternalError::store_unsupported(format!(
            "SQL DDL field-addition target drifted before publication for entity '{entity_path}': field='{}'",
            target.name(),
        )));
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
            return Err(InternalError::store_unsupported(format!(
                "SQL DDL {operation} schema mutation rejected before publication for entity '{entity_path}': {}",
                rejection.detail(),
            )));
        }
    };
    if plan.kind() != expected_kind {
        return Err(InternalError::store_unsupported(format!(
            "SQL DDL {operation} execution supports only {expected_label} for entity '{entity_path}': actual={:?}",
            plan.kind(),
        )));
    }

    Ok(plan)
}

fn publish_sql_ddl_accepted_snapshot(
    store: StoreHandle,
    entity_tag: EntityTag,
    accepted_before_identity: AcceptedCatalogIdentity,
    after: &PersistedSchemaSnapshot,
) -> Result<(), InternalError> {
    store.with_schema_mut(|schema_store| {
        debug_assert_eq!(entity_tag, accepted_before_identity.entity_tag());
        schema_store.insert_persisted_snapshot_if_latest_identity(accepted_before_identity, after)
    })
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
        return Err(InternalError::store_unsupported(format!(
            "SQL DDL index drop execution requires a drop target for entity '{entity_path}'",
        )));
    };

    validate_sql_ddl_drop_schema_gate(
        envelope.store(),
        entity_tag,
        entity_path,
        envelope.before(),
        "before cleanup",
    )?;
    let target_keys =
        sql_ddl_drop_target_index_keys(envelope.store(), entity_tag, entity_path, target)?;
    let removed = envelope.store().with_index_mut(|index_store| {
        if index_store.state() != IndexState::Ready {
            return Err(InternalError::store_unsupported(format!(
                "SQL DDL DROP INDEX requires a ready physical index store for entity '{entity_path}': target_index={} index_state={}",
                target.name(),
                index_store.state().as_str(),
            )));
        }
        let mut removed = 0usize;
        for key in &target_keys {
            if index_store.remove(key).is_some() {
                removed = removed.saturating_add(1);
            }
        }

        Ok::<_, InternalError>(removed)
    })?;
    validate_sql_ddl_drop_physical_cleanup(envelope.store(), entity_tag, entity_path, target)?;
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

fn sql_ddl_drop_target_index_keys(
    store: StoreHandle,
    entity_tag: EntityTag,
    entity_path: &'static str,
    target: &SchemaSecondaryIndexDropCleanupTarget,
) -> Result<Vec<RawIndexStoreKey>, InternalError> {
    let target_index_id = IndexId::new(entity_tag, target.ordinal());

    store.with_index(|index_store| {
        if index_store.state() != IndexState::Ready {
            return Err(InternalError::store_unsupported(format!(
                "SQL DDL DROP INDEX requires a ready physical index store for entity '{entity_path}': target_index={} index_state={}",
                target.name(),
                index_store.state().as_str(),
            )));
        }

        let mut target_keys = Vec::new();
        index_store.visit_entries(|raw_key, _| {
            let decoded = IndexKey::try_from_raw(raw_key).map_err(|error| {
                InternalError::store_corruption(format!(
                    "SQL DDL DROP INDEX key decode failed for entity '{entity_path}' while preflighting target index '{}': {error}",
                    target.name(),
                ))
            });
            match decoded {
                Ok(index_key) if *index_key.index_id() == target_index_id => {
                    target_keys.push(raw_key.clone());
                    Ok(IndexStoreVisit::Continue)
                }
                Ok(_) => Ok(IndexStoreVisit::Continue),
                Err(error) => Err(error),
            }
        })?;
        Ok(target_keys)
    })
}

fn validate_sql_ddl_drop_physical_cleanup(
    store: StoreHandle,
    entity_tag: EntityTag,
    entity_path: &'static str,
    target: &SchemaSecondaryIndexDropCleanupTarget,
) -> Result<(), InternalError> {
    let remaining = sql_ddl_drop_target_index_keys(store, entity_tag, entity_path, target)?;
    if remaining.is_empty() {
        return Ok(());
    }

    Err(InternalError::store_unsupported(format!(
        "SQL DDL DROP INDEX cleanup did not remove all target physical entries for entity '{entity_path}': target_index={} remaining_entries={}",
        target.name(),
        remaining.len(),
    )))
}

fn validate_sql_ddl_drop_schema_gate(
    store: StoreHandle,
    entity_tag: EntityTag,
    entity_path: &'static str,
    accepted_before: &PersistedSchemaSnapshot,
    boundary: &'static str,
) -> Result<(), InternalError> {
    let latest =
        store.with_schema_mut(|schema_store| schema_store.latest_persisted_snapshot(entity_tag))?;
    if latest.as_ref() == Some(accepted_before) {
        return Ok(());
    }

    Err(InternalError::store_unsupported(format!(
        "SQL DDL DROP INDEX lost exclusive schema gate {boundary} for entity '{entity_path}'",
    )))
}
