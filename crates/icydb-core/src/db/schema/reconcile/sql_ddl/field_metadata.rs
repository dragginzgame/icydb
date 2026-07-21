use crate::{
    db::{
        data::{DecodedDataStoreKey, SlotReader, StoreVisit, StructuralSlotReader},
        registry::StoreHandle,
        schema::{
            AcceptedCatalogIdentity, AcceptedSchemaSnapshot, FieldId, PersistedFieldSnapshot,
            PersistedSchemaSnapshot, SchemaDdlAcceptedSnapshotDerivation, SchemaFieldDropTarget,
            SchemaFieldNullabilityTarget, SchemaFieldRenameTarget, SchemaInsertDefaultTarget,
            SchemaRowLayout, SchemaTransitionSourceBudget,
            derive_sql_ddl_field_nullability_persisted_after,
        },
    },
    error::InternalError,
    types::EntityTag,
    value::Value,
};

use super::{
    super::user_index_domain::catalog_backed_row_contract_for_sql_ddl, SqlDdlPublicationEnvelope,
    require_exact_empty_sql_ddl_entity, validate_sql_ddl_drop_schema_gate,
};

/// Execute one SQL DDL field drop after proving no physical row requires the
/// dense-layout rewrite. Nonempty rewrites belong to the later migration
/// protocol and reject before publication in 0.209.
pub(in crate::db) fn execute_admin_sql_ddl_field_drop(
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
    let Some(target) = derivation.admission().field_drop_target() else {
        return Err(InternalError::store_unsupported());
    };
    validate_sql_ddl_field_drop_metadata_change(
        entity_path,
        envelope.before(),
        envelope.after(),
        target,
    )?;
    validate_sql_ddl_drop_schema_gate(
        store,
        entity_tag,
        entity_path,
        envelope.before(),
        "before row rewrite",
    )?;
    require_exact_empty_sql_ddl_entity(store, entity_tag, entity_path)?;
    envelope.publish()?;

    Ok(0)
}

fn validate_sql_ddl_field_drop_metadata_change(
    _entity_path: &'static str,
    before: &PersistedSchemaSnapshot,
    after: &PersistedSchemaSnapshot,
    target: &SchemaFieldDropTarget,
) -> Result<(), InternalError> {
    if before.entity_path() != after.entity_path()
        || before.entity_name() != after.entity_name()
        || before.fields().len() != after.fields().len().saturating_add(1)
    {
        return Err(InternalError::store_unsupported());
    }

    let before_field = before
        .fields()
        .iter()
        .find(|field| field.id() == target.field_id())
        .ok_or_else(InternalError::store_unsupported)?;
    if before_field.name() != target.name() || before_field.slot() != target.slot() {
        return Err(InternalError::store_unsupported());
    }
    let target_name_remains = after
        .fields()
        .iter()
        .any(|field| field.name() == target.name());
    if target_name_remains {
        return Err(InternalError::store_unsupported());
    }
    if before.row_layout().slot_for_field(target.field_id()) != Some(target.slot()) {
        return Err(InternalError::store_unsupported());
    }

    let retained_fields = before
        .fields()
        .iter()
        .filter(|field| field.id() != target.field_id())
        .collect::<Vec<_>>();
    let identities = retained_fields
        .iter()
        .enumerate()
        .map(|(offset, field)| {
            let id = u32::try_from(offset)
                .ok()
                .and_then(|offset| offset.checked_add(1))
                .map(FieldId::new)
                .ok_or_else(InternalError::store_unsupported)?;
            Ok::<_, InternalError>((
                field.id(),
                id,
                crate::db::schema::SchemaFieldSlot::from_generated_index(offset),
            ))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let map_field = |field_id: FieldId, _slot| {
        identities
            .iter()
            .find(|(before_id, _, _)| *before_id == field_id)
            .map(|(_, after_id, after_slot)| (*after_id, *after_slot))
    };
    let expected_fields = retained_fields
        .iter()
        .zip(&identities)
        .map(|(field, (_, id, slot))| field.clone_for_full_layout_rewrite(*id, *slot))
        .collect::<Vec<_>>();
    let expected_layout = after.row_layout().clone();
    let expected_primary_key = before
        .primary_key_field_ids()
        .iter()
        .map(|field_id| map_field(*field_id, target.slot()).map(|(id, _)| id))
        .collect::<Option<Vec<_>>>()
        .ok_or_else(InternalError::store_unsupported)?;
    let expected_indexes = before
        .indexes()
        .iter()
        .map(|index| index.clone_with_dense_identities(index.ordinal(), map_field))
        .collect::<Option<Vec<_>>>()
        .ok_or_else(InternalError::store_unsupported)?;
    let expected_relations = before
        .relations()
        .iter()
        .map(|relation| {
            relation.clone_with_mapped_field_ids(|field_id| {
                map_field(field_id, target.slot()).map(|(id, _)| id)
            })
        })
        .collect::<Option<Vec<_>>>()
        .ok_or_else(InternalError::store_unsupported)?;
    if after.fields() != expected_fields
        || after.row_layout() != &expected_layout
        || after.primary_key_field_ids() != expected_primary_key
        || after.indexes() != expected_indexes
        || after.relations() != expected_relations
    {
        return Err(InternalError::store_unsupported());
    }

    Ok(())
}

/// Execute one metadata-only SQL DDL field-default publication.
pub(in crate::db) fn execute_admin_sql_ddl_field_default_change(
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
    execute_admin_sql_ddl_checked_field_metadata_publication(
        envelope,
        derivation.admission().field_default_target(),
        validate_sql_ddl_field_default_metadata_change,
    )
}

fn validate_sql_ddl_field_default_metadata_change(
    entity_path: &'static str,
    before: &PersistedSchemaSnapshot,
    after: &PersistedSchemaSnapshot,
    target: &SchemaInsertDefaultTarget,
) -> Result<(), InternalError> {
    validate_sql_ddl_single_field_metadata_change(
        entity_path,
        before,
        after,
        SqlDdlSingleFieldMetadataTarget {
            field_id: target.field_id(),
            before_name: target.name(),
            after_name: target.name(),
        },
        SqlDdlSingleFieldMetadataChange::Default,
    )
}

/// Execute one SQL DDL field-nullability publication.
pub(in crate::db) fn execute_admin_sql_ddl_field_nullability_change(
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
    let Some(target) = derivation.admission().field_nullability_target() else {
        return Err(InternalError::store_unsupported());
    };
    validate_sql_ddl_field_nullability_metadata_change(
        entity_path,
        envelope.before(),
        envelope.after(),
        target,
    )?;

    let rows_scanned = if target_field_is_required(envelope.after(), target)? {
        validate_sql_ddl_set_not_null_rows(
            envelope.store(),
            entity_tag,
            entity_path,
            accepted_before,
            accepted_before_identity,
            target,
        )?
    } else {
        0
    };

    envelope.publish()?;

    Ok(rows_scanned)
}

fn validate_sql_ddl_field_nullability_metadata_change(
    _entity_path: &'static str,
    before: &PersistedSchemaSnapshot,
    after: &PersistedSchemaSnapshot,
    target: &SchemaFieldNullabilityTarget,
) -> Result<(), InternalError> {
    let after_field = after
        .fields()
        .iter()
        .find(|field| field.id() == target.field_id())
        .ok_or_else(InternalError::store_unsupported)?;
    if after_field.name() != target.name() {
        return Err(InternalError::store_unsupported());
    }
    let expected = derive_sql_ddl_field_nullability_persisted_after(
        before,
        target.field_id(),
        after_field.nullable(),
        after.version(),
    )
    .ok_or_else(InternalError::store_unsupported)?;
    if &expected != after {
        return Err(InternalError::store_unsupported());
    }

    Ok(())
}

fn target_field_is_required(
    snapshot: &PersistedSchemaSnapshot,
    target: &SchemaFieldNullabilityTarget,
) -> Result<bool, InternalError> {
    snapshot
        .fields()
        .iter()
        .find(|field| field.id() == target.field_id())
        .map(|field| !field.nullable())
        .ok_or_else(InternalError::store_unsupported)
}

fn validate_sql_ddl_set_not_null_rows(
    store: StoreHandle,
    entity_tag: EntityTag,
    entity_path: &'static str,
    accepted_before: &AcceptedSchemaSnapshot,
    accepted_before_identity: AcceptedCatalogIdentity,
    target: &SchemaFieldNullabilityTarget,
) -> Result<usize, InternalError> {
    let field = accepted_before
        .persisted_snapshot()
        .fields()
        .iter()
        .find(|field| field.id() == target.field_id())
        .ok_or_else(InternalError::store_unsupported)?;
    let contract = catalog_backed_row_contract_for_sql_ddl(
        store,
        accepted_before_identity,
        accepted_before.persisted_snapshot(),
    )?;
    let required_slot = usize::from(field.slot().get());

    store.with_data(|data_store| {
        let mut budget = SchemaTransitionSourceBudget::standard();
        data_store.visit_entries(|raw_key, raw_row| {
            let key = DecodedDataStoreKey::try_from_raw(raw_key)
                .map_err(|_error| InternalError::store_unsupported())?;
            if key.entity_tag() != entity_tag {
                return Ok(StoreVisit::Continue);
            }
            budget.consume_source_row(raw_row.len()).map_err(|error| {
                InternalError::schema_transition_budget_exceeded(error.resource())
            })?;
            let mut reader = StructuralSlotReader::from_raw_row_with_validated_contract(
                raw_row,
                contract.clone(),
            )?;
            reader.validate_primary_key(&key)?;
            let value = reader.get_value(required_slot)?;
            if matches!(value, Some(Value::Null) | None) {
                return Err(InternalError::schema_ddl_set_not_null_validation_failed(
                    entity_path,
                    target.name(),
                ));
            }
            Ok(StoreVisit::Continue)
        })?;

        Ok(budget.source_rows())
    })
}

/// Execute one metadata-only SQL DDL field-rename publication.
pub(in crate::db) fn execute_admin_sql_ddl_field_rename(
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
    execute_admin_sql_ddl_checked_field_metadata_publication(
        envelope,
        derivation.admission().field_rename_target(),
        validate_sql_ddl_field_rename_metadata_change,
    )
}

fn validate_sql_ddl_field_rename_metadata_change(
    entity_path: &'static str,
    before: &PersistedSchemaSnapshot,
    after: &PersistedSchemaSnapshot,
    target: &SchemaFieldRenameTarget,
) -> Result<(), InternalError> {
    validate_sql_ddl_single_field_metadata_change(
        entity_path,
        before,
        after,
        SqlDdlSingleFieldMetadataTarget {
            field_id: target.field_id(),
            before_name: target.old_name(),
            after_name: target.new_name(),
        },
        SqlDdlSingleFieldMetadataChange::Rename,
    )?;

    let expected_indexes = before
        .indexes()
        .iter()
        .map(|index| {
            index.clone_with_renamed_field_path_root(
                target.field_id(),
                target.old_name(),
                target.new_name(),
            )
        })
        .collect::<Vec<_>>();
    if after.indexes() != expected_indexes {
        return Err(InternalError::store_unsupported());
    }

    Ok(())
}

#[derive(Clone, Copy)]
struct SqlDdlSingleFieldMetadataTarget<'a> {
    field_id: FieldId,
    before_name: &'a str,
    after_name: &'a str,
}

#[derive(Clone, Copy)]
enum SqlDdlSingleFieldMetadataChange {
    Default,
    Rename,
}

impl SqlDdlSingleFieldMetadataChange {
    const fn require_unchanged_indexes(self) -> bool {
        !matches!(self, Self::Rename)
    }

    fn target_field_matches_allowed_change(
        self,
        before_field: &PersistedFieldSnapshot,
        after_field: &PersistedFieldSnapshot,
    ) -> bool {
        match self {
            Self::Default => {
                before_field.clone_with_insert_default(after_field.insert_default().clone())
                    == *after_field
            }
            Self::Rename => {
                before_field.clone_with_name(after_field.name().to_string()) == *after_field
            }
        }
    }

    fn target_field_changed(
        self,
        before_field: &PersistedFieldSnapshot,
        after_field: &PersistedFieldSnapshot,
    ) -> bool {
        match self {
            Self::Default => before_field.insert_default() != after_field.insert_default(),
            Self::Rename => before_field.name() != after_field.name(),
        }
    }
}

fn validate_sql_ddl_single_field_metadata_change(
    _entity_path: &'static str,
    before: &PersistedSchemaSnapshot,
    after: &PersistedSchemaSnapshot,
    target: SqlDdlSingleFieldMetadataTarget<'_>,
    change: SqlDdlSingleFieldMetadataChange,
) -> Result<(), InternalError> {
    if before.entity_path() != after.entity_path()
        || before.entity_name() != after.entity_name()
        || before.primary_key_field_ids() != after.primary_key_field_ids()
        || !row_layout_allocation_matches(before.row_layout(), after.row_layout())
        || before.fields().len() != after.fields().len()
        || (change.require_unchanged_indexes() && before.indexes() != after.indexes())
    {
        return Err(InternalError::store_unsupported());
    }

    let mut changed = 0usize;
    for (before_field, after_field) in before.fields().iter().zip(after.fields()) {
        if before_field.id() == target.field_id {
            let field_id_drifted = after_field.id() != target.field_id;
            let before_name_drifted = before_field.name() != target.before_name;
            let after_name_drifted = after_field.name() != target.after_name;
            if field_id_drifted || before_name_drifted || after_name_drifted {
                return Err(InternalError::store_unsupported());
            }
            if !change.target_field_matches_allowed_change(before_field, after_field) {
                return Err(InternalError::store_unsupported());
            }
            if change.target_field_changed(before_field, after_field) {
                changed = changed.saturating_add(1);
            }
            continue;
        }

        if before_field != after_field {
            return Err(InternalError::store_unsupported());
        }
    }

    if changed != 1 {
        return Err(InternalError::store_unsupported());
    }

    Ok(())
}

fn row_layout_allocation_matches(left: &SchemaRowLayout, right: &SchemaRowLayout) -> bool {
    left == right
}

fn execute_admin_sql_ddl_checked_field_metadata_publication<T>(
    envelope: SqlDdlPublicationEnvelope<'_>,
    target: Option<&T>,
    validate: impl FnOnce(
        &'static str,
        &PersistedSchemaSnapshot,
        &PersistedSchemaSnapshot,
        &T,
    ) -> Result<(), InternalError>,
) -> Result<(), InternalError> {
    let Some(target) = target else {
        return Err(InternalError::store_unsupported());
    };
    validate(
        envelope.entity_path(),
        envelope.before(),
        envelope.after(),
        target,
    )?;

    envelope.publish()
}
