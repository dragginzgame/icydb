use crate::{
    db::{
        data::{
            CanonicalRow, DecodedDataStoreKey, RawDataStoreKey, RawRow, SlotReader, StoreVisit,
            StructuralSlotReader, canonical_row_from_dense_slot_payloads,
            canonical_row_from_stored_raw_row,
        },
        journal::JournalRecord,
        registry::StoreHandle,
        schema::{
            AcceptedCatalogIdentity, AcceptedSchemaSnapshot, FieldId, PersistedFieldSnapshot,
            PersistedSchemaSnapshot, SchemaDdlAcceptedSnapshotDerivation, SchemaFieldDefaultTarget,
            SchemaFieldDropTarget, SchemaFieldNullabilityTarget, SchemaFieldRenameTarget,
            SchemaRowLayout, accepted_commit_schema_fingerprint,
        },
    },
    error::InternalError,
    types::EntityTag,
    value::Value,
};

use super::{
    super::startup_field_path::{
        SchemaMutationCatalogScope, catalog_backed_row_contract_for_rebuild,
    },
    SqlDdlPublicationEnvelope, validate_sql_ddl_drop_schema_gate,
};

/// One pending dense-row rewrite retaining the exact accepted-before bytes
/// until accepted-schema publication commits the new layout.
struct SqlDdlFieldDropRowRewrite {
    key: RawDataStoreKey,
    before: RawRow,
    after: CanonicalRow,
}

/// Execute one SQL DDL field drop by rewriting rows to the accepted dense
/// layout before publishing the compacted catalog snapshot.
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
    let contract = catalog_backed_row_contract_for_rebuild(
        store,
        SchemaMutationCatalogScope::sql_ddl(entity_tag, accepted_before_identity),
        entity_path,
        accepted_before.persisted_snapshot(),
    )?;
    let rewrite_slots = envelope
        .after()
        .fields()
        .iter()
        .map(|after_field| {
            envelope
                .before()
                .fields()
                .iter()
                .find(|before_field| before_field.name() == after_field.name())
                .map(|before_field| usize::from(before_field.slot().get()))
                .ok_or_else(InternalError::store_unsupported)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let rewritten = store.with_data(|data_store| {
        let mut rewritten = Vec::new();
        data_store.visit_entries(|raw_key, raw_row| {
            let key = DecodedDataStoreKey::try_from_raw(raw_key)
                .map_err(|_error| InternalError::store_corruption())?;
            if key.entity_tag() != entity_tag {
                return Ok::<StoreVisit, InternalError>(StoreVisit::Continue);
            }
            let reader = StructuralSlotReader::from_raw_row_with_validated_contract(
                raw_row,
                contract.clone(),
            )?;
            reader.validate_primary_key(&key)?;
            let payloads = rewrite_slots
                .iter()
                .map(|slot| match reader.get_bytes(*slot) {
                    Some(payload) => Ok(Vec::from(payload)),
                    None => contract.missing_slot_payload(*slot),
                })
                .collect::<Result<Vec<_>, _>>()?;
            rewritten.push(SqlDdlFieldDropRowRewrite {
                key: raw_key.clone(),
                before: raw_row.clone(),
                after: canonical_row_from_dense_slot_payloads(&payloads)?,
            });
            Ok::<StoreVisit, InternalError>(StoreVisit::Continue)
        })?;

        Ok::<_, InternalError>(rewritten)
    })?;
    let rows_scanned = rewritten.len();
    let row_puts = sql_ddl_field_drop_row_puts(entity_path, derivation, &rewritten)?;
    let rewrite_result = store.with_data_mut(|data_store| {
        for rewrite in &rewritten {
            if data_store.insert(rewrite.key.clone(), rewrite.after.clone())
                != Some(rewrite.before.clone())
            {
                return Err(InternalError::store_corruption());
            }
        }
        Ok::<_, InternalError>(())
    });
    if let Err(error) = rewrite_result {
        rollback_sql_ddl_field_drop_rows(store, &rewritten);
        return Err(error);
    }
    if let Err(error) = envelope.publish_with_row_puts(row_puts) {
        if envelope.publication_error_allows_physical_rollback() {
            rollback_sql_ddl_field_drop_rows(store, &rewritten);
        }
        return Err(error);
    }

    Ok(rows_scanned)
}

fn sql_ddl_field_drop_row_puts(
    entity_path: &'static str,
    derivation: &SchemaDdlAcceptedSnapshotDerivation,
    rewritten: &[SqlDdlFieldDropRowRewrite],
) -> Result<Vec<JournalRecord>, InternalError> {
    let accepted_after_fingerprint =
        accepted_commit_schema_fingerprint(derivation.accepted_after())?;
    rewritten
        .iter()
        .map(|rewrite| {
            JournalRecord::row_put(
                entity_path,
                rewrite.key.clone(),
                rewrite.after.as_raw_row().as_bytes().to_vec(),
                accepted_after_fingerprint,
            )
        })
        .collect()
}

fn rollback_sql_ddl_field_drop_rows(store: StoreHandle, rewritten: &[SqlDdlFieldDropRowRewrite]) {
    store.with_data_mut(|data_store| {
        for rewrite in rewritten {
            data_store.insert(
                rewrite.key.clone(),
                canonical_row_from_stored_raw_row(rewrite.before.clone()),
            );
        }
    });
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
        .map(|(field, (_, id, slot))| field.clone_with_identity(*id, *slot))
        .collect::<Vec<_>>();
    let expected_layout = SchemaRowLayout::new(
        after.row_layout().version(),
        identities
            .iter()
            .map(|(_, id, slot)| (*id, *slot))
            .collect(),
    );
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
    target: &SchemaFieldDefaultTarget,
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
    entity_path: &'static str,
    before: &PersistedSchemaSnapshot,
    after: &PersistedSchemaSnapshot,
    target: &SchemaFieldNullabilityTarget,
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
        SqlDdlSingleFieldMetadataChange::Nullability,
    )
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
    let contract = catalog_backed_row_contract_for_rebuild(
        store,
        SchemaMutationCatalogScope::sql_ddl(entity_tag, accepted_before_identity),
        entity_path,
        accepted_before.persisted_snapshot(),
    )?;
    let required_slot = usize::from(field.slot().get());

    store.with_data(|data_store| {
        let mut scanned = 0usize;
        data_store.visit_entries(|raw_key, raw_row| {
            let key = DecodedDataStoreKey::try_from_raw(raw_key)
                .map_err(|_error| InternalError::store_unsupported())?;
            if key.entity_tag() != entity_tag {
                return Ok(StoreVisit::Continue);
            }
            scanned = scanned.saturating_add(1);
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

        Ok(scanned)
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
    Nullability,
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
                before_field.clone_with_default(after_field.default().clone()) == *after_field
            }
            Self::Nullability => {
                before_field.clone_with_nullable(after_field.nullable()) == *after_field
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
            Self::Default => before_field.default() != after_field.default(),
            Self::Nullability => before_field.nullable() != after_field.nullable(),
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
    left.field_to_slot() == right.field_to_slot()
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
