//! Module: schema::sql_ddl::field_metadata
//! Responsibility: SQL DDL field metadata candidate construction and publication.
//! Does not own: SQL syntax binding or physical nonempty-row migration.
//! Boundary: applies bound field changes through accepted-schema transitions.

#[cfg(test)]
mod tests;

use crate::{
    db::{
        commit::publish_accepted_schema_candidate,
        registry::StoreHandle,
        schema::{
            AcceptedCatalogIdentity, AcceptedSchemaSnapshot, ConstraintActivationKind,
            ConstraintId, ConstraintOrigin, FieldId, PersistedSchemaSnapshot,
            SchemaDdlAcceptedSnapshotDerivation, SchemaFieldDropTarget,
            SchemaFieldNullabilityTarget, SchemaFieldRenameTarget, SchemaInsertDefaultTarget,
            derive_sql_ddl_field_nullability_persisted_after,
            mutation::{
                derive_dense_field_removal_candidate, derive_sql_ddl_field_default_persisted_after,
                derive_sql_ddl_field_rename_persisted_after,
            },
            sql_ddl::candidate_with_snapshot,
        },
    },
    error::InternalError,
    types::EntityTag,
};

use super::{
    SqlDdlPublicationEnvelope, constraint::current_sql_ddl_bundle,
    publish_sql_ddl_constraint_removal, require_exact_empty_sql_ddl_entity,
    validate_sql_ddl_drop_schema_gate,
};

/// Execute one SQL DDL field drop after proving no physical row requires the
/// dense-layout rewrite. Nonempty rewrites belong to the explicit migration
/// protocol and reject before publication.
pub(in crate::db) fn execute_admin_sql_ddl_field_drop(
    store: StoreHandle,
    entity_tag: EntityTag,
    entity_path: &str,
    accepted_before: &AcceptedSchemaSnapshot,
    accepted_before_identity: AcceptedCatalogIdentity,
    derivation: &SchemaDdlAcceptedSnapshotDerivation,
) -> Result<usize, InternalError> {
    let envelope = SqlDdlPublicationEnvelope::new(
        store,
        entity_tag,
        entity_path,
        accepted_before,
        &accepted_before_identity,
        derivation,
    );
    let Some(target) = derivation.admission().field_drop_target() else {
        return Err(InternalError::store_unsupported());
    };
    validate_sql_ddl_field_drop_metadata_change(envelope.before(), envelope.after(), target)?;
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
    before: &PersistedSchemaSnapshot,
    after: &PersistedSchemaSnapshot,
    target: &SchemaFieldDropTarget,
) -> Result<(), InternalError> {
    let before_field = before
        .fields()
        .iter()
        .find(|field| field.id() == target.field_id())
        .ok_or_else(InternalError::store_unsupported)?;
    if before_field.name() != target.name()
        || before_field.slot() != target.slot()
        || before.row_layout().slot_for_field(target.field_id()) != Some(target.slot())
    {
        return Err(InternalError::store_unsupported());
    }

    // Version admission belongs to the bound DDL request. Validate every other
    // part of the after-image against the catalog-native dense removal owner.
    let expected = derive_dense_field_removal_candidate(before, target.field_id())
        .map_err(|_| InternalError::store_unsupported())?
        .into_snapshot()
        .with_schema_version(after.version());
    if &expected != after {
        return Err(InternalError::store_unsupported());
    }

    Ok(())
}

/// Execute one metadata-only SQL DDL field-default publication.
pub(in crate::db) fn execute_admin_sql_ddl_field_default_change(
    store: StoreHandle,
    entity_tag: EntityTag,
    entity_path: &str,
    accepted_before: &AcceptedSchemaSnapshot,
    accepted_before_identity: AcceptedCatalogIdentity,
    derivation: &SchemaDdlAcceptedSnapshotDerivation,
) -> Result<(), InternalError> {
    let envelope = SqlDdlPublicationEnvelope::new(
        store,
        entity_tag,
        entity_path,
        accepted_before,
        &accepted_before_identity,
        derivation,
    );
    execute_admin_sql_ddl_checked_field_metadata_publication(
        envelope,
        derivation.admission().field_default_target(),
        validate_sql_ddl_field_default_metadata_change,
    )
}

fn validate_sql_ddl_field_default_metadata_change(
    before: &PersistedSchemaSnapshot,
    after: &PersistedSchemaSnapshot,
    target: &SchemaInsertDefaultTarget,
) -> Result<(), InternalError> {
    let before_field = before
        .fields()
        .iter()
        .find(|field| field.id() == target.field_id())
        .ok_or_else(InternalError::store_unsupported)?;
    let after_field = after
        .fields()
        .iter()
        .find(|field| field.id() == target.field_id())
        .ok_or_else(InternalError::store_unsupported)?;
    if before_field.name() != target.name()
        || before_field.insert_default() == after_field.insert_default()
    {
        return Err(InternalError::store_unsupported());
    }
    // The admitted request owns the version; derivation owns every other component.
    let expected = derive_sql_ddl_field_default_persisted_after(
        before,
        target.field_id(),
        after_field.insert_default().clone(),
    )
    .with_schema_version(after.version());
    if &expected != after {
        return Err(InternalError::store_unsupported());
    }
    Ok(())
}

/// Result of one SQL DDL field-nullability lifecycle operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum SqlDdlFieldNullabilityOutcome {
    /// A direct metadata publication or activation abort completed.
    Published,
    /// A new-write gate was published for bounded historical validation.
    ActivationPublished { constraint_id: ConstraintId },
}

/// Execute one SQL DDL field-nullability lifecycle operation.
pub(in crate::db) fn execute_admin_sql_ddl_field_nullability_change(
    store: StoreHandle,
    entity_tag: EntityTag,
    entity_path: &str,
    accepted_before: &AcceptedSchemaSnapshot,
    accepted_before_identity: AcceptedCatalogIdentity,
    derivation: &SchemaDdlAcceptedSnapshotDerivation,
) -> Result<SqlDdlFieldNullabilityOutcome, InternalError> {
    let envelope = SqlDdlPublicationEnvelope::new(
        store,
        entity_tag,
        entity_path,
        accepted_before,
        &accepted_before_identity,
        derivation,
    );
    let Some(target) = derivation.admission().field_nullability_target() else {
        return Err(InternalError::store_unsupported());
    };
    let target_is_required = validate_sql_ddl_field_nullability_metadata_change(
        envelope.before(),
        envelope.after(),
        target,
    )?;

    if target_is_required {
        return publish_sql_ddl_not_null_activation(
            envelope.store(),
            entity_tag,
            entity_path,
            accepted_before,
            accepted_before_identity,
            envelope.after().version(),
            target,
        );
    }

    envelope.publish()?;

    Ok(SqlDdlFieldNullabilityOutcome::Published)
}

fn publish_sql_ddl_not_null_activation(
    store: StoreHandle,
    entity_tag: EntityTag,
    entity_path: &str,
    accepted_before: &AcceptedSchemaSnapshot,
    accepted_before_identity: AcceptedCatalogIdentity,
    next_schema_version: crate::db::schema::SchemaVersion,
    target: &SchemaFieldNullabilityTarget,
) -> Result<SqlDdlFieldNullabilityOutcome, InternalError> {
    let (current_revision, current_fingerprint, current) = current_sql_ddl_bundle(
        store,
        entity_tag,
        entity_path,
        accepted_before,
        &accepted_before_identity,
    )?;
    let activation_epoch = current_revision
        .checked_next()
        .ok_or_else(InternalError::store_unsupported)?
        .get();
    let before = current
        .entity_snapshots()
        .get(&entity_tag)
        .ok_or_else(InternalError::store_corruption)?;
    let field = before
        .fields()
        .iter()
        .find(|field| field.id() == target.field_id() && field.nullable())
        .ok_or_else(InternalError::store_invariant)?;
    let catalog = before
        .constraint_catalog()
        .clone()
        .with_added_not_null_activation(field, current_fingerprint, activation_epoch)
        .map_err(|_| InternalError::store_unsupported())?;
    let after = before
        .clone()
        .with_constraint_catalog(catalog)
        .with_schema_version(next_schema_version);
    let constraint_id = after
        .constraint_catalog()
        .activations()
        .iter()
        .find(|activation| {
            matches!(
                activation.kind(),
                ConstraintActivationKind::NotNull { field_id } if *field_id == target.field_id()
            )
        })
        .map(crate::db::schema::ConstraintActivationSnapshot::id)
        .ok_or_else(InternalError::store_invariant)?;
    let candidate = candidate_with_snapshot(&current, entity_tag, &after)?;
    publish_accepted_schema_candidate(
        accepted_before_identity.store_path(),
        store,
        current_revision,
        &candidate,
    )?;
    Ok(SqlDdlFieldNullabilityOutcome::ActivationPublished { constraint_id })
}

#[expect(
    clippy::too_many_arguments,
    reason = "the abort boundary keeps accepted identity and exact activation state explicit"
)]
pub(in crate::db) fn execute_admin_sql_ddl_not_null_activation_abort(
    store: StoreHandle,
    entity_tag: EntityTag,
    entity_path: &str,
    accepted_before: &AcceptedSchemaSnapshot,
    accepted_before_identity: AcceptedCatalogIdentity,
    next_schema_version: crate::db::schema::SchemaVersion,
    field_id: FieldId,
    constraint_id: ConstraintId,
) -> Result<(), InternalError> {
    let (current_revision, _current_fingerprint, current) = current_sql_ddl_bundle(
        store,
        entity_tag,
        entity_path,
        accepted_before,
        &accepted_before_identity,
    )?;
    let before = current
        .entity_snapshots()
        .get(&entity_tag)
        .ok_or_else(InternalError::store_corruption)?;
    let activation = before
        .constraint_catalog()
        .activation(constraint_id)
        .filter(|activation| {
            activation.origin() == ConstraintOrigin::SqlDdl
                && matches!(
                    activation.kind(),
                    ConstraintActivationKind::NotNull {
                        field_id: activation_field_id
                    } if *activation_field_id == field_id
                )
        })
        .ok_or_else(InternalError::store_unsupported)?;
    let state = activation.state();
    let catalog = before
        .constraint_catalog()
        .clone()
        .with_aborted_activation(constraint_id)
        .map_err(|_| InternalError::store_invariant())?;
    let after = before
        .clone()
        .with_constraint_catalog(catalog)
        .with_schema_version(next_schema_version);
    let candidate = candidate_with_snapshot(&current, entity_tag, &after)?;
    publish_sql_ddl_constraint_removal(
        store,
        &accepted_before_identity,
        current_revision,
        &candidate,
        entity_tag,
        constraint_id,
        Some(state),
    )
}

// Return the target's requiredness only after validating the complete after-image.
fn validate_sql_ddl_field_nullability_metadata_change(
    before: &PersistedSchemaSnapshot,
    after: &PersistedSchemaSnapshot,
    target: &SchemaFieldNullabilityTarget,
) -> Result<bool, InternalError> {
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
    .map_err(|_| InternalError::store_unsupported())?;
    if &expected != after {
        return Err(InternalError::store_unsupported());
    }

    Ok(!after_field.nullable())
}

/// Execute one metadata-only SQL DDL field-rename publication.
pub(in crate::db) fn execute_admin_sql_ddl_field_rename(
    store: StoreHandle,
    entity_tag: EntityTag,
    entity_path: &str,
    accepted_before: &AcceptedSchemaSnapshot,
    accepted_before_identity: AcceptedCatalogIdentity,
    derivation: &SchemaDdlAcceptedSnapshotDerivation,
) -> Result<(), InternalError> {
    let envelope = SqlDdlPublicationEnvelope::new(
        store,
        entity_tag,
        entity_path,
        accepted_before,
        &accepted_before_identity,
        derivation,
    );
    execute_admin_sql_ddl_checked_field_metadata_publication(
        envelope,
        derivation.admission().field_rename_target(),
        validate_sql_ddl_field_rename_metadata_change,
    )
}

fn validate_sql_ddl_field_rename_metadata_change(
    before: &PersistedSchemaSnapshot,
    after: &PersistedSchemaSnapshot,
    target: &SchemaFieldRenameTarget,
) -> Result<(), InternalError> {
    let before_field = before
        .fields()
        .iter()
        .find(|field| field.id() == target.field_id())
        .ok_or_else(InternalError::store_unsupported)?;
    let unchanged = target.old_name() == target.new_name();
    if before_field.name() != target.old_name() || unchanged {
        return Err(InternalError::store_unsupported());
    }
    let expected =
        derive_sql_ddl_field_rename_persisted_after(before, before_field, target.new_name())
            .map_err(|_| InternalError::store_unsupported())?
            .with_schema_version(after.version());
    if &expected != after {
        return Err(InternalError::store_unsupported());
    }
    Ok(())
}

fn execute_admin_sql_ddl_checked_field_metadata_publication<T>(
    envelope: SqlDdlPublicationEnvelope<'_>,
    target: Option<&T>,
    validate: impl FnOnce(
        &PersistedSchemaSnapshot,
        &PersistedSchemaSnapshot,
        &T,
    ) -> Result<(), InternalError>,
) -> Result<(), InternalError> {
    let Some(target) = target else {
        return Err(InternalError::store_unsupported());
    };
    validate(envelope.before(), envelope.after(), target)?;

    envelope.publish()
}
