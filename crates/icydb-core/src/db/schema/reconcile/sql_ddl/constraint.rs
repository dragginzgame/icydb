use crate::{
    db::{
        commit::{
            publish_accepted_schema_candidate,
            publish_accepted_schema_candidate_with_constraint_validation_job_removal,
        },
        registry::StoreHandle,
        schema::{
            AcceptedCatalogIdentity, AcceptedSchemaRevision, AcceptedSchemaRevisionBundle,
            AcceptedSchemaSnapshot, CandidateSchemaRevision, ConstraintActivationState,
            ConstraintOrigin, PersistedSchemaSnapshot, SchemaVersion,
            validate_unpublished_check_candidate_exact,
        },
        sql::ddl::{
            BoundSqlAddCheckConstraintRequest, BoundSqlCreateIndexRequest,
            BoundSqlDropConstraintRequest, BoundSqlDropIndexRequest,
        },
    },
    error::InternalError,
    types::EntityTag,
};

/// Publish one SQL-DDL check as either a write-gated activation or an exactly
/// validated accepted constraint.
pub(in crate::db) fn execute_admin_sql_ddl_check_addition(
    store: StoreHandle,
    entity_tag: EntityTag,
    entity_path: &'static str,
    accepted_before: &AcceptedSchemaSnapshot,
    accepted_before_identity: AcceptedCatalogIdentity,
    request: &BoundSqlAddCheckConstraintRequest,
    next_schema_version: SchemaVersion,
) -> Result<(usize, crate::db::schema::ConstraintId), InternalError> {
    let (current_revision, current_fingerprint, current) = current_sql_ddl_bundle(
        store,
        entity_tag,
        entity_path,
        accepted_before,
        accepted_before_identity,
    )?;
    let activation_epoch = current_revision
        .checked_next()
        .ok_or_else(InternalError::store_unsupported)?
        .get();
    let before = current
        .entity_snapshots()
        .get(&entity_tag)
        .ok_or_else(InternalError::store_corruption)?;
    let catalog = before
        .constraint_catalog()
        .clone()
        .with_added_check_activation(
            request.constraint_name().to_string(),
            ConstraintOrigin::SqlDdl,
            request.expression().clone(),
            current_fingerprint,
            activation_epoch,
        )
        .map_err(|_| InternalError::store_unsupported())?;
    let activation_snapshot = before
        .clone()
        .with_constraint_catalog(catalog)
        .with_schema_version(next_schema_version);
    let activation_id = activation_snapshot
        .constraint_catalog()
        .activations()
        .iter()
        .find(|activation| activation.name() == request.constraint_name())
        .map(crate::db::schema::ConstraintActivationSnapshot::id)
        .ok_or_else(InternalError::store_invariant)?;
    let activation_candidate =
        candidate_with_snapshot(&current, entity_tag, activation_snapshot.clone())?;

    if request.not_valid() {
        publish_accepted_schema_candidate(
            accepted_before_identity.store_path(),
            store,
            current_revision,
            &activation_candidate,
        )?;
        return Ok((0, activation_id));
    }

    let rows_scanned = validate_unpublished_check_candidate_exact(
        store,
        accepted_before_identity.store_path(),
        entity_tag,
        entity_path,
        &activation_candidate,
        activation_id,
    )?;
    let validated_catalog = activation_snapshot
        .constraint_catalog()
        .clone()
        .with_directly_validated_activation(activation_id)
        .map_err(|_| InternalError::store_invariant())?;
    let validated = activation_snapshot.with_constraint_catalog(validated_catalog);
    let validated_candidate = candidate_with_snapshot(&current, entity_tag, validated)?;
    publish_accepted_schema_candidate(
        accepted_before_identity.store_path(),
        store,
        current_revision,
        &validated_candidate,
    )?;
    Ok((rows_scanned, activation_id))
}

/// Reserve one SQL-DDL unique index as a planner-invisible activation.
pub(in crate::db) fn execute_admin_sql_ddl_unique_index_activation(
    store: StoreHandle,
    entity_tag: EntityTag,
    entity_path: &'static str,
    accepted_before: &AcceptedSchemaSnapshot,
    accepted_before_identity: AcceptedCatalogIdentity,
    request: &BoundSqlCreateIndexRequest,
    next_schema_version: SchemaVersion,
) -> Result<crate::db::schema::ConstraintId, InternalError> {
    if !request.candidate_index().unique() {
        return Err(InternalError::store_invariant());
    }
    let (current_revision, current_fingerprint, current) = current_sql_ddl_bundle(
        store,
        entity_tag,
        entity_path,
        accepted_before,
        accepted_before_identity,
    )?;
    let activation_epoch = current_revision
        .checked_next()
        .ok_or_else(InternalError::store_unsupported)?
        .get();
    let before = current
        .entity_snapshots()
        .get(&entity_tag)
        .ok_or_else(InternalError::store_corruption)?;
    let candidate = request.candidate_index().clone_with_schema_identity(
        request.candidate_index().schema_id(),
        request.candidate_index().ordinal(),
        activation_epoch,
    );
    let after = before
        .clone()
        .with_added_unique_activation(candidate, current_fingerprint, activation_epoch)
        .map_err(|_| InternalError::store_unsupported())?
        .with_schema_version(next_schema_version);
    let activation_id = after
        .constraint_catalog()
        .activations()
        .iter()
        .find(|activation| activation.name() == request.candidate_index().name())
        .map(crate::db::schema::ConstraintActivationSnapshot::id)
        .ok_or_else(InternalError::store_invariant)?;
    let candidate = candidate_with_snapshot(&current, entity_tag, after)?;
    publish_accepted_schema_candidate(
        accepted_before_identity.store_path(),
        store,
        current_revision,
        &candidate,
    )?;
    Ok(activation_id)
}

/// Abort one SQL-DDL unique-index activation and remove its isolated owner.
pub(in crate::db) fn execute_admin_sql_ddl_unique_index_activation_abort(
    store: StoreHandle,
    entity_tag: EntityTag,
    entity_path: &'static str,
    accepted_before: &AcceptedSchemaSnapshot,
    accepted_before_identity: AcceptedCatalogIdentity,
    request: &BoundSqlDropIndexRequest,
    next_schema_version: SchemaVersion,
) -> Result<(), InternalError> {
    let constraint_id = request
        .pending_activation_id()
        .ok_or_else(InternalError::store_invariant)?;
    let (current_revision, _current_fingerprint, current) = current_sql_ddl_bundle(
        store,
        entity_tag,
        entity_path,
        accepted_before,
        accepted_before_identity,
    )?;
    let before = current
        .entity_snapshots()
        .get(&entity_tag)
        .ok_or_else(InternalError::store_corruption)?;
    if !before.candidate_indexes().contains(request.dropped_index()) {
        return Err(InternalError::schema_ddl_publication_race_lost(entity_path));
    }
    let activation_state = before
        .constraint_catalog()
        .activation(constraint_id)
        .map(crate::db::schema::ConstraintActivationSnapshot::state)
        .ok_or_else(|| InternalError::schema_ddl_publication_race_lost(entity_path))?;
    let after = before
        .with_aborted_unique_activation(constraint_id, next_schema_version)
        .map_err(|_| InternalError::store_invariant())?;
    let candidate = candidate_with_snapshot(&current, entity_tag, after)?;
    if activation_state == ConstraintActivationState::Validating {
        publish_accepted_schema_candidate_with_constraint_validation_job_removal(
            accepted_before_identity.store_path(),
            store,
            current_revision,
            &candidate,
            entity_tag,
            constraint_id,
        )
    } else {
        publish_accepted_schema_candidate(
            accepted_before_identity.store_path(),
            store,
            current_revision,
            &candidate,
        )
    }
}

/// Drop or abort one SQL-DDL-owned check through the same marker-owned schema
/// publication boundary as activation progress.
pub(in crate::db) fn execute_admin_sql_ddl_check_drop(
    store: StoreHandle,
    entity_tag: EntityTag,
    entity_path: &'static str,
    accepted_before: &AcceptedSchemaSnapshot,
    accepted_before_identity: AcceptedCatalogIdentity,
    request: &BoundSqlDropConstraintRequest,
    next_schema_version: SchemaVersion,
) -> Result<(), InternalError> {
    let (current_revision, _current_fingerprint, current) = current_sql_ddl_bundle(
        store,
        entity_tag,
        entity_path,
        accepted_before,
        accepted_before_identity,
    )?;
    let before = current
        .entity_snapshots()
        .get(&entity_tag)
        .ok_or_else(InternalError::store_corruption)?;
    let activation_state = before
        .constraint_catalog()
        .activation(request.constraint_id())
        .map(crate::db::schema::ConstraintActivationSnapshot::state);
    if request.is_activation() != activation_state.is_some() {
        return Err(InternalError::schema_ddl_publication_race_lost(entity_path));
    }
    let catalog = if request.is_activation() {
        before
            .constraint_catalog()
            .clone()
            .with_aborted_activation(request.constraint_id())
    } else {
        before
            .constraint_catalog()
            .clone()
            .with_removed_sql_ddl_check(request.constraint_id())
    }
    .map_err(|_| InternalError::store_unsupported())?;
    let after = before
        .clone()
        .with_constraint_catalog(catalog)
        .with_schema_version(next_schema_version);
    let candidate = candidate_with_snapshot(&current, entity_tag, after)?;
    if activation_state == Some(ConstraintActivationState::Validating) {
        publish_accepted_schema_candidate_with_constraint_validation_job_removal(
            accepted_before_identity.store_path(),
            store,
            current_revision,
            &candidate,
            entity_tag,
            request.constraint_id(),
        )
    } else {
        publish_accepted_schema_candidate(
            accepted_before_identity.store_path(),
            store,
            current_revision,
            &candidate,
        )
    }
}

pub(super) fn current_sql_ddl_bundle(
    store: StoreHandle,
    entity_tag: EntityTag,
    entity_path: &'static str,
    accepted_before: &AcceptedSchemaSnapshot,
    expected_identity: AcceptedCatalogIdentity,
) -> Result<
    (
        AcceptedSchemaRevision,
        crate::db::schema::AcceptedSchemaFingerprint,
        AcceptedSchemaRevisionBundle,
    ),
    InternalError,
> {
    let (root, selection, bundle) = store.with_schema(|schema_store| {
        Ok::<_, InternalError>((
            schema_store
                .current_accepted_schema_root()?
                .ok_or_else(InternalError::store_corruption)?,
            schema_store
                .current_accepted_catalog_selection(
                    entity_tag,
                    entity_path,
                    expected_identity.store_path(),
                )?
                .ok_or_else(InternalError::store_corruption)?,
            schema_store
                .current_accepted_schema_bundle()?
                .ok_or_else(InternalError::store_corruption)?,
        ))
    })?;
    if selection.identity() != expected_identity
        || bundle.store_path() != expected_identity.store_path()
        || bundle.entity_snapshots().get(&entity_tag) != Some(accepted_before.persisted_snapshot())
    {
        return Err(InternalError::schema_ddl_publication_race_lost(entity_path));
    }
    Ok((root.root().revision(), root.root().fingerprint(), bundle))
}

pub(super) fn candidate_with_snapshot(
    current: &AcceptedSchemaRevisionBundle,
    entity_tag: EntityTag,
    snapshot: PersistedSchemaSnapshot,
) -> Result<CandidateSchemaRevision, InternalError> {
    let mut entity_snapshots = current.entity_snapshots().clone();
    entity_snapshots.insert(entity_tag, snapshot);
    let revision = current
        .revision()
        .checked_next()
        .ok_or_else(InternalError::store_unsupported)?;
    let bundle = AcceptedSchemaRevisionBundle::new(
        revision,
        current.store_path(),
        current.enum_catalog().clone(),
        current.composite_catalog().clone(),
        entity_snapshots,
    )?;
    CandidateSchemaRevision::new(bundle)
}
