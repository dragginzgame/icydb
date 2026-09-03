mod constraint;
mod field_metadata;
mod user_index_domain;

use crate::{
    db::{
        registry::StoreHandle,
        schema::{
            AcceptedCatalogIdentity, AcceptedSchemaRevision, AcceptedSchemaRevisionBundle,
            AcceptedSchemaSnapshot, CandidateSchemaRevision, ConstraintActivationState,
            ConstraintId, MutationPublicationPreflight, PersistedSchemaSnapshot,
            SchemaDdlAcceptedSnapshotDerivation, SchemaStore, SchemaTransitionDecision,
            SchemaTransitionPlanKind, StagedUserIndexDomainReplacement, decide_schema_transition,
            mutation::required_empty_entity_field_addition_matches,
            transition::SchemaTransitionPlan,
        },
    },
    error::InternalError,
    types::EntityTag,
};
use user_index_domain::stage_sql_ddl_user_index_domain_replacement;

pub(in crate::db) use constraint::{
    execute_admin_sql_ddl_check_addition, execute_admin_sql_ddl_check_drop,
    execute_admin_sql_ddl_unique_index_activation,
    execute_admin_sql_ddl_unique_index_activation_abort,
};
pub(in crate::db) use field_metadata::{
    SqlDdlFieldNullabilityOutcome, execute_admin_sql_ddl_field_default_change,
    execute_admin_sql_ddl_field_drop, execute_admin_sql_ddl_field_nullability_change,
    execute_admin_sql_ddl_field_rename, execute_admin_sql_ddl_not_null_activation_abort,
};

// Publish one SQL-DDL constraint removal and retire its validation job exactly
// when the removed activation had reached the validating state.
fn publish_sql_ddl_constraint_removal(
    store: StoreHandle,
    accepted_before_identity: &AcceptedCatalogIdentity,
    expected_revision: AcceptedSchemaRevision,
    candidate: &CandidateSchemaRevision,
    entity_tag: EntityTag,
    constraint_id: ConstraintId,
    activation_state: Option<ConstraintActivationState>,
) -> Result<(), InternalError> {
    if matches!(
        activation_state,
        Some(ConstraintActivationState::Validating)
    ) {
        return crate::db::commit::publish_accepted_schema_candidate_with_constraint_validation_job_removal(
            accepted_before_identity.store_path(),
            store,
            expected_revision,
            candidate,
            entity_tag,
            constraint_id,
        );
    }

    crate::db::commit::publish_accepted_schema_candidate(
        accepted_before_identity.store_path(),
        store,
        expected_revision,
        candidate,
    )
}

fn publish_accepted_entity_snapshot_revision(
    store: StoreHandle,
    expected_identity: AcceptedCatalogIdentity,
    accepted_after: &PersistedSchemaSnapshot,
) -> Result<(), InternalError> {
    let Some((expected_revision, candidate)) =
        prepare_accepted_entity_snapshot_revision(store, &expected_identity, accepted_after)?
    else {
        return Ok(());
    };
    crate::db::commit::publish_accepted_schema_candidate(
        expected_identity.store_path(),
        store,
        expected_revision,
        &candidate,
    )
}

fn publish_accepted_entity_snapshot_revision_with_user_index_domain(
    store: StoreHandle,
    expected_identity: AcceptedCatalogIdentity,
    accepted_after: &PersistedSchemaSnapshot,
    replacement: StagedUserIndexDomainReplacement,
) -> Result<(), InternalError> {
    let Some((expected_revision, candidate)) =
        prepare_accepted_entity_snapshot_revision(store, &expected_identity, accepted_after)?
    else {
        return Err(InternalError::store_invariant());
    };
    crate::db::commit::publish_accepted_schema_candidate_with_user_index_domain(
        expected_identity.store_path(),
        store,
        expected_revision,
        &candidate,
        replacement,
    )
}

fn prepare_accepted_entity_snapshot_revision(
    store: StoreHandle,
    expected_identity: &AcceptedCatalogIdentity,
    accepted_after: &PersistedSchemaSnapshot,
) -> Result<Option<(AcceptedSchemaRevision, CandidateSchemaRevision)>, InternalError> {
    let current_selection = store
        .with_schema(|schema_store| {
            schema_store.current_accepted_catalog_selection(
                expected_identity.entity_tag(),
                expected_identity.entity_path(),
                expected_identity.store_path(),
            )
        })?
        .ok_or_else(InternalError::store_corruption)?;
    if current_selection.identity() != *expected_identity {
        return Err(InternalError::schema_ddl_publication_race_lost(
            expected_identity.entity_path(),
        ));
    }

    let current = store
        .with_schema(SchemaStore::current_accepted_schema_bundle)?
        .ok_or_else(InternalError::store_corruption)?;
    if current.store_path() != expected_identity.store_path()
        || accepted_after.entity_path() != expected_identity.entity_path()
    {
        return Err(InternalError::store_corruption());
    }
    let expected_revision = current.revision();
    let mut entity_snapshots = current.entity_snapshots().clone();
    if entity_snapshots
        .insert(expected_identity.entity_tag(), accepted_after.clone())
        .is_none()
    {
        return Err(InternalError::store_corruption());
    }
    if entity_snapshots == *current.entity_snapshots() {
        return Ok(None);
    }

    let candidate_revision = expected_revision
        .checked_next()
        .ok_or_else(InternalError::store_unsupported)?;
    let accepted_before = current
        .entity_snapshots()
        .get(&expected_identity.entity_tag())
        .ok_or_else(InternalError::store_corruption)?;
    let source_bindings = current
        .source_bindings()
        .clone()
        .with_sql_ddl_entity_transition(
            expected_identity.entity_tag(),
            accepted_before,
            accepted_after,
            candidate_revision,
        )?;
    let bundle = AcceptedSchemaRevisionBundle::new_with_source_bindings(
        candidate_revision,
        expected_identity.store_path(),
        current.enum_catalog().clone(),
        current.composite_catalog().clone(),
        source_bindings,
        entity_snapshots,
    )?;

    Ok(Some((
        expected_revision,
        CandidateSchemaRevision::new(bundle)?,
    )))
}

fn validate_publishable_transition_plan(
    _entity_path: &str,
    plan: &SchemaTransitionPlan,
) -> Result<(), InternalError> {
    match plan.publication_preflight() {
        MutationPublicationPreflight::PublishableNow => Ok(()),
        MutationPublicationPreflight::RequiresPhysicalWork => {
            Err(InternalError::store_unsupported())
        }
    }
}

pub(in crate::db) fn execute_admin_sql_ddl_field_path_index_addition(
    store: StoreHandle,
    entity_tag: EntityTag,
    entity_path: &str,
    accepted_before: &AcceptedSchemaSnapshot,
    accepted_before_identity: AcceptedCatalogIdentity,
    derivation: &SchemaDdlAcceptedSnapshotDerivation,
) -> Result<(usize, usize), InternalError> {
    let envelope = SqlDdlPublicationEnvelope::new(
        store,
        entity_tag,
        entity_path,
        accepted_before,
        &accepted_before_identity,
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
        &accepted_before_identity,
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
    entity_path: &str,
    accepted_before: &AcceptedSchemaSnapshot,
    accepted_before_identity: AcceptedCatalogIdentity,
    derivation: &SchemaDdlAcceptedSnapshotDerivation,
) -> Result<(usize, usize), InternalError> {
    let envelope = SqlDdlPublicationEnvelope::new(
        store,
        entity_tag,
        entity_path,
        accepted_before,
        &accepted_before_identity,
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
        &accepted_before_identity,
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
    let Some(target) = derivation.admission().field_addition_target() else {
        return Err(InternalError::store_unsupported());
    };
    let added_field = envelope
        .after()
        .fields()
        .iter()
        .find(|field| field.id() == target.field_id())
        .ok_or_else(InternalError::store_unsupported)?;
    if added_field.name() != target.name() || added_field.slot() != target.slot() {
        return Err(InternalError::store_unsupported());
    }
    if matches!(
        added_field.historical_fill(),
        crate::db::schema::SchemaHistoricalFill::Reject
    ) {
        if !required_empty_entity_field_addition_matches(
            envelope.before(),
            envelope.after(),
            added_field,
        ) {
            return Err(InternalError::store_unsupported());
        }
        require_exact_empty_sql_ddl_entity(store, entity_tag, entity_path)?;
    } else {
        let plan = envelope.require_transition_plan(
            "field-addition",
            SchemaTransitionPlanKind::AppendOnlyFields,
            "append-only fields",
        )?;
        validate_publishable_transition_plan(entity_path, &plan)?;
    }

    envelope.publish()
}

/// Require an exact empty-entity proof for one current physical-shape transition.
/// Missing or invalid cardinality metadata is conservatively nonempty.
pub(super) fn require_exact_empty_sql_ddl_entity(
    store: StoreHandle,
    entity_tag: EntityTag,
    entity_path: &str,
) -> Result<(), InternalError> {
    if store.exact_entity_count(entity_tag) == Some(0) {
        return Ok(());
    }

    Err(InternalError::schema_ddl_rewrite_requires_migration(
        entity_path,
    ))
}

pub(super) struct SqlDdlPublicationEnvelope<'a> {
    store: StoreHandle,
    entity_tag: EntityTag,
    entity_path: &'a str,
    accepted_before_identity: AcceptedCatalogIdentity,
    before: &'a PersistedSchemaSnapshot,
    after: &'a PersistedSchemaSnapshot,
}

impl<'a> SqlDdlPublicationEnvelope<'a> {
    pub(super) fn new(
        store: StoreHandle,
        entity_tag: EntityTag,
        entity_path: &'a str,
        accepted_before: &'a AcceptedSchemaSnapshot,
        accepted_before_identity: &AcceptedCatalogIdentity,
        derivation: &'a SchemaDdlAcceptedSnapshotDerivation,
    ) -> Self {
        Self {
            store,
            entity_tag,
            entity_path,
            accepted_before_identity: accepted_before_identity.clone(),
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

    pub(super) const fn entity_path(&self) -> &str {
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
            self.accepted_before_identity.clone(),
            self.after,
        )
    }
}

fn require_sql_ddl_transition_plan(
    entity_path: &str,
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
    if !derivation.admission().is_secondary_drop() {
        return Err(InternalError::store_unsupported());
    }
    let replacement = stage_sql_ddl_user_index_domain_replacement(
        envelope.store(),
        &accepted_before_identity,
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
    entity_path: &str,
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
