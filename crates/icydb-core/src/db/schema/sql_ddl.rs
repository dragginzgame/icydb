//! Module: schema::sql_ddl
//! Responsibility: catalog-native SQL DDL publication orchestration.
//! Does not own: SQL parsing or accepted schema transition policy.
//! Boundary: publishes validated DDL candidates through schema mutation contracts.

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
        return Err(InternalError::schema_ddl_publication_race_lost());
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
    let accepted_before = current
        .entity_snapshots()
        .get(&expected_identity.entity_tag())
        .ok_or_else(InternalError::store_corruption)?;
    if accepted_before == accepted_after {
        return Ok(None);
    }

    let candidate =
        candidate_with_snapshot(&current, expected_identity.entity_tag(), accepted_after)?;
    Ok(Some((expected_revision, candidate)))
}

// SQL DDL candidates share revision advancement and source-lineage bookkeeping.
// Callers retain their admission checks and decide whether an unchanged snapshot
// is a no-op before asking for a candidate.
fn candidate_with_snapshot(
    current: &AcceptedSchemaRevisionBundle,
    entity_tag: EntityTag,
    snapshot: &PersistedSchemaSnapshot,
) -> Result<CandidateSchemaRevision, InternalError> {
    let accepted_before = current
        .entity_snapshots()
        .get(&entity_tag)
        .ok_or_else(InternalError::store_corruption)?;
    let revision = current
        .revision()
        .checked_next()
        .ok_or_else(InternalError::store_unsupported)?;
    let source_bindings = current
        .source_bindings()
        .clone()
        .with_sql_ddl_entity_transition(entity_tag, accepted_before, snapshot, revision)?;
    CandidateSchemaRevision::from_entity_snapshot(
        current,
        revision,
        entity_tag,
        snapshot.clone(),
        source_bindings,
    )
}

fn validate_publishable_transition_plan(plan: &SchemaTransitionPlan) -> Result<(), InternalError> {
    match plan.publication_preflight() {
        MutationPublicationPreflight::PublishableNow => Ok(()),
        MutationPublicationPreflight::RequiresPhysicalWork => {
            Err(InternalError::store_unsupported())
        }
    }
}

pub(in crate::db) fn execute_admin_sql_ddl_field_path_index_addition(
    store: StoreHandle,
    accepted_before: &AcceptedSchemaSnapshot,
    accepted_before_identity: AcceptedCatalogIdentity,
    derivation: &SchemaDdlAcceptedSnapshotDerivation,
) -> Result<(usize, usize), InternalError> {
    let envelope = SqlDdlPublicationEnvelope::new(
        store,
        accepted_before,
        &accepted_before_identity,
        derivation,
    );
    let plan = envelope.require_transition_plan(SchemaTransitionPlanKind::AddFieldPathIndex)?;
    let target = plan
        .field_path_index_target()
        .ok_or_else(InternalError::store_unsupported)?;
    if Some(target) != derivation.admission().field_path_target() {
        return Err(InternalError::store_unsupported());
    }

    stage_and_publish_sql_ddl_index_addition(&envelope, accepted_before_identity)
}

/// Execute one supported SQL DDL expression index addition through the schema
/// mutation staging and publication boundary.
pub(in crate::db) fn execute_admin_sql_ddl_expression_index_addition(
    store: StoreHandle,
    accepted_before: &AcceptedSchemaSnapshot,
    accepted_before_identity: AcceptedCatalogIdentity,
    derivation: &SchemaDdlAcceptedSnapshotDerivation,
) -> Result<(usize, usize), InternalError> {
    let envelope = SqlDdlPublicationEnvelope::new(
        store,
        accepted_before,
        &accepted_before_identity,
        derivation,
    );
    let plan = envelope.require_transition_plan(SchemaTransitionPlanKind::AddExpressionIndex)?;
    let Some(target) = derivation.admission().expression_target() else {
        return Err(InternalError::store_unsupported());
    };
    if plan.expression_index_target() != Some(target) {
        return Err(InternalError::store_unsupported());
    }

    stage_and_publish_sql_ddl_index_addition(&envelope, accepted_before_identity)
}

// Both admitted index kinds publish through the same staged domain replacement.
// Kind-specific target checks remain at their entrypoints before any staging.
fn stage_and_publish_sql_ddl_index_addition(
    envelope: &SqlDdlPublicationEnvelope<'_>,
    accepted_before_identity: AcceptedCatalogIdentity,
) -> Result<(usize, usize), InternalError> {
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
    accepted_before: &AcceptedSchemaSnapshot,
    accepted_before_identity: AcceptedCatalogIdentity,
    derivation: &SchemaDdlAcceptedSnapshotDerivation,
) -> Result<(), InternalError> {
    let envelope = SqlDdlPublicationEnvelope::new(
        store,
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
        require_exact_empty_sql_ddl_entity(store, entity_tag)?;
    } else {
        let plan = envelope.require_transition_plan(SchemaTransitionPlanKind::AppendOnlyFields)?;
        validate_publishable_transition_plan(&plan)?;
    }

    envelope.publish()
}

/// Require an exact empty-entity proof for one current physical-shape transition.
/// Missing or invalid cardinality metadata is conservatively nonempty.
pub(super) fn require_exact_empty_sql_ddl_entity(
    store: StoreHandle,
    entity_tag: EntityTag,
) -> Result<(), InternalError> {
    if store.exact_entity_count(entity_tag) == Some(0) {
        return Ok(());
    }

    Err(InternalError::schema_ddl_rewrite_requires_migration())
}

pub(super) struct SqlDdlPublicationEnvelope<'a> {
    store: StoreHandle,
    accepted_before_identity: AcceptedCatalogIdentity,
    before: &'a PersistedSchemaSnapshot,
    after: &'a PersistedSchemaSnapshot,
}

impl<'a> SqlDdlPublicationEnvelope<'a> {
    pub(super) fn new(
        store: StoreHandle,
        accepted_before: &'a AcceptedSchemaSnapshot,
        accepted_before_identity: &AcceptedCatalogIdentity,
        derivation: &'a SchemaDdlAcceptedSnapshotDerivation,
    ) -> Self {
        Self {
            store,
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

    pub(super) fn require_transition_plan(
        &self,
        expected_kind: SchemaTransitionPlanKind,
    ) -> Result<SchemaTransitionPlan, InternalError> {
        require_sql_ddl_transition_plan(self.before, self.after, expected_kind)
    }

    pub(super) fn publish(&self) -> Result<(), InternalError> {
        publish_accepted_entity_snapshot_revision(
            self.store,
            self.accepted_before_identity.clone(),
            self.after,
        )
    }
}

fn require_sql_ddl_transition_plan(
    before: &PersistedSchemaSnapshot,
    after: &PersistedSchemaSnapshot,
    expected_kind: SchemaTransitionPlanKind,
) -> Result<SchemaTransitionPlan, InternalError> {
    let SchemaTransitionDecision::Accepted(plan) = decide_schema_transition(before, after) else {
        return Err(InternalError::store_unsupported());
    };
    if plan.kind() != expected_kind {
        return Err(InternalError::store_unsupported());
    }

    Ok(plan)
}

/// Execute one supported SQL DDL secondary-index drop through marker-first
/// complete-domain replacement.
pub(in crate::db) fn execute_admin_sql_ddl_secondary_index_drop(
    store: StoreHandle,
    accepted_before: &AcceptedSchemaSnapshot,
    accepted_before_identity: AcceptedCatalogIdentity,
    derivation: &SchemaDdlAcceptedSnapshotDerivation,
) -> Result<(), InternalError> {
    let envelope = SqlDdlPublicationEnvelope::new(
        store,
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
    accepted_before: &PersistedSchemaSnapshot,
) -> Result<(), InternalError> {
    let latest = store.with_schema_mut(|schema_store| {
        schema_store.current_accepted_persisted_snapshot(entity_tag)
    })?;
    if latest.as_ref() == Some(accepted_before) {
        return Ok(());
    }

    Err(InternalError::store_unsupported())
}
