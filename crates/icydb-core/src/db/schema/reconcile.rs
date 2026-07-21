//! Module: db::schema::reconcile
//! Responsibility: startup schema snapshot reconciliation.
//! Does not own: row/index recovery, generated model construction, or runtime layout authority.
//! Boundary: compares generated schema proposals with persisted schema snapshots.

#[cfg(feature = "sql")]
mod sql_ddl;
mod user_index_domain;

use crate::{
    db::{
        Db, EntityRuntimeHooks,
        registry::StoreHandle,
        schema::{
            AcceptedCatalogSnapshotSelection, AcceptedSchemaSnapshot, MutationPublicationPreflight,
            PersistedIndexSnapshot, PersistedSchemaSnapshot, SchemaStore, SchemaTransitionDecision,
            SchemaTransitionPlanKind, StagedUserIndexDomainReplacement,
            compiled_schema_proposal_for_model,
            composite_catalog::{
                AcceptedCompositeCatalog, build_initial_accepted_composite_catalog,
                generated_composite_type_ids, reconcile_accepted_composite_catalog,
            },
            decide_schema_transition,
            enum_catalog::{
                AcceptedEnumCatalog, AcceptedSchemaRevision, AcceptedSchemaRevisionBundle,
                CandidateSchemaRevision, build_initial_accepted_enum_catalog_with_composite_ids,
                reconcile_accepted_enum_catalog_with_composite_ids,
            },
            runtime::AcceptedRowLayoutRuntimeContract,
            transition::{
                SchemaAdmissionIdentityComparison, SchemaTransitionPlan,
                SchemaTransitionRejectionKind, schema_admission_rejection,
            },
        },
    },
    error::InternalError,
    metrics::sink::{
        MetricsEvent, SchemaReconcileOutcome, SchemaTransitionOutcome, record,
        record_accepted_schema_footprint_for_path, record_schema_store_footprint_for_path,
    },
    model::entity::EntityModel,
    traits::CanisterKind,
    types::EntityTag,
};
use std::collections::{BTreeMap, BTreeSet};

#[cfg(test)]
use crate::db::schema::build_initial_accepted_catalogs_for_tests;

use user_index_domain::stage_startup_user_index_domain_replacement;

struct ReconciledRuntimeSchema {
    accepted: AcceptedSchemaSnapshot,
    pending_publication: Option<StagedUserIndexDomainReplacement>,
}

struct GeneratedCatalogCandidates {
    enum_catalog: AcceptedEnumCatalog,
    composite_catalog: AcceptedCompositeCatalog,
}

#[cfg(feature = "sql")]
pub(in crate::db) use sql_ddl::{
    execute_admin_sql_ddl_expression_index_addition, execute_admin_sql_ddl_field_addition,
    execute_admin_sql_ddl_field_default_change, execute_admin_sql_ddl_field_drop,
    execute_admin_sql_ddl_field_nullability_change,
    execute_admin_sql_ddl_field_path_index_addition, execute_admin_sql_ddl_field_rename,
    execute_admin_sql_ddl_secondary_index_drop,
};

/// Reconcile registered runtime schemas with the schema metadata store.
///
/// Initial contact publishes deterministic store-local type catalogs.
/// Later contacts rebuild current-only dense catalog candidates and fail
/// closed if any surviving persisted type identity would move.
pub(in crate::db) fn reconcile_runtime_schemas<C: CanisterKind>(
    db: &Db<C>,
    entity_runtime_hooks: &[EntityRuntimeHooks<C>],
) -> Result<(), InternalError> {
    let catalogs_by_store = build_generated_catalog_candidates(db, entity_runtime_hooks)?;
    let mut accepted_snapshots_by_store =
        BTreeMap::<&'static str, BTreeMap<EntityTag, PersistedSchemaSnapshot>>::new();
    let mut pending_publications =
        BTreeMap::<&'static str, Vec<StagedUserIndexDomainReplacement>>::new();

    for hooks in entity_runtime_hooks {
        let catalogs = catalogs_by_store
            .get(hooks.store_path)
            .ok_or_else(InternalError::store_invariant)?;
        let reconciled = reconcile_runtime_schema(
            db,
            hooks,
            &catalogs.enum_catalog,
            &catalogs.composite_catalog,
        )?;
        if accepted_snapshots_by_store
            .entry(hooks.store_path)
            .or_default()
            .insert(
                hooks.entity_tag,
                reconciled.accepted.persisted_snapshot().clone(),
            )
            .is_some()
        {
            if let Some(pending) = reconciled.pending_publication {
                pending_publications
                    .entry(hooks.store_path)
                    .or_default()
                    .push(pending);
            }
            return Err(InternalError::store_invariant());
        }
        if let Some(pending) = reconciled.pending_publication {
            pending_publications
                .entry(hooks.store_path)
                .or_default()
                .push(pending);
        }
    }

    for (store_path, catalogs) in catalogs_by_store {
        let entity_snapshots = accepted_snapshots_by_store
            .remove(store_path)
            .ok_or_else(InternalError::store_invariant)?;
        let store_pending = pending_publications.remove(store_path).unwrap_or_default();
        publish_generated_accepted_schema_bundle(
            db.store_handle(store_path)?,
            store_path,
            catalogs.enum_catalog,
            catalogs.composite_catalog,
            entity_snapshots,
            store_pending,
        )?;
    }
    if !accepted_snapshots_by_store.is_empty() {
        return Err(InternalError::store_invariant());
    }

    Ok(())
}

// Return whether a failed accepted-schema publication is proven to have
// rejected before any durable commit authority or accepted-after root exists.
// Physical rollback is unsafe while a marker remains: recovery owns that
// candidate and may still publish it. Inspection failures are likewise
// commit-in-doubt and conservatively retain accepted-after physical work.
#[cfg(any(test, feature = "sql"))]
fn schema_publication_error_allows_physical_rollback(
    store: StoreHandle,
    entity_tag: EntityTag,
    accepted_before: &PersistedSchemaSnapshot,
) -> bool {
    if store.storage_capabilities().recovery()
        == crate::db::registry::StoreRecoveryCapability::StableBasePlusJournalReplay
    {
        match crate::db::commit::commit_marker_present() {
            Ok(false) => {}
            Ok(true) | Err(_) => return false,
        }
    }

    matches!(
        store.with_schema(|schema_store| {
            schema_store.current_accepted_persisted_snapshot(entity_tag)
        }),
        Ok(Some(current)) if current == *accepted_before
    )
}

// Construct every store-local type-catalog candidate before any entity
// snapshot is published into the immutable accepted bundle. Every candidate
// is dense in current canonical order; existing stores additionally prove that
// surviving row-visible identities remain unchanged.
fn build_generated_catalog_candidates<C: CanisterKind>(
    db: &Db<C>,
    entity_runtime_hooks: &[EntityRuntimeHooks<C>],
) -> Result<BTreeMap<&'static str, GeneratedCatalogCandidates>, InternalError> {
    let mut models_by_store = BTreeMap::<&'static str, Vec<&EntityModel>>::new();
    for hooks in entity_runtime_hooks {
        models_by_store
            .entry(hooks.store_path)
            .or_default()
            .push(hooks.model);
    }

    let mut catalogs_by_store = BTreeMap::new();
    for (store_path, models) in models_by_store {
        let current = db
            .store_handle(store_path)?
            .with_schema(SchemaStore::current_accepted_schema_bundle)?;
        let composite_ids = generated_composite_type_ids(&models)
            .map_err(|_error| InternalError::store_unsupported())?;
        let enum_catalog = match current.as_ref() {
            Some(current) => {
                if current.store_path() != store_path {
                    return Err(InternalError::store_corruption());
                }
                reconcile_accepted_enum_catalog_with_composite_ids(
                    current.enum_catalog(),
                    &models,
                    &composite_ids,
                )
            }
            None => build_initial_accepted_enum_catalog_with_composite_ids(&models, &composite_ids),
        }
        .map_err(|_error| InternalError::store_unsupported())?;
        let composite_catalog = match current.as_ref() {
            Some(current) => reconcile_accepted_composite_catalog(
                current.composite_catalog(),
                &models,
                &enum_catalog,
            ),
            None => build_initial_accepted_composite_catalog(&models, &enum_catalog),
        }
        .map_err(|_error| InternalError::store_unsupported())?;
        catalogs_by_store.insert(
            store_path,
            GeneratedCatalogCandidates {
                enum_catalog,
                composite_catalog,
            },
        );
    }

    Ok(catalogs_by_store)
}

fn publish_generated_accepted_schema_bundle(
    store: StoreHandle,
    store_path: &'static str,
    enum_catalog: AcceptedEnumCatalog,
    composite_catalog: AcceptedCompositeCatalog,
    entity_snapshots: BTreeMap<EntityTag, PersistedSchemaSnapshot>,
    replacements: Vec<StagedUserIndexDomainReplacement>,
) -> Result<(), InternalError> {
    let current = store.with_schema(SchemaStore::current_accepted_schema_bundle)?;
    if current
        .as_ref()
        .is_some_and(|accepted| accepted.store_path() != store_path)
    {
        return Err(InternalError::store_corruption());
    }
    let expected_revision = current.as_ref().map_or(
        AcceptedSchemaRevision::NONE,
        AcceptedSchemaRevisionBundle::revision,
    );
    let comparison_revision = current.as_ref().map_or(
        AcceptedSchemaRevision::INITIAL,
        AcceptedSchemaRevisionBundle::revision,
    );
    let comparison = AcceptedSchemaRevisionBundle::new(
        comparison_revision,
        store_path,
        enum_catalog.clone(),
        composite_catalog.clone(),
        entity_snapshots.clone(),
    )?;
    if current.as_ref() == Some(&comparison) {
        return if replacements.is_empty() {
            Ok(())
        } else {
            Err(InternalError::store_invariant())
        };
    }
    let candidate_revision = expected_revision
        .checked_next()
        .ok_or_else(InternalError::store_unsupported)?;
    let bundle = AcceptedSchemaRevisionBundle::new(
        candidate_revision,
        store_path,
        enum_catalog,
        composite_catalog,
        entity_snapshots,
    )?;
    let candidate = CandidateSchemaRevision::new(bundle)?;
    if current.is_none() {
        if !replacements.is_empty() {
            return Err(InternalError::store_invariant());
        }
        return store.with_schema_mut(|schema_store| {
            schema_store.publish_accepted_schema_candidate(expected_revision, &candidate)
        });
    }
    if replacements.is_empty() {
        crate::db::commit::publish_accepted_schema_candidate(
            store_path,
            store,
            expected_revision,
            &candidate,
        )
    } else {
        crate::db::commit::publish_accepted_schema_candidate_with_user_index_domains(
            store_path,
            store,
            expected_revision,
            &candidate,
            replacements,
        )
    }
}

#[cfg(test)]
pub(in crate::db) fn bootstrap_test_accepted_schema_snapshot(
    schema_store: &mut SchemaStore,
    entity_tag: EntityTag,
    entity_path: &'static str,
    store_path: &'static str,
    model: &'static EntityModel,
) -> Result<(), InternalError> {
    if schema_store
        .current_accepted_persisted_snapshot(entity_tag)?
        .is_some()
    {
        return Ok(());
    }

    let (catalog, composite_catalog) = build_initial_accepted_catalogs_for_tests(&[model])
        .map_err(|()| InternalError::store_unsupported())?;
    let snapshot = compiled_schema_proposal_for_model(model)
        .initial_persisted_schema_snapshot_with_catalogs(&catalog, &composite_catalog)?;
    publish_test_accepted_schema_snapshot(
        schema_store,
        entity_tag,
        entity_path,
        store_path,
        model,
        snapshot,
    )
}

#[cfg(test)]
pub(in crate::db) fn publish_test_accepted_schema_snapshot(
    schema_store: &mut SchemaStore,
    entity_tag: EntityTag,
    entity_path: &'static str,
    store_path: &'static str,
    model: &'static EntityModel,
    snapshot: PersistedSchemaSnapshot,
) -> Result<(), InternalError> {
    let (proposed_catalog, proposed_composite_catalog) =
        build_initial_accepted_catalogs_for_tests(&[model])
            .map_err(|_error| InternalError::store_unsupported())?;
    let current = schema_store.current_accepted_schema_bundle()?;
    let expected_revision = current.as_ref().map_or(
        AcceptedSchemaRevision::NONE,
        AcceptedSchemaRevisionBundle::revision,
    );
    let (catalog, composite_catalog) = if let Some(current) = &current {
        if current.store_path() != store_path {
            return Err(InternalError::store_corruption());
        }
        (
            current.enum_catalog().clone(),
            current.composite_catalog().clone(),
        )
    } else {
        (proposed_catalog, proposed_composite_catalog)
    };
    let mut entity_snapshots = current
        .as_ref()
        .map_or_else(BTreeMap::new, |current| current.entity_snapshots().clone());
    if snapshot.entity_path() != entity_path {
        return Err(InternalError::store_invariant());
    }
    entity_snapshots.insert(entity_tag, snapshot);
    let revision = expected_revision
        .checked_next()
        .ok_or_else(InternalError::store_unsupported)?;
    let bundle = AcceptedSchemaRevisionBundle::new(
        revision,
        store_path,
        catalog,
        composite_catalog,
        entity_snapshots,
    )?;
    let candidate = CandidateSchemaRevision::new(bundle)?;
    schema_store.publish_accepted_schema_candidate(expected_revision, &candidate)
}

#[cfg(feature = "sql")]
fn publish_accepted_entity_snapshot_revision(
    store: StoreHandle,
    expected_identity: crate::db::schema::AcceptedCatalogIdentity,
    accepted_after: &PersistedSchemaSnapshot,
) -> Result<(), InternalError> {
    publish_accepted_entity_snapshot_revision_with_row_puts(
        store,
        expected_identity,
        accepted_after,
        Vec::new(),
    )
}

#[cfg(feature = "sql")]
fn publish_accepted_entity_snapshot_revision_with_row_puts(
    store: StoreHandle,
    expected_identity: crate::db::schema::AcceptedCatalogIdentity,
    accepted_after: &PersistedSchemaSnapshot,
    row_puts: Vec<crate::db::journal::JournalRecord>,
) -> Result<(), InternalError> {
    let Some((expected_revision, candidate)) =
        prepare_accepted_entity_snapshot_revision(store, expected_identity, accepted_after)?
    else {
        return Ok(());
    };
    crate::db::commit::publish_accepted_schema_candidate_with_row_puts(
        expected_identity.store_path(),
        store,
        expected_revision,
        &candidate,
        row_puts,
    )
}

#[cfg(feature = "sql")]
fn publish_accepted_entity_snapshot_revision_with_user_index_domain(
    store: StoreHandle,
    expected_identity: crate::db::schema::AcceptedCatalogIdentity,
    accepted_after: &PersistedSchemaSnapshot,
    replacement: crate::db::schema::StagedUserIndexDomainReplacement,
) -> Result<(), InternalError> {
    let Some((expected_revision, candidate)) =
        prepare_accepted_entity_snapshot_revision(store, expected_identity, accepted_after)?
    else {
        return Err(InternalError::store_invariant());
    };
    crate::db::commit::publish_accepted_schema_candidate_with_user_index_domains(
        expected_identity.store_path(),
        store,
        expected_revision,
        &candidate,
        vec![replacement],
    )
}

#[cfg(feature = "sql")]
fn prepare_accepted_entity_snapshot_revision(
    store: StoreHandle,
    expected_identity: crate::db::schema::AcceptedCatalogIdentity,
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
    if current_selection.identity() != expected_identity {
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
    let previous = entity_snapshots.insert(expected_identity.entity_tag(), accepted_after.clone());
    if previous.is_none() {
        return Err(InternalError::store_corruption());
    }
    if entity_snapshots == *current.entity_snapshots() {
        return Ok(None);
    }

    let candidate_revision = expected_revision
        .checked_next()
        .ok_or_else(InternalError::store_unsupported)?;
    let bundle = AcceptedSchemaRevisionBundle::new(
        candidate_revision,
        expected_identity.store_path(),
        current.enum_catalog().clone(),
        current.composite_catalog().clone(),
        entity_snapshots,
    )?;
    let candidate = CandidateSchemaRevision::new(bundle)?;
    Ok(Some((expected_revision, candidate)))
}

fn merge_generated_indexes_with_extra_accepted_indexes(
    accepted: &PersistedSchemaSnapshot,
    generated: &PersistedSchemaSnapshot,
) -> PersistedSchemaSnapshot {
    let generated_ordinals = generated
        .indexes()
        .iter()
        .map(PersistedIndexSnapshot::ordinal)
        .collect::<BTreeSet<_>>();
    let mut indexes = generated.indexes().to_vec();
    indexes.extend(
        accepted
            .indexes()
            .iter()
            .filter(|index| !generated_ordinals.contains(&index.ordinal()))
            .cloned(),
    );

    PersistedSchemaSnapshot::new_with_primary_key_fields_and_indexes(
        generated.version(),
        generated.entity_path().to_string(),
        generated.entity_name().to_string(),
        generated.primary_key_field_ids().to_vec(),
        generated.row_layout().clone(),
        generated.fields().to_vec(),
        indexes,
    )
    .with_relations(generated.relations().to_vec())
}

// Reconcile one entity hook against its owning schema store. The generated
// proposal is compiled here so schema, not commit recovery, owns the comparison
// between generated metadata and persisted schema metadata.
fn reconcile_runtime_schema<C: CanisterKind>(
    db: &Db<C>,
    hooks: &EntityRuntimeHooks<C>,
    enum_catalog: &AcceptedEnumCatalog,
    composite_catalog: &AcceptedCompositeCatalog,
) -> Result<ReconciledRuntimeSchema, InternalError> {
    let store = db.store_handle(hooks.store_path)?;

    ensure_accepted_schema_snapshot_for_runtime_store(
        store,
        hooks.entity_tag,
        hooks.entity_path,
        hooks.store_path,
        hooks.model,
        enum_catalog,
        composite_catalog,
    )
}

/// Load one runtime snapshot from the immutable bundle selected by the current
/// accepted root.
///
/// Recovery and startup reconciliation must publish that root before live
/// execution reaches this boundary. Generated metadata proves compatibility;
/// it is never used to reconstruct missing accepted state here.
#[cfg(not(test))]
pub(in crate::db) fn ensure_accepted_schema_snapshot(
    schema_store: &SchemaStore,
    entity_tag: EntityTag,
    entity_path: &'static str,
    _store_path: &'static str,
    model: &'static EntityModel,
) -> Result<AcceptedSchemaSnapshot, InternalError> {
    load_current_accepted_schema_snapshot(schema_store, entity_tag, entity_path, model)
}

/// Select one entity snapshot and enum catalog from the current accepted root.
#[cfg(not(test))]
pub(in crate::db) fn ensure_accepted_catalog_snapshot_selection(
    schema_store: &SchemaStore,
    entity_tag: EntityTag,
    entity_path: &'static str,
    store_path: &'static str,
    _model: &'static EntityModel,
) -> Result<AcceptedCatalogSnapshotSelection, InternalError> {
    load_current_accepted_catalog_snapshot_selection(
        schema_store,
        entity_tag,
        entity_path,
        store_path,
    )
}

#[cfg(test)]
pub(in crate::db) fn ensure_accepted_schema_snapshot(
    schema_store: &mut SchemaStore,
    entity_tag: EntityTag,
    entity_path: &'static str,
    store_path: &'static str,
    model: &'static EntityModel,
) -> Result<AcceptedSchemaSnapshot, InternalError> {
    bootstrap_test_accepted_schema_snapshot(
        schema_store,
        entity_tag,
        entity_path,
        store_path,
        model,
    )?;

    load_current_accepted_schema_snapshot(schema_store, entity_tag, entity_path, model)
}

#[cfg(test)]
pub(in crate::db) fn ensure_accepted_catalog_snapshot_selection(
    schema_store: &mut SchemaStore,
    entity_tag: EntityTag,
    entity_path: &'static str,
    store_path: &'static str,
    model: &'static EntityModel,
) -> Result<AcceptedCatalogSnapshotSelection, InternalError> {
    bootstrap_test_accepted_schema_snapshot(
        schema_store,
        entity_tag,
        entity_path,
        store_path,
        model,
    )?;

    load_current_accepted_catalog_snapshot_selection(
        schema_store,
        entity_tag,
        entity_path,
        store_path,
    )
}

fn load_current_accepted_catalog_snapshot_selection(
    schema_store: &SchemaStore,
    entity_tag: EntityTag,
    entity_path: &'static str,
    store_path: &'static str,
) -> Result<AcceptedCatalogSnapshotSelection, InternalError> {
    schema_store
        .current_accepted_catalog_selection(entity_tag, entity_path, store_path)?
        .ok_or_else(InternalError::store_corruption)
}

fn load_current_accepted_schema_snapshot(
    schema_store: &SchemaStore,
    entity_tag: EntityTag,
    entity_path: &'static str,
    model: &'static EntityModel,
) -> Result<AcceptedSchemaSnapshot, InternalError> {
    let bundle = schema_store
        .current_accepted_schema_bundle()?
        .ok_or_else(InternalError::store_corruption)?;
    let snapshot = bundle
        .entity_snapshots()
        .get(&entity_tag)
        .cloned()
        .ok_or_else(InternalError::store_corruption)?;
    if snapshot.entity_path() != entity_path {
        return Err(InternalError::store_corruption());
    }
    let accepted = AcceptedSchemaSnapshot::try_new(snapshot)?;
    let _runtime_contract = AcceptedRowLayoutRuntimeContract::from_generated_compatible_schema(
        &accepted,
        model,
        bundle.enum_catalog(),
        bundle.composite_catalog(),
    )
    .map_err(|_error| InternalError::store_unsupported())?;
    validate_accepted_runtime_descriptor(&accepted)?;

    Ok(accepted)
}

// Build or update the staged entity snapshot used to construct the next
// immutable accepted bundle. This is deliberately separate from runtime reads.
#[cfg(test)]
fn reconcile_staged_schema_snapshot(
    schema_store: &mut SchemaStore,
    entity_tag: EntityTag,
    entity_path: &'static str,
    model: &EntityModel,
) -> Result<AcceptedSchemaSnapshot, InternalError> {
    let proposal = compiled_schema_proposal_for_model(model);
    let (catalog, composite_catalog) = build_initial_accepted_catalogs_for_tests(&[model])
        .map_err(|()| InternalError::store_unsupported())?;
    let expected =
        proposal.initial_persisted_schema_snapshot_with_catalogs(&catalog, &composite_catalog)?;

    let latest = match schema_store.latest_staged_persisted_snapshot(entity_tag) {
        Ok(latest) => latest,
        Err(error) => {
            record_schema_store_footprint(schema_store, entity_tag, entity_path);
            record_schema_reconcile(entity_path, SchemaReconcileOutcome::LatestSnapshotCorrupt);
            return Err(error);
        }
    };

    if let Some(actual) = latest {
        let plan = match validate_existing_schema_snapshot(entity_path, &actual, &expected) {
            Ok(plan) => plan,
            Err(error) => {
                record_schema_store_footprint(schema_store, entity_tag, entity_path);
                return Err(error);
            }
        };
        if let Err(error) = validate_publishable_transition_plan(entity_path, &plan) {
            record_schema_store_footprint(schema_store, entity_tag, entity_path);
            record_schema_reconcile(entity_path, SchemaReconcileOutcome::RejectedOther);
            return Err(error);
        }
        let accepted_snapshot = match plan.kind() {
            SchemaTransitionPlanKind::AddExpressionIndex
            | SchemaTransitionPlanKind::AddFieldPathIndex
            | SchemaTransitionPlanKind::ExactMatch => actual,
            SchemaTransitionPlanKind::AppendOnlyNullableFields => {
                if let Err(error) = schema_store.insert_persisted_snapshot(entity_tag, &expected) {
                    record_schema_store_footprint(schema_store, entity_tag, entity_path);
                    record_schema_reconcile(entity_path, SchemaReconcileOutcome::StoreWriteError);
                    return Err(error);
                }
                expected
            }
            SchemaTransitionPlanKind::MetadataOnlyIndexRename => {
                let merged =
                    merge_generated_indexes_with_extra_accepted_indexes(&actual, &expected);
                if let Err(error) = schema_store.insert_persisted_snapshot(entity_tag, &merged) {
                    record_schema_store_footprint(schema_store, entity_tag, entity_path);
                    record_schema_reconcile(entity_path, SchemaReconcileOutcome::StoreWriteError);
                    return Err(error);
                }
                merged
            }
        };
        return accept_reconciled_schema_snapshot(
            entity_path,
            accepted_snapshot,
            SchemaReconcileOutcome::ExactMatch,
            || record_schema_store_footprint(schema_store, entity_tag, entity_path),
        );
    }

    if let Err(error) = schema_store.insert_persisted_snapshot(entity_tag, &expected) {
        record_schema_store_footprint(schema_store, entity_tag, entity_path);
        record_schema_reconcile(entity_path, SchemaReconcileOutcome::StoreWriteError);
        return Err(error);
    }

    accept_reconciled_schema_snapshot(
        entity_path,
        expected,
        SchemaReconcileOutcome::FirstCreate,
        || record_schema_store_footprint(schema_store, entity_tag, entity_path),
    )
}

// Startup reconciliation owns the store-wide candidate and zero-write staging
// required before accepted-schema publication.
fn ensure_accepted_schema_snapshot_for_runtime_store(
    store: StoreHandle,
    entity_tag: EntityTag,
    entity_path: &'static str,
    store_path: &'static str,
    model: &EntityModel,
    enum_catalog: &AcceptedEnumCatalog,
    composite_catalog: &AcceptedCompositeCatalog,
) -> Result<ReconciledRuntimeSchema, InternalError> {
    let proposal = compiled_schema_proposal_for_model(model);
    let expected = proposal
        .initial_persisted_schema_snapshot_with_catalogs(enum_catalog, composite_catalog)?;

    let latest = match store
        .with_schema_mut(|schema_store| schema_store.latest_staged_persisted_snapshot(entity_tag))
    {
        Ok(latest) => latest,
        Err(error) => {
            store.with_schema(|schema_store| {
                record_schema_store_footprint(schema_store, entity_tag, entity_path);
            });
            record_schema_reconcile(entity_path, SchemaReconcileOutcome::LatestSnapshotCorrupt);
            return Err(error);
        }
    };

    if let Some(actual) = latest {
        let plan = match validate_existing_schema_snapshot(entity_path, &actual, &expected) {
            Ok(plan) => plan,
            Err(error) => {
                store.with_schema(|schema_store| {
                    record_schema_store_footprint(schema_store, entity_tag, entity_path);
                });
                return Err(error);
            }
        };

        let (accepted_snapshot, pending_publication) = match plan.kind() {
            SchemaTransitionPlanKind::AddExpressionIndex | SchemaTransitionPlanKind::ExactMatch => {
                validate_publishable_transition_plan(entity_path, &plan)?;
                (actual, None)
            }
            SchemaTransitionPlanKind::AppendOnlyNullableFields => {
                validate_publishable_transition_plan(entity_path, &plan)?;
                store.with_schema_mut(|schema_store| {
                    schema_store.insert_persisted_snapshot(entity_tag, &expected)
                })?;
                (expected, None)
            }
            SchemaTransitionPlanKind::MetadataOnlyIndexRename => {
                validate_publishable_transition_plan(entity_path, &plan)?;
                let merged =
                    merge_generated_indexes_with_extra_accepted_indexes(&actual, &expected);
                store.with_schema_mut(|schema_store| {
                    schema_store.insert_persisted_snapshot(entity_tag, &merged)
                })?;
                (merged, None)
            }
            SchemaTransitionPlanKind::AddFieldPathIndex => {
                validate_startup_field_path_target(&plan, &expected)?;
                let replacement = stage_startup_user_index_domain_replacement(
                    store,
                    entity_tag,
                    store_path,
                    entity_path,
                    &actual,
                    &expected,
                )?;
                (expected, Some(replacement))
            }
        };

        let accepted = accept_reconciled_schema_snapshot(
            entity_path,
            accepted_snapshot,
            SchemaReconcileOutcome::ExactMatch,
            || {
                store.with_schema(|schema_store| {
                    record_schema_store_footprint(schema_store, entity_tag, entity_path);
                });
            },
        )?;
        return Ok(ReconciledRuntimeSchema {
            accepted,
            pending_publication,
        });
    }

    store.with_schema_mut(|schema_store| {
        schema_store.insert_persisted_snapshot(entity_tag, &expected)
    })?;

    let accepted = accept_reconciled_schema_snapshot(
        entity_path,
        expected,
        SchemaReconcileOutcome::FirstCreate,
        || {
            store.with_schema(|schema_store| {
                record_schema_store_footprint(schema_store, entity_tag, entity_path);
            });
        },
    )?;
    Ok(ReconciledRuntimeSchema {
        accepted,
        pending_publication: None,
    })
}

fn validate_startup_field_path_target(
    plan: &SchemaTransitionPlan,
    expected: &PersistedSchemaSnapshot,
) -> Result<(), InternalError> {
    let target = plan
        .field_path_index_target()
        .ok_or_else(InternalError::store_unsupported)?;
    if expected
        .indexes()
        .iter()
        .all(|index| index.ordinal() != target.ordinal())
    {
        return Err(InternalError::store_unsupported());
    }

    Ok(())
}

fn accept_reconciled_schema_snapshot(
    entity_path: &'static str,
    snapshot: PersistedSchemaSnapshot,
    outcome: SchemaReconcileOutcome,
    record_store_footprint: impl FnOnce(),
) -> Result<AcceptedSchemaSnapshot, InternalError> {
    record_schema_reconcile(entity_path, outcome);
    record_store_footprint();
    let accepted = AcceptedSchemaSnapshot::try_new(snapshot)?;
    validate_accepted_runtime_descriptor(&accepted)?;
    record_accepted_schema_footprint(entity_path, &accepted);

    Ok(accepted)
}

// Validate that every accepted snapshot can be projected into the schema-owned
// runtime layout descriptor before callers use it as live schema authority.
fn validate_accepted_runtime_descriptor(
    accepted: &AcceptedSchemaSnapshot,
) -> Result<(), InternalError> {
    let _descriptor = AcceptedRowLayoutRuntimeContract::from_accepted_schema(accepted)?;

    Ok(())
}

// Keep runtime visibility fail-closed until the matching physical mutation
// path has completed and published its accepted snapshot.
fn validate_publishable_transition_plan(
    _entity_path: &'static str,
    plan: &SchemaTransitionPlan,
) -> Result<(), InternalError> {
    match plan.publication_preflight() {
        MutationPublicationPreflight::PublishableNow => Ok(()),
        MutationPublicationPreflight::RequiresPhysicalWork => {
            Err(InternalError::store_unsupported())
        }
    }
}

// Keep schema reconciliation instrumentation at the reconciliation boundary so
// store/codec helpers remain persistence-focused and do not depend on metrics.
fn record_schema_reconcile(entity_path: &'static str, outcome: SchemaReconcileOutcome) {
    record(MetricsEvent::SchemaReconcile {
        entity_path,
        outcome,
    });
}

// Record transition-policy decisions separately from broader reconciliation
// outcomes such as first-create writes, corrupt stores, or store failures.
fn record_schema_transition(entity_path: &'static str, outcome: SchemaTransitionOutcome) {
    record(MetricsEvent::SchemaTransition {
        entity_path,
        outcome,
    });
}

// Record raw schema-store footprint from the store boundary without decoding
// snapshot payloads or exposing schema metadata details through metrics.
fn record_schema_store_footprint(
    schema_store: &SchemaStore,
    entity_tag: EntityTag,
    entity_path: &'static str,
) {
    let footprint = schema_store.entity_footprint(entity_tag);
    record_schema_store_footprint_for_path(
        entity_path,
        footprint.snapshots(),
        footprint.encoded_bytes(),
        footprint.latest_snapshot_bytes(),
    );
}

// Record accepted live-schema field-fact footprint only after an accepted
// snapshot has passed the accepted-schema integrity boundary.
fn record_accepted_schema_footprint(entity_path: &'static str, accepted: &AcceptedSchemaSnapshot) {
    let footprint = accepted.footprint();
    record_accepted_schema_footprint_for_path(
        entity_path,
        footprint.fields(),
        footprint.nested_leaf_facts(),
    );
}

// Map schema-owned transition rejection classes into public metrics buckets.
// Detailed diagnostics stay on the rejection itself; metrics only carry stable
// low-cardinality categories.
const fn schema_reconcile_rejection_outcome(
    kind: SchemaTransitionRejectionKind,
) -> SchemaReconcileOutcome {
    match kind {
        SchemaTransitionRejectionKind::SchemaVersion => {
            SchemaReconcileOutcome::RejectedSchemaVersion
        }
        SchemaTransitionRejectionKind::RowLayout => SchemaReconcileOutcome::RejectedRowLayout,
        SchemaTransitionRejectionKind::FieldSlot => SchemaReconcileOutcome::RejectedFieldSlot,
        SchemaTransitionRejectionKind::EntityIdentity
        | SchemaTransitionRejectionKind::FieldContract
        | SchemaTransitionRejectionKind::Snapshot => SchemaReconcileOutcome::RejectedOther,
    }
}

// Map the schema-owned transition decision directly into its public metrics
// bucket so diagnostics do not collapse current mutation routes into an
// apparent exact match.
const fn schema_transition_plan_outcome(kind: SchemaTransitionPlanKind) -> SchemaTransitionOutcome {
    match kind {
        SchemaTransitionPlanKind::AddExpressionIndex => SchemaTransitionOutcome::AddExpressionIndex,
        SchemaTransitionPlanKind::AddFieldPathIndex => SchemaTransitionOutcome::AddFieldPathIndex,
        SchemaTransitionPlanKind::AppendOnlyNullableFields => {
            SchemaTransitionOutcome::AppendOnlyNullableFields
        }
        SchemaTransitionPlanKind::ExactMatch => SchemaTransitionOutcome::ExactMatch,
        SchemaTransitionPlanKind::MetadataOnlyIndexRename => {
            SchemaTransitionOutcome::MetadataOnlyIndexRename
        }
    }
}

// Map schema-owned rejection classes into the narrower transition metrics
// family. Unlike reconciliation metrics, this preserves the distinction
// between entity identity, field contract, and snapshot fallback rejections.
const fn schema_transition_rejection_outcome(
    kind: SchemaTransitionRejectionKind,
) -> SchemaTransitionOutcome {
    match kind {
        SchemaTransitionRejectionKind::EntityIdentity => {
            SchemaTransitionOutcome::RejectedEntityIdentity
        }
        SchemaTransitionRejectionKind::FieldContract => {
            SchemaTransitionOutcome::RejectedFieldContract
        }
        SchemaTransitionRejectionKind::FieldSlot => SchemaTransitionOutcome::RejectedFieldSlot,
        SchemaTransitionRejectionKind::RowLayout => SchemaTransitionOutcome::RejectedRowLayout,
        SchemaTransitionRejectionKind::SchemaVersion => {
            SchemaTransitionOutcome::RejectedSchemaVersion
        }
        SchemaTransitionRejectionKind::Snapshot => SchemaTransitionOutcome::RejectedSnapshot,
    }
}

// Decide the current schema transition against accepted persisted authority.
// Identity admission runs before non-trivial compatible plans are returned;
// every rejected shape fails closed with a typed internal cause.
fn validate_existing_schema_snapshot(
    entity_path: &'static str,
    actual: &PersistedSchemaSnapshot,
    expected: &PersistedSchemaSnapshot,
) -> Result<SchemaTransitionPlan, InternalError> {
    let transition_decision = match decide_schema_transition(actual, expected) {
        SchemaTransitionDecision::Accepted(plan)
            if matches!(
                plan.kind(),
                SchemaTransitionPlanKind::ExactMatch
                    | SchemaTransitionPlanKind::MetadataOnlyIndexRename
            ) =>
        {
            record_schema_transition(entity_path, schema_transition_plan_outcome(plan.kind()));

            return Ok(plan);
        }
        decision => decision,
    };

    if let SchemaTransitionDecision::Rejected(rejection) = &transition_decision
        && rejection.kind() == SchemaTransitionRejectionKind::EntityIdentity
    {
        let outcome = schema_reconcile_rejection_outcome(rejection.kind());
        let transition_outcome = schema_transition_rejection_outcome(rejection.kind());
        record_schema_transition(entity_path, transition_outcome);
        record_schema_reconcile(entity_path, outcome);

        return Err(InternalError::store_unsupported());
    }

    // Gate source-declared version/method/fingerprint identity before
    // compatibility classification. Passing this gate is not publication.
    let admission_identity = SchemaAdmissionIdentityComparison::from_snapshots(actual, expected)?;
    if let Some(rejection) = schema_admission_rejection(admission_identity) {
        let outcome = schema_reconcile_rejection_outcome(rejection.kind());
        let transition_outcome = schema_transition_rejection_outcome(rejection.kind());
        record_schema_transition(entity_path, transition_outcome);
        record_schema_reconcile(entity_path, outcome);

        return Err(InternalError::store_unsupported());
    }

    match transition_decision {
        SchemaTransitionDecision::Accepted(plan) => {
            record_schema_transition(entity_path, schema_transition_plan_outcome(plan.kind()));

            Ok(plan)
        }
        SchemaTransitionDecision::Rejected(rejection) => {
            let outcome = schema_reconcile_rejection_outcome(rejection.kind());
            let transition_outcome = schema_transition_rejection_outcome(rejection.kind());
            record_schema_transition(entity_path, transition_outcome);
            record_schema_reconcile(entity_path, outcome);

            Err(InternalError::store_unsupported())
        }
    }
}

///
/// TESTS
///

#[cfg(all(test, feature = "sql"))]
mod tests;
