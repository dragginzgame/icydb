//! Module: db::schema::reconcile
//! Responsibility: startup schema snapshot reconciliation.
//! Does not own: row/index recovery, generated model construction, or runtime layout authority.
//! Boundary: compares generated schema proposals with persisted schema snapshots.

#[cfg(feature = "sql")]
mod sql_ddl;
#[cfg(feature = "sql")]
mod startup_expression;
mod startup_field_path;

use crate::{
    db::{
        Db, EntityRuntimeHooks,
        registry::StoreHandle,
        schema::{
            AcceptedCatalogSnapshotSelection, AcceptedSchemaSnapshot, MutationPublicationBlocker,
            MutationPublicationPreflight, PersistedIndexSnapshot, PersistedSchemaSnapshot,
            SchemaMutationRunnerCapability, SchemaMutationRunnerContract, SchemaStore,
            SchemaTransitionDecision, SchemaTransitionPlanKind, compiled_schema_proposal_for_model,
            decide_schema_transition,
            enum_catalog::{
                AcceptedEnumCatalog, AcceptedSchemaRevision, AcceptedSchemaRevisionBundle,
                CandidateSchemaRevision, build_initial_accepted_enum_catalog,
                reconcile_accepted_enum_catalog,
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

use startup_field_path::{SchemaPublicationGate, execute_supported_field_path_index_addition};

#[cfg(feature = "sql")]
pub(in crate::db) use sql_ddl::{
    execute_sql_ddl_expression_index_addition, execute_sql_ddl_field_addition,
    execute_sql_ddl_field_default_change, execute_sql_ddl_field_drop,
    execute_sql_ddl_field_nullability_change, execute_sql_ddl_field_path_index_addition,
    execute_sql_ddl_field_rename, execute_sql_ddl_secondary_index_drop,
};

/// Reconcile registered runtime schemas with the schema metadata store.
///
/// Initial contact publishes one deterministic store-local enum catalog.
/// Later contacts reconcile proposals against the current accepted catalog so
/// existing IDs remain stable and only append-only catalog additions can reach
/// candidate publication.
pub(in crate::db) fn reconcile_runtime_schemas<C: CanisterKind>(
    db: &Db<C>,
    entity_runtime_hooks: &[EntityRuntimeHooks<C>],
) -> Result<(), InternalError> {
    let catalogs_by_store = build_generated_enum_catalog_candidates(db, entity_runtime_hooks)?;
    let mut accepted_snapshots_by_store =
        BTreeMap::<&'static str, BTreeMap<EntityTag, PersistedSchemaSnapshot>>::new();

    for hooks in entity_runtime_hooks {
        let enum_catalog = catalogs_by_store
            .get(hooks.store_path)
            .ok_or_else(InternalError::store_invariant)?;
        let accepted = reconcile_runtime_schema(db, hooks, enum_catalog)?;
        if accepted_snapshots_by_store
            .entry(hooks.store_path)
            .or_default()
            .insert(hooks.entity_tag, accepted.persisted_snapshot().clone())
            .is_some()
        {
            return Err(InternalError::store_invariant());
        }
    }

    for (store_path, enum_catalog) in catalogs_by_store {
        let entity_snapshots = accepted_snapshots_by_store
            .remove(store_path)
            .ok_or_else(InternalError::store_invariant)?;
        publish_generated_accepted_schema_bundle(
            db.store_handle(store_path)?,
            store_path,
            enum_catalog,
            entity_snapshots,
        )?;
    }
    if !accepted_snapshots_by_store.is_empty() {
        return Err(InternalError::store_invariant());
    }

    Ok(())
}

// Construct every store-local enum catalog candidate before any entity
// snapshot is published into the immutable accepted bundle. Existing stores
// reconcile from accepted IDs; only virgin stores allocate from path order.
fn build_generated_enum_catalog_candidates<C: CanisterKind>(
    db: &Db<C>,
    entity_runtime_hooks: &[EntityRuntimeHooks<C>],
) -> Result<BTreeMap<&'static str, AcceptedEnumCatalog>, InternalError> {
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
        let catalog = match current {
            Some(current) => {
                if current.store_path() != store_path {
                    return Err(InternalError::store_corruption());
                }
                reconcile_accepted_enum_catalog(current.enum_catalog(), &models)
            }
            None => build_initial_accepted_enum_catalog(&models),
        }
        .map_err(|_error| InternalError::store_unsupported())?;
        catalogs_by_store.insert(store_path, catalog);
    }

    Ok(catalogs_by_store)
}

fn publish_generated_accepted_schema_bundle(
    store: StoreHandle,
    store_path: &'static str,
    enum_catalog: AcceptedEnumCatalog,
    entity_snapshots: BTreeMap<EntityTag, PersistedSchemaSnapshot>,
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
        entity_snapshots.clone(),
    )?;
    if current.as_ref() == Some(&comparison) {
        return Ok(());
    }
    let candidate_revision = expected_revision
        .checked_next()
        .ok_or_else(InternalError::store_unsupported)?;
    let bundle = AcceptedSchemaRevisionBundle::new(
        candidate_revision,
        store_path,
        enum_catalog,
        entity_snapshots,
    )?;
    let candidate = CandidateSchemaRevision::new(bundle)?;
    if current.is_none() {
        return store.with_schema_mut(|schema_store| {
            schema_store.publish_accepted_schema_candidate(expected_revision, &candidate)
        });
    }
    crate::db::commit::publish_accepted_schema_candidate(
        store_path,
        store,
        expected_revision,
        &candidate,
    )
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

    let catalog = build_initial_accepted_enum_catalog(&[model])
        .map_err(|_| InternalError::store_unsupported())?;
    let snapshot = compiled_schema_proposal_for_model(model)
        .initial_persisted_schema_snapshot_with_enum_catalog(&catalog)?;
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
    let proposed_catalog = build_initial_accepted_enum_catalog(&[model])
        .map_err(|_error| InternalError::store_unsupported())?;
    let current = schema_store.current_accepted_schema_bundle()?;
    let expected_revision = current.as_ref().map_or(
        AcceptedSchemaRevision::NONE,
        AcceptedSchemaRevisionBundle::revision,
    );
    let catalog = if let Some(current) = &current {
        if current.store_path() != store_path {
            return Err(InternalError::store_corruption());
        }
        current.enum_catalog().clone()
    } else {
        proposed_catalog
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
    let bundle =
        AcceptedSchemaRevisionBundle::new(revision, store_path, catalog, entity_snapshots)?;
    let candidate = CandidateSchemaRevision::new(bundle)?;
    schema_store.publish_accepted_schema_candidate(expected_revision, &candidate)
}

#[cfg(feature = "sql")]
fn publish_accepted_entity_snapshot_revision(
    store: StoreHandle,
    expected_identity: crate::db::schema::AcceptedCatalogIdentity,
    accepted_after: &PersistedSchemaSnapshot,
) -> Result<(), InternalError> {
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
        return Ok(());
    }

    let candidate_revision = expected_revision
        .checked_next()
        .ok_or_else(InternalError::store_unsupported)?;
    let bundle = AcceptedSchemaRevisionBundle::new(
        candidate_revision,
        expected_identity.store_path(),
        current.enum_catalog().clone(),
        entity_snapshots,
    )?;
    let candidate = CandidateSchemaRevision::new(bundle)?;
    crate::db::commit::publish_accepted_schema_candidate(
        expected_identity.store_path(),
        store,
        expected_revision,
        &candidate,
    )
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
) -> Result<AcceptedSchemaSnapshot, InternalError> {
    let store = db.store_handle(hooks.store_path)?;

    ensure_accepted_schema_snapshot_for_runtime_store(
        store,
        hooks.entity_tag,
        hooks.entity_path,
        hooks.store_path,
        hooks.model,
        enum_catalog,
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
    let snapshot = schema_store
        .current_accepted_persisted_snapshot(entity_tag)?
        .ok_or_else(InternalError::store_corruption)?;
    if snapshot.entity_path() != entity_path {
        return Err(InternalError::store_corruption());
    }
    let accepted = AcceptedSchemaSnapshot::try_new(snapshot)?;
    let _runtime_contract =
        AcceptedRowLayoutRuntimeContract::from_generated_compatible_schema(&accepted, model)
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
    let catalog = build_initial_accepted_enum_catalog(&[model])
        .map_err(|_| InternalError::store_unsupported())?;
    let expected = proposal.initial_persisted_schema_snapshot_with_enum_catalog(&catalog)?;

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

// Startup reconciliation owns the wider store handle, so it can execute the
// single supported physical schema mutation before publishing the accepted
// root.
fn ensure_accepted_schema_snapshot_for_runtime_store(
    store: StoreHandle,
    entity_tag: EntityTag,
    entity_path: &'static str,
    store_path: &'static str,
    model: &EntityModel,
    enum_catalog: &AcceptedEnumCatalog,
) -> Result<AcceptedSchemaSnapshot, InternalError> {
    let proposal = compiled_schema_proposal_for_model(model);
    let expected = proposal.initial_persisted_schema_snapshot_with_enum_catalog(enum_catalog)?;

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

        let accepted_snapshot = match plan.kind() {
            SchemaTransitionPlanKind::AddExpressionIndex | SchemaTransitionPlanKind::ExactMatch => {
                validate_publishable_transition_plan(entity_path, &plan)?;
                actual
            }
            SchemaTransitionPlanKind::AppendOnlyNullableFields => {
                validate_publishable_transition_plan(entity_path, &plan)?;
                store.with_schema_mut(|schema_store| {
                    schema_store.insert_persisted_snapshot(entity_tag, &expected)
                })?;
                expected
            }
            SchemaTransitionPlanKind::MetadataOnlyIndexRename => {
                validate_publishable_transition_plan(entity_path, &plan)?;
                let merged =
                    merge_generated_indexes_with_extra_accepted_indexes(&actual, &expected);
                store.with_schema_mut(|schema_store| {
                    schema_store.insert_persisted_snapshot(entity_tag, &merged)
                })?;
                merged
            }
            SchemaTransitionPlanKind::AddFieldPathIndex => {
                execute_supported_field_path_index_addition(
                    store,
                    SchemaPublicationGate::startup(entity_tag, store_path),
                    entity_path,
                    &actual,
                    &expected,
                    &plan,
                )?;
                expected
            }
        };

        return accept_reconciled_schema_snapshot(
            entity_path,
            accepted_snapshot,
            SchemaReconcileOutcome::ExactMatch,
            || {
                store.with_schema(|schema_store| {
                    record_schema_store_footprint(schema_store, entity_tag, entity_path);
                });
            },
        );
    }

    store.with_schema_mut(|schema_store| {
        schema_store.insert_persisted_snapshot(entity_tag, &expected)
    })?;

    accept_reconciled_schema_snapshot(
        entity_path,
        expected,
        SchemaReconcileOutcome::FirstCreate,
        || {
            store.with_schema(|schema_store| {
                record_schema_store_footprint(schema_store, entity_tag, entity_path);
            });
        },
    )
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

// Keep runtime visibility fail-closed until rebuild orchestration can make
// index/full-rewrite mutation plans physically true before publication.
fn validate_publishable_transition_plan(
    entity_path: &'static str,
    plan: &SchemaTransitionPlan,
) -> Result<(), InternalError> {
    let runner = SchemaMutationRunnerContract::new(&[]);

    match plan.publication_preflight(&runner) {
        MutationPublicationPreflight::PublishableNow => Ok(()),
        MutationPublicationPreflight::PhysicalWorkReady {
            step_count,
            required,
        } => Err(supported_physical_work_unavailable_error(
            entity_path,
            plan,
            step_count,
            required.as_slice(),
        )),
        MutationPublicationPreflight::MissingRunnerCapabilities { missing } => Err(
            missing_physical_runner_error(entity_path, plan, missing.as_slice()),
        ),
        MutationPublicationPreflight::Rejected { requirement } => {
            let _ = &requirement;

            Err(InternalError::store_unsupported())
        }
        MutationPublicationPreflight::Blocked(MutationPublicationBlocker::NotMetadataSafe(
            compatibility,
        )) => {
            let _ = &compatibility;

            Err(InternalError::store_unsupported())
        }
        MutationPublicationPreflight::Blocked(MutationPublicationBlocker::RebuildRequired(
            rebuild,
        )) => {
            let _ = &rebuild;

            Err(InternalError::store_unsupported())
        }
    }
}

// Keep supported physical schema mutation diagnostics distinct from generic
// unsupported mutation shapes. Reconciliation still fails closed until the
// startup runner owns row/index/schema publication together.
fn supported_physical_work_unavailable_error(
    entity_path: &'static str,
    plan: &SchemaTransitionPlan,
    step_count: usize,
    required: &[SchemaMutationRunnerCapability],
) -> InternalError {
    let _ = (entity_path, step_count, required);

    #[cfg(not(test))]
    {
        let _ = plan;

        InternalError::store_unsupported()
    }
    #[cfg(test)]
    {
        match plan.supported_developer_physical_path() {
            Ok(_path) => InternalError::store_unsupported(),
            Err(_rejection) => InternalError::store_unsupported(),
        }
    }
}

fn missing_physical_runner_error(
    entity_path: &'static str,
    plan: &SchemaTransitionPlan,
    missing: &[SchemaMutationRunnerCapability],
) -> InternalError {
    let _ = (entity_path, missing);

    #[cfg(not(test))]
    {
        let _ = plan;

        InternalError::store_unsupported()
    }
    #[cfg(test)]
    {
        match plan.supported_developer_physical_path() {
            Ok(_path) => InternalError::store_unsupported(),
            Err(_rejection) => InternalError::store_unsupported(),
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

// Map accepted transition plans into public transition metrics. The only
// accepted plan today is exact-match, but the match keeps future plan kinds
// visible at the policy boundary instead of hiding them in reconciliation.
const fn schema_transition_plan_outcome(kind: SchemaTransitionPlanKind) -> SchemaTransitionOutcome {
    match kind {
        SchemaTransitionPlanKind::AppendOnlyNullableFields => {
            SchemaTransitionOutcome::AppendOnlyNullableFields
        }
        SchemaTransitionPlanKind::AddExpressionIndex
        | SchemaTransitionPlanKind::AddFieldPathIndex
        | SchemaTransitionPlanKind::ExactMatch
        | SchemaTransitionPlanKind::MetadataOnlyIndexRename => SchemaTransitionOutcome::ExactMatch,
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

// Fail closed when generated code no longer matches an accepted persisted
// schema. Later schema-evolution work will replace this exact-match boundary
// with compatibility checks and explicit migrations.
fn validate_existing_schema_snapshot(
    entity_path: &'static str,
    actual: &PersistedSchemaSnapshot,
    expected: &PersistedSchemaSnapshot,
) -> Result<SchemaTransitionPlan, InternalError> {
    let transition_decision = decide_schema_transition(actual, expected);

    if let SchemaTransitionDecision::Accepted(plan) = &transition_decision
        && matches!(
            plan.kind(),
            SchemaTransitionPlanKind::ExactMatch
                | SchemaTransitionPlanKind::MetadataOnlyIndexRename
        )
    {
        record_schema_transition(entity_path, schema_transition_plan_outcome(plan.kind()));

        return match transition_decision {
            SchemaTransitionDecision::Accepted(plan) => Ok(plan),
            SchemaTransitionDecision::Rejected(_) => unreachable!("accepted transition matched"),
        };
    }

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
