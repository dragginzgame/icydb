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
            AcceptedSchemaSnapshot, MutationPublicationBlocker, MutationPublicationPreflight,
            PersistedIndexSnapshot, PersistedSchemaSnapshot, SchemaMutationRunnerCapability,
            SchemaMutationRunnerContract, SchemaStore, SchemaTransitionDecision,
            SchemaTransitionPlanKind, compiled_schema_proposal_for_model, decide_schema_transition,
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
use std::collections::BTreeSet;

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
/// The 0.146 path intentionally supports only exact generated-proposal
/// equality: first contact writes the generated initial snapshot, and later
/// contacts load the latest stored snapshot before accepting only exact
/// matches. Schema evolution comes after this persistence boundary is live.
pub(in crate::db) fn reconcile_runtime_schemas<C: CanisterKind>(
    db: &Db<C>,
    entity_runtime_hooks: &[EntityRuntimeHooks<C>],
) -> Result<(), InternalError> {
    for hooks in entity_runtime_hooks {
        reconcile_runtime_schema(db, hooks)?;
    }

    Ok(())
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
) -> Result<(), InternalError> {
    let store = db.store_handle(hooks.store_path)?;

    ensure_accepted_schema_snapshot_for_runtime_store(
        store,
        hooks.entity_tag,
        hooks.entity_path,
        hooks.model,
    )
    .map(|_| ())
}

/// Ensure one store contains an accepted persisted schema snapshot for a model.
///
/// This is the shared schema-owned boundary used by runtime-hook reconciliation
/// and metadata-only session paths. It writes first-create initial snapshots,
/// loads the latest stored snapshot, accepts exact matches, and rejects drift
/// until explicit evolution rules exist.
pub(in crate::db) fn ensure_accepted_schema_snapshot(
    schema_store: &mut SchemaStore,
    entity_tag: EntityTag,
    entity_path: &'static str,
    model: &EntityModel,
) -> Result<AcceptedSchemaSnapshot, InternalError> {
    let proposal = compiled_schema_proposal_for_model(model);
    let expected = proposal.initial_persisted_schema_snapshot();

    let latest = match schema_store.latest_persisted_snapshot(entity_tag) {
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
// snapshot. Metadata-only callers keep using `ensure_accepted_schema_snapshot`.
fn ensure_accepted_schema_snapshot_for_runtime_store(
    store: StoreHandle,
    entity_tag: EntityTag,
    entity_path: &'static str,
    model: &EntityModel,
) -> Result<AcceptedSchemaSnapshot, InternalError> {
    let proposal = compiled_schema_proposal_for_model(model);
    let expected = proposal.initial_persisted_schema_snapshot();

    let latest = match store
        .with_schema_mut(|schema_store| schema_store.latest_persisted_snapshot(entity_tag))
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
                    SchemaPublicationGate::startup(entity_tag),
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
            Err(InternalError::store_unsupported(format!(
                "schema mutation plan is rejected before publication for entity '{entity_path}': rebuild={requirement:?}",
            )))
        }
        MutationPublicationPreflight::Blocked(MutationPublicationBlocker::NotMetadataSafe(
            compatibility,
        )) => Err(InternalError::store_unsupported(format!(
            "schema mutation plan is not metadata-safe for entity '{entity_path}': compatibility={compatibility:?}",
        ))),
        MutationPublicationPreflight::Blocked(MutationPublicationBlocker::RebuildRequired(
            rebuild,
        )) => Err(InternalError::store_unsupported(format!(
            "schema mutation plan requires rebuild before publication for entity '{entity_path}': rebuild={rebuild:?}",
        ))),
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
    match plan.supported_developer_physical_path() {
        Ok(path) => InternalError::store_unsupported(format!(
            "supported schema mutation physical work is preflight-ready but startup execution is unavailable for entity '{entity_path}': mutation=add_field_path_index target='{}' store='{}' steps={step_count} capabilities={required:?}",
            path.target().name(),
            path.target().store(),
        )),
        Err(rejection) => InternalError::store_unsupported(format!(
            "schema mutation physical work is preflight-ready but unsupported for entity '{entity_path}': rejection={rejection:?} steps={step_count} capabilities={required:?}",
        )),
    }
}

fn missing_physical_runner_error(
    entity_path: &'static str,
    plan: &SchemaTransitionPlan,
    missing: &[SchemaMutationRunnerCapability],
) -> InternalError {
    match plan.supported_developer_physical_path() {
        Ok(path) => InternalError::store_unsupported(format!(
            "supported schema mutation requires startup runner execution before publication for entity '{entity_path}': mutation=add_field_path_index target='{}' store='{}' missing_capabilities={missing:?}",
            path.target().name(),
            path.target().store(),
        )),
        Err(rejection) => InternalError::store_unsupported(format!(
            "schema mutation plan requires runner preflight before publication for entity '{entity_path}': missing_capabilities={missing:?} supported_path_rejection={rejection:?}",
        )),
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

        return Err(InternalError::store_unsupported(format!(
            "schema evolution is not yet supported for entity '{entity_path}': {}",
            rejection.detail(),
        )));
    }

    // Gate source-declared version/method/fingerprint identity before
    // compatibility classification. Passing this gate is not publication.
    let admission_identity = SchemaAdmissionIdentityComparison::from_snapshots(actual, expected)?;
    if let Some(rejection) = schema_admission_rejection(admission_identity) {
        let outcome = schema_reconcile_rejection_outcome(rejection.kind());
        let transition_outcome = schema_transition_rejection_outcome(rejection.kind());
        record_schema_transition(entity_path, transition_outcome);
        record_schema_reconcile(entity_path, outcome);
        let first_shape_difference = match &transition_decision {
            SchemaTransitionDecision::Rejected(transition_rejection) => {
                format!(
                    "; first_shape_difference: {}",
                    transition_rejection.detail()
                )
            }
            SchemaTransitionDecision::Accepted(_) => String::new(),
        };

        return Err(InternalError::store_unsupported(format!(
            "schema evolution is not yet supported for entity '{entity_path}': {}{}",
            rejection.detail(),
            first_shape_difference,
        )));
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

            Err(InternalError::store_unsupported(format!(
                "schema evolution is not yet supported for entity '{entity_path}': {}",
                rejection.detail(),
            )))
        }
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests;
