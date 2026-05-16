//! Module: db::schema::reconcile
//! Responsibility: startup schema snapshot reconciliation.
//! Does not own: row/index recovery, generated model construction, or runtime layout authority.
//! Boundary: compares generated schema proposals with persisted schema snapshots.

mod startup_field_path;

use crate::{
    db::{
        Db, EntityRuntimeHooks,
        index::{IndexId, IndexKey, IndexState, RawIndexKey},
        registry::StoreHandle,
        schema::{
            AcceptedSchemaSnapshot, MutationPublicationBlocker, MutationPublicationPreflight,
            PersistedSchemaSnapshot, SchemaDdlAcceptedSnapshotDerivation,
            SchemaMutationRunnerCapability, SchemaMutationRunnerContract,
            SchemaSecondaryIndexDropCleanupTarget, SchemaStore, SchemaTransitionDecision,
            SchemaTransitionPlanKind, compiled_schema_proposal_for_model, decide_schema_transition,
            runtime::AcceptedRowLayoutRuntimeDescriptor,
            transition::{SchemaTransitionPlan, SchemaTransitionRejectionKind},
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

use startup_field_path::execute_supported_field_path_index_addition;

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

/// Execute one supported SQL DDL field-path index addition through the same
/// physical runner and publication boundary used by startup reconciliation.
pub(in crate::db) fn execute_sql_ddl_field_path_index_addition(
    store: StoreHandle,
    entity_tag: EntityTag,
    entity_path: &'static str,
    accepted_before: &AcceptedSchemaSnapshot,
    derivation: &SchemaDdlAcceptedSnapshotDerivation,
) -> Result<(usize, usize), InternalError> {
    let before = accepted_before.persisted_snapshot();
    let after = derivation.accepted_after().persisted_snapshot();
    let plan = match decide_schema_transition(before, after) {
        SchemaTransitionDecision::Accepted(plan) => plan,
        SchemaTransitionDecision::Rejected(rejection) => {
            return Err(InternalError::store_unsupported(format!(
                "SQL DDL schema mutation rejected before physical execution for entity '{entity_path}': {}",
                rejection.detail(),
            )));
        }
    };
    if plan.kind() != SchemaTransitionPlanKind::AddNonUniqueFieldPathIndex {
        return Err(InternalError::store_unsupported(format!(
            "SQL DDL execution supports only add_non_unique_field_path_index for entity '{entity_path}': actual={:?}",
            plan.kind(),
        )));
    }
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
        store,
        entity_tag,
        entity_path,
        before,
        after,
        &plan,
    )?;

    Ok((report.rows_scanned(), report.index_keys_written()))
}

/// Execute one supported SQL DDL secondary-index drop by cleaning the target
/// physical index namespace before publishing the accepted-after schema.
pub(in crate::db) fn execute_sql_ddl_secondary_index_drop(
    store: StoreHandle,
    entity_tag: EntityTag,
    entity_path: &'static str,
    accepted_before: &AcceptedSchemaSnapshot,
    derivation: &SchemaDdlAcceptedSnapshotDerivation,
) -> Result<usize, InternalError> {
    let Some(target) = derivation.admission().drop_target() else {
        return Err(InternalError::store_unsupported(format!(
            "SQL DDL index drop execution requires a drop target for entity '{entity_path}'",
        )));
    };
    let before = accepted_before.persisted_snapshot();
    let after = derivation.accepted_after().persisted_snapshot();

    validate_sql_ddl_drop_schema_gate(store, entity_tag, entity_path, before, "before cleanup")?;
    let target_keys = sql_ddl_drop_target_index_keys(store, entity_tag, entity_path, target)?;
    let removed = store.with_index_mut(|index_store| {
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
    validate_sql_ddl_drop_physical_cleanup(store, entity_tag, entity_path, target)?;
    validate_sql_ddl_drop_schema_gate(
        store,
        entity_tag,
        entity_path,
        before,
        "before publication",
    )?;
    store.with_schema_mut(|schema_store| {
        schema_store.insert_persisted_snapshot(entity_tag, after)
    })?;

    Ok(removed)
}

fn sql_ddl_drop_target_index_keys(
    store: StoreHandle,
    entity_tag: EntityTag,
    entity_path: &'static str,
    target: &SchemaSecondaryIndexDropCleanupTarget,
) -> Result<Vec<RawIndexKey>, InternalError> {
    let target_index_id = IndexId::new(entity_tag, target.ordinal());

    store.with_index(|index_store| {
        if index_store.state() != IndexState::Ready {
            return Err(InternalError::store_unsupported(format!(
                "SQL DDL DROP INDEX requires a ready physical index store for entity '{entity_path}': target_index={} index_state={}",
                target.name(),
                index_store.state().as_str(),
            )));
        }

        index_store
            .entries()
            .into_iter()
            .filter_map(|(raw_key, _)| {
                let decoded = IndexKey::try_from_raw(&raw_key).map_err(|error| {
                    InternalError::store_corruption(format!(
                        "SQL DDL DROP INDEX key decode failed for entity '{entity_path}' while preflighting target index '{}': {error}",
                        target.name(),
                    ))
                });
                match decoded {
                    Ok(index_key) if *index_key.index_id() == target_index_id => Some(Ok(raw_key)),
                    Ok(_) => None,
                    Err(error) => Some(Err(error)),
                }
            })
            .collect()
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
            SchemaTransitionPlanKind::AddNonUniqueFieldPathIndex
            | SchemaTransitionPlanKind::ExactMatch => actual,
            SchemaTransitionPlanKind::AppendOnlyNullableFields
            | SchemaTransitionPlanKind::MetadataOnlyIndexRename => {
                if let Err(error) = schema_store.insert_persisted_snapshot(entity_tag, &expected) {
                    record_schema_store_footprint(schema_store, entity_tag, entity_path);
                    record_schema_reconcile(entity_path, SchemaReconcileOutcome::StoreWriteError);
                    return Err(error);
                }
                expected
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
            SchemaTransitionPlanKind::ExactMatch => {
                validate_publishable_transition_plan(entity_path, &plan)?;
                actual
            }
            SchemaTransitionPlanKind::AppendOnlyNullableFields
            | SchemaTransitionPlanKind::MetadataOnlyIndexRename => {
                validate_publishable_transition_plan(entity_path, &plan)?;
                store.with_schema_mut(|schema_store| {
                    schema_store.insert_persisted_snapshot(entity_tag, &expected)
                })?;
                expected
            }
            SchemaTransitionPlanKind::AddNonUniqueFieldPathIndex => {
                execute_supported_field_path_index_addition(
                    store,
                    entity_tag,
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
    let _descriptor = AcceptedRowLayoutRuntimeDescriptor::from_accepted_schema(accepted)?;

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
            "supported schema mutation physical work is preflight-ready but startup execution is unavailable for entity '{entity_path}': mutation=add_non_unique_field_path_index target='{}' store='{}' steps={step_count} capabilities={required:?}",
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
            "supported schema mutation requires startup runner execution before publication for entity '{entity_path}': mutation=add_non_unique_field_path_index target='{}' store='{}' missing_capabilities={missing:?}",
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
        SchemaTransitionPlanKind::AddNonUniqueFieldPathIndex
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
    match decide_schema_transition(actual, expected) {
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
mod tests {
    use super::startup_field_path;
    use crate::{
        db::{
            Db, EntityRuntimeHooks,
            data::{CanonicalRow, DataKey, DataStore, StorageKey, StructuralRowContract},
            index::{IndexId, IndexKey, IndexKeyKind, IndexState, IndexStore, RawIndexEntry},
            registry::StoreRegistry,
            schema::{
                AcceptedSchemaSnapshot, FieldId, PersistedFieldKind, PersistedFieldSnapshot,
                PersistedIndexSnapshot, PersistedNestedLeafSnapshot, PersistedSchemaSnapshot,
                SchemaFieldDefault, SchemaFieldPathIndexRebuildRow, SchemaFieldPathIndexRunner,
                SchemaFieldSlot, SchemaMutationRunnerInput, SchemaRowLayout, SchemaStore,
                SchemaVersion, compiled_schema_proposal_for_model,
            },
        },
        error::ErrorClass,
        metrics::{metrics_report, metrics_reset_all},
        model::{
            entity::EntityModel,
            field::{FieldKind, FieldModel, FieldStorageDecode, LeafCodec, ScalarCodec},
            index::IndexModel,
        },
        testing::{entity_model_from_static, test_memory},
        traits::{EntityKind, EntitySchema, Path},
        types::{EntityTag, Ulid},
    };
    use icydb_derive::{FieldProjection, PersistedRow};
    use serde::Deserialize;
    use std::cell::RefCell;

    crate::test_canister! {
        ident = SchemaReconcileTestCanister,
        commit_memory_id = crate::testing::test_commit_memory_id(),
    }

    crate::test_store! {
        ident = SchemaReconcileTestStore,
        canister = SchemaReconcileTestCanister,
    }

    #[derive(Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, PersistedRow)]
    struct SchemaReconcileEntity {
        id: Ulid,
        name: String,
    }

    crate::test_entity_schema! {
        ident = SchemaReconcileEntity,
        id = Ulid,
        id_field = id,
        entity_name = "SchemaReconcileEntity",
        entity_tag = EntityTag::new(0x7465_7374_7363_6865),
        pk_index = 0,
        fields = [
            ("id", FieldKind::Ulid),
            ("name", FieldKind::Text { max_len: None }),
        ],
        indexes = [],
        store = SchemaReconcileTestStore,
        canister = SchemaReconcileTestCanister,
    }

    static INDEXED_SCHEMA_NAME_INDEX: IndexModel = IndexModel::generated_with_ordinal(
        1,
        "by_name",
        "schema::reconcile::tests::IndexedSchemaEntity::by_name",
        &["name"],
        false,
    );

    #[derive(Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, PersistedRow)]
    struct IndexedSchemaEntity {
        id: Ulid,
        name: String,
    }

    crate::test_entity_schema! {
        ident = IndexedSchemaEntity,
        id = Ulid,
        id_field = id,
        entity_name = "IndexedSchemaEntity",
        entity_tag = EntityTag::new(0x696e_6478_7363_6865),
        pk_index = 0,
        fields = [
            ("id", FieldKind::Ulid),
            ("name", FieldKind::Text { max_len: None }),
        ],
        indexes = [&INDEXED_SCHEMA_NAME_INDEX],
        store = SchemaReconcileTestStore,
        canister = SchemaReconcileTestCanister,
    }

    static NESTED_PROFILE_FIELDS: [FieldModel; 1] = [FieldModel::generated("rank", FieldKind::Nat)];
    static NESTED_SCHEMA_FIELDS: [FieldModel; 2] = [
        FieldModel::generated("id", FieldKind::Ulid),
        FieldModel::generated_with_storage_decode_nullability_write_policies_and_nested_fields(
            "profile",
            FieldKind::Structured { queryable: true },
            FieldStorageDecode::Value,
            false,
            None,
            None,
            &NESTED_PROFILE_FIELDS,
        ),
    ];
    static NESTED_SCHEMA_INDEXES: [&IndexModel; 0] = [];
    static NESTED_SCHEMA_MODEL: EntityModel = entity_model_from_static(
        "schema::reconcile::tests::NestedSchemaEntity",
        "NestedSchemaEntity",
        &NESTED_SCHEMA_FIELDS[0],
        0,
        &NESTED_SCHEMA_FIELDS,
        &NESTED_SCHEMA_INDEXES,
    );
    const NESTED_SCHEMA_ENTITY_TAG: EntityTag = EntityTag::new(0x6e65_7374_7363_6865);
    static ADDITIVE_NULLABLE_SCHEMA_FIELDS: [FieldModel; 3] = [
        FieldModel::generated("id", FieldKind::Ulid),
        FieldModel::generated("name", FieldKind::Text { max_len: None }),
        FieldModel::generated_with_storage_decode_and_nullability(
            "nickname",
            FieldKind::Text { max_len: None },
            FieldStorageDecode::ByKind,
            true,
        ),
    ];
    static ADDITIVE_NULLABLE_SCHEMA_INDEXES: [&IndexModel; 0] = [];
    static ADDITIVE_NULLABLE_SCHEMA_MODEL: EntityModel = entity_model_from_static(
        "schema::reconcile::tests::AdditiveNullableSchemaEntity",
        "AdditiveNullableSchemaEntity",
        &ADDITIVE_NULLABLE_SCHEMA_FIELDS[0],
        0,
        &ADDITIVE_NULLABLE_SCHEMA_FIELDS,
        &ADDITIVE_NULLABLE_SCHEMA_INDEXES,
    );
    const ADDITIVE_NULLABLE_ENTITY_TAG: EntityTag = EntityTag::new(0x6164_6469_7469_7665);

    thread_local! {
        static RECONCILE_DATA_STORE: RefCell<DataStore> =
            RefCell::new(DataStore::init(test_memory(252)));
        static RECONCILE_INDEX_STORE: RefCell<IndexStore> =
            RefCell::new(IndexStore::init(test_memory(253)));
        static RECONCILE_SCHEMA_STORE: RefCell<SchemaStore> =
            RefCell::new(SchemaStore::init(test_memory(254)));
        static RECONCILE_STORE_REGISTRY: StoreRegistry = {
            let mut registry = StoreRegistry::new();
            registry
                .register_store(
                    SchemaReconcileTestStore::PATH,
                    &RECONCILE_DATA_STORE,
                    &RECONCILE_INDEX_STORE,
                    &RECONCILE_SCHEMA_STORE,
                )
                .expect("schema reconcile test store should register");
            registry
        };
    }

    static RECONCILE_RUNTIME_HOOKS: &[EntityRuntimeHooks<SchemaReconcileTestCanister>] =
        &[EntityRuntimeHooks::for_entity::<SchemaReconcileEntity>()];
    static RECONCILE_DB: Db<SchemaReconcileTestCanister> =
        Db::new_with_hooks(&RECONCILE_STORE_REGISTRY, RECONCILE_RUNTIME_HOOKS);

    fn reset_schema_store() {
        RECONCILE_SCHEMA_STORE.with_borrow_mut(SchemaStore::clear);
    }

    fn reset_reconcile_stores() {
        RECONCILE_SCHEMA_STORE.with_borrow_mut(SchemaStore::clear);
        RECONCILE_DATA_STORE.with_borrow_mut(DataStore::clear);
        RECONCILE_INDEX_STORE.with_borrow_mut(|store| {
            store.clear();
            store.mark_ready();
        });
    }

    fn indexed_schema_snapshot_without_indexes() -> PersistedSchemaSnapshot {
        let proposal = compiled_schema_proposal_for_model(IndexedSchemaEntity::MODEL);
        let expected = proposal.initial_persisted_schema_snapshot();
        PersistedSchemaSnapshot::new_with_indexes(
            expected.version(),
            expected.entity_path().to_string(),
            expected.entity_name().to_string(),
            expected.primary_key_field_id(),
            expected.row_layout().clone(),
            expected.fields().to_vec(),
            Vec::new(),
        )
    }

    fn indexed_schema_snapshot_with_renamed_index(index_name: &str) -> PersistedSchemaSnapshot {
        let proposal = compiled_schema_proposal_for_model(IndexedSchemaEntity::MODEL);
        let expected = proposal.initial_persisted_schema_snapshot();
        let [expected_index] = expected.indexes() else {
            panic!("indexed schema fixture should have one generated index");
        };
        let renamed_index = PersistedIndexSnapshot::new(
            expected_index.ordinal(),
            index_name.to_string(),
            expected_index.store().to_string(),
            expected_index.unique(),
            expected_index.key().clone(),
            expected_index.predicate_sql().map(str::to_string),
        );

        PersistedSchemaSnapshot::new_with_indexes(
            expected.version(),
            expected.entity_path().to_string(),
            expected.entity_name().to_string(),
            expected.primary_key_field_id(),
            expected.row_layout().clone(),
            expected.fields().to_vec(),
            vec![renamed_index],
        )
    }

    fn insert_indexed_schema_row(id: u128, name: &str) {
        let id = Ulid::from_u128(id);
        let data_key = DataKey::try_new::<IndexedSchemaEntity>(id).expect("test key should encode");
        let raw_key = data_key.to_raw().expect("test key should encode to raw");
        let row = CanonicalRow::from_generated_entity_for_test(&IndexedSchemaEntity {
            id,
            name: name.to_string(),
        })
        .expect("indexed schema row should encode");
        RECONCILE_DATA_STORE.with_borrow_mut(|store| {
            let _ = store.insert(raw_key, row);
        });
    }

    fn indexed_schema_field_path_publication_context() -> (
        PersistedSchemaSnapshot,
        PersistedSchemaSnapshot,
        super::SchemaTransitionPlan,
    ) {
        let proposal = compiled_schema_proposal_for_model(IndexedSchemaEntity::MODEL);
        let expected = proposal.initial_persisted_schema_snapshot();
        let stored_without_index = indexed_schema_snapshot_without_indexes();
        RECONCILE_SCHEMA_STORE.with_borrow_mut(|store| {
            store
                .insert_persisted_snapshot(IndexedSchemaEntity::ENTITY_TAG, &stored_without_index)
                .expect("stored index-free schema snapshot should encode");
        });
        insert_indexed_schema_row(15_401, "Ada");

        let plan = super::validate_existing_schema_snapshot(
            IndexedSchemaEntity::MODEL.path(),
            &stored_without_index,
            &expected,
        )
        .expect("single field-path index addition should produce a transition plan");

        (stored_without_index, expected, plan)
    }

    #[test]
    fn reconcile_runtime_schemas_writes_initial_snapshot_on_first_contact() {
        reset_schema_store();
        metrics_reset_all();

        super::reconcile_runtime_schemas(&RECONCILE_DB, RECONCILE_RUNTIME_HOOKS)
            .expect("initial schema reconciliation should write generated snapshot");

        let snapshot = RECONCILE_SCHEMA_STORE
            .with_borrow(|store| {
                store.get_persisted_snapshot(
                    SchemaReconcileEntity::ENTITY_TAG,
                    SchemaVersion::initial(),
                )
            })
            .expect("persisted schema snapshot should decode");
        let snapshot = snapshot.expect("initial schema snapshot should be persisted");

        assert_eq!(snapshot.entity_path(), SchemaReconcileEntity::PATH);
        assert_eq!(snapshot.fields().len(), 2);

        let report = metrics_report(None);
        let counters = report
            .counters()
            .expect("schema reconciliation should record metrics");
        assert_eq!(counters.ops().schema_reconcile_checks(), 1);
        assert_eq!(counters.ops().schema_reconcile_first_create(), 1);
        assert_eq!(
            counters.ops().schema_transition_checks(),
            0,
            "first-create reconciliation has no existing schema transition decision",
        );
        assert_eq!(counters.ops().schema_store_snapshots(), 1);
        assert!(counters.ops().schema_store_encoded_bytes() > 0);
        assert_eq!(
            counters.ops().schema_store_latest_snapshot_bytes(),
            counters.ops().schema_store_encoded_bytes(),
        );
        assert_eq!(counters.ops().accepted_schema_fields(), 2);
        assert_eq!(counters.ops().accepted_schema_nested_leaf_facts(), 0);
    }

    #[test]
    fn reconcile_runtime_schemas_accepts_existing_matching_snapshot() {
        reset_schema_store();
        metrics_reset_all();
        super::reconcile_runtime_schemas(&RECONCILE_DB, RECONCILE_RUNTIME_HOOKS)
            .expect("initial schema reconciliation should write generated snapshot");

        super::reconcile_runtime_schemas(&RECONCILE_DB, RECONCILE_RUNTIME_HOOKS)
            .expect("matching persisted schema should be accepted");

        assert_eq!(RECONCILE_SCHEMA_STORE.with_borrow(SchemaStore::len), 1);

        let report = metrics_report(None);
        let counters = report
            .counters()
            .expect("schema reconciliation should record metrics");
        assert_eq!(counters.ops().schema_reconcile_checks(), 2);
        assert_eq!(counters.ops().schema_reconcile_first_create(), 1);
        assert_eq!(counters.ops().schema_reconcile_exact_match(), 1);
        assert_eq!(counters.ops().schema_transition_checks(), 1);
        assert_eq!(counters.ops().schema_transition_exact_match(), 1);
        assert_eq!(
            counters.ops().accepted_schema_fields(),
            2,
            "accepted-schema footprint should stay a replaced entity gauge instead of double-counting exact-match reconciliation",
        );
        assert_eq!(counters.ops().accepted_schema_nested_leaf_facts(), 0);
    }

    #[test]
    fn ensure_accepted_schema_snapshot_accepts_append_only_nullable_field() {
        let mut schema_store = SchemaStore::init(test_memory(243));
        metrics_reset_all();

        let proposal = compiled_schema_proposal_for_model(&ADDITIVE_NULLABLE_SCHEMA_MODEL);
        let expected = proposal.initial_persisted_schema_snapshot();
        let stored_prefix = PersistedSchemaSnapshot::new(
            expected.version(),
            expected.entity_path().to_string(),
            expected.entity_name().to_string(),
            expected.primary_key_field_id(),
            SchemaRowLayout::new(
                expected.row_layout().version(),
                vec![
                    (FieldId::new(1), SchemaFieldSlot::new(0)),
                    (FieldId::new(2), SchemaFieldSlot::new(1)),
                ],
            ),
            expected.fields()[..2].to_vec(),
        );
        schema_store
            .insert_persisted_snapshot(ADDITIVE_NULLABLE_ENTITY_TAG, &stored_prefix)
            .expect("stored prefix schema snapshot should encode");

        let accepted = super::ensure_accepted_schema_snapshot(
            &mut schema_store,
            ADDITIVE_NULLABLE_ENTITY_TAG,
            ADDITIVE_NULLABLE_SCHEMA_MODEL.path(),
            &ADDITIVE_NULLABLE_SCHEMA_MODEL,
        )
        .expect("append-only nullable generated field should be accepted");
        let latest = schema_store
            .latest_persisted_snapshot(ADDITIVE_NULLABLE_ENTITY_TAG)
            .expect("schema store latest snapshot should decode")
            .expect("schema store should retain accepted additive snapshot");

        assert_eq!(accepted.footprint().fields(), 3);
        assert_eq!(latest.fields().len(), 3);
        assert_eq!(schema_store.len(), 1);

        let report = metrics_report(None);
        let counters = report
            .counters()
            .expect("schema reconciliation should record metrics");
        assert_eq!(counters.ops().schema_transition_checks(), 1);
        assert_eq!(
            counters
                .ops()
                .schema_transition_append_only_nullable_fields(),
            1
        );
        assert_eq!(
            counters.ops().schema_transition_rejected_field_contract(),
            0
        );
        assert_eq!(counters.ops().accepted_schema_fields(), 3);
    }

    #[test]
    fn ensure_accepted_schema_snapshot_publishes_metadata_only_index_rename() {
        let mut schema_store = SchemaStore::init(test_memory(240));
        let stored = indexed_schema_snapshot_with_renamed_index("IndexedSchemaEntity|name");
        schema_store
            .insert_persisted_snapshot(IndexedSchemaEntity::ENTITY_TAG, &stored)
            .expect("stored renamed-index schema snapshot should encode");

        let accepted = super::ensure_accepted_schema_snapshot(
            &mut schema_store,
            IndexedSchemaEntity::ENTITY_TAG,
            IndexedSchemaEntity::MODEL.path(),
            IndexedSchemaEntity::MODEL,
        )
        .expect("metadata-only generated index rename should be accepted");
        let latest = schema_store
            .latest_persisted_snapshot(IndexedSchemaEntity::ENTITY_TAG)
            .expect("schema store latest snapshot should decode")
            .expect("schema store should retain accepted renamed-index snapshot");

        assert_eq!(accepted.persisted_snapshot().indexes()[0].name(), "by_name");
        assert_eq!(latest.indexes()[0].name(), "by_name");
        assert_eq!(schema_store.len(), 1);
    }

    #[test]
    fn reconcile_runtime_schemas_executes_supported_field_path_index_addition() {
        reset_reconcile_stores();
        metrics_reset_all();

        let proposal = compiled_schema_proposal_for_model(IndexedSchemaEntity::MODEL);
        let expected = proposal.initial_persisted_schema_snapshot();
        let stored_without_index = PersistedSchemaSnapshot::new_with_indexes(
            expected.version(),
            expected.entity_path().to_string(),
            expected.entity_name().to_string(),
            expected.primary_key_field_id(),
            expected.row_layout().clone(),
            expected.fields().to_vec(),
            Vec::new(),
        );
        RECONCILE_SCHEMA_STORE.with_borrow_mut(|store| {
            store
                .insert_persisted_snapshot(IndexedSchemaEntity::ENTITY_TAG, &stored_without_index)
                .expect("stored index-free schema snapshot should encode");
        });

        let id = Ulid::from_u128(15_401);
        let data_key = DataKey::try_new::<IndexedSchemaEntity>(id).expect("test key should encode");
        let raw_key = data_key.to_raw().expect("test key should encode to raw");
        let row = CanonicalRow::from_generated_entity_for_test(&IndexedSchemaEntity {
            id,
            name: "Ada".to_string(),
        })
        .expect("indexed schema row should encode");
        RECONCILE_DATA_STORE.with_borrow_mut(|store| {
            let _ = store.insert(raw_key, row);
        });

        let hooks = [EntityRuntimeHooks::for_entity::<IndexedSchemaEntity>()];
        super::reconcile_runtime_schemas(&RECONCILE_DB, &hooks)
            .expect("supported field-path index addition should rebuild and publish");

        let latest = RECONCILE_SCHEMA_STORE
            .with_borrow(|store| store.latest_persisted_snapshot(IndexedSchemaEntity::ENTITY_TAG))
            .expect("latest schema snapshot should decode")
            .expect("indexed schema snapshot should be published");
        assert_eq!(latest.indexes().len(), 1);
        assert_eq!(latest.indexes()[0].name(), "by_name");
        RECONCILE_INDEX_STORE.with_borrow(|store| {
            assert_eq!(store.len(), 1);
            assert_eq!(store.state(), crate::db::index::IndexState::Ready);
        });

        let report = metrics_report(None);
        let counters = report
            .counters()
            .expect("schema reconciliation should record metrics");
        assert_eq!(counters.ops().schema_reconcile_checks(), 1);
        assert_eq!(counters.ops().schema_reconcile_exact_match(), 1);
        assert_eq!(counters.ops().accepted_schema_fields(), 2);
    }

    #[test]
    fn reconcile_runtime_schemas_rejects_field_path_index_addition_with_populated_target_index() {
        reset_reconcile_stores();
        metrics_reset_all();

        let proposal = compiled_schema_proposal_for_model(IndexedSchemaEntity::MODEL);
        let expected = proposal.initial_persisted_schema_snapshot();
        let stored_without_index = PersistedSchemaSnapshot::new_with_indexes(
            expected.version(),
            expected.entity_path().to_string(),
            expected.entity_name().to_string(),
            expected.primary_key_field_id(),
            expected.row_layout().clone(),
            expected.fields().to_vec(),
            Vec::new(),
        );
        RECONCILE_SCHEMA_STORE.with_borrow_mut(|store| {
            store
                .insert_persisted_snapshot(IndexedSchemaEntity::ENTITY_TAG, &stored_without_index)
                .expect("stored index-free schema snapshot should encode");
        });

        RECONCILE_INDEX_STORE.with_borrow_mut(|store| {
            let sentinel_id = IndexId::new(IndexedSchemaEntity::ENTITY_TAG, 1);
            let sentinel_key = IndexKey::empty_with_kind(&sentinel_id, IndexKeyKind::User).to_raw();
            let sentinel_entry = RawIndexEntry::try_from_keys([StorageKey::Nat(99)])
                .expect("sentinel index entry should encode");
            store.insert(sentinel_key, sentinel_entry);
        });

        let hooks = [EntityRuntimeHooks::for_entity::<IndexedSchemaEntity>()];
        super::reconcile_runtime_schemas(&RECONCILE_DB, &hooks)
            .expect_err("populated target physical index should fail closed");

        let latest = RECONCILE_SCHEMA_STORE
            .with_borrow(|store| store.latest_persisted_snapshot(IndexedSchemaEntity::ENTITY_TAG))
            .expect("latest schema snapshot should decode")
            .expect("index-free schema snapshot should remain accepted");
        assert_eq!(latest.indexes().len(), 0);
        RECONCILE_INDEX_STORE.with_borrow(|store| {
            assert_eq!(store.len(), 1);
            assert_eq!(store.state(), IndexState::Ready);
        });
    }

    #[test]
    fn reconcile_runtime_schemas_rejects_field_path_index_addition_with_building_index_store() {
        reset_reconcile_stores();
        metrics_reset_all();

        let proposal = compiled_schema_proposal_for_model(IndexedSchemaEntity::MODEL);
        let expected = proposal.initial_persisted_schema_snapshot();
        let stored_without_index = PersistedSchemaSnapshot::new_with_indexes(
            expected.version(),
            expected.entity_path().to_string(),
            expected.entity_name().to_string(),
            expected.primary_key_field_id(),
            expected.row_layout().clone(),
            expected.fields().to_vec(),
            Vec::new(),
        );
        RECONCILE_SCHEMA_STORE.with_borrow_mut(|store| {
            store
                .insert_persisted_snapshot(IndexedSchemaEntity::ENTITY_TAG, &stored_without_index)
                .expect("stored index-free schema snapshot should encode");
        });
        insert_indexed_schema_row(15_401, "Ada");

        RECONCILE_INDEX_STORE.with_borrow_mut(IndexStore::mark_building);

        let hooks = [EntityRuntimeHooks::for_entity::<IndexedSchemaEntity>()];
        super::reconcile_runtime_schemas(&RECONCILE_DB, &hooks)
            .expect_err("building physical index store should fail closed before schema publish");

        let latest = RECONCILE_SCHEMA_STORE
            .with_borrow(|store| store.latest_persisted_snapshot(IndexedSchemaEntity::ENTITY_TAG))
            .expect("latest schema snapshot should decode")
            .expect("index-free schema snapshot should remain accepted");
        assert_eq!(latest.indexes().len(), 0);
        RECONCILE_INDEX_STORE.with_borrow(|store| {
            assert_eq!(store.len(), 0);
            assert_eq!(store.state(), IndexState::Building);
        });

        RECONCILE_INDEX_STORE.with_borrow_mut(IndexStore::mark_ready);
    }

    #[test]
    fn reconcile_runtime_schemas_accepts_field_path_index_addition_with_unrelated_index_entries() {
        reset_reconcile_stores();
        metrics_reset_all();

        let proposal = compiled_schema_proposal_for_model(IndexedSchemaEntity::MODEL);
        let expected = proposal.initial_persisted_schema_snapshot();
        let stored_without_index = PersistedSchemaSnapshot::new_with_indexes(
            expected.version(),
            expected.entity_path().to_string(),
            expected.entity_name().to_string(),
            expected.primary_key_field_id(),
            expected.row_layout().clone(),
            expected.fields().to_vec(),
            Vec::new(),
        );
        RECONCILE_SCHEMA_STORE.with_borrow_mut(|store| {
            store
                .insert_persisted_snapshot(IndexedSchemaEntity::ENTITY_TAG, &stored_without_index)
                .expect("stored index-free schema snapshot should encode");
        });

        RECONCILE_INDEX_STORE.with_borrow_mut(|store| {
            let unrelated_id = IndexId::new(IndexedSchemaEntity::ENTITY_TAG, 99);
            let unrelated_key =
                IndexKey::empty_with_kind(&unrelated_id, IndexKeyKind::User).to_raw();
            let unrelated_entry = RawIndexEntry::try_from_keys([StorageKey::Nat(99)])
                .expect("unrelated index entry should encode");
            store.insert(unrelated_key, unrelated_entry);
        });

        let hooks = [EntityRuntimeHooks::for_entity::<IndexedSchemaEntity>()];
        super::reconcile_runtime_schemas(&RECONCILE_DB, &hooks)
            .expect("unrelated physical index entries should not block target index addition");

        let latest = RECONCILE_SCHEMA_STORE
            .with_borrow(|store| store.latest_persisted_snapshot(IndexedSchemaEntity::ENTITY_TAG))
            .expect("latest schema snapshot should decode")
            .expect("indexed schema snapshot should be published");
        assert_eq!(latest.indexes().len(), 1);
        RECONCILE_INDEX_STORE.with_borrow(|store| {
            assert_eq!(store.len(), 1);
            assert_eq!(store.state(), IndexState::Ready);
        });
    }

    #[test]
    fn field_path_startup_index_store_preflight_classifies_target_and_other_entries() {
        reset_reconcile_stores();

        let proposal = compiled_schema_proposal_for_model(IndexedSchemaEntity::MODEL);
        let expected = proposal.initial_persisted_schema_snapshot();
        let stored_without_index = PersistedSchemaSnapshot::new_with_indexes(
            expected.version(),
            expected.entity_path().to_string(),
            expected.entity_name().to_string(),
            expected.primary_key_field_id(),
            expected.row_layout().clone(),
            expected.fields().to_vec(),
            Vec::new(),
        );
        let plan = super::validate_existing_schema_snapshot(
            IndexedSchemaEntity::MODEL.path(),
            &stored_without_index,
            &expected,
        )
        .expect("single field-path index addition should produce a transition plan");
        let supported = plan
            .supported_developer_physical_path()
            .expect("single field-path index addition should be the supported path");
        let target = supported.target();

        RECONCILE_INDEX_STORE.with_borrow_mut(|store| {
            let target_id = IndexId::new(IndexedSchemaEntity::ENTITY_TAG, target.ordinal());
            let target_key = IndexKey::empty_with_kind(&target_id, IndexKeyKind::User).to_raw();
            let target_entry = RawIndexEntry::try_from_keys([StorageKey::Nat(1)])
                .expect("target index entry should encode");
            store.insert(target_key, target_entry);

            let other_id = IndexId::new(IndexedSchemaEntity::ENTITY_TAG, target.ordinal() + 1);
            let other_key = IndexKey::empty_with_kind(&other_id, IndexKeyKind::User).to_raw();
            let other_entry = RawIndexEntry::try_from_keys([StorageKey::Nat(2)])
                .expect("other index entry should encode");
            store.insert(other_key, other_entry);
        });

        let preflight = RECONCILE_INDEX_STORE
            .with_borrow(|store| {
                startup_field_path::field_path_startup_index_store_preflight(
                    store,
                    IndexedSchemaEntity::ENTITY_TAG,
                    target,
                    IndexedSchemaEntity::MODEL.path(),
                )
            })
            .expect("preflight should decode canonical index keys");

        assert_eq!(preflight.target_index_entries(), 1);
        assert_eq!(preflight.other_index_entries(), 1);
        assert_eq!(preflight.total_entries(), 2);
    }

    #[test]
    fn field_path_startup_rebuild_gate_accepts_unchanged_rows_and_schema() {
        reset_reconcile_stores();

        let proposal = compiled_schema_proposal_for_model(IndexedSchemaEntity::MODEL);
        let expected = proposal.initial_persisted_schema_snapshot();
        let stored_without_index = indexed_schema_snapshot_without_indexes();
        RECONCILE_SCHEMA_STORE.with_borrow_mut(|store| {
            store
                .insert_persisted_snapshot(IndexedSchemaEntity::ENTITY_TAG, &stored_without_index)
                .expect("stored index-free schema snapshot should encode");
        });
        insert_indexed_schema_row(15_401, "Ada");

        let plan = super::validate_existing_schema_snapshot(
            IndexedSchemaEntity::MODEL.path(),
            &stored_without_index,
            &expected,
        )
        .expect("single field-path index addition should produce a transition plan");
        let supported = plan
            .supported_developer_physical_path()
            .expect("single field-path index addition should be the supported path");
        let store = RECONCILE_DB
            .store_handle(SchemaReconcileTestStore::PATH)
            .expect("reconcile store should be registered");
        let raw_rows = startup_field_path::field_path_rebuild_raw_rows_for_entity(
            store,
            IndexedSchemaEntity::ENTITY_TAG,
            IndexedSchemaEntity::MODEL.path(),
        )
        .expect("indexed rows should scan");
        let gate = startup_field_path::StartupFieldPathRebuildGate::from_raw_rows(
            IndexedSchemaEntity::ENTITY_TAG,
            IndexedSchemaEntity::MODEL.path(),
            &stored_without_index,
            raw_rows.as_slice(),
        )
        .expect("startup rebuild gate should capture scanned rows");

        gate.validate_before_physical_work(store, supported.target(), raw_rows.len())
            .expect("unchanged rows and schema should keep startup rebuild gate valid");
    }

    #[test]
    fn field_path_startup_rebuild_gate_rejects_row_changes_before_physical_work() {
        reset_reconcile_stores();

        let proposal = compiled_schema_proposal_for_model(IndexedSchemaEntity::MODEL);
        let expected = proposal.initial_persisted_schema_snapshot();
        let stored_without_index = indexed_schema_snapshot_without_indexes();
        RECONCILE_SCHEMA_STORE.with_borrow_mut(|store| {
            store
                .insert_persisted_snapshot(IndexedSchemaEntity::ENTITY_TAG, &stored_without_index)
                .expect("stored index-free schema snapshot should encode");
        });
        insert_indexed_schema_row(15_401, "Ada");

        let plan = super::validate_existing_schema_snapshot(
            IndexedSchemaEntity::MODEL.path(),
            &stored_without_index,
            &expected,
        )
        .expect("single field-path index addition should produce a transition plan");
        let supported = plan
            .supported_developer_physical_path()
            .expect("single field-path index addition should be the supported path");
        let store = RECONCILE_DB
            .store_handle(SchemaReconcileTestStore::PATH)
            .expect("reconcile store should be registered");
        let raw_rows = startup_field_path::field_path_rebuild_raw_rows_for_entity(
            store,
            IndexedSchemaEntity::ENTITY_TAG,
            IndexedSchemaEntity::MODEL.path(),
        )
        .expect("indexed rows should scan");
        let gate = startup_field_path::StartupFieldPathRebuildGate::from_raw_rows(
            IndexedSchemaEntity::ENTITY_TAG,
            IndexedSchemaEntity::MODEL.path(),
            &stored_without_index,
            raw_rows.as_slice(),
        )
        .expect("startup rebuild gate should capture scanned rows");
        insert_indexed_schema_row(15_402, "Grace");

        gate.validate_before_physical_work(store, supported.target(), raw_rows.len())
            .expect_err("row changes after scan should fail closed before physical work");
    }

    #[test]
    fn field_path_startup_rebuild_gate_rejects_schema_changes_before_physical_work() {
        reset_reconcile_stores();

        let proposal = compiled_schema_proposal_for_model(IndexedSchemaEntity::MODEL);
        let expected = proposal.initial_persisted_schema_snapshot();
        let stored_without_index = indexed_schema_snapshot_without_indexes();
        RECONCILE_SCHEMA_STORE.with_borrow_mut(|store| {
            store
                .insert_persisted_snapshot(IndexedSchemaEntity::ENTITY_TAG, &stored_without_index)
                .expect("stored index-free schema snapshot should encode");
        });
        insert_indexed_schema_row(15_401, "Ada");

        let plan = super::validate_existing_schema_snapshot(
            IndexedSchemaEntity::MODEL.path(),
            &stored_without_index,
            &expected,
        )
        .expect("single field-path index addition should produce a transition plan");
        let supported = plan
            .supported_developer_physical_path()
            .expect("single field-path index addition should be the supported path");
        let store = RECONCILE_DB
            .store_handle(SchemaReconcileTestStore::PATH)
            .expect("reconcile store should be registered");
        let raw_rows = startup_field_path::field_path_rebuild_raw_rows_for_entity(
            store,
            IndexedSchemaEntity::ENTITY_TAG,
            IndexedSchemaEntity::MODEL.path(),
        )
        .expect("indexed rows should scan");
        let gate = startup_field_path::StartupFieldPathRebuildGate::from_raw_rows(
            IndexedSchemaEntity::ENTITY_TAG,
            IndexedSchemaEntity::MODEL.path(),
            &stored_without_index,
            raw_rows.as_slice(),
        )
        .expect("startup rebuild gate should capture scanned rows");
        RECONCILE_SCHEMA_STORE.with_borrow_mut(|store| {
            store
                .insert_persisted_snapshot(IndexedSchemaEntity::ENTITY_TAG, &expected)
                .expect("moved schema snapshot should encode");
        });

        gate.validate_before_physical_work(store, supported.target(), raw_rows.len())
            .expect_err("schema changes after planning should fail closed before physical work");
    }

    #[test]
    fn field_path_startup_publication_decision_publishes_after_runner_and_gate() {
        reset_reconcile_stores();

        let (stored_without_index, expected, plan) =
            indexed_schema_field_path_publication_context();
        let supported = plan
            .supported_developer_physical_path()
            .expect("single field-path index addition should be the supported path");
        let store = RECONCILE_DB
            .store_handle(SchemaReconcileTestStore::PATH)
            .expect("reconcile store should be registered");
        let accepted = AcceptedSchemaSnapshot::try_new(stored_without_index.clone())
            .expect("index-free snapshot should be accepted");
        let row_contract = StructuralRowContract::from_accepted_schema_snapshot(
            IndexedSchemaEntity::MODEL.path(),
            &accepted,
        )
        .expect("accepted row contract should build");
        let raw_rows = startup_field_path::field_path_rebuild_raw_rows_for_entity(
            store,
            IndexedSchemaEntity::ENTITY_TAG,
            IndexedSchemaEntity::MODEL.path(),
        )
        .expect("indexed rows should scan");
        let rebuild_gate = startup_field_path::StartupFieldPathRebuildGate::from_raw_rows(
            IndexedSchemaEntity::ENTITY_TAG,
            IndexedSchemaEntity::MODEL.path(),
            &stored_without_index,
            raw_rows.as_slice(),
        )
        .expect("startup rebuild gate should capture scanned rows");
        let rows = startup_field_path::decode_field_path_rebuild_rows(
            raw_rows.as_slice(),
            IndexedSchemaEntity::ENTITY_TAG,
            IndexedSchemaEntity::MODEL.path(),
            row_contract,
        )
        .expect("accepted rows should decode");
        let input =
            SchemaMutationRunnerInput::new(&stored_without_index, &expected, plan.execution_plan())
                .expect("runner input should bind accepted snapshots");
        let mut invalidation_sink = startup_field_path::StartupSchemaMutationInvalidationSink;
        let mut publication_sink = startup_field_path::StartupSchemaMutationPublicationSink;
        let report = RECONCILE_INDEX_STORE
            .with_borrow_mut(|index_store| {
                let rebuild_rows = rows
                    .iter()
                    .map(|row| SchemaFieldPathIndexRebuildRow::new(row.storage_key, &row.slots));
                SchemaFieldPathIndexRunner::run(
                    &input,
                    IndexedSchemaEntity::ENTITY_TAG,
                    supported.target().clone(),
                    rebuild_rows,
                    index_store,
                    &mut invalidation_sink,
                    &mut publication_sink,
                )
            })
            .expect("field-path runner should publish physical work");

        let decision = startup_field_path::StartupFieldPathPublicationDecision::from_runner_report(
            store,
            &rebuild_gate,
            supported.target(),
            &report,
        )
        .expect("publishable runner report and valid gate should allow schema publication");
        decision
            .publish_accepted_snapshot(store, IndexedSchemaEntity::ENTITY_TAG, &expected)
            .expect("publication decision should write accepted schema");

        let latest = RECONCILE_SCHEMA_STORE
            .with_borrow(|store| store.latest_persisted_snapshot(IndexedSchemaEntity::ENTITY_TAG))
            .expect("latest schema snapshot should decode")
            .expect("indexed schema snapshot should be published");
        assert_eq!(latest.indexes().len(), 1);
    }

    #[test]
    fn field_path_startup_publication_decision_rejects_gate_drift_without_schema_publish() {
        reset_reconcile_stores();

        let (stored_without_index, expected, plan) =
            indexed_schema_field_path_publication_context();
        let supported = plan
            .supported_developer_physical_path()
            .expect("single field-path index addition should be the supported path");
        let store = RECONCILE_DB
            .store_handle(SchemaReconcileTestStore::PATH)
            .expect("reconcile store should be registered");
        let accepted = AcceptedSchemaSnapshot::try_new(stored_without_index.clone())
            .expect("index-free snapshot should be accepted");
        let row_contract = StructuralRowContract::from_accepted_schema_snapshot(
            IndexedSchemaEntity::MODEL.path(),
            &accepted,
        )
        .expect("accepted row contract should build");
        let raw_rows = startup_field_path::field_path_rebuild_raw_rows_for_entity(
            store,
            IndexedSchemaEntity::ENTITY_TAG,
            IndexedSchemaEntity::MODEL.path(),
        )
        .expect("indexed rows should scan");
        let rebuild_gate = startup_field_path::StartupFieldPathRebuildGate::from_raw_rows(
            IndexedSchemaEntity::ENTITY_TAG,
            IndexedSchemaEntity::MODEL.path(),
            &stored_without_index,
            raw_rows.as_slice(),
        )
        .expect("startup rebuild gate should capture scanned rows");
        let rows = startup_field_path::decode_field_path_rebuild_rows(
            raw_rows.as_slice(),
            IndexedSchemaEntity::ENTITY_TAG,
            IndexedSchemaEntity::MODEL.path(),
            row_contract,
        )
        .expect("accepted rows should decode");
        let input =
            SchemaMutationRunnerInput::new(&stored_without_index, &expected, plan.execution_plan())
                .expect("runner input should bind accepted snapshots");
        let mut invalidation_sink = startup_field_path::StartupSchemaMutationInvalidationSink;
        let mut publication_sink = startup_field_path::StartupSchemaMutationPublicationSink;
        let report = RECONCILE_INDEX_STORE
            .with_borrow_mut(|index_store| {
                let rebuild_rows = rows
                    .iter()
                    .map(|row| SchemaFieldPathIndexRebuildRow::new(row.storage_key, &row.slots));
                SchemaFieldPathIndexRunner::run(
                    &input,
                    IndexedSchemaEntity::ENTITY_TAG,
                    supported.target().clone(),
                    rebuild_rows,
                    index_store,
                    &mut invalidation_sink,
                    &mut publication_sink,
                )
            })
            .expect("field-path runner should publish physical work");
        insert_indexed_schema_row(15_402, "Grace");

        startup_field_path::StartupFieldPathPublicationDecision::from_runner_report(
            store,
            &rebuild_gate,
            supported.target(),
            &report,
        )
        .expect_err("row drift after runner should reject schema publication");

        let latest = RECONCILE_SCHEMA_STORE
            .with_borrow(|store| store.latest_persisted_snapshot(IndexedSchemaEntity::ENTITY_TAG))
            .expect("latest schema snapshot should decode")
            .expect("index-free schema snapshot should remain accepted");
        assert_eq!(latest.indexes().len(), 0);
    }

    #[test]
    fn field_path_startup_publication_decision_rejects_physical_store_drift_without_schema_publish()
    {
        reset_reconcile_stores();

        let (stored_without_index, expected, plan) =
            indexed_schema_field_path_publication_context();
        let supported = plan
            .supported_developer_physical_path()
            .expect("single field-path index addition should be the supported path");
        let store = RECONCILE_DB
            .store_handle(SchemaReconcileTestStore::PATH)
            .expect("reconcile store should be registered");
        let accepted = AcceptedSchemaSnapshot::try_new(stored_without_index.clone())
            .expect("index-free snapshot should be accepted");
        let row_contract = StructuralRowContract::from_accepted_schema_snapshot(
            IndexedSchemaEntity::MODEL.path(),
            &accepted,
        )
        .expect("accepted row contract should build");
        let raw_rows = startup_field_path::field_path_rebuild_raw_rows_for_entity(
            store,
            IndexedSchemaEntity::ENTITY_TAG,
            IndexedSchemaEntity::MODEL.path(),
        )
        .expect("indexed rows should scan");
        let rebuild_gate = startup_field_path::StartupFieldPathRebuildGate::from_raw_rows(
            IndexedSchemaEntity::ENTITY_TAG,
            IndexedSchemaEntity::MODEL.path(),
            &stored_without_index,
            raw_rows.as_slice(),
        )
        .expect("startup rebuild gate should capture scanned rows");
        let rows = startup_field_path::decode_field_path_rebuild_rows(
            raw_rows.as_slice(),
            IndexedSchemaEntity::ENTITY_TAG,
            IndexedSchemaEntity::MODEL.path(),
            row_contract,
        )
        .expect("accepted rows should decode");
        let input =
            SchemaMutationRunnerInput::new(&stored_without_index, &expected, plan.execution_plan())
                .expect("runner input should bind accepted snapshots");
        let mut invalidation_sink = startup_field_path::StartupSchemaMutationInvalidationSink;
        let mut publication_sink = startup_field_path::StartupSchemaMutationPublicationSink;
        let report = RECONCILE_INDEX_STORE
            .with_borrow_mut(|index_store| {
                let rebuild_rows = rows
                    .iter()
                    .map(|row| SchemaFieldPathIndexRebuildRow::new(row.storage_key, &row.slots));
                SchemaFieldPathIndexRunner::run(
                    &input,
                    IndexedSchemaEntity::ENTITY_TAG,
                    supported.target().clone(),
                    rebuild_rows,
                    index_store,
                    &mut invalidation_sink,
                    &mut publication_sink,
                )
            })
            .expect("field-path runner should publish physical work");

        let decision = startup_field_path::StartupFieldPathPublicationDecision::from_runner_report(
            store,
            &rebuild_gate,
            supported.target(),
            &report,
        )
        .expect("publishable runner report and valid gate should allow a decision");
        RECONCILE_INDEX_STORE.with_borrow_mut(|store| {
            let target_id = IndexId::new(
                IndexedSchemaEntity::ENTITY_TAG,
                supported.target().ordinal(),
            );
            let extra_key = IndexKey::empty_with_kind(&target_id, IndexKeyKind::User).to_raw();
            let extra_entry = RawIndexEntry::try_from_keys([StorageKey::Nat(99)])
                .expect("extra target entry should encode");
            store.insert(extra_key, extra_entry);
        });

        decision
            .publish_accepted_snapshot(store, IndexedSchemaEntity::ENTITY_TAG, &expected)
            .expect_err("physical store drift after runner should reject schema publication");

        let latest = RECONCILE_SCHEMA_STORE
            .with_borrow(|store| store.latest_persisted_snapshot(IndexedSchemaEntity::ENTITY_TAG))
            .expect("latest schema snapshot should decode")
            .expect("index-free schema snapshot should remain accepted");
        assert_eq!(latest.indexes().len(), 0);
    }

    #[test]
    fn ensure_accepted_schema_snapshot_rejects_field_path_index_addition_without_runtime_store() {
        let mut schema_store = SchemaStore::init(test_memory(244));
        metrics_reset_all();

        let proposal = compiled_schema_proposal_for_model(IndexedSchemaEntity::MODEL);
        let expected = proposal.initial_persisted_schema_snapshot();
        let stored_without_index = PersistedSchemaSnapshot::new_with_indexes(
            expected.version(),
            expected.entity_path().to_string(),
            expected.entity_name().to_string(),
            expected.primary_key_field_id(),
            expected.row_layout().clone(),
            expected.fields().to_vec(),
            Vec::new(),
        );
        schema_store
            .insert_persisted_snapshot(IndexedSchemaEntity::ENTITY_TAG, &stored_without_index)
            .expect("stored index-free schema snapshot should encode");

        let err = super::ensure_accepted_schema_snapshot(
            &mut schema_store,
            IndexedSchemaEntity::ENTITY_TAG,
            IndexedSchemaEntity::MODEL.path(),
            IndexedSchemaEntity::MODEL,
        )
        .expect_err("metadata-only reconciliation must not execute physical index addition");

        assert_eq!(err.class, ErrorClass::Unsupported);
        let latest = schema_store
            .latest_persisted_snapshot(IndexedSchemaEntity::ENTITY_TAG)
            .expect("latest schema snapshot should decode")
            .expect("index-free schema snapshot should remain accepted");
        assert_eq!(latest.indexes().len(), 0);
    }

    #[test]
    fn ensure_accepted_schema_snapshot_records_nested_leaf_footprint() {
        let mut schema_store = SchemaStore::init(test_memory(241));
        metrics_reset_all();

        let accepted = super::ensure_accepted_schema_snapshot(
            &mut schema_store,
            NESTED_SCHEMA_ENTITY_TAG,
            NESTED_SCHEMA_MODEL.path(),
            &NESTED_SCHEMA_MODEL,
        )
        .expect("nested schema snapshot should be accepted on first contact");

        let footprint = accepted.footprint();
        assert_eq!(footprint.fields(), 2);
        assert_eq!(footprint.nested_leaf_facts(), 1);

        let report = metrics_report(None);
        let counters = report
            .counters()
            .expect("accepted nested schema should record metrics");
        assert_eq!(counters.ops().accepted_schema_fields(), 2);
        assert_eq!(counters.ops().accepted_schema_nested_leaf_facts(), 1);

        let summary = report
            .entity_counters()
            .iter()
            .find(|summary| summary.path() == NESTED_SCHEMA_MODEL.path())
            .expect("accepted nested schema should record an entity summary");
        assert_eq!(summary.accepted_schema_fields(), 2);
        assert_eq!(summary.accepted_schema_nested_leaf_facts(), 1);
    }

    #[test]
    fn ensure_accepted_schema_snapshot_rejects_nested_leaf_drift_as_field_contract() {
        let mut schema_store = SchemaStore::init(test_memory(242));
        metrics_reset_all();

        let proposal = compiled_schema_proposal_for_model(&NESTED_SCHEMA_MODEL);
        let expected = proposal.initial_persisted_schema_snapshot();
        let mut stored_fields = expected.fields().to_vec();
        let profile = &expected.fields()[1];
        stored_fields[1] = PersistedFieldSnapshot::new(
            profile.id(),
            profile.name().to_string(),
            profile.slot(),
            profile.kind().clone(),
            vec![PersistedNestedLeafSnapshot::new(
                vec!["legacy_rank".to_string()],
                PersistedFieldKind::Nat,
                false,
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Nat64),
            )],
            profile.nullable(),
            profile.default().clone(),
            profile.storage_decode(),
            profile.leaf_codec(),
        );
        let stored_with_nested_leaf_drift = PersistedSchemaSnapshot::new(
            expected.version(),
            expected.entity_path().to_string(),
            expected.entity_name().to_string(),
            expected.primary_key_field_id(),
            expected.row_layout().clone(),
            stored_fields,
        );
        schema_store
            .insert_persisted_snapshot(NESTED_SCHEMA_ENTITY_TAG, &stored_with_nested_leaf_drift)
            .expect("stored nested-leaf drift snapshot should encode");

        let err = super::ensure_accepted_schema_snapshot(
            &mut schema_store,
            NESTED_SCHEMA_ENTITY_TAG,
            NESTED_SCHEMA_MODEL.path(),
            &NESTED_SCHEMA_MODEL,
        )
        .expect_err("nested leaf schema drift should still be rejected");

        assert_eq!(err.class, ErrorClass::Unsupported);
        assert!(
            err.message
                .contains("field[1] nested leaf metadata changed"),
            "nested leaf drift should name the owning field"
        );
        assert!(
            err.message.contains("stored_path='legacy_rank'"),
            "nested leaf drift should include the stored nested path"
        );
        assert!(
            err.message.contains("generated_path='rank'"),
            "nested leaf drift should include the generated nested path"
        );

        let report = metrics_report(None);
        let counters = report
            .counters()
            .expect("schema reconciliation should record metrics");
        assert_eq!(counters.ops().schema_reconcile_checks(), 1);
        assert_eq!(counters.ops().schema_reconcile_rejected_other(), 1);
        assert_eq!(
            counters.ops().schema_reconcile_rejected_row_layout(),
            0,
            "nested leaf drift should stay in field-contract transition buckets",
        );
        assert_eq!(counters.ops().schema_transition_checks(), 1);
        assert_eq!(
            counters.ops().schema_transition_rejected_field_contract(),
            1
        );
    }

    #[test]
    fn reconcile_runtime_schemas_rejects_changed_initial_snapshot() {
        reset_schema_store();
        metrics_reset_all();

        let proposal = compiled_schema_proposal_for_model(SchemaReconcileEntity::MODEL);
        let expected = proposal.initial_persisted_schema_snapshot();
        let changed = PersistedSchemaSnapshot::new(
            expected.version(),
            expected.entity_path().to_string(),
            "ChangedSchemaReconcileEntity".to_string(),
            expected.primary_key_field_id(),
            expected.row_layout().clone(),
            expected.fields().to_vec(),
        );
        RECONCILE_SCHEMA_STORE.with_borrow_mut(|store| {
            store
                .insert_persisted_snapshot(SchemaReconcileEntity::ENTITY_TAG, &changed)
                .expect("changed schema snapshot should encode");
        });

        let err = super::reconcile_runtime_schemas(&RECONCILE_DB, RECONCILE_RUNTIME_HOOKS)
            .expect_err("schema reconciliation should reject changed persisted snapshot");

        assert_eq!(err.class, ErrorClass::Unsupported);
        assert!(
            err.message
                .contains("schema evolution is not yet supported"),
            "schema mismatch should fail at the explicit evolution boundary"
        );
        assert!(
            err.message
                .contains("entity name changed: stored='ChangedSchemaReconcileEntity' generated='SchemaReconcileEntity'"),
            "schema mismatch should include the first rejected difference"
        );

        let report = metrics_report(None);
        let counters = report
            .counters()
            .expect("schema reconciliation should record metrics");
        assert_eq!(counters.ops().schema_reconcile_checks(), 1);
        assert_eq!(counters.ops().schema_reconcile_rejected_other(), 1);
        assert_eq!(counters.ops().schema_transition_checks(), 1);
        assert_eq!(
            counters.ops().schema_transition_rejected_entity_identity(),
            1
        );
    }

    #[test]
    fn reconcile_runtime_schemas_rejects_generated_additive_field_as_field_contract() {
        reset_schema_store();
        metrics_reset_all();

        let proposal = compiled_schema_proposal_for_model(SchemaReconcileEntity::MODEL);
        let expected = proposal.initial_persisted_schema_snapshot();
        let stored_prefix = PersistedSchemaSnapshot::new(
            expected.version(),
            expected.entity_path().to_string(),
            expected.entity_name().to_string(),
            expected.primary_key_field_id(),
            SchemaRowLayout::new(
                expected.row_layout().version(),
                vec![(FieldId::new(1), SchemaFieldSlot::new(0))],
            ),
            expected.fields()[..1].to_vec(),
        );
        RECONCILE_SCHEMA_STORE.with_borrow_mut(|store| {
            store
                .insert_persisted_snapshot(SchemaReconcileEntity::ENTITY_TAG, &stored_prefix)
                .expect("stored prefix schema snapshot should encode");
        });

        let err = super::reconcile_runtime_schemas(&RECONCILE_DB, RECONCILE_RUNTIME_HOOKS)
            .expect_err("additive generated schema drift should still be rejected");

        assert_eq!(err.class, ErrorClass::Unsupported);
        assert!(
            err.message
                .contains("unsupported additive field transition"),
            "additive schema drift should name the future transition shape"
        );

        let report = metrics_report(None);
        let counters = report
            .counters()
            .expect("schema reconciliation should record metrics");
        assert_eq!(counters.ops().schema_reconcile_checks(), 1);
        assert_eq!(counters.ops().schema_reconcile_rejected_other(), 1);
        assert_eq!(
            counters.ops().schema_reconcile_rejected_row_layout(),
            0,
            "append-only generated fields should no longer be bucketed as generic row-layout drift",
        );
        assert_eq!(counters.ops().schema_transition_checks(), 1);
        assert_eq!(
            counters.ops().schema_transition_rejected_field_contract(),
            1
        );
    }

    #[test]
    fn reconcile_runtime_schemas_rejects_generated_removed_field_as_field_contract() {
        reset_schema_store();
        metrics_reset_all();

        let proposal = compiled_schema_proposal_for_model(SchemaReconcileEntity::MODEL);
        let expected = proposal.initial_persisted_schema_snapshot();
        let mut stored_fields = expected.fields().to_vec();
        stored_fields.push(PersistedFieldSnapshot::new(
            FieldId::new(3),
            "legacy_score".to_string(),
            SchemaFieldSlot::new(2),
            PersistedFieldKind::Nat,
            Vec::new(),
            false,
            SchemaFieldDefault::None,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Nat64),
        ));
        let stored_with_removed_field = PersistedSchemaSnapshot::new(
            expected.version(),
            expected.entity_path().to_string(),
            expected.entity_name().to_string(),
            expected.primary_key_field_id(),
            SchemaRowLayout::new(
                expected.row_layout().version(),
                vec![
                    (FieldId::new(1), SchemaFieldSlot::new(0)),
                    (FieldId::new(2), SchemaFieldSlot::new(1)),
                    (FieldId::new(3), SchemaFieldSlot::new(2)),
                ],
            ),
            stored_fields,
        );
        RECONCILE_SCHEMA_STORE.with_borrow_mut(|store| {
            store
                .insert_persisted_snapshot(
                    SchemaReconcileEntity::ENTITY_TAG,
                    &stored_with_removed_field,
                )
                .expect("stored removed-field schema snapshot should encode");
        });

        let err = super::reconcile_runtime_schemas(&RECONCILE_DB, RECONCILE_RUNTIME_HOOKS)
            .expect_err("generated field removal should still be rejected");

        assert_eq!(err.class, ErrorClass::Unsupported);
        assert!(
            err.message.contains("unsupported removed field transition"),
            "removed field drift should name the future transition shape"
        );

        let report = metrics_report(None);
        let counters = report
            .counters()
            .expect("schema reconciliation should record metrics");
        assert_eq!(counters.ops().schema_reconcile_checks(), 1);
        assert_eq!(counters.ops().schema_reconcile_rejected_other(), 1);
        assert_eq!(
            counters.ops().schema_reconcile_rejected_row_layout(),
            0,
            "append-only stored fields should no longer be bucketed as generic row-layout drift",
        );
        assert_eq!(counters.ops().schema_transition_checks(), 1);
        assert_eq!(
            counters.ops().schema_transition_rejected_field_contract(),
            1
        );
    }

    #[test]
    fn reconcile_runtime_schemas_rejects_newer_schema_snapshot() {
        reset_schema_store();

        let proposal = compiled_schema_proposal_for_model(SchemaReconcileEntity::MODEL);
        let expected = proposal.initial_persisted_schema_snapshot();
        let newer_row_layout = SchemaRowLayout::new(
            SchemaVersion::new(2),
            expected.row_layout().field_to_slot().to_vec(),
        );
        let newer = PersistedSchemaSnapshot::new(
            SchemaVersion::new(2),
            expected.entity_path().to_string(),
            expected.entity_name().to_string(),
            expected.primary_key_field_id(),
            newer_row_layout,
            expected.fields().to_vec(),
        );
        RECONCILE_SCHEMA_STORE.with_borrow_mut(|store| {
            store
                .insert_persisted_snapshot(SchemaReconcileEntity::ENTITY_TAG, &newer)
                .expect("newer schema snapshot should encode");
        });

        let err = super::reconcile_runtime_schemas(&RECONCILE_DB, RECONCILE_RUNTIME_HOOKS)
            .expect_err("schema reconciliation must not ignore newer persisted versions");

        assert_eq!(err.class, ErrorClass::Unsupported);
        assert!(
            err.message
                .contains("schema version changed: stored=2 generated=1"),
            "schema reconciliation should compare against the latest persisted version"
        );
    }
}
