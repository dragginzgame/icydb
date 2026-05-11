//! Module: db::schema::reconcile
//! Responsibility: startup schema snapshot reconciliation.
//! Does not own: row/index recovery, generated model construction, or runtime layout authority.
//! Boundary: compares generated schema proposals with persisted schema snapshots.

use crate::{
    db::{
        Db, EntityRuntimeHooks,
        schema::{
            AcceptedSchemaSnapshot, MutationCompatibility, PersistedSchemaSnapshot,
            RebuildRequirement, SchemaStore, SchemaTransitionDecision, SchemaTransitionPlanKind,
            compiled_schema_proposal_for_model, decide_schema_transition,
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

// Reconcile one entity hook against its owning schema store. The generated
// proposal is compiled here so schema, not commit recovery, owns the comparison
// between generated metadata and persisted schema metadata.
fn reconcile_runtime_schema<C: CanisterKind>(
    db: &Db<C>,
    hooks: &EntityRuntimeHooks<C>,
) -> Result<(), InternalError> {
    let store = db.store_handle(hooks.store_path)?;

    store.with_schema_mut(|schema_store| {
        ensure_accepted_schema_snapshot(
            schema_store,
            hooks.entity_tag,
            hooks.entity_path,
            hooks.model,
        )
        .map(|_| ())
    })
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
            SchemaTransitionPlanKind::ExactMatch => actual,
            SchemaTransitionPlanKind::AppendOnlyNullableFields => {
                if let Err(error) = schema_store.insert_persisted_snapshot(entity_tag, &expected) {
                    record_schema_store_footprint(schema_store, entity_tag, entity_path);
                    record_schema_reconcile(entity_path, SchemaReconcileOutcome::StoreWriteError);
                    return Err(error);
                }
                expected
            }
        };
        record_schema_reconcile(entity_path, SchemaReconcileOutcome::ExactMatch);
        record_schema_store_footprint(schema_store, entity_tag, entity_path);
        let accepted = AcceptedSchemaSnapshot::try_new(accepted_snapshot)?;
        validate_accepted_runtime_descriptor(&accepted)?;
        record_accepted_schema_footprint(entity_path, &accepted);

        return Ok(accepted);
    }

    if let Err(error) = schema_store.insert_persisted_snapshot(entity_tag, &expected) {
        record_schema_store_footprint(schema_store, entity_tag, entity_path);
        record_schema_reconcile(entity_path, SchemaReconcileOutcome::StoreWriteError);
        return Err(error);
    }

    record_schema_reconcile(entity_path, SchemaReconcileOutcome::FirstCreate);
    record_schema_store_footprint(schema_store, entity_tag, entity_path);
    let accepted = AcceptedSchemaSnapshot::try_new(expected)?;
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
    if plan.mutation_compatibility() != MutationCompatibility::MetadataOnlySafe {
        return Err(InternalError::store_unsupported(format!(
            "schema mutation plan is not metadata-safe for entity '{entity_path}': compatibility={:?}",
            plan.mutation_compatibility(),
        )));
    }

    if plan.rebuild_requirement() != RebuildRequirement::NoRebuildRequired {
        return Err(InternalError::store_unsupported(format!(
            "schema mutation plan requires rebuild before publication for entity '{entity_path}': rebuild={:?}",
            plan.rebuild_requirement(),
        )));
    }

    Ok(())
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
        SchemaTransitionPlanKind::ExactMatch => SchemaTransitionOutcome::ExactMatch,
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
    use crate::{
        db::{
            Db, EntityRuntimeHooks,
            data::DataStore,
            index::IndexStore,
            registry::StoreRegistry,
            schema::{
                FieldId, PersistedFieldKind, PersistedFieldSnapshot, PersistedNestedLeafSnapshot,
                PersistedSchemaSnapshot, SchemaFieldDefault, SchemaFieldSlot, SchemaRowLayout,
                SchemaStore, SchemaVersion, compiled_schema_proposal_for_model,
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

    static NESTED_PROFILE_FIELDS: [FieldModel; 1] =
        [FieldModel::generated("rank", FieldKind::Uint)];
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
                PersistedFieldKind::Uint,
                false,
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Uint64),
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
            PersistedFieldKind::Uint,
            Vec::new(),
            false,
            SchemaFieldDefault::None,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Uint64),
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
