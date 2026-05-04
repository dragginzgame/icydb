//! Module: db::schema::reconcile
//! Responsibility: startup schema snapshot reconciliation.
//! Does not own: row/index recovery, generated model construction, or runtime layout authority.
//! Boundary: compares generated schema proposals with persisted schema snapshots.

use crate::{
    db::{
        Db, EntityRuntimeHooks,
        schema::{
            AcceptedSchemaSnapshot, PersistedSchemaSnapshot, SchemaStore, SchemaTransitionDecision,
            compiled_schema_proposal_for_model, decide_schema_transition,
            transition::SchemaTransitionRejectionKind,
        },
    },
    error::InternalError,
    metrics::sink::{
        MetricsEvent, SchemaReconcileOutcome, record, record_schema_store_footprint_for_path,
    },
    model::entity::EntityModel,
    traits::CanisterKind,
    types::EntityTag,
};

/// Reconcile registered runtime schemas with the schema metadata store.
///
/// The 0.146 path intentionally supports only the initial schema authority:
/// first contact writes the generated initial snapshot, and later contacts must
/// decode to the exact same snapshot. Schema evolution comes after this
/// persistence boundary is live.
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
        ensure_initial_schema_snapshot(
            schema_store,
            hooks.entity_tag,
            hooks.entity_path,
            hooks.model,
        )
        .map(|_| ())
    })
}

/// Ensure one store contains the initial persisted schema snapshot for a model.
///
/// This is the shared schema-owned boundary used by runtime-hook reconciliation
/// and metadata-only session paths. It writes first-create snapshots, accepts
/// exact matches, and rejects drift until explicit evolution rules exist.
pub(in crate::db) fn ensure_initial_schema_snapshot(
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
        let outcome = match validate_existing_schema_snapshot(entity_path, &actual, &expected) {
            Ok(outcome) => outcome,
            Err(error) => {
                record_schema_store_footprint(schema_store, entity_tag, entity_path);
                return Err(error);
            }
        };
        record_schema_reconcile(entity_path, outcome);
        record_schema_store_footprint(schema_store, entity_tag, entity_path);

        return AcceptedSchemaSnapshot::try_new(actual);
    }

    if let Err(error) = schema_store.insert_persisted_snapshot(entity_tag, &expected) {
        record_schema_store_footprint(schema_store, entity_tag, entity_path);
        record_schema_reconcile(entity_path, SchemaReconcileOutcome::StoreWriteError);
        return Err(error);
    }

    record_schema_reconcile(entity_path, SchemaReconcileOutcome::FirstCreate);
    record_schema_store_footprint(schema_store, entity_tag, entity_path);

    AcceptedSchemaSnapshot::try_new(expected)
}

// Keep schema reconciliation instrumentation at the reconciliation boundary so
// store/codec helpers remain persistence-focused and do not depend on metrics.
fn record_schema_reconcile(entity_path: &'static str, outcome: SchemaReconcileOutcome) {
    record(MetricsEvent::SchemaReconcile {
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

// Fail closed when generated code no longer matches an accepted persisted
// schema. Later schema-evolution work will replace this exact-match boundary
// with compatibility checks and explicit migrations.
fn validate_existing_schema_snapshot(
    entity_path: &'static str,
    actual: &PersistedSchemaSnapshot,
    expected: &PersistedSchemaSnapshot,
) -> Result<SchemaReconcileOutcome, InternalError> {
    match decide_schema_transition(actual, expected) {
        SchemaTransitionDecision::ExactMatch => Ok(SchemaReconcileOutcome::ExactMatch),
        SchemaTransitionDecision::Rejected(rejection) => {
            let outcome = schema_reconcile_rejection_outcome(rejection.kind());
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
                PersistedSchemaSnapshot, SchemaRowLayout, SchemaStore, SchemaVersion,
                compiled_schema_proposal_for_model,
            },
        },
        error::ErrorClass,
        metrics::{metrics_report, metrics_reset_all},
        model::field::FieldKind,
        testing::test_memory,
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
        assert_eq!(counters.ops().schema_store_snapshots(), 1);
        assert!(counters.ops().schema_store_encoded_bytes() > 0);
        assert_eq!(
            counters.ops().schema_store_latest_snapshot_bytes(),
            counters.ops().schema_store_encoded_bytes(),
        );
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
