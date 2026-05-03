//! Module: db::schema::reconcile
//! Responsibility: startup schema snapshot reconciliation.
//! Does not own: row/index recovery, generated model construction, or runtime layout authority.
//! Boundary: compares generated schema proposals with persisted schema snapshots.

use crate::{
    db::{
        Db, EntityRuntimeHooks,
        schema::{
            AcceptedSchemaSnapshot, PersistedFieldSnapshot, PersistedSchemaSnapshot, SchemaStore,
            compiled_schema_proposal_for_model,
        },
    },
    error::InternalError,
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
    entity_path: &str,
    model: &EntityModel,
) -> Result<AcceptedSchemaSnapshot, InternalError> {
    let proposal = compiled_schema_proposal_for_model(model);
    let expected = proposal.initial_persisted_schema_snapshot();

    if let Some(actual) = schema_store.get_persisted_snapshot(entity_tag, expected.version())? {
        validate_existing_schema_snapshot(entity_path, &actual, &expected)?;
        return Ok(AcceptedSchemaSnapshot::new(actual));
    }

    schema_store.insert_persisted_snapshot(entity_tag, &expected)?;

    Ok(AcceptedSchemaSnapshot::new(expected))
}

// Fail closed when generated code no longer matches an accepted persisted
// schema. Later schema-evolution work will replace this exact-match boundary
// with compatibility checks and explicit migrations.
fn validate_existing_schema_snapshot(
    entity_path: &str,
    actual: &crate::db::schema::PersistedSchemaSnapshot,
    expected: &crate::db::schema::PersistedSchemaSnapshot,
) -> Result<(), InternalError> {
    if actual == expected {
        return Ok(());
    }

    let detail = schema_snapshot_mismatch_detail(actual, expected);

    Err(InternalError::store_unsupported(format!(
        "schema evolution is not yet supported for entity '{entity_path}': {detail}",
    )))
}

// Return the first human-readable schema difference between the stored
// snapshot and the current generated proposal. This is diagnostic-only: the
// acceptance policy remains exact equality until schema transitions exist.
fn schema_snapshot_mismatch_detail(
    actual: &PersistedSchemaSnapshot,
    expected: &PersistedSchemaSnapshot,
) -> String {
    if actual.version() != expected.version() {
        return format!(
            "schema version changed: stored={} generated={}",
            actual.version().get(),
            expected.version().get(),
        );
    }

    if actual.entity_path() != expected.entity_path() {
        return format!(
            "entity path changed: stored='{}' generated='{}'",
            actual.entity_path(),
            expected.entity_path(),
        );
    }

    if actual.entity_name() != expected.entity_name() {
        return format!(
            "entity name changed: stored='{}' generated='{}'",
            actual.entity_name(),
            expected.entity_name(),
        );
    }

    schema_snapshot_structural_mismatch_detail(actual, expected)
}

// Compare schema internals after version/path/name have already matched. The
// split keeps the top-level diagnostic helper readable while preserving a
// deterministic first-difference order for startup failures.
fn schema_snapshot_structural_mismatch_detail(
    actual: &PersistedSchemaSnapshot,
    expected: &PersistedSchemaSnapshot,
) -> String {
    if actual.primary_key_field_id() != expected.primary_key_field_id() {
        return format!(
            "primary key field id changed: stored={} generated={}",
            actual.primary_key_field_id().get(),
            expected.primary_key_field_id().get(),
        );
    }

    if actual.row_layout() != expected.row_layout() {
        return format!(
            "row layout changed: stored={:?} generated={:?}",
            actual.row_layout(),
            expected.row_layout(),
        );
    }

    if actual.fields().len() != expected.fields().len() {
        return format!(
            "field count changed: stored={} generated={}",
            actual.fields().len(),
            expected.fields().len(),
        );
    }

    for (index, (actual_field, expected_field)) in
        actual.fields().iter().zip(expected.fields()).enumerate()
    {
        if let Some(detail) = field_snapshot_mismatch_detail(index, actual_field, expected_field) {
            return detail;
        }
    }

    "schema snapshot changed".to_string()
}

// Compare one field snapshot in a stable order so diagnostics point at the
// first durable field contract that would require explicit migration support.
fn field_snapshot_mismatch_detail(
    index: usize,
    actual: &PersistedFieldSnapshot,
    expected: &PersistedFieldSnapshot,
) -> Option<String> {
    if actual.id() != expected.id() {
        return Some(format!(
            "field[{index}] id changed: stored={} generated={}",
            actual.id().get(),
            expected.id().get(),
        ));
    }

    if actual.name() != expected.name() {
        return Some(format!(
            "field[{index}] name changed: stored='{}' generated='{}'",
            actual.name(),
            expected.name(),
        ));
    }

    field_snapshot_contract_mismatch_detail(index, actual, expected)
}

// Compare non-identity field metadata separately from durable ID/name so the
// mismatch order stays explicit without turning reconciliation into a large
// monolithic branch list.
fn field_snapshot_contract_mismatch_detail(
    index: usize,
    actual: &PersistedFieldSnapshot,
    expected: &PersistedFieldSnapshot,
) -> Option<String> {
    if actual.slot() != expected.slot() {
        return Some(format!(
            "field[{index}] slot changed: stored={} generated={}",
            actual.slot().get(),
            expected.slot().get(),
        ));
    }

    if actual.kind() != expected.kind() {
        return Some(format!(
            "field[{index}] kind changed: stored={:?} generated={:?}",
            actual.kind(),
            expected.kind(),
        ));
    }

    if actual.nested_leaves() != expected.nested_leaves() {
        return Some(format!(
            "field[{index}] nested leaf metadata changed: stored={} generated={}",
            actual.nested_leaves().len(),
            expected.nested_leaves().len(),
        ));
    }

    field_snapshot_storage_mismatch_detail(index, actual, expected)
}

// Compare nullable/default/storage codec metadata last. These are still schema
// contracts, but they are subordinate to field identity and physical layout
// when reporting the first rejected transition.
fn field_snapshot_storage_mismatch_detail(
    index: usize,
    actual: &PersistedFieldSnapshot,
    expected: &PersistedFieldSnapshot,
) -> Option<String> {
    if actual.nullable() != expected.nullable() {
        return Some(format!(
            "field[{index}] nullability changed: stored={} generated={}",
            actual.nullable(),
            expected.nullable(),
        ));
    }

    if actual.default() != expected.default() {
        return Some(format!(
            "field[{index}] default changed: stored={:?} generated={:?}",
            actual.default(),
            expected.default(),
        ));
    }

    if actual.storage_decode() != expected.storage_decode() {
        return Some(format!(
            "field[{index}] storage decode changed: stored={:?} generated={:?}",
            actual.storage_decode(),
            expected.storage_decode(),
        ));
    }

    if actual.leaf_codec() != expected.leaf_codec() {
        return Some(format!(
            "field[{index}] leaf codec changed: stored={:?} generated={:?}",
            actual.leaf_codec(),
            expected.leaf_codec(),
        ));
    }

    None
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
                PersistedSchemaSnapshot, SchemaStore, SchemaVersion,
                compiled_schema_proposal_for_model,
            },
        },
        error::ErrorClass,
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
    }

    #[test]
    fn reconcile_runtime_schemas_accepts_existing_matching_snapshot() {
        reset_schema_store();
        super::reconcile_runtime_schemas(&RECONCILE_DB, RECONCILE_RUNTIME_HOOKS)
            .expect("initial schema reconciliation should write generated snapshot");

        super::reconcile_runtime_schemas(&RECONCILE_DB, RECONCILE_RUNTIME_HOOKS)
            .expect("matching persisted schema should be accepted");

        assert_eq!(RECONCILE_SCHEMA_STORE.with_borrow(SchemaStore::len), 1);
    }

    #[test]
    fn reconcile_runtime_schemas_rejects_changed_initial_snapshot() {
        reset_schema_store();

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
    }
}
