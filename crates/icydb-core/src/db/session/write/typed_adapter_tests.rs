mod binding_diagnostics_tests;
mod incarnation_tests;
mod input_handoff_tests;

use super::{
    AcceptedFieldKind, DbSession, DynamicTypedBindingError, DynamicTypedEntityBinding,
    DynamicTypedMutation, DynamicWriteCell, TypedEntityDescriptor, TypedFieldType,
    typed_adapter_field_kind_matches, typed_descriptor_field_type,
};
use crate::{
    db::{
        TypedFieldDescriptor,
        data::DataStore,
        index::IndexStore,
        registry::{StoreAllocationIdentities, StoreRegistry, StoreRuntimeStorageCapabilities},
        schema::{
            AcceptedSchemaRevision, FieldId, FieldStorageDecode, LeafCodec, PersistedFieldSnapshot,
            PersistedSchemaSnapshot, ScalarCodec, SchemaFieldSlot, SchemaInsertDefault,
            SchemaRowLayout, SchemaStore, SchemaVersion,
            accepted_schema_candidate_with_field_bindings_for_tests,
        },
    },
    traits::{CanisterKind, Path},
    types::EntityTag,
    value::InputValue,
};
use icydb_schema::{FieldSourceKey, ScalarType};
use std::{cell::RefCell, collections::BTreeMap};

const STORE_PATH: &str = "session::write::typed_adapter_tests::Store";
const OTHER_STORE_PATH: &str = "session::write::typed_adapter_tests::OtherStore";
const ENTITY_SOURCE: &str = "session::write::typed_adapter_tests::Entity";
const OTHER_ENTITY_SOURCE: &str = "session::write::typed_adapter_tests::OtherEntity";
const ID_SOURCE: &str = "session::write::typed_adapter_tests::Entity::id";
const VALUE_SOURCE: &str = "session::write::typed_adapter_tests::Entity::value";
const REPLACEMENT_SOURCE: &str = "session::write::typed_adapter_tests::Entity::replacement_value";
const OTHER_ID_SOURCE: &str = "session::write::typed_adapter_tests::OtherEntity::id";
const ENTITY_DESCRIPTOR: TypedEntityDescriptor = TypedEntityDescriptor::new(
    ENTITY_SOURCE,
    &[ID_SOURCE],
    &[
        TypedFieldDescriptor::new(ID_SOURCE, TypedFieldType::Scalar(ScalarType::Nat64), false),
        TypedFieldDescriptor::new(
            VALUE_SOURCE,
            TypedFieldType::Scalar(ScalarType::Nat64),
            false,
        ),
    ],
);
const OTHER_ENTITY_DESCRIPTOR: TypedEntityDescriptor = TypedEntityDescriptor::new(
    OTHER_ENTITY_SOURCE,
    &[OTHER_ID_SOURCE],
    &[TypedFieldDescriptor::new(
        OTHER_ID_SOURCE,
        TypedFieldType::Scalar(ScalarType::Nat64),
        false,
    )],
);
const REPLACEMENT_DESCRIPTOR: TypedEntityDescriptor = TypedEntityDescriptor::new(
    ENTITY_SOURCE,
    &[ID_SOURCE],
    &[
        TypedFieldDescriptor::new(ID_SOURCE, TypedFieldType::Scalar(ScalarType::Nat64), false),
        TypedFieldDescriptor::new(
            REPLACEMENT_SOURCE,
            TypedFieldType::Scalar(ScalarType::Nat64),
            false,
        ),
    ],
);

struct TestCanister;

impl Path for TestCanister {
    const PATH: &'static str = "session::write::typed_adapter_tests::Canister";
}

impl CanisterKind for TestCanister {
    fn commit_memory_id() -> Result<u8, ic_memory::RuntimeOpenError> {
        Ok(41)
    }
    const COMMIT_STABLE_KEY: &'static str = "icydb.typed_adapter_tests.commit.v1";
    fn startup_memory_id() -> Result<u8, ic_memory::RuntimeOpenError> {
        Ok(49)
    }
    const STARTUP_STABLE_KEY: &'static str = "icydb.typed_adapter_tests.startup.control.v1";
    fn integrity_progress_memory_id() -> Result<u8, ic_memory::RuntimeOpenError> {
        Ok(42)
    }
    const INTEGRITY_PROGRESS_STABLE_KEY: &'static str =
        "icydb.typed_adapter_tests.integrity.progress.v1";
}

thread_local! {
    static DATA_STORE: RefCell<DataStore> = const { RefCell::new(DataStore::init_heap()) };
    static INDEX_STORE: RefCell<IndexStore> = const { RefCell::new(IndexStore::init_heap()) };
    static SCHEMA_STORE: RefCell<SchemaStore> =
        const { RefCell::new(SchemaStore::init_heap()) };
    static OTHER_DATA_STORE: RefCell<DataStore> = const { RefCell::new(DataStore::init_heap()) };
    static OTHER_INDEX_STORE: RefCell<IndexStore> = const { RefCell::new(IndexStore::init_heap()) };
    static OTHER_SCHEMA_STORE: RefCell<SchemaStore> =
        const { RefCell::new(SchemaStore::init_heap()) };
    static STORE_REGISTRY: StoreRegistry = {
        let mut registry = StoreRegistry::new();
        registry.register_store(
            STORE_PATH,
            &DATA_STORE,
            &INDEX_STORE,
            &SCHEMA_STORE,
            StoreAllocationIdentities::absent(),
            StoreRuntimeStorageCapabilities::heap(),
        ).expect("typed adapter test store should register");
        registry.register_store(
            OTHER_STORE_PATH,
            &OTHER_DATA_STORE,
            &OTHER_INDEX_STORE,
            &OTHER_SCHEMA_STORE,
            StoreAllocationIdentities::absent(),
            StoreRuntimeStorageCapabilities::heap(),
        ).expect("second typed adapter test store should register");
        registry
    };
}

fn nat64_field(id: u32, name: &str, slot: u16) -> PersistedFieldSnapshot {
    PersistedFieldSnapshot::new_initial(
        FieldId::new(id),
        name.to_string(),
        SchemaFieldSlot::new(slot),
        AcceptedFieldKind::Nat64,
        Vec::new(),
        false,
        SchemaInsertDefault::None,
        FieldStorageDecode::ByKind,
        LeafCodec::Scalar(ScalarCodec::Nat64),
    )
}

fn snapshot(
    entity_source: &str,
    entity_name: &str,
    fields: Vec<PersistedFieldSnapshot>,
) -> PersistedSchemaSnapshot {
    let layout = SchemaRowLayout::initial(
        fields
            .iter()
            .map(|field| (field.id(), field.slot()))
            .collect(),
    );
    PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        entity_source.to_string(),
        entity_name.to_string(),
        FieldId::new(1),
        layout,
        fields,
    )
}

fn field_source(source: &str) -> FieldSourceKey {
    FieldSourceKey::try_new(source).expect("typed field source should admit")
}

fn publish(
    session: &DbSession<TestCanister>,
    expected: AcceptedSchemaRevision,
    revision: AcceptedSchemaRevision,
    snapshots: BTreeMap<EntityTag, PersistedSchemaSnapshot>,
    fields: BTreeMap<(EntityTag, FieldSourceKey), FieldId>,
) {
    publish_to_store(session, STORE_PATH, expected, revision, snapshots, fields);
}

fn publish_to_store(
    session: &DbSession<TestCanister>,
    store_path: &'static str,
    expected: AcceptedSchemaRevision,
    revision: AcceptedSchemaRevision,
    snapshots: BTreeMap<EntityTag, PersistedSchemaSnapshot>,
    fields: BTreeMap<(EntityTag, FieldSourceKey), FieldId>,
) {
    let candidate = accepted_schema_candidate_with_field_bindings_for_tests(
        store_path, revision, snapshots, fields,
    );
    let store = session
        .db
        .store_handle(store_path)
        .expect("typed adapter test store should resolve");
    crate::db::commit::publish_accepted_schema_candidate(store_path, store, expected, &candidate)
        .expect("typed binding candidate should publish");
}

fn initialize_typed_session() -> DbSession<TestCanister> {
    let entity_tag = EntityTag::new(91);
    DATA_STORE.with(|store| *store.borrow_mut() = DataStore::init_heap());
    INDEX_STORE.with(|store| *store.borrow_mut() = IndexStore::init_heap());
    SCHEMA_STORE.with(|store| *store.borrow_mut() = SchemaStore::init_heap());
    OTHER_DATA_STORE.with(|store| *store.borrow_mut() = DataStore::init_heap());
    OTHER_INDEX_STORE.with(|store| *store.borrow_mut() = IndexStore::init_heap());
    OTHER_SCHEMA_STORE.with(|store| *store.borrow_mut() = SchemaStore::init_heap());
    let session = DbSession::<TestCanister>::new(
        &STORE_REGISTRY,
        &crate::db::RequestExecutionRoot::__new_runtime_root(),
    );
    session
        .db
        .drive_startup_recovery_page()
        .expect("typed adapter test database should initialize");
    publish(
        &session,
        AcceptedSchemaRevision::NONE,
        AcceptedSchemaRevision::INITIAL,
        BTreeMap::from([(
            entity_tag,
            snapshot(
                ENTITY_SOURCE,
                "Entity",
                vec![nat64_field(1, "id", 0), nat64_field(2, "value", 1)],
            ),
        )]),
        BTreeMap::from([
            ((entity_tag, field_source(ID_SOURCE)), FieldId::new(1)),
            ((entity_tag, field_source(VALUE_SOURCE)), FieldId::new(2)),
        ]),
    );
    session
}

fn initialize_mixed_typed_session(other_store: bool) -> DbSession<TestCanister> {
    let entity_tag = EntityTag::new(91);
    let other_entity_tag = EntityTag::new(92);
    DATA_STORE.with(|store| *store.borrow_mut() = DataStore::init_heap());
    INDEX_STORE.with(|store| *store.borrow_mut() = IndexStore::init_heap());
    SCHEMA_STORE.with(|store| *store.borrow_mut() = SchemaStore::init_heap());
    OTHER_DATA_STORE.with(|store| *store.borrow_mut() = DataStore::init_heap());
    OTHER_INDEX_STORE.with(|store| *store.borrow_mut() = IndexStore::init_heap());
    OTHER_SCHEMA_STORE.with(|store| *store.borrow_mut() = SchemaStore::init_heap());
    let session = DbSession::<TestCanister>::new(
        &STORE_REGISTRY,
        &crate::db::RequestExecutionRoot::__new_runtime_root(),
    );
    session
        .db
        .drive_startup_recovery_page()
        .expect("mixed typed adapter database should initialize");

    let entity_snapshot = snapshot(
        ENTITY_SOURCE,
        "Entity",
        vec![nat64_field(1, "id", 0), nat64_field(2, "value", 1)],
    );
    let other_snapshot = snapshot(
        OTHER_ENTITY_SOURCE,
        "OtherEntity",
        vec![nat64_field(1, "id", 0)],
    );
    let entity_fields = BTreeMap::from([
        ((entity_tag, field_source(ID_SOURCE)), FieldId::new(1)),
        ((entity_tag, field_source(VALUE_SOURCE)), FieldId::new(2)),
    ]);
    if other_store {
        publish(
            &session,
            AcceptedSchemaRevision::NONE,
            AcceptedSchemaRevision::INITIAL,
            BTreeMap::from([(entity_tag, entity_snapshot)]),
            entity_fields,
        );
        publish_to_store(
            &session,
            OTHER_STORE_PATH,
            AcceptedSchemaRevision::NONE,
            AcceptedSchemaRevision::INITIAL,
            BTreeMap::from([(other_entity_tag, other_snapshot)]),
            BTreeMap::from([(
                (other_entity_tag, field_source(OTHER_ID_SOURCE)),
                FieldId::new(1),
            )]),
        );
    } else {
        let mut fields = entity_fields;
        fields.insert(
            (other_entity_tag, field_source(OTHER_ID_SOURCE)),
            FieldId::new(1),
        );
        publish(
            &session,
            AcceptedSchemaRevision::NONE,
            AcceptedSchemaRevision::INITIAL,
            BTreeMap::from([
                (entity_tag, entity_snapshot),
                (other_entity_tag, other_snapshot),
            ]),
            fields,
        );
    }
    session
}

fn typed_insert(binding: &DynamicTypedEntityBinding, id: u64, value: u64) -> DynamicTypedMutation {
    let patch = binding
        .bind_write_ordinals(vec![
            (0, DynamicWriteCell::Value(InputValue::nat64(id))),
            (1, DynamicWriteCell::Value(InputValue::nat64(value))),
        ])
        .expect("typed insert patch should bind");
    DynamicTypedMutation::Insert { patch }
}

fn typed_other_insert(binding: &DynamicTypedEntityBinding, id: u64) -> DynamicTypedMutation {
    let patch = binding
        .bind_write_ordinals(vec![(0, DynamicWriteCell::Value(InputValue::nat64(id)))])
        .expect("other typed insert patch should bind");
    DynamicTypedMutation::Insert { patch }
}

fn typed_delete(id: u64) -> DynamicTypedMutation {
    DynamicTypedMutation::Delete {
        key: InputValue::nat64(id),
    }
}

fn typed_value_patch(
    binding: &DynamicTypedEntityBinding,
    value: u64,
) -> super::DynamicTypedStructuralPatch {
    binding
        .bind_write_ordinals(vec![(1, DynamicWriteCell::Value(InputValue::nat64(value)))])
        .expect("typed value patch should bind")
}

fn assert_query_diagnostic(
    error: crate::db::QueryError,
    code: icydb_diagnostic_code::DiagnosticCode,
    origin: icydb_diagnostic_code::ErrorOrigin,
    detail: icydb_diagnostic_code::DiagnosticDetail,
) {
    let diagnostic = error.diagnostic();
    assert_eq!(diagnostic.code(), code);
    assert_eq!(diagnostic.origin(), origin);
    assert_eq!(diagnostic.detail(), Some(&detail));
}

#[test]
fn typed_adapter_kind_matching_is_exact_but_accepts_relation_key_wrappers() {
    let relation = AcceptedFieldKind::Relation {
        target_path: "test::Target".to_string(),
        target_entity_name: "Target".to_string(),
        target_entity_tag: EntityTag::new(7),
        target_store_path: "test::Store".to_string(),
        key_kind: Box::new(AcceptedFieldKind::Nat64),
    };

    assert!(typed_adapter_field_kind_matches(
        &relation,
        &AcceptedFieldKind::Nat64,
    ));
    assert!(typed_adapter_field_kind_matches(
        &AcceptedFieldKind::List(Box::new(relation)),
        &AcceptedFieldKind::List(Box::new(AcceptedFieldKind::Nat64)),
    ));
    assert!(!typed_adapter_field_kind_matches(
        &AcceptedFieldKind::Nat64,
        &AcceptedFieldKind::Nat32,
    ));
}

#[test]
fn typed_adapter_field_contract_rejects_invalid_named_source_identity() {
    const NAT64: TypedFieldType = TypedFieldType::Scalar(ScalarType::Nat64);

    assert!(matches!(
        typed_descriptor_field_type(TypedFieldType::Named("")),
        Err(icydb_schema::SchemaContractError::EmptyIdentity),
    ));
    assert!(matches!(
        typed_descriptor_field_type(TypedFieldType::Scalar(ScalarType::Nat16)),
        Ok(icydb_schema::FieldType::Scalar(ScalarType::Nat16)),
    ));
    assert!(matches!(
        typed_descriptor_field_type(TypedFieldType::List(&NAT64)),
        Ok(icydb_schema::FieldType::List(item))
            if *item == icydb_schema::FieldType::Scalar(ScalarType::Nat64),
    ));
}

#[test]
fn typed_descriptor_primary_key_must_match_accepted_source_order() {
    const PRIMARY_KEY_MISMATCH: TypedEntityDescriptor =
        TypedEntityDescriptor::new(ENTITY_SOURCE, &[VALUE_SOURCE], ENTITY_DESCRIPTOR.fields);
    const NULLABILITY_MISMATCH: TypedEntityDescriptor = TypedEntityDescriptor::new(
        ENTITY_SOURCE,
        &[ID_SOURCE],
        &[
            TypedFieldDescriptor::new(ID_SOURCE, TypedFieldType::Scalar(ScalarType::Nat64), false),
            TypedFieldDescriptor::new(
                VALUE_SOURCE,
                TypedFieldType::Scalar(ScalarType::Nat64),
                true,
            ),
        ],
    );

    let session = initialize_typed_session();
    assert!(matches!(
        session.issue_typed_entity_binding(&PRIMARY_KEY_MISMATCH),
        Err(DynamicTypedBindingError::IncompatibleField),
    ));
    assert!(matches!(
        session.issue_typed_entity_binding(&NULLABILITY_MISMATCH),
        Err(DynamicTypedBindingError::IncompatibleField),
    ));
}

#[test]
fn typed_mutation_batch_is_bounded_and_atomic() {
    let session = initialize_typed_session();
    let binding = session
        .issue_typed_entity_binding(&ENTITY_DESCRIPTOR)
        .expect("typed batch binding should issue");

    session
        .execute_trusted_typed_mutation_batch(Vec::new())
        .expect_err("empty typed batch should reject");
    let insert = typed_insert(&binding, 1, 10);
    let oversized = (0..=super::MAX_STRUCTURAL_MUTATION_BATCH_OPERATIONS)
        .map(|_| (binding.clone(), insert.clone()))
        .collect();
    session
        .execute_trusted_typed_mutation_batch(oversized)
        .expect_err("oversized typed batch should reject");

    let duplicate = vec![
        (binding.clone(), insert.clone()),
        (binding.clone(), typed_insert(&binding, 1, 11)),
    ];
    session
        .execute_trusted_typed_mutation_batch(duplicate)
        .expect_err("late duplicate key should reject the whole typed batch");
    let empty = session
        .execute_trusted_live_page(&crate::db::DynamicQuery::new("Entity"), None)
        .expect("failed typed batch should leave the entity readable");
    assert!(empty.rows.is_empty());

    let result = session
        .execute_trusted_typed_mutation_batch(vec![
            (binding.clone(), insert),
            (binding.clone(), typed_insert(&binding, 2, 20)),
        ])
        .expect("valid typed batch should execute")
        .expect("exact binding should remain current");
    assert_eq!(result.len(), 2);
    assert!(result.iter().all(|item| item.affected_rows == 1));
    assert_eq!(
        result
            .into_iter()
            .map(|item| item.rows.into_iter().next().expect("one row per request"))
            .collect::<Vec<_>>(),
        vec![
            vec![
                crate::value::OutputValue::nat64(1),
                crate::value::OutputValue::nat64(10),
            ],
            vec![
                crate::value::OutputValue::nat64(2),
                crate::value::OutputValue::nat64(20),
            ],
        ]
    );

    let mut mismatched = binding.clone();
    mismatched.accepted_revision = mismatched.accepted_revision.saturating_add(1);
    let mismatch = session
        .execute_trusted_typed_mutation_batch(vec![
            (binding.clone(), typed_insert(&binding, 3, 30)),
            (mismatched.clone(), typed_insert(&binding, 4, 40)),
        ])
        .expect("mismatched typed batch should fail closed");
    assert!(mismatch.is_none());
    let stale = session
        .execute_trusted_typed_mutation_batch(vec![(mismatched, typed_insert(&binding, 5, 50))])
        .expect("stale typed batch should fail closed");
    assert!(stale.is_none());
}

#[test]
fn same_entity_typed_mutation_batch_rejects_empty_oversized_and_stale_input() {
    let session = initialize_typed_session();
    let binding = session
        .issue_typed_entity_binding(&ENTITY_DESCRIPTOR)
        .expect("typed batch binding should issue");

    session
        .execute_trusted_same_entity_typed_mutation_batch(&binding, Vec::new())
        .expect_err("empty same-entity typed batch should reject");
    let insert = typed_insert(&binding, 1, 10);
    session
        .execute_trusted_same_entity_typed_mutation_batch(
            &binding,
            vec![insert.clone(); super::MAX_STRUCTURAL_MUTATION_BATCH_OPERATIONS + 1],
        )
        .expect_err("oversized same-entity typed batch should reject");

    let mut stale = binding;
    stale.accepted_revision = stale.accepted_revision.saturating_add(1);
    let result = session
        .execute_trusted_same_entity_typed_mutation_batch(&stale, vec![insert])
        .expect("stale same-entity typed admission should remain an adapter outcome");
    assert!(result.is_none());
}

#[test]
fn typed_mutation_batch_accepts_mixed_same_store_bindings_and_rejects_late_stale_input() {
    let session = initialize_mixed_typed_session(false);
    let binding = session
        .issue_typed_entity_binding(&ENTITY_DESCRIPTOR)
        .expect("first typed entity should bind");
    let other = session
        .issue_typed_entity_binding(&OTHER_ENTITY_DESCRIPTOR)
        .expect("second typed entity should bind");

    let mut stale_other = other.clone();
    stale_other.accepted_revision = stale_other.accepted_revision.saturating_add(1);
    let stale = session
        .execute_trusted_typed_mutation_batch(vec![
            (binding.clone(), typed_insert(&binding, 1, 10)),
            (stale_other, typed_other_insert(&other, 1)),
        ])
        .expect("stale typed admission should remain an adapter outcome");
    assert!(stale.is_none());
    for entity in ["Entity", "OtherEntity"] {
        let rows = session
            .execute_trusted_live_page(&crate::db::DynamicQuery::new(entity), None)
            .expect("failed mixed admission should leave both entities readable");
        assert!(rows.rows.is_empty());
    }

    let results = session
        .execute_trusted_typed_mutation_batch(vec![
            (other.clone(), typed_other_insert(&other, 2)),
            (binding.clone(), typed_insert(&binding, 3, 30)),
        ])
        .expect("same-store typed batch should execute")
        .expect("both typed bindings should remain current");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].entity, "OtherEntity");
    assert_eq!(
        results[0].rows,
        vec![vec![crate::value::OutputValue::nat64(2)]]
    );
    assert_eq!(results[1].entity, "Entity");
    assert_eq!(
        results[1].rows,
        vec![vec![
            crate::value::OutputValue::nat64(3),
            crate::value::OutputValue::nat64(30),
        ]],
    );
}

#[test]
fn typed_mutation_batch_rejects_cross_store_bindings_before_writes() {
    let session = initialize_mixed_typed_session(true);
    let binding = session
        .issue_typed_entity_binding(&ENTITY_DESCRIPTOR)
        .expect("first store typed entity should bind");
    let other = session
        .issue_typed_entity_binding(&OTHER_ENTITY_DESCRIPTOR)
        .expect("second store typed entity should bind");

    let error = session
        .execute_trusted_typed_mutation_batch(vec![
            (binding.clone(), typed_insert(&binding, 1, 10)),
            (other.clone(), typed_other_insert(&other, 1)),
        ])
        .expect_err("typed cross-store rows must reject");
    assert!(matches!(
        error.diagnostic().detail(),
        Some(icydb_diagnostic_code::DiagnosticDetail::RuntimeBoundary {
            boundary: icydb_diagnostic_code::RuntimeBoundaryCode::MutationBatchStoreMismatch,
        })
    ));
    for entity in ["Entity", "OtherEntity"] {
        let rows = session
            .execute_trusted_live_page(&crate::db::DynamicQuery::new(entity), None)
            .expect("cross-store rejection should leave both entities readable");
        assert!(rows.rows.is_empty());
    }
}

#[test]
fn typed_mutation_batch_rechecks_late_field_identity_under_current_authority() {
    let session = initialize_typed_session();
    let binding = session
        .issue_typed_entity_binding(&ENTITY_DESCRIPTOR)
        .expect("entity should bind");

    // Matching entity/revision/fingerprint is insufficient: every supplied
    // field mapping must still agree with the accepted source binding.
    for (field_id, slot) in [(3, 1), (2, 2)] {
        let mismatched = DynamicTypedEntityBinding::new(
            binding.database_incarnation,
            binding.entity_source.clone(),
            binding.entity_label.clone(),
            binding.entity_tag,
            binding.accepted_revision,
            binding.accepted_fingerprint,
            binding.entity_generation,
            vec![
                (ID_SOURCE.to_string(), 1, 0, "id".to_string()),
                (
                    VALUE_SOURCE.to_string(),
                    field_id,
                    slot,
                    "value".to_string(),
                ),
            ],
            binding.named_types.clone(),
            binding.enum_variants.clone(),
            binding.composite_fields.clone(),
        )
        .expect("distinct field mapping should form an opaque binding");
        let result = session
            .execute_trusted_typed_mutation_batch(vec![
                (binding.clone(), typed_insert(&binding, 1, 10)),
                (mismatched, typed_insert(&binding, 2, 20)),
            ])
            .expect("mismatched mapping should remain an adapter rejection");
        assert!(result.is_none());
        DATA_STORE.with(|store| assert_eq!(store.borrow().len(), 0));
    }

    let result = session
        .execute_trusted_typed_mutation_batch(vec![
            (binding.clone(), typed_insert(&binding, 1, 10)),
            (binding.clone(), typed_insert(&binding, 2, 20)),
        ])
        .expect("corrected batch should execute after rejected borrows")
        .expect("current binding should remain valid");
    assert_eq!(result.len(), 2);
}

#[test]
fn same_entity_typed_mutation_batch_preserves_mixed_result_order() {
    let session = initialize_typed_session();
    let binding = session
        .issue_typed_entity_binding(&ENTITY_DESCRIPTOR)
        .expect("typed batch binding should issue");
    session
        .execute_trusted_same_entity_typed_mutation_batch(
            &binding,
            vec![
                typed_insert(&binding, 1, 10),
                typed_insert(&binding, 2, 20),
                typed_insert(&binding, 4, 40),
            ],
        )
        .expect("typed fixture batch should execute")
        .expect("typed fixture binding should be current");

    let result = session
        .execute_trusted_same_entity_typed_mutation_batch(
            &binding,
            vec![
                DynamicTypedMutation::Update {
                    key: InputValue::nat64(1),
                    patch: typed_value_patch(&binding, 11),
                },
                DynamicTypedMutation::Replace {
                    key: InputValue::nat64(2),
                    patch: typed_value_patch(&binding, 22),
                },
                typed_insert(&binding, 3, 30),
                typed_delete(4),
            ],
        )
        .expect("mixed typed batch should execute")
        .expect("mixed typed binding should remain current");
    assert_eq!(result.len(), 4);
    assert_eq!(result.affected_rows, 4);
    assert_eq!(
        result.rows,
        vec![
            vec![
                crate::value::OutputValue::nat64(1),
                crate::value::OutputValue::nat64(11),
            ],
            vec![
                crate::value::OutputValue::nat64(2),
                crate::value::OutputValue::nat64(22),
            ],
            vec![
                crate::value::OutputValue::nat64(3),
                crate::value::OutputValue::nat64(30),
            ],
            vec![
                crate::value::OutputValue::nat64(4),
                crate::value::OutputValue::nat64(40),
            ],
        ],
    );
}

// Keep the full rename, stale-binding, and old-name-reuse lifecycle in one
// regression so each issued binding is checked against the next revision.
#[expect(clippy::too_many_lines)]
#[test]
fn typed_binding_uses_accepted_ids_and_slots_across_renames_and_name_reuse() {
    let entity_tag = EntityTag::new(91);
    let other_entity_tag = EntityTag::new(92);
    DATA_STORE.with(|store| *store.borrow_mut() = DataStore::init_heap());
    INDEX_STORE.with(|store| *store.borrow_mut() = IndexStore::init_heap());
    SCHEMA_STORE.with(|store| *store.borrow_mut() = SchemaStore::init_heap());
    OTHER_DATA_STORE.with(|store| *store.borrow_mut() = DataStore::init_heap());
    OTHER_INDEX_STORE.with(|store| *store.borrow_mut() = IndexStore::init_heap());
    OTHER_SCHEMA_STORE.with(|store| *store.borrow_mut() = SchemaStore::init_heap());

    let session = DbSession::<TestCanister>::new(
        &STORE_REGISTRY,
        &crate::db::RequestExecutionRoot::__new_runtime_root(),
    );
    session
        .db
        .drive_startup_recovery_page()
        .expect("typed adapter test database should initialize");
    publish(
        &session,
        AcceptedSchemaRevision::NONE,
        AcceptedSchemaRevision::INITIAL,
        BTreeMap::from([(
            entity_tag,
            snapshot(
                ENTITY_SOURCE,
                "Entity",
                vec![nat64_field(1, "id", 0), nat64_field(2, "value", 1)],
            ),
        )]),
        BTreeMap::from([
            ((entity_tag, field_source(ID_SOURCE)), FieldId::new(1)),
            ((entity_tag, field_source(VALUE_SOURCE)), FieldId::new(2)),
        ]),
    );

    let initial_catalog = session
        .find_accepted_schema_catalog_context_for_entity_source_key(ENTITY_SOURCE)
        .expect("initial source catalog lookup should inspect")
        .expect("initial source catalog should exist");
    assert_eq!(initial_catalog.identity().entity_tag(), entity_tag);
    let initial = session
        .issue_typed_entity_binding(&ENTITY_DESCRIPTOR)
        .expect("initial typed binding should issue");
    assert_eq!(initial.field_slot(ID_SOURCE), Some(0));
    assert_eq!(initial.field_slot(VALUE_SOURCE), Some(1));
    assert_eq!(initial.output_field_slot("value"), Some(1));
    let initial_patch = initial
        .bind_write_ordinals(vec![(1, DynamicWriteCell::Value(InputValue::nat64(7)))])
        .expect("source-bound patch should lower");
    assert_eq!(
        initial_patch.fields(),
        &[(1, DynamicWriteCell::Value(InputValue::nat64(7)))]
    );
    assert!(
        initial
            .bind_write_ordinals(vec![(2, DynamicWriteCell::Value(InputValue::nat64(8)),)])
            .is_none(),
        "out-of-range descriptor ordinals must fail closed",
    );
    assert!(
        initial
            .bind_write_ordinals(vec![
                (1, DynamicWriteCell::Omitted),
                (1, DynamicWriteCell::Default),
            ])
            .is_none(),
        "duplicate descriptor ordinals must fail closed",
    );
    assert!(
        initial
            .bind_write_ordinals(vec![
                (1, DynamicWriteCell::Omitted),
                (0, DynamicWriteCell::Default),
            ])
            .is_none(),
        "out-of-order descriptor ordinals must fail closed",
    );

    publish(
        &session,
        AcceptedSchemaRevision::INITIAL,
        AcceptedSchemaRevision::new(2),
        BTreeMap::from([
            (
                entity_tag,
                snapshot(
                    ENTITY_SOURCE,
                    "RenamedEntity",
                    vec![
                        nat64_field(1, "id", 0),
                        nat64_field(2, "renamed_value", 1),
                        nat64_field(3, "value", 2),
                    ],
                ),
            ),
            (
                other_entity_tag,
                snapshot(OTHER_ENTITY_SOURCE, "Entity", vec![nat64_field(1, "id", 0)]),
            ),
        ]),
        BTreeMap::from([
            ((entity_tag, field_source(ID_SOURCE)), FieldId::new(1)),
            ((entity_tag, field_source(VALUE_SOURCE)), FieldId::new(2)),
            (
                (entity_tag, field_source(REPLACEMENT_SOURCE)),
                FieldId::new(3),
            ),
            (
                (other_entity_tag, field_source(OTHER_ID_SOURCE)),
                FieldId::new(1),
            ),
        ]),
    );

    let stale_authority = session
        .ensure_accepted_schema_authority_is_current_for_store_path(
            STORE_PATH,
            initial_catalog.value_catalog_handle().authority(),
        )
        .expect_err("the initial accepted authority must be stale after revision two");
    assert_eq!(
        stale_authority.diagnostic_facts(),
        vec![
            (
                icydb_diagnostic_code::DiagnosticFactTag::ExpectedRevision,
                AcceptedSchemaRevision::INITIAL.get(),
            ),
            (
                icydb_diagnostic_code::DiagnosticFactTag::CurrentRevision,
                AcceptedSchemaRevision::new(2).get(),
            ),
        ],
    );

    assert!(
        !session
            .typed_entity_binding_is_current(&initial)
            .expect("renamed binding currentness should inspect")
    );
    let renamed = session
        .issue_typed_entity_binding(&ENTITY_DESCRIPTOR)
        .expect("renamed source-bound adapter should rebind");
    assert_eq!(renamed.entity(), "RenamedEntity");
    assert_eq!(renamed.field_slot(VALUE_SOURCE), Some(1));
    assert_eq!(renamed.output_field_slot("renamed_value"), Some(1));
    assert_eq!(renamed.output_field_slot("value"), None);

    publish(
        &session,
        AcceptedSchemaRevision::new(2),
        AcceptedSchemaRevision::new(3),
        BTreeMap::from([
            (
                entity_tag,
                snapshot(
                    ENTITY_SOURCE,
                    "RenamedEntity",
                    vec![nat64_field(1, "id", 0), nat64_field(2, "value", 1)],
                ),
            ),
            (
                other_entity_tag,
                snapshot(OTHER_ENTITY_SOURCE, "Entity", vec![nat64_field(1, "id", 0)]),
            ),
        ]),
        BTreeMap::from([
            ((entity_tag, field_source(ID_SOURCE)), FieldId::new(1)),
            (
                (entity_tag, field_source(REPLACEMENT_SOURCE)),
                FieldId::new(2),
            ),
            (
                (other_entity_tag, field_source(OTHER_ID_SOURCE)),
                FieldId::new(1),
            ),
        ]),
    );

    assert!(matches!(
        session.issue_typed_entity_binding(&ENTITY_DESCRIPTOR),
        Err(DynamicTypedBindingError::SourceUnavailable(context))
            if context.entity_source() == ENTITY_SOURCE
                && context.field_source() == Some(VALUE_SOURCE),
    ));
    assert!(
        !session
            .typed_entity_binding_is_current(&renamed)
            .expect("removed source binding should become stale")
    );

    let replacement = session
        .issue_typed_entity_binding(&REPLACEMENT_DESCRIPTOR)
        .expect("explicit replacement source should bind");
    assert!(
        session
            .execute_trusted_typed_mutation(
                &replacement,
                DynamicTypedMutation::Insert {
                    patch: initial_patch
                },
            )
            .expect("cross-binding patch should fail closed")
            .is_none()
    );
    let patch = replacement
        .bind_write_ordinals(vec![
            (0, DynamicWriteCell::Value(InputValue::nat64(1))),
            (1, DynamicWriteCell::Value(InputValue::nat64(9))),
        ])
        .expect("replacement source write should bind by accepted IDs and slots");
    let result = session
        .execute_trusted_typed_mutation(&replacement, DynamicTypedMutation::Insert { patch })
        .expect("typed insert should use the accepted mutation pipeline")
        .expect("replacement binding should remain current");
    assert_eq!(result.entity, "RenamedEntity");
    assert_eq!(result.columns, vec!["id".to_string(), "value".to_string()]);
    assert_eq!(
        result.rows,
        vec![vec![
            crate::value::OutputValue::nat64(1),
            crate::value::OutputValue::nat64(9)
        ]]
    );
    assert_eq!(result.affected_rows, 1);

    let second_patch = replacement
        .bind_write_ordinals(vec![
            (0, DynamicWriteCell::Value(InputValue::nat64(2))),
            (1, DynamicWriteCell::Value(InputValue::nat64(10))),
        ])
        .expect("second source-bound patch should lower");
    session
        .execute_trusted_typed_mutation(
            &replacement,
            DynamicTypedMutation::Insert {
                patch: second_patch,
            },
        )
        .expect("second typed insert should use the accepted mutation pipeline")
        .expect("replacement binding should remain current");

    {
        let query = crate::db::DynamicQuery::new("RenamedEntity")
            .select(["id", "value"])
            .order_by(crate::db::asc("id"))
            .limit(1);
        let result = session
            .execute_trusted_live_page(&query, None)
            .expect("SQL-free dynamic execution should use accepted authority");
        assert_eq!(result.entity, "RenamedEntity");
        assert_eq!(result.columns, vec!["id".to_string(), "value".to_string()]);
        assert_eq!(
            result.rows,
            vec![vec![
                crate::value::OutputValue::nat64(1),
                crate::value::OutputValue::nat64(9)
            ]]
        );
        assert_eq!(result.row_count, 1);
        assert_query_diagnostic(
            session
                .execute_trusted_live_page(&query.cursor("00"), None)
                .expect_err("scalar execution must reject grouped cursor state"),
            icydb_diagnostic_code::DiagnosticCode::QueryIntent,
            icydb_diagnostic_code::ErrorOrigin::Query,
            icydb_diagnostic_code::DiagnosticDetail::QueryKind {
                kind: icydb_diagnostic_code::QueryErrorKind::Intent,
            },
        );
        assert_query_diagnostic(
            session
                .execute_public_dynamic_grouped_query(
                    &crate::db::DynamicQuery::new("RenamedEntity").grouped_limits(1, 1024),
                )
                .expect_err("grouped execution must reject scalar query state"),
            icydb_diagnostic_code::DiagnosticCode::QueryIntent,
            icydb_diagnostic_code::ErrorOrigin::Query,
            icydb_diagnostic_code::DiagnosticDetail::QueryKind {
                kind: icydb_diagnostic_code::QueryErrorKind::Intent,
            },
        );

        let grouped_query = crate::db::DynamicQuery::new("RenamedEntity")
            .filter(crate::db::FieldRef::new("id").eq(1_u64))
            .group_by("value")
            .aggregate(crate::db::count())
            .grouped_limits(1, 16 * 1024)
            .limit(1);
        let grouped = session
            .execute_public_dynamic_grouped_query(&grouped_query)
            .expect("SQL-free grouped execution should use accepted authority");
        let typed_grouped = session
            .execute_public_dynamic_grouped_query_for_typed_binding(&replacement, &grouped_query)
            .expect("typed grouped execution should inspect accepted authority")
            .expect("replacement binding should remain current");
        assert_eq!(typed_grouped, grouped);
        assert!(
            session
                .execute_public_dynamic_grouped_query_for_typed_binding(&renamed, &grouped_query,)
                .expect("stale grouped binding should inspect accepted authority")
                .is_none(),
            "stale typed grouped bindings must fail closed before execution"
        );
        assert_eq!(grouped.entity, "RenamedEntity");
        assert_eq!(grouped.row_count, 1);
        assert_eq!(grouped.rows.len(), 1);
        assert_eq!(
            grouped.rows[0].group_key(),
            &[crate::value::OutputValue::nat64(9)]
        );
        assert_eq!(
            grouped.rows[0].aggregate_values(),
            &[crate::value::OutputValue::nat64(1)]
        );
        assert_eq!(grouped.next_cursor, None);

        let grouped_state_error = session
            .execute_trusted_dynamic_grouped_query(&grouped_query.clone().grouped_limits(1, 1))
            .expect_err("grouped retained state must respect its explicit byte ceiling");
        assert!(matches!(
            grouped_state_error.diagnostic().detail(),
            Some(icydb_diagnostic_code::DiagnosticDetail::RuntimeBoundary {
                boundary: icydb_diagnostic_code::RuntimeBoundaryCode::ExecutionBudgetExceeded,
            })
        ));
        assert_eq!(
            grouped_state_error.diagnostic_facts()[0],
            (
                icydb_diagnostic_code::DiagnosticFactTag::BudgetResource,
                icydb_diagnostic_code::DiagnosticExecutionBudgetResource::GroupDistinctStateBytes
                    .raw(),
            ),
        );

        assert_query_diagnostic(
            session
                .execute_public_dynamic_grouped_query(&grouped_query.clone().select(["value"]))
                .expect_err("grouped output must reject scalar selection"),
            icydb_diagnostic_code::DiagnosticCode::QueryIntent,
            icydb_diagnostic_code::ErrorOrigin::Query,
            icydb_diagnostic_code::DiagnosticDetail::QueryKind {
                kind: icydb_diagnostic_code::QueryErrorKind::Intent,
            },
        );
        assert_query_diagnostic(
            session
                .execute_public_dynamic_grouped_query(
                    &crate::db::DynamicQuery::new("RenamedEntity")
                        .group_by("value")
                        .aggregate(crate::db::count()),
                )
                .expect_err("public grouped execution must require explicit limits"),
            icydb_diagnostic_code::DiagnosticCode::QueryReadAdmission,
            icydb_diagnostic_code::ErrorOrigin::Query,
            icydb_diagnostic_code::DiagnosticDetail::QueryReadAdmission {
                reason: icydb_diagnostic_code::QueryReadAdmissionCode::GroupedQueryRequiresLimits,
            },
        );
        assert_query_diagnostic(
            session
                .execute_trusted_dynamic_grouped_query(
                    &crate::db::DynamicQuery::new("RenamedEntity")
                        .group_by("value")
                        .aggregate(crate::db::count())
                        .grouped_limits(0, 1024),
                )
                .expect_err("trusted grouped execution must reject zero limits"),
            icydb_diagnostic_code::DiagnosticCode::QueryReadAdmission,
            icydb_diagnostic_code::ErrorOrigin::Query,
            icydb_diagnostic_code::DiagnosticDetail::QueryReadAdmission {
                reason: icydb_diagnostic_code::QueryReadAdmissionCode::GroupedQueryRequiresLimits,
            },
        );
        assert_query_diagnostic(
            session
                .execute_public_dynamic_grouped_query(&grouped_query.grouped_limits(101, 1024))
                .expect_err("public grouped execution must enforce its group budget"),
            icydb_diagnostic_code::DiagnosticCode::QueryReadAdmission,
            icydb_diagnostic_code::ErrorOrigin::Query,
            icydb_diagnostic_code::DiagnosticDetail::QueryReadAdmission {
                reason: icydb_diagnostic_code::QueryReadAdmissionCode::GroupedQueryExceedsBudget,
            },
        );

        let paged_query = crate::db::DynamicQuery::new("RenamedEntity")
            .group_by("value")
            .aggregate(crate::db::count())
            .grouped_limits(2, 16 * 1024)
            .limit(1);
        assert_query_diagnostic(
            session
                .execute_public_dynamic_grouped_query(&paged_query)
                .expect_err("public grouped execution must reject an unbounded full scan"),
            icydb_diagnostic_code::DiagnosticCode::QueryReadAdmission,
            icydb_diagnostic_code::ErrorOrigin::Query,
            icydb_diagnostic_code::DiagnosticDetail::QueryReadAdmission {
                reason: icydb_diagnostic_code::QueryReadAdmissionCode::UnboundedFullScanRejected,
            },
        );
        let first_page = session
            .execute_trusted_dynamic_grouped_query(&paged_query)
            .expect("SQL-free grouped first page should execute");
        assert_eq!(first_page.row_count, 1);
        assert_eq!(
            first_page.rows[0].group_key(),
            &[crate::value::OutputValue::nat64(9)]
        );
        let cursor = first_page
            .next_cursor
            .expect("first grouped page should return a continuation cursor");
        assert_query_diagnostic(
            session
                .execute_trusted_dynamic_grouped_query(
                    &paged_query.clone().cursor(format!("{cursor}0")),
                )
                .expect_err("tampered grouped cursor must fail closed"),
            icydb_diagnostic_code::DiagnosticCode::QueryInvalidContinuationCursor,
            icydb_diagnostic_code::ErrorOrigin::Cursor,
            icydb_diagnostic_code::DiagnosticDetail::QueryKind {
                kind: icydb_diagnostic_code::QueryErrorKind::InvalidContinuationCursor,
            },
        );
        let second_page = session
            .execute_trusted_dynamic_grouped_query(&paged_query.cursor(cursor))
            .expect("SQL-free grouped continuation should execute");
        assert_eq!(second_page.row_count, 1);
        assert_eq!(
            second_page.rows[0].group_key(),
            &[crate::value::OutputValue::nat64(10)]
        );
        assert_eq!(second_page.next_cursor, None);
    }
}
