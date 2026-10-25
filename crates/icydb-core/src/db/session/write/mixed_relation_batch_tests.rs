use super::{DbSession, DynamicMutation, DynamicStructuralPatch, DynamicWriteCell};
use crate::{
    db::{
        DynamicQuery, asc,
        data::DataStore,
        desc,
        index::IndexStore,
        query::expr::FilterExpr,
        registry::{StoreAllocationIdentities, StoreRegistry, StoreRuntimeStorageCapabilities},
        schema::{
            AcceptedConstraintCatalog, AcceptedFieldKind, AcceptedSchemaRevision, FieldId,
            FieldStorageDecode, FieldWriteManagement, LeafCodec, PersistedFieldSnapshot,
            PersistedIndexFieldPathSnapshot, PersistedIndexKeySnapshot, PersistedIndexSnapshot,
            PersistedRelationEdgeSnapshot, PersistedSchemaSnapshot, RelationId, ScalarCodec,
            SchemaFieldSlot, SchemaFieldWritePolicy, SchemaIndexId, SchemaInsertDefault,
            SchemaRowLayout, SchemaStore, SchemaVersion,
            accepted_schema_candidate_with_field_bindings_for_tests,
        },
    },
    error::{ErrorClass, ErrorOrigin},
    traits::{CanisterKind, Path},
    types::EntityTag,
    value::{InputValue, OutputValue},
};
use icydb_schema::FieldSourceKey;
use std::{cell::RefCell, collections::BTreeMap};

const STORE_PATH: &str = "session::write::mixed_relation_batch_tests::Store";
const ENTITY_SOURCE: &str = "session::write::mixed_relation_batch_tests::Node";
const ID_SOURCE: &str = "session::write::mixed_relation_batch_tests::Node::id";
const PARENT_SOURCE: &str = "session::write::mixed_relation_batch_tests::Node::parent_id";
const CODE_SOURCE: &str = "session::write::mixed_relation_batch_tests::Node::code";
const ENTITY_NAME: &str = "MixedRelationNode";
const ENTITY_TAG: EntityTag = EntityTag::new(94);
const OTHER_ENTITY_SOURCE: &str = "session::write::mixed_relation_batch_tests::Other";
const OTHER_ID_SOURCE: &str = "session::write::mixed_relation_batch_tests::Other::id";
const OTHER_VALUE_SOURCE: &str = "session::write::mixed_relation_batch_tests::Other::value";
const OTHER_NODE_SOURCE: &str = "session::write::mixed_relation_batch_tests::Other::node_id";
const OTHER_ENTITY_NAME: &str = "MixedRelationOther";
const OTHER_ENTITY_TAG: EntityTag = EntityTag::new(95);
const CROSS_STORE_PATH: &str = "session::write::mixed_relation_batch_tests::OtherStore";
const CROSS_ENTITY_SOURCE: &str = "session::write::mixed_relation_batch_tests::CrossStore";
const CROSS_ID_SOURCE: &str = "session::write::mixed_relation_batch_tests::CrossStore::id";
const CROSS_ENTITY_NAME: &str = "MixedCrossStore";
const CROSS_ENTITY_TAG: EntityTag = EntityTag::new(2_000);
fn batch_rows(results: &[crate::db::DynamicMutationResult]) -> Vec<Vec<OutputValue>> {
    results
        .iter()
        .flat_map(|result| result.rows.iter().cloned())
        .collect()
}

struct TestCanister;

impl Path for TestCanister {
    const PATH: &'static str = "session::write::mixed_relation_batch_tests::Canister";
}

impl CanisterKind for TestCanister {
    fn commit_memory_id() -> Result<u8, ic_memory::RuntimeOpenError> {
        Ok(47)
    }
    const COMMIT_STABLE_KEY: &'static str = "icydb.mixed_relation_batch_tests.commit.v1";
    fn startup_memory_id() -> Result<u8, ic_memory::RuntimeOpenError> {
        Ok(50)
    }
    const STARTUP_STABLE_KEY: &'static str = "icydb.mixed_relation_batch_tests.startup.control.v1";
    fn integrity_progress_memory_id() -> Result<u8, ic_memory::RuntimeOpenError> {
        Ok(48)
    }
    const INTEGRITY_PROGRESS_STABLE_KEY: &'static str =
        "icydb.mixed_relation_batch_tests.integrity.progress.v1";
}

thread_local! {
    static DATA_STORE: RefCell<DataStore> = const { RefCell::new(DataStore::init_heap()) };
    static INDEX_STORE: RefCell<IndexStore> = const { RefCell::new(IndexStore::init_heap()) };
    static SCHEMA_STORE: RefCell<SchemaStore> =
        const { RefCell::new(SchemaStore::init_heap()) };
    static CROSS_DATA_STORE: RefCell<DataStore> = const { RefCell::new(DataStore::init_heap()) };
    static CROSS_INDEX_STORE: RefCell<IndexStore> = const { RefCell::new(IndexStore::init_heap()) };
    static CROSS_SCHEMA_STORE: RefCell<SchemaStore> =
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
        ).expect("mixed relation test store should register");
        registry.register_store(
            CROSS_STORE_PATH,
            &CROSS_DATA_STORE,
            &CROSS_INDEX_STORE,
            &CROSS_SCHEMA_STORE,
            StoreAllocationIdentities::absent(),
            StoreRuntimeStorageCapabilities::heap(),
        ).expect("cross-store test store should register");
        registry
    };
}

fn source_key(source: &str) -> FieldSourceKey {
    FieldSourceKey::try_new(source).expect("mixed relation field source should admit")
}

fn relation_snapshot() -> PersistedSchemaSnapshot {
    let fields = vec![
        PersistedFieldSnapshot::new_initial(
            FieldId::new(1),
            "id".to_string(),
            SchemaFieldSlot::new(0),
            AcceptedFieldKind::Nat64,
            Vec::new(),
            false,
            SchemaInsertDefault::None,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Nat64),
        ),
        PersistedFieldSnapshot::new_initial(
            FieldId::new(2),
            "parent_id".to_string(),
            SchemaFieldSlot::new(1),
            AcceptedFieldKind::Nat64,
            Vec::new(),
            true,
            SchemaInsertDefault::None,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Nat64),
        ),
        PersistedFieldSnapshot::new_initial(
            FieldId::new(3),
            "code".to_string(),
            SchemaFieldSlot::new(2),
            AcceptedFieldKind::Nat64,
            Vec::new(),
            false,
            SchemaInsertDefault::None,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Nat64),
        ),
    ];
    let relation = PersistedRelationEdgeSnapshot::new_direct(
        RelationId::new(1).expect("mixed relation identity should be non-zero"),
        "parent".to_string(),
        ENTITY_SOURCE.to_string(),
        vec![FieldId::new(2)],
    );
    let snapshot = PersistedSchemaSnapshot::new_with_indexes(
        SchemaVersion::initial(),
        ENTITY_SOURCE.to_string(),
        ENTITY_NAME.to_string(),
        FieldId::new(1),
        SchemaRowLayout::initial(
            fields
                .iter()
                .map(|field| (field.id(), field.slot()))
                .collect(),
        ),
        fields,
        vec![PersistedIndexSnapshot::new(
            SchemaIndexId::new(1).expect("mixed unique index identity should be non-zero"),
            1,
            "by_code".to_string(),
            STORE_PATH.to_string(),
            true,
            PersistedIndexKeySnapshot::from_field_paths(vec![
                PersistedIndexFieldPathSnapshot::new(
                    FieldId::new(3),
                    SchemaFieldSlot::new(2),
                    vec!["code".to_string()],
                    AcceptedFieldKind::Nat64,
                    false,
                ),
            ]),
            None,
        )],
    )
    .with_relations(vec![relation]);
    let constraints = AcceptedConstraintCatalog::initial(
        snapshot.fields(),
        snapshot.indexes(),
        snapshot.relations(),
    )
    .expect("mixed relation constraints should close");
    snapshot.with_constraint_catalog(constraints)
}

fn other_snapshot() -> PersistedSchemaSnapshot {
    let fields = vec![
        PersistedFieldSnapshot::new_initial(
            FieldId::new(1),
            "id".to_string(),
            SchemaFieldSlot::new(0),
            AcceptedFieldKind::Nat64,
            Vec::new(),
            false,
            SchemaInsertDefault::None,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Nat64),
        ),
        PersistedFieldSnapshot::new_initial(
            FieldId::new(2),
            "value".to_string(),
            SchemaFieldSlot::new(1),
            AcceptedFieldKind::Nat64,
            Vec::new(),
            false,
            SchemaInsertDefault::None,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Nat64),
        ),
        PersistedFieldSnapshot::new_initial(
            FieldId::new(3),
            "node_id".to_string(),
            SchemaFieldSlot::new(2),
            AcceptedFieldKind::Nat64,
            Vec::new(),
            true,
            SchemaInsertDefault::None,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Nat64),
        ),
    ];
    let relation = PersistedRelationEdgeSnapshot::new_direct(
        RelationId::new(1).expect("cross-entity relation identity should be non-zero"),
        "node".to_string(),
        ENTITY_SOURCE.to_string(),
        vec![FieldId::new(3)],
    );
    let snapshot = PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        OTHER_ENTITY_SOURCE.to_string(),
        OTHER_ENTITY_NAME.to_string(),
        FieldId::new(1),
        SchemaRowLayout::initial(
            fields
                .iter()
                .map(|field| (field.id(), field.slot()))
                .collect(),
        ),
        fields,
    )
    .with_relations(vec![relation]);
    let constraints = AcceptedConstraintCatalog::initial(
        snapshot.fields(),
        snapshot.indexes(),
        snapshot.relations(),
    )
    .expect("cross-entity relation constraints should close");
    snapshot.with_constraint_catalog(constraints)
}

fn bounded_entity_snapshot(index: usize) -> PersistedSchemaSnapshot {
    let fields = vec![
        PersistedFieldSnapshot::new_initial(
            FieldId::new(1),
            "id".to_string(),
            SchemaFieldSlot::new(0),
            AcceptedFieldKind::Nat64,
            Vec::new(),
            false,
            SchemaInsertDefault::None,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Nat64),
        ),
        PersistedFieldSnapshot::new_initial_with_write_policy(
            FieldId::new(2),
            "updated_at".to_string(),
            SchemaFieldSlot::new(1),
            AcceptedFieldKind::Timestamp,
            Vec::new(),
            false,
            SchemaInsertDefault::None,
            SchemaFieldWritePolicy::from_model_policies(
                None,
                Some(FieldWriteManagement::UpdatedAt),
            ),
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Timestamp),
        ),
    ];
    PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        format!("session::write::mixed_relation_batch_tests::Bounded{index}"),
        format!("MixedBounded{index}"),
        FieldId::new(1),
        SchemaRowLayout::initial(
            fields
                .iter()
                .map(|field| (field.id(), field.slot()))
                .collect(),
        ),
        fields,
    )
}

fn cross_store_snapshot() -> PersistedSchemaSnapshot {
    let field = PersistedFieldSnapshot::new_initial(
        FieldId::new(1),
        "id".to_string(),
        SchemaFieldSlot::new(0),
        AcceptedFieldKind::Nat64,
        Vec::new(),
        false,
        SchemaInsertDefault::None,
        FieldStorageDecode::ByKind,
        LeafCodec::Scalar(ScalarCodec::Nat64),
    );
    PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        CROSS_ENTITY_SOURCE.to_string(),
        CROSS_ENTITY_NAME.to_string(),
        FieldId::new(1),
        SchemaRowLayout::initial(vec![(field.id(), field.slot())]),
        vec![field],
    )
}

fn initialize() -> DbSession<TestCanister> {
    DATA_STORE.with(|store| *store.borrow_mut() = DataStore::init_heap());
    INDEX_STORE.with(|store| *store.borrow_mut() = IndexStore::init_heap());
    SCHEMA_STORE.with(|store| *store.borrow_mut() = SchemaStore::init_heap());
    CROSS_DATA_STORE.with(|store| *store.borrow_mut() = DataStore::init_heap());
    CROSS_INDEX_STORE.with(|store| *store.borrow_mut() = IndexStore::init_heap());
    CROSS_SCHEMA_STORE.with(|store| *store.borrow_mut() = SchemaStore::init_heap());
    let session = DbSession::<TestCanister>::new(
        &STORE_REGISTRY,
        &crate::db::RequestExecutionRoot::__new_runtime_root(),
    );
    session
        .db
        .drive_startup_recovery_page()
        .expect("mixed relation database should initialize");
    let mut snapshots = BTreeMap::from([
        (ENTITY_TAG, relation_snapshot()),
        (OTHER_ENTITY_TAG, other_snapshot()),
    ]);
    let mut field_bindings = BTreeMap::from([
        ((ENTITY_TAG, source_key(ID_SOURCE)), FieldId::new(1)),
        ((ENTITY_TAG, source_key(PARENT_SOURCE)), FieldId::new(2)),
        ((ENTITY_TAG, source_key(CODE_SOURCE)), FieldId::new(3)),
        (
            (OTHER_ENTITY_TAG, source_key(OTHER_ID_SOURCE)),
            FieldId::new(1),
        ),
        (
            (OTHER_ENTITY_TAG, source_key(OTHER_VALUE_SOURCE)),
            FieldId::new(2),
        ),
        (
            (OTHER_ENTITY_TAG, source_key(OTHER_NODE_SOURCE)),
            FieldId::new(3),
        ),
    ]);
    for index in 0..65 {
        let tag = EntityTag::new(1_000 + index as u64);
        snapshots.insert(tag, bounded_entity_snapshot(index));
        field_bindings.insert(
            (
                tag,
                source_key(
                    format!("session::write::mixed_relation_batch_tests::Bounded{index}::id")
                        .as_str(),
                ),
            ),
            FieldId::new(1),
        );
        field_bindings.insert(
            (
                tag,
                source_key(
                    format!(
                        "session::write::mixed_relation_batch_tests::Bounded{index}::updated_at"
                    )
                    .as_str(),
                ),
            ),
            FieldId::new(2),
        );
    }
    let candidate = accepted_schema_candidate_with_field_bindings_for_tests(
        STORE_PATH,
        AcceptedSchemaRevision::INITIAL,
        snapshots,
        field_bindings,
    );
    let store = session
        .db
        .store_handle(STORE_PATH)
        .expect("mixed relation store should resolve");
    crate::db::commit::publish_accepted_schema_candidate(
        STORE_PATH,
        store,
        AcceptedSchemaRevision::NONE,
        &candidate,
    )
    .expect("mixed relation candidate should publish");
    let cross_candidate = accepted_schema_candidate_with_field_bindings_for_tests(
        CROSS_STORE_PATH,
        AcceptedSchemaRevision::INITIAL,
        BTreeMap::from([(CROSS_ENTITY_TAG, cross_store_snapshot())]),
        BTreeMap::from([(
            (CROSS_ENTITY_TAG, source_key(CROSS_ID_SOURCE)),
            FieldId::new(1),
        )]),
    );
    let cross_store = session
        .db
        .store_handle(CROSS_STORE_PATH)
        .expect("cross-store fixture should resolve");
    crate::db::commit::publish_accepted_schema_candidate(
        CROSS_STORE_PATH,
        cross_store,
        AcceptedSchemaRevision::NONE,
        &cross_candidate,
    )
    .expect("cross-store candidate should publish");
    session
}

fn patch(id: Option<u64>, parent: Option<u64>, code: Option<u64>) -> DynamicStructuralPatch {
    let mut fields = Vec::new();
    if let Some(id) = id {
        fields.push((
            "id".to_string(),
            DynamicWriteCell::Value(InputValue::nat64(id)),
        ));
    }
    fields.push((
        "parent_id".to_string(),
        parent.map_or(DynamicWriteCell::Null, |parent| {
            DynamicWriteCell::Value(InputValue::nat64(parent))
        }),
    ));
    if let Some(code) = code {
        fields.push((
            "code".to_string(),
            DynamicWriteCell::Value(InputValue::nat64(code)),
        ));
    }
    DynamicStructuralPatch::new(fields)
}

fn insert(id: u64, parent: Option<u64>) -> DynamicMutation {
    insert_with_code(id, parent, id)
}

fn insert_with_code(id: u64, parent: Option<u64>, code: u64) -> DynamicMutation {
    DynamicMutation::Insert {
        entity: ENTITY_NAME.to_string(),
        patch: patch(Some(id), parent, Some(code)),
    }
}

fn update_parent(id: u64, parent: Option<u64>) -> DynamicMutation {
    DynamicMutation::Update {
        entity: ENTITY_NAME.to_string(),
        key: InputValue::nat64(id),
        patch: patch(None, parent, None),
    }
}

fn update_code(id: u64, code: u64) -> DynamicMutation {
    DynamicMutation::Update {
        entity: ENTITY_NAME.to_string(),
        key: InputValue::nat64(id),
        patch: DynamicStructuralPatch::new(vec![(
            "code".to_string(),
            DynamicWriteCell::Value(InputValue::nat64(code)),
        )]),
    }
}

fn delete(id: u64) -> DynamicMutation {
    DynamicMutation::Delete {
        entity: ENTITY_NAME.to_string(),
        key: InputValue::nat64(id),
    }
}

fn expected_row(id: u64, parent: Option<u64>) -> Vec<OutputValue> {
    expected_row_with_code(id, parent, id)
}

fn expected_row_with_code(id: u64, parent: Option<u64>, code: u64) -> Vec<OutputValue> {
    vec![
        OutputValue::nat64(id),
        parent.map_or_else(OutputValue::null, OutputValue::nat64),
        OutputValue::nat64(code),
    ]
}

fn other_patch(id: Option<u64>, value: u64) -> DynamicStructuralPatch {
    other_patch_with_node(id, value, None)
}

fn other_patch_with_node(
    id: Option<u64>,
    value: u64,
    node_id: Option<u64>,
) -> DynamicStructuralPatch {
    let mut fields = Vec::new();
    if let Some(id) = id {
        fields.push((
            "id".to_string(),
            DynamicWriteCell::Value(InputValue::nat64(id)),
        ));
    }
    fields.push((
        "value".to_string(),
        DynamicWriteCell::Value(InputValue::nat64(value)),
    ));
    fields.push((
        "node_id".to_string(),
        node_id.map_or(DynamicWriteCell::Null, |node_id| {
            DynamicWriteCell::Value(InputValue::nat64(node_id))
        }),
    ));
    DynamicStructuralPatch::new(fields)
}

fn assert_relation_violation(error: &crate::error::InternalError) {
    assert!(error.diagnostic_facts().contains(&(
        icydb_diagnostic_code::DiagnosticFactTag::ConstraintKind,
        icydb_diagnostic_code::DiagnosticConstraintKind::Relation.raw(),
    )));
}

#[test]
fn live_pages_finish_exact_full_windows_without_an_empty_followup() {
    for count in [0_u64, 1, 2, 3, 4, 5] {
        let session = initialize();
        if count > 0 {
            session
                .execute_trusted_dynamic_mutation_batch(
                    (1..=count)
                        .map(|id| insert_with_code(id, None, id * 10))
                        .collect(),
                )
                .expect("page fixture rows should insert");
        }
        if count == 2 {
            let query = DynamicQuery::new(ENTITY_NAME)
                .select(["id"])
                .filter(crate::db::FieldRef::new("code").gte(InputValue::nat64(10)))
                .filter(crate::db::FieldRef::new("code").lte(InputValue::nat64(20)))
                .order_by(desc("code"));
            let page = session.execute_public_live_page(&query, None).unwrap();
            assert_eq!(page.rows.len(), 2);
            assert_eq!(page.continuation, None);
        }
        for order in [asc("id"), desc("code"), desc("parent_id")] {
            for identity in [false, true] {
                let mut query = DynamicQuery::new(ENTITY_NAME).order_by(order.clone());
                if !identity {
                    query = query.select(["id"]);
                }
                let mut cursor = None;
                let mut observed = 0;
                loop {
                    let page = session
                        .execute_trusted_live_page(&query, cursor.as_deref())
                        .expect("bounded live page should execute");
                    observed += page.rows.len();
                    assert_eq!(
                        page.continuation.is_some(),
                        observed < usize::try_from(count).unwrap()
                    );
                    if page.continuation.is_none() {
                        break;
                    }
                    assert!(!page.rows.is_empty());
                    cursor = page.continuation;
                }
                assert_eq!(observed, usize::try_from(count).unwrap());
            }
        }
    }
}

#[test]
fn live_pages_resume_mixed_projection_from_authenticated_hidden_order_values() {
    let session = initialize();
    session
        .execute_trusted_dynamic_mutation_batch(vec![
            insert_with_code(1, None, 10),
            insert_with_code(2, Some(1), 20),
            insert_with_code(3, None, 30),
        ])
        .expect("live-page rows should insert");
    let query = DynamicQuery::new(ENTITY_NAME)
        .select(["id"])
        // Keep the public read selective while exercising hidden order
        // values and continuation across the three fixture rows.
        .filter(crate::db::FieldRef::new("code").gte(InputValue::nat64(10)))
        .filter(crate::db::FieldRef::new("code").lte(InputValue::nat64(30)))
        .order_by(desc("code"));

    let first = session
        .execute_public_live_page(&query, None)
        .expect("initial live page should execute");
    assert_eq!(
        first.rows,
        vec![vec![OutputValue::nat64(3)], vec![OutputValue::nat64(2)]]
    );
    let cursor = first
        .continuation
        .as_deref()
        .expect("unreturned matching row should produce continuation");
    let second = session
        .execute_public_live_page(&query, Some(cursor))
        .expect("authenticated live continuation should resume");
    assert_eq!(second.rows, vec![vec![OutputValue::nat64(1)]]);
    assert_eq!(second.continuation, None);

    let total_limit = session
        .execute_public_live_page(&query.clone().limit(2), None)
        .expect("total live-page limit should execute");
    assert_eq!(
        total_limit.rows,
        vec![vec![OutputValue::nat64(3)], vec![OutputValue::nat64(2)]],
    );
    assert_eq!(
        total_limit.continuation, None,
        "query LIMIT is a total traversal window rather than a page size",
    );

    let three_row_window = query.clone().limit(3);
    let limited_first = session
        .execute_public_live_page(&three_row_window, None)
        .expect("first total-window page should execute");
    let limited_cursor = limited_first
        .continuation
        .as_deref()
        .expect("a partially consumed total window should continue");
    let limited_second = session
        .execute_public_live_page(&three_row_window, Some(limited_cursor))
        .expect("remaining total window should preserve the plan signature");
    assert_eq!(limited_second.rows, vec![vec![OutputValue::nat64(1)]]);
    assert_eq!(limited_second.continuation, None);

    let mixed_order = DynamicQuery::new(ENTITY_NAME)
        .select(["id"])
        .order_by(desc("parent_id"))
        .order_by(asc("id"));
    let mixed_first = session
        .execute_trusted_live_page(&mixed_order, None)
        .expect("mixed-direction nullable order should execute");
    assert_eq!(
        mixed_first.rows,
        vec![vec![OutputValue::nat64(2)], vec![OutputValue::nat64(1)]],
    );
    let mixed_cursor = mixed_first
        .continuation
        .as_deref()
        .expect("duplicate null order values should retain continuation");
    let mixed_second = session
        .execute_trusted_live_page(&mixed_order, Some(mixed_cursor))
        .expect("mixed-direction nullable order should resume");
    assert_eq!(mixed_second.rows, vec![vec![OutputValue::nat64(3)]]);
    assert_eq!(mixed_second.continuation, None);

    let mismatched_window = session
        .execute_public_live_page(&query.clone().limit(3), Some(cursor))
        .expect_err("a changed total limit must invalidate the continuation");
    assert_eq!(
        mismatched_window.diagnostic_code(),
        icydb_diagnostic_code::DiagnosticCode::QueryInvalidContinuationCursor,
    );

    let mut tampered = cursor.as_bytes().to_vec();
    let last = tampered.len().saturating_sub(1);
    tampered[last] = if tampered[last] == b'0' { b'1' } else { b'0' };
    let tampered = String::from_utf8(tampered).expect("Base64 cursor should remain UTF-8");
    let error = session
        .execute_public_live_page(&query, Some(tampered.as_str()))
        .expect_err("tampered cursor must fail closed");
    assert_eq!(
        error.diagnostic_code(),
        icydb_diagnostic_code::DiagnosticCode::QueryInvalidContinuationCursor,
    );
}

#[test]
fn live_pages_resume_across_changed_output_work_envelopes() {
    let session = initialize();
    session
        .execute_trusted_dynamic_mutation_batch(vec![
            insert(1, None),
            insert(2, None),
            insert(3, None),
        ])
        .expect("output-envelope rows should insert");
    let query = DynamicQuery::new(ENTITY_NAME)
        .select(["id"])
        .order_by(desc("code"));
    let first = session
        .execute_trusted_live_page_with_result_bytes_limit_for_tests(&query, None, 32)
        .expect("small output envelope should publish the first bounded page");
    assert_eq!(first.rows, vec![vec![OutputValue::nat64(3)]]);
    let continuation = first
        .continuation
        .expect("small output envelope should leave authenticated progress");

    let second = session
        .execute_trusted_live_page_with_result_bytes_limit_for_tests(
            &query,
            Some(continuation.as_str()),
            64,
        )
        .unwrap_or_else(|error| {
            panic!(
                "larger output envelope should resume the same query: {error:?}, facts={:?}",
                error.diagnostic_facts(),
            )
        });
    assert_eq!(
        second.rows,
        vec![vec![OutputValue::nat64(2)], vec![OutputValue::nat64(1)]]
    );
    assert_eq!(second.continuation, None, "lookahead proves exhaustion");
    assert_ne!(first.work.envelope_identity, second.work.envelope_identity);

    assert_eq!(
        [first.rows, second.rows].concat(),
        vec![
            vec![OutputValue::nat64(3)],
            vec![OutputValue::nat64(2)],
            vec![OutputValue::nat64(1)],
        ]
    );
}

#[test]
fn distinct_live_pages_resume_adjacent_groups_and_global_replay_end_to_end() {
    let session = initialize();
    session
        .execute_trusted_dynamic_mutation_batch(vec![
            insert(1, None),
            insert(2, None),
            insert(3, Some(1)),
            insert(4, Some(2)),
            insert(5, Some(1)),
            insert(6, Some(3)),
            insert(7, Some(2)),
        ])
        .expect("DISTINCT continuation rows should insert atomically");

    let adjacent = DynamicQuery::new(ENTITY_NAME)
        .select(["parent_id"])
        .order_by(asc("parent_id"))
        .order_by(asc("id"))
        .distinct_for_internal_execution();
    let global = DynamicQuery::new(ENTITY_NAME)
        .select(["parent_id"])
        .order_by(asc("id"))
        .distinct_for_internal_execution();

    let traverse = |query: &DynamicQuery, strategy: &str| {
        let mut continuation = None;
        let mut rows = Vec::new();
        let mut cursors = std::collections::BTreeSet::new();
        let mut pages = 0_u32;
        let mut entries_visited = 0_u64;
        loop {
            let page = session
                .execute_trusted_live_page(query, continuation.as_deref())
                .unwrap_or_else(|error| {
                    panic!("{strategy} DISTINCT page should execute: {error:?}")
                });
            pages = pages.saturating_add(1);
            entries_visited = entries_visited.saturating_add(page.work.entries_visited);
            assert_eq!(page.row_count as usize, page.rows.len());
            assert_eq!(page.work.result_rows, page.row_count);
            rows.extend(page.rows);
            let Some(cursor) = page.continuation else {
                break;
            };
            assert!(
                cursors.insert(cursor.clone()),
                "{strategy} DISTINCT continuation must advance monotonically",
            );
            continuation = Some(cursor);
            assert!(pages < 8, "{strategy} DISTINCT traversal must terminate");
        }

        (rows, pages, entries_visited)
    };

    let expected = vec![
        vec![OutputValue::null()],
        vec![OutputValue::nat64(1)],
        vec![OutputValue::nat64(2)],
        vec![OutputValue::nat64(3)],
    ];
    let (adjacent_rows, adjacent_pages, adjacent_entries) = traverse(&adjacent, "adjacent");
    let (global_rows, global_pages, global_entries) = traverse(&global, "global");

    assert_eq!(adjacent_rows, expected);
    assert_eq!(global_rows, expected);
    assert_eq!(adjacent_pages, 2);
    assert_eq!(global_pages, 2);
    assert!(adjacent_entries > 0);
    assert!(global_entries > 0);
}

#[test]
fn selective_live_pages_publish_monotonic_empty_physical_progress() {
    let session = initialize();
    session
        .execute_trusted_dynamic_mutation_batch(
            (1..=9)
                .map(|id| {
                    let parent = match id {
                        1 => Some(2),
                        9 => Some(1),
                        _ => None,
                    };
                    insert(id, parent)
                })
                .collect(),
        )
        .expect("selective live-page rows should insert");
    let query = DynamicQuery::new(ENTITY_NAME)
        .select(["id"])
        .filter(FilterExpr::eq("parent_id", 1_u64))
        .order_by(asc("id"))
        .limit(1);

    let first = session
        .execute_trusted_live_page(&query, None)
        .expect("first selective page should stop with physical progress");
    assert!(first.rows.is_empty());
    assert_eq!(first.work.entries_visited, 4);
    let first_cursor = first
        .continuation
        .expect("filtered physical progress must return a continuation");

    let second = session
        .execute_trusted_live_page(&query, Some(first_cursor.as_str()))
        .expect("second selective page should resume after the first physical frontier");
    assert!(second.rows.is_empty());
    assert_eq!(second.work.entries_visited, 4);
    let second_cursor = second
        .continuation
        .expect("second filtered frontier must remain resumable");
    assert_ne!(second_cursor, first_cursor);

    let third = session
        .execute_trusted_live_page(&query, Some(second_cursor.as_str()))
        .expect("final selective page should return the late match");
    assert_eq!(third.rows, vec![vec![OutputValue::nat64(9)]]);
    assert_eq!(third.work.entries_visited, 1);
    assert_eq!(third.continuation, None);

    let descending = DynamicQuery::new(ENTITY_NAME)
        .select(["id"])
        .filter(FilterExpr::eq("parent_id", 2_u64))
        .order_by(desc("id"))
        .limit(1);
    let descending_first = session
        .execute_trusted_live_page(&descending, None)
        .expect("descending selective page should stop with physical progress");
    assert!(descending_first.rows.is_empty());
    let descending_first_cursor = descending_first
        .continuation
        .expect("descending filtered progress must return a continuation");
    let descending_second = session
        .execute_trusted_live_page(&descending, Some(descending_first_cursor.as_str()))
        .expect("descending progress should resume after its physical frontier");
    assert!(descending_second.rows.is_empty());
    let descending_second_cursor = descending_second
        .continuation
        .expect("descending second frontier must remain resumable");
    assert_ne!(descending_second_cursor, descending_first_cursor);
    let descending_third = session
        .execute_trusted_live_page(&descending, Some(descending_second_cursor.as_str()))
        .expect("descending final page should return the late match");
    assert_eq!(descending_third.rows, vec![vec![OutputValue::nat64(1)]]);
    assert_eq!(descending_third.continuation, None);
}

#[test]
fn accepted_relation_edges_drive_catalog_and_describe_introspection() {
    let session = initialize();
    let entities = session
        .show_entities()
        .expect("accepted entity catalog should resolve");
    let source = entities
        .iter()
        .find(|entity| entity.entity_name() == ENTITY_NAME)
        .expect("relation source should be listed");
    assert_eq!(source.relations(), 1);

    let description = session
        .try_describe_entity_by_name(ENTITY_NAME)
        .expect("accepted relation source should describe");
    let [relation] = description.relations() else {
        panic!("accepted relation edge should produce one relation row");
    };
    assert_eq!(relation.field(), "parent_id");
    assert_eq!(relation.target_path(), ENTITY_SOURCE);
    assert_eq!(relation.target_entity_name(), ENTITY_NAME);
    assert_eq!(relation.target_store_path(), STORE_PATH);
    assert_eq!(
        relation.cardinality(),
        crate::db::EntityRelationCardinality::Single,
    );
}

#[test]
fn mixed_relation_validation_uses_the_complete_final_row_overlay() {
    let session = initialize();
    session
        .execute_trusted_dynamic_mutation_batch(vec![insert(1, None), insert(2, Some(1))])
        .expect("the initial relation should commit");

    let blocked = session
        .execute_trusted_dynamic_mutation(&delete(1))
        .expect_err("an unaffected committed source must block target deletion");
    assert_relation_violation(&blocked);

    let deleted = session
        .execute_trusted_dynamic_mutation_batch(vec![delete(2), delete(1)])
        .expect("a source and its target should delete atomically");
    assert_eq!(
        batch_rows(&deleted),
        vec![expected_row(2, Some(1)), expected_row(1, None)],
    );

    session
        .execute_trusted_dynamic_mutation_batch(vec![insert(3, None), insert(4, Some(3))])
        .expect("the update-away fixture should commit");
    let updated_away = session
        .execute_trusted_dynamic_mutation_batch(vec![update_parent(4, None), delete(3)])
        .expect("an updated final source may release a deleted target");
    assert_eq!(
        batch_rows(&updated_away),
        vec![expected_row(4, None), expected_row(3, None)],
    );

    session
        .execute_trusted_dynamic_mutation_batch(vec![insert(5, None), insert(6, Some(5))])
        .expect("the retained-reference fixture should commit");
    let retained = session
        .execute_trusted_dynamic_mutation_batch(vec![update_parent(6, Some(5)), delete(5)])
        .expect_err("a final updated source must still block target deletion");
    assert_relation_violation(&retained);

    session
        .execute_trusted_dynamic_mutation(&insert(7, None))
        .expect("the inserted-reference fixture target should commit");
    let inserted_reference = session
        .execute_trusted_dynamic_mutation_batch(vec![insert(8, Some(7)), delete(7)])
        .expect_err("a final inserted source must not reference a deleted target");
    assert_relation_violation(&inserted_reference);

    let inserted_target = session
        .execute_trusted_dynamic_mutation_batch(vec![insert(10, Some(9)), insert(9, None)])
        .expect("an inserted relation should see its batch-final target");
    assert_eq!(
        batch_rows(&inserted_target),
        vec![expected_row(10, Some(9)), expected_row(9, None)],
    );

    session
        .execute_trusted_dynamic_mutation(&insert(11, None))
        .expect("the updated-reference fixture source should commit");
    let updated_target = session
        .execute_trusted_dynamic_mutation_batch(vec![update_parent(11, Some(12)), insert(12, None)])
        .expect("an updated relation should see its batch-final target");
    assert_eq!(
        batch_rows(&updated_target),
        vec![expected_row(11, Some(12)), expected_row(12, None)],
    );
}

#[test]
fn mixed_batch_commits_cross_entity_then_rejects_late_failures_atomically() {
    let session = initialize();
    session
        .execute_trusted_dynamic_mutation(&insert(1, None))
        .expect("the primary mixed fixture row should commit");
    session
        .execute_trusted_dynamic_mutation(&DynamicMutation::Insert {
            entity: OTHER_ENTITY_NAME.to_string(),
            patch: other_patch(Some(1), 10),
        })
        .expect("the secondary mixed fixture row should commit");

    let mixed_entity = session
        .execute_trusted_dynamic_mutation_batch(vec![
            update_code(1, 11),
            DynamicMutation::Update {
                entity: OTHER_ENTITY_NAME.to_string(),
                key: InputValue::nat64(1),
                patch: other_patch(None, 11),
            },
        ])
        .expect("one atomic batch may span accepted entities in the same store");
    assert_eq!(
        batch_rows(&mixed_entity),
        vec![
            expected_row_with_code(1, None, 11),
            vec![
                OutputValue::nat64(1),
                OutputValue::nat64(11),
                OutputValue::null(),
            ],
        ],
    );

    let missing = session
        .execute_trusted_dynamic_mutation_batch(vec![update_code(1, 12), delete(99)])
        .expect_err("a late missing delete must reject the earlier staged update");
    assert_eq!(missing.class(), ErrorClass::NotFound);

    session
        .execute_trusted_dynamic_mutation(&insert(2, None))
        .expect("the collision fixture should commit");
    let collision = session
        .execute_trusted_dynamic_mutation_batch(vec![update_code(1, 13), insert(2, None)])
        .expect_err("an insert collision must reject the earlier staged update");
    assert_eq!(collision.class(), ErrorClass::Conflict);
    let failures_unchanged = session
        .execute_trusted_dynamic_mutation(&update_code(1, 11))
        .expect("failed batches must preserve the original unique value");
    assert_eq!(failures_unchanged.affected_rows, 0);

    let replaced = session
        .execute_trusted_dynamic_mutation_batch(vec![
            update_code(1, 14),
            DynamicMutation::Replace {
                entity: ENTITY_NAME.to_string(),
                key: InputValue::nat64(99),
                patch: patch(None, None, Some(99)),
            },
        ])
        .expect("ordinary caller-key replace should insert its absent final row");
    assert_eq!(
        batch_rows(&replaced),
        vec![
            expected_row_with_code(1, None, 14),
            expected_row_with_code(99, None, 99),
        ],
    );

    let unchanged = session
        .execute_trusted_dynamic_mutation(&update_code(1, 14))
        .expect("the successful mixed replace must publish its preceding update");
    assert_eq!(unchanged.affected_rows, 0);
    let other_unchanged = session
        .execute_trusted_dynamic_mutation(&DynamicMutation::Update {
            entity: OTHER_ENTITY_NAME.to_string(),
            key: InputValue::nat64(1),
            patch: other_patch(None, 11),
        })
        .expect("the cross-entity commit must publish the secondary row");
    assert_eq!(other_unchanged.affected_rows, 0);
}

#[test]
fn structural_unknown_root_and_dotted_subpath_reject_before_commit() {
    let session = initialize();
    session
        .execute_trusted_dynamic_mutation_batch(vec![insert(1, None), insert(2, None)])
        .expect("structural rejection fixtures should commit");

    let unknown_root = session
        .execute_trusted_dynamic_mutation(&DynamicMutation::Update {
            entity: ENTITY_NAME.to_string(),
            key: InputValue::nat64(1),
            patch: DynamicStructuralPatch::new(vec![(
                "missing".to_string(),
                DynamicWriteCell::Value(InputValue::nat64(10)),
            )]),
        })
        .expect_err("an unknown structural root field must reject");
    assert_eq!(unknown_root.class(), ErrorClass::Unsupported);
    assert_eq!(unknown_root.origin(), ErrorOrigin::Executor);
    assert_eq!(
        unknown_root.diagnostic_code(),
        icydb_diagnostic_code::DiagnosticCode::RuntimeUnsupported,
    );
    assert!(unknown_root.diagnostic_facts().is_empty());

    let dotted_subpath = session
        .execute_trusted_dynamic_mutation_batch(vec![
            update_code(1, 11),
            DynamicMutation::Update {
                entity: ENTITY_NAME.to_string(),
                key: InputValue::nat64(2),
                patch: DynamicStructuralPatch::new(vec![(
                    "code.value".to_string(),
                    DynamicWriteCell::Value(InputValue::nat64(12)),
                )]),
            },
        ])
        .expect_err("a dotted structural subpath must reject the complete batch");
    assert_eq!(dotted_subpath.class(), ErrorClass::Unsupported);
    assert_eq!(dotted_subpath.origin(), ErrorOrigin::Executor);
    assert_eq!(
        dotted_subpath.diagnostic_code(),
        icydb_diagnostic_code::DiagnosticCode::RuntimeUnsupported,
    );
    assert!(dotted_subpath.diagnostic_facts().is_empty());

    let unchanged = session
        .execute_trusted_dynamic_mutation(&update_code(1, 1))
        .expect("the rejected batch must preserve the earlier row");
    assert_eq!(unchanged.affected_rows, 0);

    let whole_field = session
        .execute_trusted_dynamic_mutation(&update_code(2, 12))
        .expect("a complete root-field update must remain supported");
    assert_eq!(whole_field.affected_rows, 1);
    assert_eq!(whole_field.rows, vec![expected_row_with_code(2, None, 12)]);
}

#[test]
fn cross_entity_relations_observe_one_complete_final_overlay() {
    let session = initialize();
    let inserted = session
        .execute_trusted_dynamic_mutation_batch(vec![
            DynamicMutation::Insert {
                entity: OTHER_ENTITY_NAME.to_string(),
                patch: other_patch_with_node(Some(20), 200, Some(42)),
            },
            insert(42, None),
        ])
        .expect("a source may precede its same-batch target in another entity");
    assert_eq!(inserted.len(), 2);

    session
        .execute_trusted_dynamic_mutation_batch(vec![
            delete(42),
            DynamicMutation::Delete {
                entity: OTHER_ENTITY_NAME.to_string(),
                key: InputValue::nat64(20),
            },
        ])
        .expect("a target and cross-entity source may delete in either request order");

    session
        .execute_trusted_dynamic_mutation_batch(vec![
            insert(43, None),
            DynamicMutation::Insert {
                entity: OTHER_ENTITY_NAME.to_string(),
                patch: other_patch_with_node(Some(21), 210, Some(43)),
            },
        ])
        .expect("the retained cross-entity relation fixture should commit");
    let blocked = session
        .execute_trusted_dynamic_mutation_batch(vec![delete(43)])
        .expect_err("a retained source in another entity must protect its target");
    assert_relation_violation(&blocked);
}

#[test]
fn mixed_batch_admits_64_entities_with_one_timestamp_and_rejects_the_65th() {
    let session = initialize();
    let requests = (0..64)
        .map(|index| DynamicMutation::Insert {
            entity: format!("MixedBounded{index}"),
            patch: DynamicStructuralPatch::new(vec![(
                "id".to_string(),
                DynamicWriteCell::Value(InputValue::nat64(1)),
            )]),
        })
        .collect();
    let admitted = session
        .execute_trusted_dynamic_mutation_batch(requests)
        .expect("exactly 64 same-store entities should admit");
    assert_eq!(admitted.len(), 64);
    let timestamps = admitted
        .iter()
        .map(|result| {
            result
                .rows
                .first()
                .and_then(|row| row.get(1))
                .expect("every bounded entity should return its managed timestamp")
        })
        .collect::<Vec<_>>();
    assert!(timestamps.windows(2).all(|pair| pair[0] == pair[1]));

    let over_limit = (0..65)
        .map(|index| DynamicMutation::Insert {
            entity: format!("MixedBounded{index}"),
            patch: DynamicStructuralPatch::new(vec![(
                "id".to_string(),
                DynamicWriteCell::Value(InputValue::nat64(2)),
            )]),
        })
        .collect();
    let error = session
        .execute_trusted_dynamic_mutation_batch(over_limit)
        .expect_err("the 65th distinct entity must reject before staging");
    assert_eq!(error.class(), ErrorClass::Unsupported);
    assert_eq!(
        error.diagnostic_facts(),
        vec![
            (icydb_diagnostic_code::DiagnosticFactTag::ActualCount, 65),
            (icydb_diagnostic_code::DiagnosticFactTag::Limit, 64),
        ],
    );
}

#[test]
fn mixed_batch_rejects_a_cross_store_item_with_bounded_tags() {
    let session = initialize();
    let error = session
        .execute_trusted_dynamic_mutation_batch(vec![
            insert(70, None),
            DynamicMutation::Insert {
                entity: CROSS_ENTITY_NAME.to_string(),
                patch: DynamicStructuralPatch::new(vec![(
                    "id".to_string(),
                    DynamicWriteCell::Value(InputValue::nat64(70)),
                )]),
            },
        ])
        .expect_err("a structural batch must remain inside one accepted store");
    assert_eq!(error.class(), ErrorClass::Conflict);
    assert_eq!(
        error.diagnostic_facts(),
        vec![
            (icydb_diagnostic_code::DiagnosticFactTag::BatchPosition, 1),
            (
                icydb_diagnostic_code::DiagnosticFactTag::ExpectedEntityTag,
                ENTITY_TAG.value(),
            ),
            (
                icydb_diagnostic_code::DiagnosticFactTag::ActualEntityTag,
                CROSS_ENTITY_TAG.value(),
            ),
        ],
    );
    session
        .execute_trusted_dynamic_mutation(&insert(70, None))
        .expect("cross-store rejection must publish no first-item effect");
}

#[test]
fn mixed_batch_unique_swap_and_delete_release_use_the_final_overlay() {
    let session = initialize();
    session
        .execute_trusted_dynamic_mutation_batch(vec![
            insert_with_code(1, None, 10),
            insert_with_code(2, None, 20),
        ])
        .expect("the unique-overlay fixture should commit");

    let conflict = session
        .execute_trusted_dynamic_mutation_batch(vec![update_code(1, 30), update_code(2, 30)])
        .expect_err("the final row must still reject a duplicate unique membership");
    assert_eq!(
        conflict.diagnostic().error_code(),
        icydb_diagnostic_code::ErrorCode::RUNTIME_BOUNDARY_CONSTRAINT_VIOLATION,
    );
    for (id, code) in [(1, 10), (2, 20)] {
        let unchanged = session
            .execute_trusted_dynamic_mutation(&update_code(id, code))
            .expect("rejected preflight must preserve both original unique values");
        assert_eq!(unchanged.affected_rows, 0);
    }

    let swapped = session
        .execute_trusted_dynamic_mutation_batch(vec![update_code(1, 20), update_code(2, 10)])
        .expect("two final rows should atomically swap unique memberships");
    assert_eq!(
        batch_rows(&swapped),
        vec![
            expected_row_with_code(1, None, 20),
            expected_row_with_code(2, None, 10),
        ],
    );

    let released = session
        .execute_trusted_dynamic_mutation_batch(vec![delete(1), insert_with_code(3, None, 20)])
        .expect("a delete should release unique membership to a final inserted row");
    assert_eq!(
        batch_rows(&released),
        vec![
            expected_row_with_code(1, None, 20),
            expected_row_with_code(3, None, 20),
        ],
    );
}
