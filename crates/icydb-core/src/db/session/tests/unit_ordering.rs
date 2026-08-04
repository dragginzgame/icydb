//! End-to-end accepted-schema coverage for the trivial total order of `Unit`.

use crate::{
    db::{
        DbSession, DynamicQuery, DynamicStructuralPatch, DynamicTypedFieldBindingRequest,
        DynamicTypedFieldType, DynamicWriteCell, FieldRef, SqlStatementResult, asc,
        data::DataStore,
        index::IndexStore,
        registry::{StoreAllocationIdentities, StoreRegistry, StoreRuntimeStorageCapabilities},
        schema::{
            AcceptedFieldKind, AcceptedSchemaRevision, FieldId, FieldStorageDecode,
            PersistedFieldSnapshot, PersistedSchemaSnapshot, SchemaFieldSlot, SchemaInsertDefault,
            SchemaRowLayout, SchemaStore, SchemaVersion,
            accepted_schema_candidate_with_field_bindings_for_tests,
        },
    },
    traits::{CanisterKind, Path},
    types::EntityTag,
    value::{InputValue, OutputValue},
};
use icydb_schema::{FieldSourceKey, ScalarType};
use std::{cell::RefCell, collections::BTreeMap};

const STORE_PATH: &str = "db::session::tests::unit_ordering::Store";
const ENTITY_SOURCE: &str = "db::session::tests::unit_ordering::Singleton";
const ENTITY_NAME: &str = "Singleton";
const ID_SOURCE: &str = "db::session::tests::unit_ordering::Singleton::id";
const LABEL_SOURCE: &str = "db::session::tests::unit_ordering::Singleton::label";
const ENTITY_TAG: EntityTag = EntityTag::new(220);

struct TestCanister;

impl Path for TestCanister {
    const PATH: &'static str = "db::session::tests::unit_ordering::Canister";
}

impl CanisterKind for TestCanister {
    const COMMIT_MEMORY_ID: u8 = 220;
    const COMMIT_STABLE_KEY: &'static str = "icydb.test.unit-ordering.commit.v1";
    const INTEGRITY_PROGRESS_MEMORY_ID: u8 = 221;
    const INTEGRITY_PROGRESS_STABLE_KEY: &'static str =
        "icydb.test.unit-ordering.integrity.progress.v1";
}

thread_local! {
    static DATA_STORE: RefCell<DataStore> = const { RefCell::new(DataStore::init_heap()) };
    static INDEX_STORE: RefCell<IndexStore> = const { RefCell::new(IndexStore::init_heap()) };
    static SCHEMA_STORE: RefCell<SchemaStore> = const { RefCell::new(SchemaStore::init_heap()) };
    static STORE_REGISTRY: StoreRegistry = {
        let mut registry = StoreRegistry::new();
        registry.register_store(
            STORE_PATH,
            &DATA_STORE,
            &INDEX_STORE,
            &SCHEMA_STORE,
            StoreAllocationIdentities::absent(),
            StoreRuntimeStorageCapabilities::heap(),
        ).expect("Unit ordering test store should register");
        registry
    };
}

#[test]
fn unit_primary_key_ordering_is_consistent_across_query_surfaces() {
    let session = initialize();
    seed_singleton(&session);

    assert_eq!(
        sql_rows(
            &session,
            "SELECT id, label FROM Singleton ORDER BY id LIMIT 100 OFFSET 0",
        ),
        vec![singleton_row()],
    );
    assert!(
        sql_rows(
            &session,
            "SELECT id, label FROM Singleton ORDER BY id LIMIT 100 OFFSET 1",
        )
        .is_empty(),
    );
    assert_eq!(
        sql_rows(
            &session,
            "SELECT id, label FROM Singleton ORDER BY label, id LIMIT 100",
        ),
        vec![singleton_row()],
    );

    // Ordering by the authored field relies on the planner appending the Unit
    // primary key as its deterministic tie-breaker.
    let tie_break_query = DynamicQuery::new(ENTITY_NAME)
        .select(["id", "label"])
        .order_by(asc("label"))
        .limit(100);
    let dynamic = session
        .execute_trusted_dynamic_query(&tie_break_query)
        .expect("dynamic Unit primary-key tie-break should plan and execute");
    assert_eq!(dynamic.rows, vec![singleton_row()]);

    let primary_key_query = DynamicQuery::new(ENTITY_NAME)
        .filter(FieldRef::new("id").eq(InputValue::Unit))
        .select(["id", "label"])
        .order_by(asc("id"))
        .limit(100);
    let binding = session
        .issue_typed_entity_binding(
            ENTITY_SOURCE,
            &[
                DynamicTypedFieldBindingRequest::new(
                    ID_SOURCE.to_string(),
                    DynamicTypedFieldType::Scalar(ScalarType::Unit),
                    false,
                ),
                DynamicTypedFieldBindingRequest::new(
                    LABEL_SOURCE.to_string(),
                    DynamicTypedFieldType::Scalar(ScalarType::Text { max_len: None }),
                    false,
                ),
            ],
        )
        .expect("generated-style typed binding should resolve accepted Unit authority");
    let typed = session
        .execute_public_dynamic_query_for_typed_binding(&binding, &primary_key_query)
        .expect("typed Unit primary-key tie-break should execute")
        .expect("typed binding should remain current");
    assert_eq!(typed.rows, vec![singleton_row()]);

    for (filter, expected_rows) in [
        (FieldRef::new("id").eq(InputValue::Unit), 1),
        (FieldRef::new("id").lt(InputValue::Unit), 0),
        (FieldRef::new("id").lte(InputValue::Unit), 1),
        (FieldRef::new("id").gt(InputValue::Unit), 0),
        (FieldRef::new("id").gte(InputValue::Unit), 1),
    ] {
        let query = DynamicQuery::new(ENTITY_NAME)
            .filter(filter)
            .select(["id"])
            .order_by(asc("id"))
            .limit(100);
        let output = session
            .execute_trusted_dynamic_query(&query)
            .expect("Unit comparison should plan and execute");

        assert_eq!(output.row_count, expected_rows);
    }
}

fn initialize() -> DbSession<TestCanister> {
    DATA_STORE.with(|store| *store.borrow_mut() = DataStore::init_heap());
    INDEX_STORE.with(|store| *store.borrow_mut() = IndexStore::init_heap());
    SCHEMA_STORE.with(|store| *store.borrow_mut() = SchemaStore::init_heap());

    let fields = vec![
        field(1, "id", 0, AcceptedFieldKind::Unit),
        field(2, "label", 1, AcceptedFieldKind::Text { max_len: None }),
    ];
    let snapshot = PersistedSchemaSnapshot::new(
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
    );
    let field_bindings = BTreeMap::from([
        ((ENTITY_TAG, field_source(ID_SOURCE)), FieldId::new(1)),
        ((ENTITY_TAG, field_source(LABEL_SOURCE)), FieldId::new(2)),
    ]);
    let candidate = accepted_schema_candidate_with_field_bindings_for_tests(
        STORE_PATH,
        AcceptedSchemaRevision::INITIAL,
        BTreeMap::from([(ENTITY_TAG, snapshot)]),
        field_bindings,
    );
    let session = DbSession::<TestCanister>::new(&STORE_REGISTRY);
    session
        .db
        .ensure_recovered_state()
        .expect("Unit ordering database should initialize");
    let store = session
        .db
        .store_handle(STORE_PATH)
        .expect("Unit ordering store should resolve");
    crate::db::commit::publish_accepted_schema_candidate(
        STORE_PATH,
        store,
        AcceptedSchemaRevision::NONE,
        &candidate,
    )
    .expect("Unit ordering accepted schema should publish");

    session
}

fn field(id: u32, name: &str, slot: u16, kind: AcceptedFieldKind) -> PersistedFieldSnapshot {
    let storage_decode = FieldStorageDecode::ByKind;
    let leaf_codec = kind.leaf_codec_for_storage(storage_decode);
    PersistedFieldSnapshot::new_initial(
        FieldId::new(id),
        name.to_string(),
        SchemaFieldSlot::new(slot),
        kind,
        Vec::new(),
        false,
        SchemaInsertDefault::None,
        storage_decode,
        leaf_codec,
    )
}

fn field_source(source: &str) -> FieldSourceKey {
    FieldSourceKey::try_new(source).expect("Unit ordering field source should admit")
}

fn seed_singleton(session: &DbSession<TestCanister>) {
    session
        .execute_trusted_dynamic_insert_batch(
            ENTITY_NAME,
            vec![DynamicStructuralPatch::new(vec![
                ("id".to_string(), DynamicWriteCell::Value(InputValue::Unit)),
                (
                    "label".to_string(),
                    DynamicWriteCell::Value(InputValue::Text("singleton".to_string())),
                ),
            ])],
        )
        .expect("singleton row should insert through accepted write authority");
}

fn sql_rows(session: &DbSession<TestCanister>, sql: &str) -> Vec<Vec<OutputValue>> {
    let SqlStatementResult::Projection { rows, .. } = session
        .execute_trusted_sql_query(sql)
        .expect("Unit ORDER BY query should execute")
    else {
        panic!("Unit ORDER BY query should return a projection");
    };
    rows
}

fn singleton_row() -> Vec<OutputValue> {
    vec![
        OutputValue::Unit,
        OutputValue::Text("singleton".to_string()),
    ]
}
