use super::{DeleteExecutor, LoadExecutor, SaveExecutor, UniqueIndexHandle};
use crate::{
    db::{
        CommitDataOp, CommitIndexOp, CommitKind, CommitMarker, Db, begin_commit,
        commit::commit_marker_present,
        force_recovery_for_tests,
        index::{IndexEntry, IndexKey, IndexStore, IndexStoreRegistry, RawIndexEntry},
        query::v2::plan::{AccessPath, LogicalPlan, OrderDirection, OrderSpec, PageSpec},
        store::{DataKey, DataStore, DataStoreRegistry, RawRow},
        write::{fail_checkpoint_label, fail_next_checkpoint},
    },
    model::index::IndexModel,
    serialize::serialize,
    traits::{
        CanisterKind, EntityKind, FieldValues, Path, SanitizeAuto, SanitizeCustom, StoreKind,
        ValidateAuto, ValidateCustom, View, Visitable,
    },
    types::Ulid,
    value::Value,
};
use icydb_schema::{
    build::schema_write,
    node::{
        Canister, Def, Entity, Field, FieldList, Index, Item, ItemTarget, SchemaNode, Store, Type,
        Value as SchemaValue,
    },
    types::{Cardinality, Primitive, StoreType},
};
use serde::{Deserialize, Serialize};
use std::{cell::RefCell, sync::Once};

const CANISTER_PATH: &str = "write_unit_test::TestCanister";
const DATA_STORE_PATH: &str = "write_unit_test::TestDataStore";
const INDEX_STORE_PATH: &str = "write_unit_test::TestIndexStore";
const ENTITY_PATH: &str = "write_unit_test::TestEntity";

const INDEX_FIELDS: [&str; 1] = ["name"];
const INDEX_MODEL: IndexModel = IndexModel::new(INDEX_STORE_PATH, &INDEX_FIELDS, true);
const INDEXES: [&IndexModel; 1] = [&INDEX_MODEL];

const ORDER_ENTITY_PATH: &str = "write_unit_test::OrderEntity";
const ORDER_FIELDS: [&str; 3] = ["id", "primary", "secondary"];

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
struct TestEntity {
    id: Ulid,
    name: String,
}

impl Path for TestEntity {
    const PATH: &'static str = ENTITY_PATH;
}

impl View for TestEntity {
    type ViewType = Self;

    fn to_view(&self) -> Self::ViewType {
        self.clone()
    }

    fn from_view(view: Self::ViewType) -> Self {
        view
    }
}

impl SanitizeAuto for TestEntity {}
impl SanitizeCustom for TestEntity {}
impl ValidateAuto for TestEntity {}
impl ValidateCustom for TestEntity {}
impl Visitable for TestEntity {}

impl FieldValues for TestEntity {
    fn get_value(&self, field: &str) -> Option<Value> {
        match field {
            "id" => Some(Value::Ulid(self.id)),
            "name" => Some(Value::Text(self.name.clone())),
            _ => None,
        }
    }
}

#[derive(Clone, Copy)]
struct TestCanister;

impl Path for TestCanister {
    const PATH: &'static str = CANISTER_PATH;
}

impl CanisterKind for TestCanister {}

struct TestStore;

impl Path for TestStore {
    const PATH: &'static str = DATA_STORE_PATH;
}

impl StoreKind for TestStore {
    type Canister = TestCanister;
}

impl EntityKind for TestEntity {
    type PrimaryKey = Ulid;
    type Store = TestStore;
    type Canister = TestCanister;

    const ENTITY_NAME: &'static str = "TestEntity";
    const PRIMARY_KEY: &'static str = "id";
    const FIELDS: &'static [&'static str] = &["id", "name"];
    const INDEXES: &'static [&'static IndexModel] = &INDEXES;

    fn key(&self) -> crate::key::Key {
        self.id.into()
    }

    fn primary_key(&self) -> Self::PrimaryKey {
        self.id
    }

    fn set_primary_key(&mut self, key: Self::PrimaryKey) {
        self.id = key;
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct OrderEntity {
    id: Ulid,
    primary: Value,
    secondary: i64,
}

impl Default for OrderEntity {
    fn default() -> Self {
        Self {
            id: Ulid::nil(),
            primary: Value::None,
            secondary: 0,
        }
    }
}

impl Path for OrderEntity {
    const PATH: &'static str = ORDER_ENTITY_PATH;
}

impl View for OrderEntity {
    type ViewType = Self;

    fn to_view(&self) -> Self::ViewType {
        self.clone()
    }

    fn from_view(view: Self::ViewType) -> Self {
        view
    }
}

impl SanitizeAuto for OrderEntity {}
impl SanitizeCustom for OrderEntity {}
impl ValidateAuto for OrderEntity {}
impl ValidateCustom for OrderEntity {}
impl Visitable for OrderEntity {}

impl FieldValues for OrderEntity {
    fn get_value(&self, field: &str) -> Option<Value> {
        match field {
            "id" => Some(Value::Ulid(self.id)),
            "primary" => Some(self.primary.clone()),
            "secondary" => Some(Value::Int(self.secondary)),
            _ => None,
        }
    }
}

impl EntityKind for OrderEntity {
    type PrimaryKey = Ulid;
    type Store = TestStore;
    type Canister = TestCanister;

    const ENTITY_NAME: &'static str = "OrderEntity";
    const PRIMARY_KEY: &'static str = "id";
    const FIELDS: &'static [&'static str] = &ORDER_FIELDS;
    const INDEXES: &'static [&'static IndexModel] = &[];

    fn key(&self) -> crate::key::Key {
        self.id.into()
    }

    fn primary_key(&self) -> Self::PrimaryKey {
        self.id
    }

    fn set_primary_key(&mut self, key: Self::PrimaryKey) {
        self.id = key;
    }
}

canic_memory::eager_static! {
    static TEST_DATA_STORE: RefCell<DataStore> =
        RefCell::new(DataStore::init(canic_memory::ic_memory!(DataStore, 10)));
}

canic_memory::eager_static! {
    static TEST_INDEX_STORE: RefCell<IndexStore> =
        RefCell::new(IndexStore::init(canic_memory::ic_memory!(IndexStore, 11)));
}

thread_local! {
    static DATA_REGISTRY: DataStoreRegistry = {
        let mut reg = DataStoreRegistry::new();
        reg.register(DATA_STORE_PATH, &TEST_DATA_STORE);
        reg
    };

    static INDEX_REGISTRY: IndexStoreRegistry = {
        let mut reg = IndexStoreRegistry::new();
        reg.register(INDEX_STORE_PATH, &TEST_INDEX_STORE);
        reg
    };
}

static DB: Db<TestCanister> = Db::new(&DATA_REGISTRY, &INDEX_REGISTRY);

canic_memory::eager_init!({
    canic_memory::ic_memory_range!(0, 32);
});

static INIT_SCHEMA: Once = Once::new();

#[expect(clippy::too_many_lines)]
fn init_schema() {
    INIT_SCHEMA.call_once(|| {
        static TEST_INDEXES: [Index; 1] = [Index {
            store: INDEX_STORE_PATH,
            fields: &INDEX_FIELDS,
            unique: true,
        }];

        static TEST_FIELDS: [Field; 2] = [
            Field {
                ident: "id",
                value: SchemaValue {
                    cardinality: Cardinality::One,
                    item: Item {
                        target: ItemTarget::Primitive(Primitive::Ulid),
                        relation: None,
                        validators: &[],
                        sanitizers: &[],
                        indirect: false,
                    },
                },
                default: None,
            },
            Field {
                ident: "name",
                value: SchemaValue {
                    cardinality: Cardinality::One,
                    item: Item {
                        target: ItemTarget::Primitive(Primitive::Text),
                        relation: None,
                        validators: &[],
                        sanitizers: &[],
                        indirect: false,
                    },
                },
                default: None,
            },
        ];

        static ORDER_FIELDS_DEF: [Field; 3] = [
            Field {
                ident: "id",
                value: SchemaValue {
                    cardinality: Cardinality::One,
                    item: Item {
                        target: ItemTarget::Primitive(Primitive::Ulid),
                        relation: None,
                        validators: &[],
                        sanitizers: &[],
                        indirect: false,
                    },
                },
                default: None,
            },
            Field {
                ident: "primary",
                value: SchemaValue {
                    cardinality: Cardinality::One,
                    item: Item {
                        target: ItemTarget::Primitive(Primitive::Text),
                        relation: None,
                        validators: &[],
                        sanitizers: &[],
                        indirect: false,
                    },
                },
                default: None,
            },
            Field {
                ident: "secondary",
                value: SchemaValue {
                    cardinality: Cardinality::One,
                    item: Item {
                        target: ItemTarget::Primitive(Primitive::Int),
                        relation: None,
                        validators: &[],
                        sanitizers: &[],
                        indirect: false,
                    },
                },
                default: None,
            },
        ];

        let mut schema = schema_write();

        let canister = Canister {
            def: Def {
                module_path: "write_unit_test",
                ident: "TestCanister",
                comments: None,
            },
            memory_min: 0,
            memory_max: 1,
        };

        let data_store = Store {
            def: Def {
                module_path: "write_unit_test",
                ident: "TestDataStore",
                comments: None,
            },
            ident: "TEST_DATA_STORE",
            ty: StoreType::Data,
            canister: CANISTER_PATH,
            memory_id: 10,
        };

        let index_store = Store {
            def: Def {
                module_path: "write_unit_test",
                ident: "TestIndexStore",
                comments: None,
            },
            ident: "TEST_INDEX_STORE",
            ty: StoreType::Index,
            canister: CANISTER_PATH,
            memory_id: 11,
        };

        let test_entity = Entity {
            def: Def {
                module_path: "write_unit_test",
                ident: "TestEntity",
                comments: None,
            },
            store: DATA_STORE_PATH,
            primary_key: "id",
            name: None,
            indexes: &TEST_INDEXES,
            fields: FieldList {
                fields: &TEST_FIELDS,
            },
            ty: Type {
                sanitizers: &[],
                validators: &[],
            },
        };

        let order_entity = Entity {
            def: Def {
                module_path: "write_unit_test",
                ident: "OrderEntity",
                comments: None,
            },
            store: DATA_STORE_PATH,
            primary_key: "id",
            name: None,
            indexes: &[],
            fields: FieldList {
                fields: &ORDER_FIELDS_DEF,
            },
            ty: Type {
                sanitizers: &[],
                validators: &[],
            },
        };

        schema.insert_node(SchemaNode::Canister(canister));
        schema.insert_node(SchemaNode::Store(data_store));
        schema.insert_node(SchemaNode::Store(index_store));
        schema.insert_node(SchemaNode::Entity(test_entity));
        schema.insert_node(SchemaNode::Entity(order_entity));
    });
}

fn reset_stores() {
    TEST_DATA_STORE.with_borrow_mut(|store| store.clear());
    TEST_INDEX_STORE.with_borrow_mut(|store| store.clear());
}

fn assert_commit_marker_clear() {
    assert!(!commit_marker_present().unwrap());
}

fn assert_entity_present(entity: &TestEntity) {
    let data_key = DataKey::new::<TestEntity>(entity.id);
    let raw_key = data_key.to_raw();
    let data_present = DB
        .context::<TestEntity>()
        .with_store(|s| s.get(&raw_key))
        .unwrap();
    assert!(data_present.is_some());

    let index_key = IndexKey::new(entity, &INDEX_MODEL).expect("index key");
    let raw_index_key = index_key.to_raw();
    let index_present = TEST_INDEX_STORE.with_borrow(|s| s.get(&raw_index_key));
    assert!(index_present.is_some());
}

fn assert_entity_missing(entity: &TestEntity) {
    let data_key = DataKey::new::<TestEntity>(entity.id);
    let raw_key = data_key.to_raw();
    let data_present = DB
        .context::<TestEntity>()
        .with_store(|s| s.get(&raw_key))
        .unwrap();
    assert!(data_present.is_none());

    let index_key = IndexKey::new(entity, &INDEX_MODEL).expect("index key");
    let raw_index_key = index_key.to_raw();
    let index_present = TEST_INDEX_STORE.with_borrow(|s| s.get(&raw_index_key));
    assert!(index_present.is_none());
}

#[test]
fn save_rolls_back_on_forced_failure() {
    reset_stores();
    fail_next_checkpoint();

    let entity = TestEntity {
        id: Ulid::nil(),
        name: "alpha".to_string(),
    };

    let saver = SaveExecutor::<TestEntity>::new(DB, false);
    let result = saver.insert(entity.clone());
    assert!(result.is_err());

    let data_key = DataKey::new::<TestEntity>(entity.id);
    let raw_key = data_key.to_raw();
    let data_present = DB
        .context::<TestEntity>()
        .with_store(|s| s.get(&raw_key))
        .unwrap();
    assert!(data_present.is_none());

    let index_key = IndexKey::new(&entity, &INDEX_MODEL).expect("index key");
    let raw_index_key = index_key.to_raw();
    let index_present = TEST_INDEX_STORE.with_borrow(|s| s.get(&raw_index_key));
    assert!(index_present.is_none());
}

#[test]
fn delete_by_unique_index_is_idempotent() {
    reset_stores();
    init_schema();

    let deleter = DeleteExecutor::<TestEntity>::new(DB, false);
    let handle = UniqueIndexHandle::new::<TestEntity>(&INDEX_MODEL).unwrap();
    let entity = TestEntity {
        id: Ulid::nil(),
        name: "missing".to_string(),
    };

    let response = deleter.by_unique_index(handle, entity).unwrap();
    assert!(response.0.is_empty());
}

#[test]
fn delete_unique_rolls_back_after_index_removal() {
    reset_stores();
    init_schema();

    let saver = SaveExecutor::<TestEntity>::new(DB, false);
    let entity = TestEntity {
        id: Ulid::from_u128(1),
        name: "alpha".to_string(),
    };
    saver.insert(entity.clone()).unwrap();
    assert_entity_present(&entity);

    let deleter = DeleteExecutor::<TestEntity>::new(DB, false);
    let handle = UniqueIndexHandle::new::<TestEntity>(&INDEX_MODEL).unwrap();

    fail_checkpoint_label("delete_unique_after_indexes");
    let result = deleter.clone().by_unique_index(handle, entity.clone());
    assert!(result.is_err());
    assert_entity_present(&entity);
    assert_commit_marker_clear();

    let response = deleter
        .clone()
        .by_unique_index(handle, entity.clone())
        .unwrap();
    assert_eq!(response.0.len(), 1);
    assert_entity_missing(&entity);
    assert_commit_marker_clear();

    let response = deleter.by_unique_index(handle, entity).unwrap();
    assert!(response.0.is_empty());
    assert_commit_marker_clear();
}

#[test]
fn delete_scan_rolls_back_after_data_removal() {
    reset_stores();
    init_schema();

    let saver = SaveExecutor::<TestEntity>::new(DB, false);
    let entity_a = TestEntity {
        id: Ulid::from_u128(10),
        name: "alpha".to_string(),
    };
    let entity_b = TestEntity {
        id: Ulid::from_u128(11),
        name: "beta".to_string(),
    };
    saver.insert(entity_a.clone()).unwrap();
    saver.insert(entity_b.clone()).unwrap();
    assert_entity_present(&entity_a);
    assert_entity_present(&entity_b);

    let deleter = DeleteExecutor::<TestEntity>::new(DB, false);
    let plan = LogicalPlan::new(AccessPath::FullScan);

    fail_checkpoint_label("delete_after_data");
    let result = deleter.clone().execute(plan);
    assert!(result.is_err());
    assert_entity_present(&entity_a);
    assert_entity_present(&entity_b);
    assert_commit_marker_clear();

    let response = deleter
        .clone()
        .execute(LogicalPlan::new(AccessPath::FullScan))
        .unwrap();
    assert_eq!(response.0.len(), 2);
    assert_entity_missing(&entity_a);
    assert_entity_missing(&entity_b);
    assert_commit_marker_clear();

    let response = deleter
        .execute(LogicalPlan::new(AccessPath::FullScan))
        .unwrap();
    assert!(response.0.is_empty());
    assert_commit_marker_clear();
}

#[test]
fn commit_marker_recovery_replays_ops() {
    reset_stores();
    init_schema();

    let entity = TestEntity {
        id: Ulid::from_u128(42),
        name: "alpha".to_string(),
    };
    let data_key = DataKey::new::<TestEntity>(entity.id);
    let raw_data_key = data_key.to_raw();
    let raw_row = RawRow::try_new(serialize(&entity).unwrap()).unwrap();

    let index_key = IndexKey::new(&entity, &INDEX_MODEL).unwrap();
    let raw_index_key = index_key.to_raw();
    let entry = IndexEntry::new(entity.key());
    let raw_index_entry = RawIndexEntry::try_from_entry(&entry).unwrap();

    let marker = CommitMarker::new(
        CommitKind::Save,
        vec![CommitIndexOp {
            store: INDEX_STORE_PATH.to_string(),
            key: raw_index_key.as_bytes().to_vec(),
            value: Some(raw_index_entry.as_bytes().to_vec()),
        }],
        vec![CommitDataOp {
            store: DATA_STORE_PATH.to_string(),
            key: raw_data_key.as_bytes().to_vec(),
            value: Some(raw_row.as_bytes().to_vec()),
        }],
    )
    .unwrap();

    let mut guard = begin_commit(marker).unwrap();
    assert!(commit_marker_present().unwrap());

    TEST_INDEX_STORE.with_borrow_mut(|store| {
        store.insert(raw_index_key, raw_index_entry);
    });
    guard.mark_index_written();

    force_recovery_for_tests();
    let saver = SaveExecutor::<TestEntity>::new(DB, false);
    let result = saver.insert(entity.clone());
    assert!(result.is_err());

    assert_entity_present(&entity);
    assert_commit_marker_clear();

    let stored_entry = TEST_INDEX_STORE
        .with_borrow(|store| store.get(&raw_index_key))
        .expect("index entry restored");
    let decoded = stored_entry.try_decode().unwrap();
    assert_eq!(decoded.len(), 1);
    assert!(decoded.contains(&entity.key()));

    let deleter = DeleteExecutor::<TestEntity>::new(DB, false);
    let handle = UniqueIndexHandle::new::<TestEntity>(&INDEX_MODEL).unwrap();
    let response = deleter.by_unique_index(handle, entity.clone()).unwrap();
    assert_eq!(response.0.len(), 1);
    assert_entity_missing(&entity);
    assert_commit_marker_clear();

    let saver = SaveExecutor::<TestEntity>::new(DB, false);
    saver.insert(entity).unwrap();
    assert_commit_marker_clear();
}

#[test]
fn load_orders_with_incomparable_primary_uses_secondary() {
    reset_stores();
    init_schema();

    let saver = SaveExecutor::<OrderEntity>::new(DB, false);
    let entities = vec![
        OrderEntity {
            id: Ulid::from_u128(1),
            primary: Value::Int(2),
            secondary: 0,
        },
        OrderEntity {
            id: Ulid::from_u128(2),
            primary: Value::Text("b".to_string()),
            secondary: 1,
        },
        OrderEntity {
            id: Ulid::from_u128(3),
            primary: Value::Int(1),
            secondary: 0,
        },
        OrderEntity {
            id: Ulid::from_u128(4),
            primary: Value::Text("a".to_string()),
            secondary: 1,
        },
    ];

    for entity in entities {
        saver.insert(entity).unwrap();
    }

    let mut plan = LogicalPlan::new(AccessPath::FullScan);
    plan.order = Some(OrderSpec {
        fields: vec![
            ("primary".to_string(), OrderDirection::Asc),
            ("secondary".to_string(), OrderDirection::Asc),
        ],
    });

    let loader = LoadExecutor::<OrderEntity>::new(DB, false);
    let ordered = loader.execute(plan).unwrap().entities();
    let ordered_ids = ordered.iter().map(|entity| entity.id).collect::<Vec<_>>();

    assert_eq!(
        ordered_ids,
        vec![
            Ulid::from_u128(3),
            Ulid::from_u128(1),
            Ulid::from_u128(4),
            Ulid::from_u128(2),
        ]
    );
}

#[test]
fn load_paginates_after_ordering() {
    reset_stores();
    init_schema();

    let saver = SaveExecutor::<TestEntity>::new(DB, false);
    let entities = vec![
        TestEntity {
            id: Ulid::from_u128(1),
            name: "delta".to_string(),
        },
        TestEntity {
            id: Ulid::from_u128(2),
            name: "alpha".to_string(),
        },
        TestEntity {
            id: Ulid::from_u128(3),
            name: "charlie".to_string(),
        },
        TestEntity {
            id: Ulid::from_u128(4),
            name: "bravo".to_string(),
        },
    ];

    for entity in entities {
        saver.insert(entity).unwrap();
    }

    let mut plan = LogicalPlan::new(AccessPath::FullScan);
    plan.order = Some(OrderSpec {
        fields: vec![("name".to_string(), OrderDirection::Asc)],
    });
    plan.page = Some(PageSpec {
        limit: Some(2),
        offset: 1,
    });

    let loader = LoadExecutor::<TestEntity>::new(DB, false);
    let page = loader.execute(plan).unwrap().entities();
    let names = page
        .iter()
        .map(|entity| entity.name.as_str())
        .collect::<Vec<_>>();
    assert_eq!(names, vec!["bravo", "charlie"]);

    let mut plan = LogicalPlan::new(AccessPath::FullScan);
    plan.order = Some(OrderSpec {
        fields: vec![("name".to_string(), OrderDirection::Asc)],
    });
    plan.page = Some(PageSpec {
        limit: Some(10),
        offset: 0,
    });
    let names = loader
        .execute(plan)
        .unwrap()
        .entities()
        .into_iter()
        .map(|entity| entity.name)
        .collect::<Vec<_>>();
    assert_eq!(names, vec!["alpha", "bravo", "charlie", "delta"]);

    let mut plan = LogicalPlan::new(AccessPath::FullScan);
    plan.order = Some(OrderSpec {
        fields: vec![("name".to_string(), OrderDirection::Asc)],
    });
    plan.page = Some(PageSpec {
        limit: Some(2),
        offset: 10,
    });
    assert!(
        loader
            .execute(plan)
            .unwrap()
            .entities()
            .into_iter()
            .map(|entity| entity.name)
            .next()
            .is_none()
    );
}

#[test]
fn ordering_does_not_break_ties_by_primary_key() {
    reset_stores();
    init_schema();

    let saver = SaveExecutor::<OrderEntity>::new(DB, false);
    let entities = vec![
        OrderEntity {
            id: Ulid::from_u128(3),
            primary: Value::Text("same".to_string()),
            secondary: 1,
        },
        OrderEntity {
            id: Ulid::from_u128(1),
            primary: Value::Text("same".to_string()),
            secondary: 2,
        },
        OrderEntity {
            id: Ulid::from_u128(4),
            primary: Value::Text("same".to_string()),
            secondary: 3,
        },
        OrderEntity {
            id: Ulid::from_u128(2),
            primary: Value::Text("same".to_string()),
            secondary: 4,
        },
    ];

    for entity in entities.clone() {
        saver.insert(entity).unwrap();
    }

    let mut plan = LogicalPlan::new(AccessPath::FullScan);
    plan.order = Some(OrderSpec {
        fields: vec![("primary".to_string(), OrderDirection::Asc)],
    });

    let loader = LoadExecutor::<OrderEntity>::new(DB, false);
    let ordered = loader.execute(plan).unwrap().entities();
    let ordered_ids = ordered.iter().map(|entity| entity.id).collect::<Vec<_>>();
    let inserted_ids = entities.iter().map(|entity| entity.id).collect::<Vec<_>>();

    assert_eq!(ordered_ids, inserted_ids);
}

#[test]
fn ordering_does_not_compare_incomparable_values() {
    reset_stores();
    init_schema();

    let saver = SaveExecutor::<OrderEntity>::new(DB, false);
    let entities = vec![
        OrderEntity {
            id: Ulid::from_u128(10),
            primary: Value::Int(1),
            secondary: 1,
        },
        OrderEntity {
            id: Ulid::from_u128(11),
            primary: Value::Text("a".to_string()),
            secondary: 2,
        },
        OrderEntity {
            id: Ulid::from_u128(12),
            primary: Value::Int(2),
            secondary: 3,
        },
        OrderEntity {
            id: Ulid::from_u128(13),
            primary: Value::Text("b".to_string()),
            secondary: 4,
        },
    ];

    for entity in entities.clone() {
        saver.insert(entity).unwrap();
    }

    let mut plan = LogicalPlan::new(AccessPath::FullScan);
    plan.order = Some(OrderSpec {
        fields: vec![("primary".to_string(), OrderDirection::Asc)],
    });

    let loader = LoadExecutor::<OrderEntity>::new(DB, false);
    let ordered = loader.execute(plan).unwrap().entities();
    let ordered_ids = ordered.iter().map(|entity| entity.id).collect::<Vec<_>>();
    let inserted_ids = entities.iter().map(|entity| entity.id).collect::<Vec<_>>();

    assert_eq!(ordered_ids, inserted_ids);
}
