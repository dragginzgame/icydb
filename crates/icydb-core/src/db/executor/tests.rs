use super::trace::{QueryTraceEvent, QueryTraceSink, TraceAccess, TraceExecutorKind, TracePhase};
use super::{DeleteExecutor, LoadExecutor, SaveExecutor};
use crate::{
    db::{
        Db, DbSession, EntityRegistryEntry,
        commit::{
            CommitDataOp, CommitIndexOp, CommitKind, CommitMarker, begin_commit,
            commit_marker_present, force_recovery_for_tests,
        },
        index::{
            IndexEntry, IndexKey, IndexStore as DbIndexStore, IndexStoreRegistry, RawIndexEntry,
        },
        query::{
            DeleteSpec, FieldRef, LoadSpec, Query, QueryError, QueryMode, ReadConsistency,
            plan::{
                AccessPath, AccessPlan, ExecutablePlan, ExplainAccessPath, OrderDirection,
                OrderSpec, PageSpec, PlanError, logical::LogicalPlan,
            },
            predicate::Predicate,
        },
        store::{DataKey, DataStore as DbDataStore, DataStoreRegistry, RawRow},
        write::{fail_checkpoint_label, fail_next_checkpoint},
    },
    error::{ErrorClass, ErrorOrigin},
    model::{
        entity::EntityModel,
        field::{EntityFieldKind, EntityFieldModel},
        index::IndexModel,
    },
    serialize::serialize,
    traits::{
        CanisterKind, DataStoreKind, EntityKind, FieldValue, FieldValues, Path, SanitizeAuto,
        SanitizeCustom, UnitKey, ValidateAuto, ValidateCustom, View, Visitable,
    },
    types::{Ref, Timestamp, Ulid, Unit},
    value::{Value, ValueEnum},
};
use canic_memory::runtime::registry::MemoryRegistryRuntime;
use icydb_schema::{
    build::schema_write,
    node::{
        Canister, DataStore as SchemaDataStore, Def, Entity, Field, FieldList, Index,
        IndexStore as SchemaIndexStore, Item, ItemTarget, SchemaNode, Type, Value as SchemaValue,
    },
    types::{Cardinality, Primitive},
};
use serde::{Deserialize, Serialize};
use std::{
    cell::RefCell,
    fmt::Write,
    mem,
    sync::{Mutex, Once},
};

const CANISTER_PATH: &str = "write_unit_test::TestCanister";
const DATA_STORE_PATH: &str = "write_unit_test::TestDataStore";
const INDEX_STORE_PATH: &str = "write_unit_test::TestIndexStore";
const ENTITY_PATH: &str = "write_unit_test::TestEntity";

const INDEX_FIELDS: [&str; 1] = ["name"];
const INDEX_MODEL: IndexModel =
    IndexModel::new("test::index_name", INDEX_STORE_PATH, &INDEX_FIELDS, true);
const INDEXES: [&IndexModel; 1] = [&INDEX_MODEL];
const TEST_FIELDS: [EntityFieldModel; 2] = [
    EntityFieldModel {
        name: "id",
        kind: EntityFieldKind::Ulid,
    },
    EntityFieldModel {
        name: "name",
        kind: EntityFieldKind::Text,
    },
];
const TEST_MODEL: EntityModel = EntityModel {
    path: ENTITY_PATH,
    entity_name: "TestEntity",
    primary_key: &TEST_FIELDS[0],
    fields: &TEST_FIELDS,
    indexes: &INDEXES,
};

const ORDER_ENTITY_PATH: &str = "write_unit_test::OrderEntity";
const ORDER_FIELDS: [&str; 3] = ["id", "primary", "secondary"];
const ORDER_FIELD_MODELS: [EntityFieldModel; 3] = [
    EntityFieldModel {
        name: "id",
        kind: EntityFieldKind::Ulid,
    },
    EntityFieldModel {
        name: "primary",
        kind: EntityFieldKind::Int,
    },
    EntityFieldModel {
        name: "secondary",
        kind: EntityFieldKind::Int,
    },
];
const ORDER_MODEL: EntityModel = EntityModel {
    path: ORDER_ENTITY_PATH,
    entity_name: "OrderEntity",
    primary_key: &ORDER_FIELD_MODELS[0],
    fields: &ORDER_FIELD_MODELS,
    indexes: &[],
};

const TIMESTAMP_ENTITY_PATH: &str = "write_unit_test::TimestampEntity";
const TIMESTAMP_FIELDS: [&str; 1] = ["id"];
const TIMESTAMP_FIELD_MODELS: [EntityFieldModel; 1] = [EntityFieldModel {
    name: "id",
    kind: EntityFieldKind::Timestamp,
}];
const TIMESTAMP_MODEL: EntityModel = EntityModel {
    path: TIMESTAMP_ENTITY_PATH,
    entity_name: "TimestampEntity",
    primary_key: &TIMESTAMP_FIELD_MODELS[0],
    fields: &TIMESTAMP_FIELD_MODELS,
    indexes: &[],
};

const UNIT_ENTITY_PATH: &str = "write_unit_test::UnitEntity";
const UNIT_FIELDS: [&str; 1] = ["id"];
const UNIT_FIELD_MODELS: [EntityFieldModel; 1] = [EntityFieldModel {
    name: "id",
    kind: EntityFieldKind::Unit,
}];
const UNIT_MODEL: EntityModel = EntityModel {
    path: UNIT_ENTITY_PATH,
    entity_name: "UnitEntity",
    primary_key: &UNIT_FIELD_MODELS[0],
    fields: &UNIT_FIELD_MODELS,
    indexes: &[],
};

const OWNER_ENTITY_PATH: &str = "write_unit_test::OwnerEntity";
const DIRECT_REF_ENTITY_PATH: &str = "write_unit_test::DirectRefEntity";
const RECORD_REF_ENTITY_PATH: &str = "write_unit_test::RecordRefEntity";
const ENUM_REF_ENTITY_PATH: &str = "write_unit_test::EnumRefEntity";
const COLLECTION_REF_ENTITY_PATH: &str = "write_unit_test::CollectionRefEntity";

const OWNER_KEY_KIND: EntityFieldKind = EntityFieldKind::Ulid;

const OWNER_FIELDS: [EntityFieldModel; 1] = [EntityFieldModel {
    name: "id",
    kind: EntityFieldKind::Ulid,
}];
const DIRECT_REF_FIELDS: [EntityFieldModel; 2] = [
    EntityFieldModel {
        name: "id",
        kind: EntityFieldKind::Ulid,
    },
    EntityFieldModel {
        name: "owner",
        kind: EntityFieldKind::Ref {
            target_path: OWNER_ENTITY_PATH,
            key_kind: &OWNER_KEY_KIND,
        },
    },
];
const RECORD_REF_FIELDS: [EntityFieldModel; 2] = [
    EntityFieldModel {
        name: "id",
        kind: EntityFieldKind::Ulid,
    },
    EntityFieldModel {
        name: "profile",
        kind: EntityFieldKind::Unsupported,
    },
];
const ENUM_REF_FIELDS: [EntityFieldModel; 2] = [
    EntityFieldModel {
        name: "id",
        kind: EntityFieldKind::Ulid,
    },
    EntityFieldModel {
        name: "status",
        kind: EntityFieldKind::Enum,
    },
];
const COLLECTION_REF_FIELDS: [EntityFieldModel; 2] = [
    EntityFieldModel {
        name: "id",
        kind: EntityFieldKind::Ulid,
    },
    EntityFieldModel {
        name: "owners",
        kind: EntityFieldKind::List(&EntityFieldKind::Ref {
            target_path: OWNER_ENTITY_PATH,
            key_kind: &OWNER_KEY_KIND,
        }),
    },
];

const OWNER_MODEL: EntityModel = EntityModel {
    path: OWNER_ENTITY_PATH,
    entity_name: "OwnerEntity",
    primary_key: &OWNER_FIELDS[0],
    fields: &OWNER_FIELDS,
    indexes: &[],
};
const DIRECT_REF_MODEL: EntityModel = EntityModel {
    path: DIRECT_REF_ENTITY_PATH,
    entity_name: "DirectRefEntity",
    primary_key: &DIRECT_REF_FIELDS[0],
    fields: &DIRECT_REF_FIELDS,
    indexes: &[],
};
const RECORD_REF_MODEL: EntityModel = EntityModel {
    path: RECORD_REF_ENTITY_PATH,
    entity_name: "RecordRefEntity",
    primary_key: &RECORD_REF_FIELDS[0],
    fields: &RECORD_REF_FIELDS,
    indexes: &[],
};
const ENUM_REF_MODEL: EntityModel = EntityModel {
    path: ENUM_REF_ENTITY_PATH,
    entity_name: "EnumRefEntity",
    primary_key: &ENUM_REF_FIELDS[0],
    fields: &ENUM_REF_FIELDS,
    indexes: &[],
};
const COLLECTION_REF_MODEL: EntityModel = EntityModel {
    path: COLLECTION_REF_ENTITY_PATH,
    entity_name: "CollectionRefEntity",
    primary_key: &COLLECTION_REF_FIELDS[0],
    fields: &COLLECTION_REF_FIELDS,
    indexes: &[],
};

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
struct TestEntity {
    id: Ref<Self>,
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
            "id" => Some(self.id.to_value()),
            "name" => Some(Value::Text(self.name.clone())),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
struct OwnerEntity {
    id: Ref<Self>,
}

impl Path for OwnerEntity {
    const PATH: &'static str = OWNER_ENTITY_PATH;
}

impl View for OwnerEntity {
    type ViewType = Self;

    fn to_view(&self) -> Self::ViewType {
        self.clone()
    }

    fn from_view(view: Self::ViewType) -> Self {
        view
    }
}

impl SanitizeAuto for OwnerEntity {}
impl SanitizeCustom for OwnerEntity {}
impl ValidateAuto for OwnerEntity {}
impl ValidateCustom for OwnerEntity {}
impl Visitable for OwnerEntity {}

impl FieldValues for OwnerEntity {
    fn get_value(&self, field: &str) -> Option<Value> {
        match field {
            "id" => Some(self.id.to_value()),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
struct DirectRefEntity {
    id: Ref<Self>,
    owner: Option<Ref<OwnerEntity>>,
}

impl Path for DirectRefEntity {
    const PATH: &'static str = DIRECT_REF_ENTITY_PATH;
}

impl View for DirectRefEntity {
    type ViewType = Self;

    fn to_view(&self) -> Self::ViewType {
        self.clone()
    }

    fn from_view(view: Self::ViewType) -> Self {
        view
    }
}

impl SanitizeAuto for DirectRefEntity {}
impl SanitizeCustom for DirectRefEntity {}
impl ValidateAuto for DirectRefEntity {}
impl ValidateCustom for DirectRefEntity {}
impl Visitable for DirectRefEntity {}

impl FieldValues for DirectRefEntity {
    fn get_value(&self, field: &str) -> Option<Value> {
        match field {
            "id" => Some(self.id.to_value()),
            "owner" => Some(self.owner.to_value()),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
struct RecordRefPayload {
    owner: Ref<OwnerEntity>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
struct RecordRefEntity {
    id: Ref<Self>,
    profile: RecordRefPayload,
}

impl Path for RecordRefEntity {
    const PATH: &'static str = RECORD_REF_ENTITY_PATH;
}

impl View for RecordRefEntity {
    type ViewType = Self;

    fn to_view(&self) -> Self::ViewType {
        self.clone()
    }

    fn from_view(view: Self::ViewType) -> Self {
        view
    }
}

impl SanitizeAuto for RecordRefEntity {}
impl SanitizeCustom for RecordRefEntity {}
impl ValidateAuto for RecordRefEntity {}
impl ValidateCustom for RecordRefEntity {}
impl Visitable for RecordRefEntity {}

impl FieldValues for RecordRefEntity {
    fn get_value(&self, field: &str) -> Option<Value> {
        match field {
            "id" => Some(self.id.to_value()),
            "profile" => Some(Value::Unsupported),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
enum RefEnum {
    Missing(Ref<OwnerEntity>),
    #[default]
    Empty,
}

impl Path for RefEnum {
    const PATH: &'static str = "write_unit_test::RefEnum";
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
struct EnumRefEntity {
    id: Ref<Self>,
    status: RefEnum,
}

impl Path for EnumRefEntity {
    const PATH: &'static str = ENUM_REF_ENTITY_PATH;
}

impl View for EnumRefEntity {
    type ViewType = Self;

    fn to_view(&self) -> Self::ViewType {
        self.clone()
    }

    fn from_view(view: Self::ViewType) -> Self {
        view
    }
}

impl SanitizeAuto for EnumRefEntity {}
impl SanitizeCustom for EnumRefEntity {}
impl ValidateAuto for EnumRefEntity {}
impl ValidateCustom for EnumRefEntity {}
impl Visitable for EnumRefEntity {}

impl FieldValues for EnumRefEntity {
    fn get_value(&self, field: &str) -> Option<Value> {
        match field {
            "id" => Some(self.id.to_value()),
            "status" => Some(Value::Enum(match &self.status {
                RefEnum::Missing(owner) => {
                    ValueEnum::strict::<RefEnum>("Missing").with_payload(owner.to_value())
                }
                RefEnum::Empty => ValueEnum::strict::<RefEnum>("Empty"),
            })),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
struct CollectionRefEntity {
    id: Ref<Self>,
    owners: Vec<Ref<OwnerEntity>>,
}

impl Path for CollectionRefEntity {
    const PATH: &'static str = COLLECTION_REF_ENTITY_PATH;
}

impl View for CollectionRefEntity {
    type ViewType = Self;

    fn to_view(&self) -> Self::ViewType {
        self.clone()
    }

    fn from_view(view: Self::ViewType) -> Self {
        view
    }
}

impl SanitizeAuto for CollectionRefEntity {}
impl SanitizeCustom for CollectionRefEntity {}
impl ValidateAuto for CollectionRefEntity {}
impl ValidateCustom for CollectionRefEntity {}
impl Visitable for CollectionRefEntity {}

impl FieldValues for CollectionRefEntity {
    fn get_value(&self, field: &str) -> Option<Value> {
        match field {
            "id" => Some(self.id.to_value()),
            "owners" => Some(self.owners.to_value()),
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

struct TestDataStore;

impl Path for TestDataStore {
    const PATH: &'static str = DATA_STORE_PATH;
}

impl DataStoreKind for TestDataStore {
    type Canister = TestCanister;
}

impl EntityKind for TestEntity {
    type PrimaryKey = Ref<Self>;
    type DataStore = TestDataStore;
    type Canister = TestCanister;

    const ENTITY_NAME: &'static str = "TestEntity";
    const PRIMARY_KEY: &'static str = "id";
    const FIELDS: &'static [&'static str] = &["id", "name"];
    const INDEXES: &'static [&'static IndexModel] = &INDEXES;
    const MODEL: &'static EntityModel = &TEST_MODEL;

    fn key(&self) -> Self::PrimaryKey {
        self.id
    }

    fn primary_key(&self) -> Self::PrimaryKey {
        self.id
    }

    fn set_primary_key(&mut self, key: Self::PrimaryKey) {
        self.id = key;
    }
}

impl EntityKind for OwnerEntity {
    type PrimaryKey = Ref<Self>;
    type DataStore = TestDataStore;
    type Canister = TestCanister;

    const ENTITY_NAME: &'static str = "OwnerEntity";
    const PRIMARY_KEY: &'static str = "id";
    const FIELDS: &'static [&'static str] = &["id"];
    const INDEXES: &'static [&'static IndexModel] = &[];
    const MODEL: &'static EntityModel = &OWNER_MODEL;

    fn key(&self) -> Self::PrimaryKey {
        self.id
    }

    fn primary_key(&self) -> Self::PrimaryKey {
        self.id
    }

    fn set_primary_key(&mut self, key: Self::PrimaryKey) {
        self.id = key;
    }
}

impl EntityKind for DirectRefEntity {
    type PrimaryKey = Ref<Self>;
    type DataStore = TestDataStore;
    type Canister = TestCanister;

    const ENTITY_NAME: &'static str = "DirectRefEntity";
    const PRIMARY_KEY: &'static str = "id";
    const FIELDS: &'static [&'static str] = &["id", "owner"];
    const INDEXES: &'static [&'static IndexModel] = &[];
    const MODEL: &'static EntityModel = &DIRECT_REF_MODEL;

    fn key(&self) -> Self::PrimaryKey {
        self.id
    }

    fn primary_key(&self) -> Self::PrimaryKey {
        self.id
    }

    fn set_primary_key(&mut self, key: Self::PrimaryKey) {
        self.id = key;
    }
}

impl EntityKind for RecordRefEntity {
    type PrimaryKey = Ref<Self>;
    type DataStore = TestDataStore;
    type Canister = TestCanister;

    const ENTITY_NAME: &'static str = "RecordRefEntity";
    const PRIMARY_KEY: &'static str = "id";
    const FIELDS: &'static [&'static str] = &["id", "profile"];
    const INDEXES: &'static [&'static IndexModel] = &[];
    const MODEL: &'static EntityModel = &RECORD_REF_MODEL;

    fn key(&self) -> Self::PrimaryKey {
        self.id
    }

    fn primary_key(&self) -> Self::PrimaryKey {
        self.id
    }

    fn set_primary_key(&mut self, key: Self::PrimaryKey) {
        self.id = key;
    }
}

impl EntityKind for EnumRefEntity {
    type PrimaryKey = Ref<Self>;
    type DataStore = TestDataStore;
    type Canister = TestCanister;

    const ENTITY_NAME: &'static str = "EnumRefEntity";
    const PRIMARY_KEY: &'static str = "id";
    const FIELDS: &'static [&'static str] = &["id", "status"];
    const INDEXES: &'static [&'static IndexModel] = &[];
    const MODEL: &'static EntityModel = &ENUM_REF_MODEL;

    fn key(&self) -> Self::PrimaryKey {
        self.id
    }

    fn primary_key(&self) -> Self::PrimaryKey {
        self.id
    }

    fn set_primary_key(&mut self, key: Self::PrimaryKey) {
        self.id = key;
    }
}

impl EntityKind for CollectionRefEntity {
    type PrimaryKey = Ref<Self>;
    type DataStore = TestDataStore;
    type Canister = TestCanister;

    const ENTITY_NAME: &'static str = "CollectionRefEntity";
    const PRIMARY_KEY: &'static str = "id";
    const FIELDS: &'static [&'static str] = &["id", "owners"];
    const INDEXES: &'static [&'static IndexModel] = &[];
    const MODEL: &'static EntityModel = &COLLECTION_REF_MODEL;

    fn key(&self) -> Self::PrimaryKey {
        self.id
    }

    fn primary_key(&self) -> Self::PrimaryKey {
        self.id
    }

    fn set_primary_key(&mut self, key: Self::PrimaryKey) {
        self.id = key;
    }
}

/// UnitEntity
/// Test-only singleton entity with a unit primary key.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
struct UnitEntity {
    id: Ref<Self>,
}

impl Path for UnitEntity {
    const PATH: &'static str = UNIT_ENTITY_PATH;
}

impl View for UnitEntity {
    type ViewType = Self;

    fn to_view(&self) -> Self::ViewType {
        self.clone()
    }

    fn from_view(view: Self::ViewType) -> Self {
        view
    }
}

impl SanitizeAuto for UnitEntity {}
impl SanitizeCustom for UnitEntity {}
impl ValidateAuto for UnitEntity {}
impl ValidateCustom for UnitEntity {}
impl Visitable for UnitEntity {}

impl FieldValues for UnitEntity {
    fn get_value(&self, field: &str) -> Option<Value> {
        match field {
            "id" => Some(self.id.to_value()),
            _ => None,
        }
    }
}

impl EntityKind for UnitEntity {
    type PrimaryKey = Ref<Self>;
    type DataStore = TestDataStore;
    type Canister = TestCanister;

    const ENTITY_NAME: &'static str = "UnitEntity";
    const PRIMARY_KEY: &'static str = "id";
    const FIELDS: &'static [&'static str] = &UNIT_FIELDS;
    const INDEXES: &'static [&'static IndexModel] = &[];
    const MODEL: &'static EntityModel = &UNIT_MODEL;

    fn key(&self) -> Self::PrimaryKey {
        self.id
    }

    fn primary_key(&self) -> Self::PrimaryKey {
        self.id
    }

    fn set_primary_key(&mut self, key: Self::PrimaryKey) {
        self.id = key;
    }
}

impl UnitKey for UnitEntity {}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct OrderEntity {
    id: Ref<Self>,
    primary: Value,
    secondary: i64,
}

impl Default for OrderEntity {
    fn default() -> Self {
        Self {
            id: Ref::new(Ulid::nil()),
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
            "id" => Some(self.id.to_value()),
            "primary" => Some(self.primary.clone()),
            "secondary" => Some(Value::Int(self.secondary)),
            _ => None,
        }
    }
}

impl EntityKind for OrderEntity {
    type PrimaryKey = Ref<Self>;
    type DataStore = TestDataStore;
    type Canister = TestCanister;

    const ENTITY_NAME: &'static str = "OrderEntity";
    const PRIMARY_KEY: &'static str = "id";
    const FIELDS: &'static [&'static str] = &ORDER_FIELDS;
    const INDEXES: &'static [&'static IndexModel] = &[];
    const MODEL: &'static EntityModel = &ORDER_MODEL;

    fn key(&self) -> Self::PrimaryKey {
        self.id
    }

    fn primary_key(&self) -> Self::PrimaryKey {
        self.id
    }

    fn set_primary_key(&mut self, key: Self::PrimaryKey) {
        self.id = key;
    }
}

// Timestamp-typed entity used to verify ByKey planning and strict consistency behavior.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
struct TimestampEntity {
    id: Ref<Self>,
}

impl Path for TimestampEntity {
    const PATH: &'static str = TIMESTAMP_ENTITY_PATH;
}

impl View for TimestampEntity {
    type ViewType = Self;

    fn to_view(&self) -> Self::ViewType {
        self.clone()
    }

    fn from_view(view: Self::ViewType) -> Self {
        view
    }
}

impl SanitizeAuto for TimestampEntity {}
impl SanitizeCustom for TimestampEntity {}
impl ValidateAuto for TimestampEntity {}
impl ValidateCustom for TimestampEntity {}
impl Visitable for TimestampEntity {}

impl FieldValues for TimestampEntity {
    fn get_value(&self, field: &str) -> Option<Value> {
        match field {
            "id" => Some(self.id.to_value()),
            _ => None,
        }
    }
}

impl EntityKind for TimestampEntity {
    type PrimaryKey = Ref<Self>;
    type DataStore = TestDataStore;
    type Canister = TestCanister;

    const ENTITY_NAME: &'static str = "TimestampEntity";
    const PRIMARY_KEY: &'static str = "id";
    const FIELDS: &'static [&'static str] = &TIMESTAMP_FIELDS;
    const INDEXES: &'static [&'static IndexModel] = &[];
    const MODEL: &'static EntityModel = &TIMESTAMP_MODEL;

    fn key(&self) -> Self::PrimaryKey {
        self.id
    }

    fn primary_key(&self) -> Self::PrimaryKey {
        self.id
    }

    fn set_primary_key(&mut self, key: Self::PrimaryKey) {
        self.id = key;
    }
}

canic_memory::eager_static! {
    static TEST_DATA_STORE: RefCell<DbDataStore> =
        RefCell::new(DbDataStore::init(canic_memory::ic_memory!(DbDataStore, 10)));
}

canic_memory::eager_static! {
    static TEST_INDEX_STORE: RefCell<DbIndexStore> =
        RefCell::new(DbIndexStore::init(
            canic_memory::ic_memory!(DbIndexStore, 11),
            canic_memory::ic_memory!(DbIndexStore, 12),
        ));
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

static DB: Db<TestCanister> = Db::new(&DATA_REGISTRY, &INDEX_REGISTRY, &[]);

const RI_ENTITY_REGISTRY: [EntityRegistryEntry; 5] = [
    EntityRegistryEntry {
        entity_path: OWNER_ENTITY_PATH,
        store_path: DATA_STORE_PATH,
    },
    EntityRegistryEntry {
        entity_path: DIRECT_REF_ENTITY_PATH,
        store_path: DATA_STORE_PATH,
    },
    EntityRegistryEntry {
        entity_path: RECORD_REF_ENTITY_PATH,
        store_path: DATA_STORE_PATH,
    },
    EntityRegistryEntry {
        entity_path: ENUM_REF_ENTITY_PATH,
        store_path: DATA_STORE_PATH,
    },
    EntityRegistryEntry {
        entity_path: COLLECTION_REF_ENTITY_PATH,
        store_path: DATA_STORE_PATH,
    },
];

static RI_DB: Db<TestCanister> = Db::new(&DATA_REGISTRY, &INDEX_REGISTRY, &RI_ENTITY_REGISTRY);

canic_memory::eager_init!({
    canic_memory::ic_memory_range!(0, 40);
});

static INIT_SCHEMA: Once = Once::new();
thread_local! {
    static INIT_REGISTRY: Once = const { Once::new() };
}

fn init_memory_registry() {
    INIT_REGISTRY.with(|once| {
        once.call_once(|| {
            MemoryRegistryRuntime::init(Some((env!("CARGO_PKG_NAME"), 0, 40)))
                .expect("memory registry init");
        });
    });
}

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
                        target: ItemTarget::Primitive(Primitive::Int),
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
        static TIMESTAMP_FIELDS_DEF: [Field; 1] = [Field {
            ident: "id",
            value: SchemaValue {
                cardinality: Cardinality::One,
                item: Item {
                    target: ItemTarget::Primitive(Primitive::Timestamp),
                    relation: None,
                    validators: &[],
                    sanitizers: &[],
                    indirect: false,
                },
            },
            default: None,
        }];

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

        let data_store = SchemaDataStore {
            def: Def {
                module_path: "write_unit_test",
                ident: "TestDataStore",
                comments: None,
            },
            ident: "TEST_DATA_STORE",
            canister: CANISTER_PATH,
            memory_id: 10,
        };

        let index_store = SchemaIndexStore {
            def: Def {
                module_path: "write_unit_test",
                ident: "TestIndexStore",
                comments: None,
            },
            ident: "TEST_INDEX_STORE",
            canister: CANISTER_PATH,
            entry_memory_id: 11,
            fingerprint_memory_id: 12,
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
        let timestamp_entity = Entity {
            def: Def {
                module_path: "write_unit_test",
                ident: "TimestampEntity",
                comments: None,
            },
            store: DATA_STORE_PATH,
            primary_key: "id",
            name: None,
            indexes: &[],
            fields: FieldList {
                fields: &TIMESTAMP_FIELDS_DEF,
            },
            ty: Type {
                sanitizers: &[],
                validators: &[],
            },
        };

        schema.insert_node(SchemaNode::Canister(canister));
        schema.insert_node(SchemaNode::DataStore(data_store));
        schema.insert_node(SchemaNode::IndexStore(index_store));
        schema.insert_node(SchemaNode::Entity(test_entity));
        schema.insert_node(SchemaNode::Entity(order_entity));
        schema.insert_node(SchemaNode::Entity(timestamp_entity));
    });
}

fn setup_schema() {
    reset_stores();
    init_schema();
}

fn reset_stores() {
    TEST_DATA_STORE.with_borrow_mut(DbDataStore::clear);
    TEST_INDEX_STORE.with_borrow_mut(DbIndexStore::clear);
    init_memory_registry();
}

struct TestTraceSink {
    events: Mutex<Vec<QueryTraceEvent>>,
}

impl QueryTraceSink for TestTraceSink {
    fn on_event(&self, event: QueryTraceEvent) {
        self.events.lock().unwrap().push(event);
    }
}

static TRACE_SINK: TestTraceSink = TestTraceSink {
    events: Mutex::new(Vec::new()),
};
static TRACE_GUARD: Mutex<()> = Mutex::new(());

fn with_trace_events<F, R>(f: F) -> (R, Vec<QueryTraceEvent>)
where
    F: FnOnce() -> R,
{
    let _guard = TRACE_GUARD.lock().unwrap();
    TRACE_SINK.events.lock().unwrap().clear();
    let result = f();
    let events = mem::take(&mut *TRACE_SINK.events.lock().unwrap());
    (result, events)
}

fn assert_commit_marker_clear() {
    assert!(!commit_marker_present().unwrap());
}

// Build a commit marker that inserts a single entity row and index entry.
fn commit_marker_for_entity(entity: &TestEntity) -> CommitMarker {
    let data_key = DataKey::new::<TestEntity>(entity.id);
    let raw_data_key = data_key.to_raw().expect("data key encode");
    let raw_row = RawRow::try_new(serialize(entity).unwrap()).unwrap();

    let index_key = IndexKey::new(entity, &INDEX_MODEL)
        .expect("index key")
        .expect("index key missing");
    let raw_index_key = index_key.to_raw();
    let entry = IndexEntry::new(entity.key());
    let raw_index_entry = RawIndexEntry::try_from_entry(&entry).unwrap();

    CommitMarker::new(
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
    .unwrap()
}

fn assert_entity_present(entity: &TestEntity) {
    let data_key = DataKey::new::<TestEntity>(entity.id);
    let raw_key = data_key.to_raw().expect("data key encode");
    let data_present = DB
        .context::<TestEntity>()
        .with_store(|s| s.get(&raw_key))
        .unwrap();
    assert!(data_present.is_some());

    let index_key = IndexKey::new(entity, &INDEX_MODEL)
        .expect("index key")
        .expect("index key missing");
    let raw_index_key = index_key.to_raw();
    let index_present = DB
        .with_index(|reg| reg.with_store(INDEX_STORE_PATH, |s| s.get(&raw_index_key)))
        .unwrap();
    assert!(index_present.is_some());
}

fn assert_row_present<E: EntityKind<PrimaryKey = Ref<E>>>(db: Db<E::Canister>, key: E::PrimaryKey) {
    let data_key = DataKey::new::<E>(key);
    let raw_key = data_key.to_raw().expect("data key encode");
    let data_present = db.context::<E>().with_store(|s| s.get(&raw_key)).unwrap();
    assert!(data_present.is_some());
}

fn assert_row_missing<E: EntityKind<PrimaryKey = Ref<E>>>(db: Db<E::Canister>, key: E::PrimaryKey) {
    let data_key = DataKey::new::<E>(key);
    let raw_key = data_key.to_raw().expect("data key encode");
    let data_present = db.context::<E>().with_store(|s| s.get(&raw_key)).unwrap();
    assert!(data_present.is_none());
}

fn assert_entity_missing(entity: &TestEntity) {
    let data_key = DataKey::new::<TestEntity>(entity.id);
    let raw_key = data_key.to_raw().expect("data key encode");
    let data_present = DB
        .context::<TestEntity>()
        .with_store(|s| s.get(&raw_key))
        .unwrap();
    assert!(data_present.is_none());

    let index_key = IndexKey::new(entity, &INDEX_MODEL)
        .expect("index key")
        .expect("index key missing");
    let raw_index_key = index_key.to_raw();
    let index_present = DB
        .with_index(|reg| reg.with_store(INDEX_STORE_PATH, |s| s.get(&raw_index_key)))
        .unwrap();
    assert!(index_present.is_none());
}

#[cfg(debug_assertions)]
#[test]
fn debug_index_fingerprint_flow_demo() {
    setup_schema();

    let saver = SaveExecutor::<TestEntity>::new(DB, false);
    let entity_a = TestEntity {
        id: Ref::new(Ulid::from_u128(100)),
        name: "alpha".to_string(),
    };
    let entity_b = TestEntity {
        id: Ref::new(Ulid::from_u128(101)),
        name: "bravo".to_string(),
    };

    let to_hex = |bytes: [u8; 16]| {
        let mut out = String::with_capacity(32);
        for byte in bytes {
            let _ = write!(&mut out, "{byte:02x}");
        }
        out
    };
    let load_fingerprint = |raw_key| {
        DB.with_index(|reg| reg.with_store(INDEX_STORE_PATH, |s| s.debug_fingerprint_for(raw_key)))
            .expect("index store access")
    };
    let print_fingerprint = |label: &str, key: &IndexKey, raw_key| match load_fingerprint(raw_key) {
        Some(bytes) => {
            println!(
                "[fingerprint-demo] {label}: key={key:?} fingerprint=0x{}",
                to_hex(bytes)
            );
        }
        None => {
            println!("[fingerprint-demo] {label}: key={key:?} fingerprint=<removed>");
        }
    };

    // Phase 1: insert entities to create index entries + fingerprints.
    saver.insert(entity_a.clone()).unwrap();
    saver.insert(entity_b.clone()).unwrap();

    let key_a = IndexKey::new(&entity_a, &INDEX_MODEL)
        .expect("index key")
        .expect("index key missing");
    let key_b = IndexKey::new(&entity_b, &INDEX_MODEL)
        .expect("index key")
        .expect("index key missing");
    let raw_key_a = key_a.to_raw();
    let raw_key_b = key_b.to_raw();

    print_fingerprint("after insert (a)", &key_a, &raw_key_a);
    print_fingerprint("after insert (b)", &key_b, &raw_key_b);

    // Phase 2: delete one entity and show fingerprint removal.
    let session = DbSession::new(DB);
    let deleted = session
        .delete::<TestEntity>()
        .many(vec![entity_a.id])
        .execute()
        .unwrap();
    println!(
        "[fingerprint-demo] delete: removed {} entity",
        deleted.entities().len()
    );
    print_fingerprint("after delete (a)", &key_a, &raw_key_a);

    // Phase 3: rebuild indexes to regenerate fingerprints.
    crate::db::index::rebuild::rebuild_indexes_for_entity::<TestEntity>(&DB).unwrap();
    print_fingerprint("after rebuild (b)", &key_b, &raw_key_b);
}

#[test]
fn save_rolls_back_on_forced_failure() {
    reset_stores();
    fail_next_checkpoint();

    let entity = TestEntity {
        id: Ref::new(Ulid::nil()),
        name: "alpha".to_string(),
    };

    let saver = SaveExecutor::<TestEntity>::new(DB, false);
    let result = saver.insert(entity.clone());
    assert!(result.is_err());

    let data_key = DataKey::new::<TestEntity>(entity.id);
    let raw_key = data_key.to_raw().expect("data key encode");
    let data_present = DB
        .context::<TestEntity>()
        .with_store(|s| s.get(&raw_key))
        .unwrap();
    assert!(data_present.is_none());

    let index_key = IndexKey::new(&entity, &INDEX_MODEL)
        .expect("index key")
        .expect("index key missing");
    let raw_index_key = index_key.to_raw();
    let index_present = DB
        .with_index(|reg| reg.with_store(INDEX_STORE_PATH, |s| s.get(&raw_index_key)))
        .unwrap();
    assert!(index_present.is_none());
}

#[test]
fn save_update_rejects_row_key_mismatch() {
    setup_schema();

    let stored = TestEntity {
        id: Ref::new(Ulid::from_u128(1)),
        name: "alpha".to_string(),
    };
    let data_key = DataKey::new::<TestEntity>(Ref::new(Ulid::from_u128(2)));
    let raw_key = data_key.to_raw().expect("data key encode");
    let raw_row = RawRow::try_new(serialize(&stored).unwrap()).unwrap();
    DB.context::<TestEntity>()
        .with_store_mut(|store| store.insert(raw_key, raw_row))
        .unwrap();

    let saver = SaveExecutor::<TestEntity>::new(DB, false);
    let entity = TestEntity {
        id: Ref::new(Ulid::from_u128(2)),
        name: "beta".to_string(),
    };
    let err = saver.update(entity).unwrap_err();
    assert_eq!(err.class, ErrorClass::Corruption);
    assert_eq!(err.origin, ErrorOrigin::Store);
}

#[test]
fn save_insert_rejects_row_key_mismatch() {
    setup_schema();

    let stored = TestEntity {
        id: Ref::new(Ulid::from_u128(1)),
        name: "alpha".to_string(),
    };
    let data_key = DataKey::new::<TestEntity>(Ref::new(Ulid::from_u128(2)));
    let raw_key = data_key.to_raw().expect("data key encode");
    let raw_row = RawRow::try_new(serialize(&stored).unwrap()).unwrap();
    DB.context::<TestEntity>()
        .with_store_mut(|store| store.insert(raw_key, raw_row))
        .unwrap();

    let saver = SaveExecutor::<TestEntity>::new(DB, false);
    let entity = TestEntity {
        id: Ref::new(Ulid::from_u128(2)),
        name: "beta".to_string(),
    };
    let err = saver.insert(entity).unwrap_err();
    assert_eq!(err.class, ErrorClass::Corruption);
    assert_eq!(err.origin, ErrorOrigin::Store);
}

#[test]
fn save_replace_rejects_row_key_mismatch() {
    setup_schema();

    let stored = TestEntity {
        id: Ref::new(Ulid::from_u128(3)),
        name: "alpha".to_string(),
    };
    let data_key = DataKey::new::<TestEntity>(Ref::new(Ulid::from_u128(4)));
    let raw_key = data_key.to_raw().expect("data key encode");
    let raw_row = RawRow::try_new(serialize(&stored).unwrap()).unwrap();
    DB.context::<TestEntity>()
        .with_store_mut(|store| store.insert(raw_key, raw_row))
        .unwrap();

    let saver = SaveExecutor::<TestEntity>::new(DB, false);
    let entity = TestEntity {
        id: Ref::new(Ulid::from_u128(4)),
        name: "beta".to_string(),
    };
    let err = saver.replace(entity).unwrap_err();
    assert_eq!(err.class, ErrorClass::Corruption);
    assert_eq!(err.origin, ErrorOrigin::Store);
}

#[test]
fn load_recovers_when_commit_marker_present() {
    setup_schema();

    let entity = TestEntity {
        id: Ref::new(Ulid::from_u128(9)),
        name: "alpha".to_string(),
    };

    let _guard = begin_commit(commit_marker_for_entity(&entity)).unwrap();
    assert!(commit_marker_present().unwrap());
    force_recovery_for_tests();

    let plan = Query::<TestEntity>::new(ReadConsistency::Strict)
        .filter(FieldRef::new("id").eq(entity.id))
        .plan()
        .expect("plan");
    let loader = LoadExecutor::<TestEntity>::new(DB, false);
    let response = loader.execute(plan).unwrap();

    assert_eq!(response.entities(), vec![entity]);
    assert_commit_marker_clear();
}

#[test]
fn context_reads_enforce_recovery() {
    setup_schema();

    let entity = TestEntity {
        id: Ref::new(Ulid::from_u128(12)),
        name: "alpha".to_string(),
    };
    let data_key = DataKey::new::<TestEntity>(entity.id);

    let _guard = begin_commit(commit_marker_for_entity(&entity)).unwrap();
    assert!(commit_marker_present().unwrap());
    force_recovery_for_tests();

    let ctx = DB.recovered_context::<TestEntity>().unwrap();
    let _ = ctx.read(&data_key).unwrap();
    assert_commit_marker_clear();

    let _guard = begin_commit(commit_marker_for_entity(&entity)).unwrap();
    assert!(commit_marker_present().unwrap());
    force_recovery_for_tests();

    let ctx = DB.recovered_context::<TestEntity>().unwrap();
    let _ = ctx.read_strict(&data_key).unwrap();
    assert_commit_marker_clear();

    let _guard = begin_commit(commit_marker_for_entity(&entity)).unwrap();
    assert!(commit_marker_present().unwrap());
    force_recovery_for_tests();

    let ctx = DB.recovered_context::<TestEntity>().unwrap();
    let access = AccessPath::IndexPrefix {
        index: INDEX_MODEL,
        values: vec![Value::Text(entity.name.clone())],
    };
    let rows = ctx
        .rows_from_access(&access, ReadConsistency::Strict)
        .unwrap();
    let decoded = ctx.deserialize_rows(rows).unwrap();
    assert_eq!(decoded.len(), 1);
    assert_eq!(decoded[0].1, entity);
    assert_commit_marker_clear();
}

#[test]
fn delete_scan_rolls_back_after_data_removal() {
    setup_schema();

    let saver = SaveExecutor::<TestEntity>::new(DB, false);
    let entity_a = TestEntity {
        id: Ref::new(Ulid::from_u128(10)),
        name: "alpha".to_string(),
    };
    let entity_b = TestEntity {
        id: Ref::new(Ulid::from_u128(11)),
        name: "beta".to_string(),
    };
    saver.insert(entity_a.clone()).unwrap();
    saver.insert(entity_b.clone()).unwrap();
    assert_entity_present(&entity_a);
    assert_entity_present(&entity_b);

    let deleter = DeleteExecutor::<TestEntity>::new(DB, false);
    let mut plan = LogicalPlan::new(AccessPath::FullScan, ReadConsistency::MissingOk);
    plan.mode = QueryMode::Delete(DeleteSpec::new());
    let plan = ExecutablePlan::new(plan);

    fail_checkpoint_label("delete_after_data");
    let result = deleter.clone().execute(plan);
    assert!(result.is_err());
    assert_entity_present(&entity_a);
    assert_entity_present(&entity_b);
    assert_commit_marker_clear();

    let mut plan = LogicalPlan::new(AccessPath::FullScan, ReadConsistency::MissingOk);
    plan.mode = QueryMode::Delete(DeleteSpec::new());
    let response = deleter.clone().execute(ExecutablePlan::new(plan)).unwrap();
    assert_eq!(response.0.len(), 2);
    assert_entity_missing(&entity_a);
    assert_entity_missing(&entity_b);
    assert_commit_marker_clear();

    let mut plan = LogicalPlan::new(AccessPath::FullScan, ReadConsistency::MissingOk);
    plan.mode = QueryMode::Delete(DeleteSpec::new());
    let response = deleter.execute(ExecutablePlan::new(plan)).unwrap();
    assert!(response.0.is_empty());
    assert_commit_marker_clear();
}

#[test]
fn delete_limit_deletes_oldest_rows() {
    setup_schema();

    let saver = SaveExecutor::<TestEntity>::new(DB, false);
    let entities = vec![
        TestEntity {
            id: Ref::new(Ulid::from_u128(1)),
            name: "delta".to_string(),
        },
        TestEntity {
            id: Ref::new(Ulid::from_u128(2)),
            name: "alpha".to_string(),
        },
        TestEntity {
            id: Ref::new(Ulid::from_u128(3)),
            name: "charlie".to_string(),
        },
        TestEntity {
            id: Ref::new(Ulid::from_u128(4)),
            name: "bravo".to_string(),
        },
    ];

    for entity in entities {
        saver.insert(entity).unwrap();
    }

    let deleter = DeleteExecutor::<TestEntity>::new(DB, false);
    let plan = Query::<TestEntity>::new(ReadConsistency::MissingOk)
        .order_by("name")
        .delete()
        .limit(2)
        .plan()
        .unwrap();
    let deleted_entities = deleter.execute(plan).unwrap().entities();
    let deleted_names = deleted_entities
        .iter()
        .map(|entity| entity.name.as_str())
        .collect::<Vec<_>>();
    assert_eq!(deleted_names, vec!["alpha", "bravo"]);

    let loader = LoadExecutor::<TestEntity>::new(DB, false);
    let remaining = loader
        .execute(
            Query::<TestEntity>::new(ReadConsistency::MissingOk)
                .order_by("name")
                .plan()
                .unwrap(),
        )
        .unwrap()
        .entities();
    let remaining_names = remaining
        .iter()
        .map(|entity| entity.name.as_str())
        .collect::<Vec<_>>();
    assert_eq!(remaining_names, vec!["charlie", "delta"]);
}

#[test]
fn delete_limit_without_order_is_rejected() {
    setup_schema();

    let deleter = DeleteExecutor::<TestEntity>::new(DB, false);
    let mut plan = LogicalPlan::new(AccessPath::FullScan, ReadConsistency::MissingOk);
    plan.mode = QueryMode::Delete(DeleteSpec { limit: Some(1) });
    plan.delete_limit = Some(crate::db::query::plan::DeleteLimitSpec { max_rows: 1 });

    let err = deleter.execute(ExecutablePlan::new(plan)).unwrap_err();
    assert_eq!(err.class, ErrorClass::InvariantViolation);
    assert_eq!(err.origin, ErrorOrigin::Query);
}

#[test]
fn load_by_key_missing_is_ok() {
    setup_schema();

    let plan = ExecutablePlan::new(LogicalPlan::new(
        AccessPath::ByKey(Ref::new(Ulid::from_u128(1))),
        ReadConsistency::MissingOk,
    ));
    let loader = LoadExecutor::<TestEntity>::new(DB, false);
    let response = loader.execute(plan).unwrap();

    assert!(response.is_empty());
}

#[test]
fn load_by_keys_dedups_duplicates() {
    setup_schema();

    let saver = SaveExecutor::<TestEntity>::new(DB, false);
    let entity = TestEntity {
        id: Ref::new(Ulid::from_u128(7)),
        name: "alpha".to_string(),
    };
    saver.insert(entity.clone()).unwrap();

    let plan = ExecutablePlan::new(LogicalPlan::new(
        AccessPath::ByKeys(vec![entity.id, entity.id, entity.id]),
        ReadConsistency::MissingOk,
    ));
    let loader = LoadExecutor::<TestEntity>::new(DB, false);
    let rows = loader.execute(plan).unwrap().entities();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, entity.id);
}

#[test]
fn load_by_keys_skips_missing_after_dedup() {
    setup_schema();

    let saver = SaveExecutor::<TestEntity>::new(DB, false);
    let entity = TestEntity {
        id: Ref::new(Ulid::from_u128(1)),
        name: "alpha".to_string(),
    };
    saver.insert(entity.clone()).unwrap();

    let plan = ExecutablePlan::new(LogicalPlan::new(
        AccessPath::ByKeys(vec![
            Ref::new(Ulid::from_u128(1)),
            Ref::new(Ulid::from_u128(2)),
            Ref::new(Ulid::from_u128(1)),
            Ref::new(Ulid::from_u128(3)),
        ]),
        ReadConsistency::MissingOk,
    ));
    let loader = LoadExecutor::<TestEntity>::new(DB, false);
    let rows = loader.execute(plan).unwrap().entities();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, entity.id);
}

#[test]
fn session_many_empty_is_noop() {
    setup_schema();

    let saver = SaveExecutor::<TestEntity>::new(DB, false);
    let entity = TestEntity {
        id: Ref::new(Ulid::from_u128(1)),
        name: "alpha".to_string(),
    };
    saver.insert(entity).unwrap();

    let session = DbSession::new(DB);
    let resp = session
        .load::<TestEntity>()
        .many(Vec::<Ref<TestEntity>>::new())
        .execute()
        .unwrap();

    assert!(resp.is_empty());
    assert_eq!(resp.count(), 0);
}

#[test]
fn session_many_dedups_duplicate_keys() {
    setup_schema();

    let saver = SaveExecutor::<TestEntity>::new(DB, false);
    let entity = TestEntity {
        id: Ref::new(Ulid::from_u128(7)),
        name: "alpha".to_string(),
    };
    saver.insert(entity.clone()).unwrap();

    let session = DbSession::new(DB);
    let resp = session
        .load::<TestEntity>()
        .many(vec![entity.id, entity.id, entity.id])
        .execute()
        .unwrap();

    let entities = resp.entities();
    assert_eq!(entities.len(), 1);
    assert_eq!(entities[0].id, entity.id);
}

#[test]
fn session_many_missing_ok_skips_missing() {
    setup_schema();

    let saver = SaveExecutor::<TestEntity>::new(DB, false);
    let entity = TestEntity {
        id: Ref::new(Ulid::from_u128(1)),
        name: "alpha".to_string(),
    };
    saver.insert(entity.clone()).unwrap();

    let session = DbSession::new(DB);
    let resp = session
        .load::<TestEntity>()
        .many(vec![entity.id, Ref::new(Ulid::from_u128(2))])
        .execute()
        .unwrap();

    let entities = resp.entities();
    assert_eq!(entities.len(), 1);
    assert_eq!(entities[0].id, entity.id);
}

#[test]
fn session_many_strict_missing_errors() {
    setup_schema();

    let session = DbSession::new(DB);
    let err = session
        .load_with_consistency::<TestEntity>(ReadConsistency::Strict)
        .many(vec![Ref::new(Ulid::from_u128(99))])
        .execute()
        .expect_err("strict missing should error");

    assert!(matches!(
        err,
        QueryError::Execute(inner) if inner.class == ErrorClass::Corruption
    ));
}

#[test]
fn session_many_views_materializes() {
    setup_schema();

    let saver = SaveExecutor::<TestEntity>::new(DB, false);
    let entity = TestEntity {
        id: Ref::new(Ulid::from_u128(42)),
        name: "alpha".to_string(),
    };
    saver.insert(entity.clone()).unwrap();

    let session = DbSession::new(DB);
    let views = session
        .load::<TestEntity>()
        .many(vec![entity.id])
        .execute()
        .unwrap()
        .views();

    assert_eq!(views.len(), 1);
    assert_eq!(views[0], entity);
}

#[test]
fn session_delete_many_by_primary_key() {
    setup_schema();

    let saver = SaveExecutor::<TestEntity>::new(DB, false);
    let a = TestEntity {
        id: Ref::new(Ulid::from_u128(10)),
        name: "alpha".to_string(),
    };
    let b = TestEntity {
        id: Ref::new(Ulid::from_u128(11)),
        name: "beta".to_string(),
    };
    saver.insert(a.clone()).unwrap();
    saver.insert(b.clone()).unwrap();

    let session = DbSession::new(DB);
    let deleted = session
        .delete::<TestEntity>()
        .many(vec![a.id, b.id])
        .execute()
        .unwrap()
        .entities();

    assert_eq!(deleted.len(), 2);
    let remaining = session.load::<TestEntity>().execute().unwrap().entities();
    assert!(remaining.is_empty());
}

#[test]
fn session_only_missing_ok_skips_missing() {
    setup_schema();

    let session = DbSession::new(DB);
    let rows = session
        .load::<UnitEntity>()
        .only()
        .execute()
        .unwrap()
        .entities();

    assert!(rows.is_empty());
}

#[test]
fn session_only_strict_missing_errors() {
    setup_schema();

    let session = DbSession::new(DB);
    let err = session
        .load_with_consistency::<UnitEntity>(ReadConsistency::Strict)
        .only()
        .execute()
        .expect_err("strict missing should error");

    assert!(matches!(
        err,
        QueryError::Execute(inner) if inner.class == ErrorClass::Corruption
    ));
}

#[test]
fn session_only_delete_is_idempotent_missing_ok() {
    setup_schema();

    let session = DbSession::new(DB);
    let deleted = session
        .delete::<UnitEntity>()
        .only()
        .execute()
        .unwrap()
        .entities();

    assert!(deleted.is_empty());
}

#[test]
fn session_only_loads_singleton() {
    setup_schema();

    let saver = SaveExecutor::<UnitEntity>::new(DB, false);
    let entity = UnitEntity { id: Ref::new(Unit) };
    saver.insert(entity.clone()).unwrap();

    let session = DbSession::new(DB);
    let loaded = session
        .load::<UnitEntity>()
        .only()
        .execute()
        .unwrap()
        .entity()
        .unwrap();

    assert_eq!(loaded, entity);
}

#[test]
fn load_or_predicate_executes_union() {
    setup_schema();

    let saver = SaveExecutor::<TestEntity>::new(DB, false);
    let a = TestEntity {
        id: Ref::new(Ulid::from_u128(1)),
        name: "alpha".to_string(),
    };
    let b = TestEntity {
        id: Ref::new(Ulid::from_u128(2)),
        name: "beta".to_string(),
    };
    saver.insert(a.clone()).unwrap();
    saver.insert(b.clone()).unwrap();

    let predicate = Predicate::Or(vec![
        FieldRef::new("id").eq(a.id),
        FieldRef::new("id").eq(b.id),
    ]);

    let plan = Query::<TestEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate)
        .plan()
        .expect("plan");

    let loader = LoadExecutor::<TestEntity>::new(DB, false);
    let rows = loader.execute(plan).unwrap().entities();

    assert_eq!(rows.len(), 2);
}

#[test]
fn load_in_predicate_executes_union() {
    setup_schema();

    let saver = SaveExecutor::<TestEntity>::new(DB, false);
    let a = TestEntity {
        id: Ref::new(Ulid::from_u128(10)),
        name: "alpha".to_string(),
    };
    let b = TestEntity {
        id: Ref::new(Ulid::from_u128(11)),
        name: "beta".to_string(),
    };
    saver.insert(a).unwrap();
    saver.insert(b).unwrap();

    let predicate = FieldRef::new("name").in_list(["alpha", "beta"]);

    let plan = Query::<TestEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate)
        .plan()
        .expect("plan");

    let loader = LoadExecutor::<TestEntity>::new(DB, false);
    let rows = loader.execute(plan).unwrap().entities();

    assert_eq!(rows.len(), 2);
}

#[test]
fn load_or_strict_missing_errors() {
    setup_schema();

    let saver = SaveExecutor::<TestEntity>::new(DB, false);
    let entity = TestEntity {
        id: Ref::new(Ulid::from_u128(42)),
        name: "alpha".to_string(),
    };
    saver.insert(entity.clone()).unwrap();

    let predicate = Predicate::Or(vec![
        FieldRef::new("id").eq(entity.id),
        FieldRef::new("id").eq(Ulid::from_u128(43)),
    ]);

    let plan = Query::<TestEntity>::new(ReadConsistency::Strict)
        .filter(predicate)
        .plan()
        .expect("plan");

    let loader = LoadExecutor::<TestEntity>::new(DB, false);
    let err = loader.execute(plan).unwrap_err();

    assert_eq!(err.class, ErrorClass::Corruption);
    assert_eq!(err.origin, ErrorOrigin::Store);
}

#[test]
fn timestamp_pk_plans_by_key_and_strict_missing_is_corruption() {
    setup_schema();

    let ts = Timestamp::from_seconds(123);
    let plan = Query::<TimestampEntity>::new(ReadConsistency::Strict)
        .filter(FieldRef::new("id").eq(ts))
        .plan()
        .expect("plan");

    let fingerprint = plan.fingerprint();

    assert_eq!(
        plan.explain().access,
        ExplainAccessPath::ByKey {
            key: Value::Timestamp(ts),
        }
    );

    let loader = LoadExecutor::<TimestampEntity>::new(DB, false).with_trace_sink(Some(&TRACE_SINK));
    let (result, events) = with_trace_events(|| loader.execute(plan));
    let err = result.unwrap_err();

    assert_eq!(err.class, ErrorClass::Corruption);
    assert_eq!(err.origin, ErrorOrigin::Store);
    assert_eq!(
        events,
        vec![
            QueryTraceEvent::Start {
                fingerprint,
                executor: TraceExecutorKind::Load,
                access: Some(TraceAccess::ByKey),
            },
            QueryTraceEvent::Error {
                fingerprint,
                executor: TraceExecutorKind::Load,
                access: Some(TraceAccess::ByKey),
                class: ErrorClass::Corruption,
                origin: ErrorOrigin::Store,
            },
        ]
    );
}
#[test]
fn trace_emits_start_and_finish_for_load() {
    setup_schema();

    let saver = SaveExecutor::<TestEntity>::new(DB, false);
    let entity = TestEntity {
        id: Ref::new(Ulid::from_u128(55)),
        name: "alpha".to_string(),
    };
    saver.insert(entity).unwrap();

    let plan = LogicalPlan::new(AccessPath::FullScan, ReadConsistency::MissingOk);
    let fingerprint = plan.fingerprint();
    let loader = LoadExecutor::<TestEntity>::new(DB, false).with_trace_sink(Some(&TRACE_SINK));
    let access = Some(TraceAccess::FullScan);

    let (result, events) = with_trace_events(|| loader.execute(ExecutablePlan::new(plan)));
    let response = result.unwrap();

    assert_eq!(response.0.len(), 1);
    assert_eq!(
        events,
        vec![
            QueryTraceEvent::Start {
                fingerprint,
                executor: TraceExecutorKind::Load,
                access,
            },
            QueryTraceEvent::Phase {
                fingerprint,
                executor: TraceExecutorKind::Load,
                access,
                phase: TracePhase::Access,
                rows: 1,
            },
            QueryTraceEvent::Phase {
                fingerprint,
                executor: TraceExecutorKind::Load,
                access,
                phase: TracePhase::Filter,
                rows: 1,
            },
            QueryTraceEvent::Phase {
                fingerprint,
                executor: TraceExecutorKind::Load,
                access,
                phase: TracePhase::Order,
                rows: 1,
            },
            QueryTraceEvent::Phase {
                fingerprint,
                executor: TraceExecutorKind::Load,
                access,
                phase: TracePhase::Page,
                rows: 1,
            },
            QueryTraceEvent::Finish {
                fingerprint,
                executor: TraceExecutorKind::Load,
                access,
                rows: 1,
            },
        ]
    );
}

#[test]
fn trace_access_includes_composite_plan() {
    setup_schema();

    let saver = SaveExecutor::<TestEntity>::new(DB, false);
    let entity_a = TestEntity {
        id: Ref::new(Ulid::from_u128(1)),
        name: "alpha".to_string(),
    };
    let entity_b = TestEntity {
        id: Ref::new(Ulid::from_u128(2)),
        name: "beta".to_string(),
    };
    saver.insert(entity_a).unwrap();
    saver.insert(entity_b).unwrap();

    let plan = LogicalPlan {
        mode: QueryMode::Load(LoadSpec::new()),
        access: AccessPlan::Union(vec![
            AccessPlan::Path(AccessPath::ByKey(Ref::new(Ulid::from_u128(1)))),
            AccessPlan::Path(AccessPath::ByKey(Ref::new(Ulid::from_u128(2)))),
        ]),
        predicate: None,
        order: None,
        delete_limit: None,
        page: None,
        consistency: ReadConsistency::MissingOk,
    };
    let fingerprint = plan.fingerprint();
    let access = Some(TraceAccess::Union { branches: 2 });
    let loader = LoadExecutor::<TestEntity>::new(DB, false).with_trace_sink(Some(&TRACE_SINK));

    let (result, events) = with_trace_events(|| loader.execute(ExecutablePlan::new(plan)));
    let response = result.unwrap();

    assert_eq!(response.0.len(), 2);
    assert_eq!(
        events,
        vec![
            QueryTraceEvent::Start {
                fingerprint,
                executor: TraceExecutorKind::Load,
                access,
            },
            QueryTraceEvent::Phase {
                fingerprint,
                executor: TraceExecutorKind::Load,
                access,
                phase: TracePhase::Access,
                rows: 2,
            },
            QueryTraceEvent::Phase {
                fingerprint,
                executor: TraceExecutorKind::Load,
                access,
                phase: TracePhase::Filter,
                rows: 2,
            },
            QueryTraceEvent::Phase {
                fingerprint,
                executor: TraceExecutorKind::Load,
                access,
                phase: TracePhase::Order,
                rows: 2,
            },
            QueryTraceEvent::Phase {
                fingerprint,
                executor: TraceExecutorKind::Load,
                access,
                phase: TracePhase::Page,
                rows: 2,
            },
            QueryTraceEvent::Finish {
                fingerprint,
                executor: TraceExecutorKind::Load,
                access,
                rows: 2,
            },
        ]
    );
}

#[test]
fn trace_emits_phase_counts_for_filter_order_page() {
    setup_schema();

    let saver = SaveExecutor::<TestEntity>::new(DB, false);
    let entities = [
        (Ref::new(Ulid::from_u128(10)), "alpha"),
        (Ref::new(Ulid::from_u128(11)), "beta"),
        (Ref::new(Ulid::from_u128(12)), "gamma"),
        (Ref::new(Ulid::from_u128(13)), "delta"),
        (Ref::new(Ulid::from_u128(14)), "epsilon"),
    ];
    for (id, name) in entities {
        saver
            .insert(TestEntity {
                id,
                name: name.to_string(),
            })
            .unwrap();
    }

    let predicate = FieldRef::new("name").in_list(["beta", "delta", "epsilon"]);

    let plan = LogicalPlan {
        mode: QueryMode::Load(LoadSpec::new()),
        access: AccessPlan::Path(AccessPath::FullScan),
        predicate: Some(predicate),
        order: Some(OrderSpec {
            fields: vec![("id".to_string(), OrderDirection::Asc)],
        }),
        delete_limit: None,
        page: Some(PageSpec {
            limit: Some(1),
            offset: 1,
        }),
        consistency: ReadConsistency::MissingOk,
    };

    let fingerprint = plan.fingerprint();
    let access = Some(TraceAccess::FullScan);
    let loader = LoadExecutor::<TestEntity>::new(DB, false).with_trace_sink(Some(&TRACE_SINK));

    let (result, events) = with_trace_events(|| loader.execute(ExecutablePlan::new(plan)));
    let response = result.unwrap();

    assert_eq!(response.0.len(), 1);
    assert_eq!(
        events,
        vec![
            QueryTraceEvent::Start {
                fingerprint,
                executor: TraceExecutorKind::Load,
                access,
            },
            QueryTraceEvent::Phase {
                fingerprint,
                executor: TraceExecutorKind::Load,
                access,
                phase: TracePhase::Access,
                rows: 5,
            },
            QueryTraceEvent::Phase {
                fingerprint,
                executor: TraceExecutorKind::Load,
                access,
                phase: TracePhase::Filter,
                rows: 3,
            },
            QueryTraceEvent::Phase {
                fingerprint,
                executor: TraceExecutorKind::Load,
                access,
                phase: TracePhase::Order,
                rows: 3,
            },
            QueryTraceEvent::Phase {
                fingerprint,
                executor: TraceExecutorKind::Load,
                access,
                phase: TracePhase::Page,
                rows: 1,
            },
            QueryTraceEvent::Finish {
                fingerprint,
                executor: TraceExecutorKind::Load,
                access,
                rows: 1,
            },
        ]
    );
}

#[test]
fn trace_emits_error_for_strict_missing_row() {
    setup_schema();

    let plan = LogicalPlan::new(
        AccessPath::ByKey(Ref::new(Ulid::from_u128(1))),
        ReadConsistency::Strict,
    );
    let fingerprint = plan.fingerprint();
    let loader = LoadExecutor::<TestEntity>::new(DB, false).with_trace_sink(Some(&TRACE_SINK));

    let (result, events) = with_trace_events(|| loader.execute(ExecutablePlan::new(plan)));

    assert!(result.is_err());
    assert_eq!(events.len(), 2);
    assert_eq!(
        events[0],
        QueryTraceEvent::Start {
            fingerprint,
            executor: TraceExecutorKind::Load,
            access: Some(TraceAccess::ByKey),
        }
    );
    assert_eq!(
        events[1],
        QueryTraceEvent::Error {
            fingerprint,
            executor: TraceExecutorKind::Load,
            access: Some(TraceAccess::ByKey),
            class: ErrorClass::Corruption,
            origin: ErrorOrigin::Store,
        }
    );
}

#[test]
fn trace_disabled_emits_no_events() {
    setup_schema();

    let saver = SaveExecutor::<TestEntity>::new(DB, false);
    let entity = TestEntity {
        id: Ref::new(Ulid::from_u128(99)),
        name: "alpha".to_string(),
    };
    saver.insert(entity).unwrap();

    let plan = ExecutablePlan::new(LogicalPlan::new(
        AccessPath::FullScan,
        ReadConsistency::MissingOk,
    ));
    let loader = LoadExecutor::<TestEntity>::new(DB, false);

    let (result, events) = with_trace_events(|| loader.execute(plan));

    assert!(result.is_ok());
    assert!(events.is_empty());
}

#[test]
fn trace_emits_start_and_finish_for_save() {
    setup_schema();

    let save_executor =
        SaveExecutor::<TestEntity>::new(DB, false).with_trace_sink(Some(&TRACE_SINK));
    let entity = TestEntity {
        id: Ref::new(Ulid::from_u128(1001)),
        name: "alpha".to_string(),
    };

    let (result, events) = with_trace_events(|| save_executor.insert(entity));
    let saved = result.unwrap();

    assert_eq!(saved.id, Ref::new(Ulid::from_u128(1001)));
    assert_eq!(events.len(), 2);

    let (start_fp, finish_fp) = match (&events[0], &events[1]) {
        (
            QueryTraceEvent::Start {
                fingerprint,
                executor,
                access,
            },
            QueryTraceEvent::Finish {
                fingerprint: finish,
                executor: finish_exec,
                access: finish_access,
                rows,
            },
        ) => {
            assert_eq!(*executor, TraceExecutorKind::Save);
            assert_eq!(*finish_exec, TraceExecutorKind::Save);
            assert_eq!(*access, None);
            assert_eq!(*finish_access, None);
            assert_eq!(*rows, 1);
            (*fingerprint, *finish)
        }
        _ => panic!("unexpected trace events: {events:?}"),
    };

    assert_eq!(start_fp, finish_fp);
}

#[test]
fn trace_emits_phases_for_delete() {
    setup_schema();

    let saver = SaveExecutor::<TestEntity>::new(DB, false);
    let entity_id = Ref::new(Ulid::from_u128(2002));
    let entity = TestEntity {
        id: entity_id,
        name: "beta".to_string(),
    };
    saver.insert(entity).unwrap();

    let query = Query::<TestEntity>::new(ReadConsistency::MissingOk)
        .filter(FieldRef::new("id").eq(entity_id))
        .delete();
    let plan = query.plan().expect("plan");
    let fingerprint = plan.fingerprint();
    let deleter = DeleteExecutor::<TestEntity>::new(DB, false).with_trace_sink(Some(&TRACE_SINK));

    let (result, events) = with_trace_events(|| deleter.execute(plan));
    let response = result.unwrap();

    assert_eq!(response.0.len(), 1);
    assert_eq!(events.len(), 6);
    assert_eq!(
        events[0],
        QueryTraceEvent::Start {
            fingerprint,
            executor: TraceExecutorKind::Delete,
            access: Some(TraceAccess::ByKey),
        }
    );
    assert_eq!(
        events[1],
        QueryTraceEvent::Phase {
            fingerprint,
            executor: TraceExecutorKind::Delete,
            access: Some(TraceAccess::ByKey),
            phase: TracePhase::Access,
            rows: 1,
        }
    );
    assert_eq!(
        events[2],
        QueryTraceEvent::Phase {
            fingerprint,
            executor: TraceExecutorKind::Delete,
            access: Some(TraceAccess::ByKey),
            phase: TracePhase::Filter,
            rows: 1,
        }
    );
    assert_eq!(
        events[3],
        QueryTraceEvent::Phase {
            fingerprint,
            executor: TraceExecutorKind::Delete,
            access: Some(TraceAccess::ByKey),
            phase: TracePhase::Order,
            rows: 1,
        }
    );
    assert_eq!(
        events[4],
        QueryTraceEvent::Phase {
            fingerprint,
            executor: TraceExecutorKind::Delete,
            access: Some(TraceAccess::ByKey),
            phase: TracePhase::DeleteLimit,
            rows: 1,
        }
    );
    assert_eq!(
        events[5],
        QueryTraceEvent::Finish {
            fingerprint,
            executor: TraceExecutorKind::Delete,
            access: Some(TraceAccess::ByKey),
            rows: 1,
        }
    );
}

#[test]
fn diagnose_query_matches_queryspec_explain() {
    setup_schema();

    let query = Query::<TestEntity>::new(ReadConsistency::MissingOk);
    let session = DbSession::new(DB);

    let expected = query.explain().expect("explain");
    let diagnostics = session
        .diagnose_query::<TestEntity>(&query)
        .expect("diagnose");

    assert_eq!(diagnostics.explain, expected);
    assert_eq!(diagnostics.fingerprint, expected.fingerprint());
}

#[test]
fn diagnose_query_fingerprint_matches_plan() {
    setup_schema();

    let query = Query::<TestEntity>::new(ReadConsistency::MissingOk);
    let session = DbSession::new(DB);

    let plan = query.plan().expect("plan");
    let diagnostics = session
        .diagnose_query::<TestEntity>(&query)
        .expect("diagnose");

    assert_eq!(diagnostics.fingerprint, plan.fingerprint());
}

#[test]
fn diagnose_query_invalid_matches_explain() {
    setup_schema();

    let query = Query::<TestEntity>::new(ReadConsistency::MissingOk).order_by("missing");
    let session = DbSession::new(DB);

    let expected = query.explain().expect_err("invalid order");
    let err = session
        .diagnose_query::<TestEntity>(&query)
        .expect_err("invalid order");

    assert!(matches!(
        expected,
        QueryError::Plan(PlanError::UnknownOrderField { .. })
    ));
    assert!(matches!(
        err,
        QueryError::Plan(PlanError::UnknownOrderField { .. })
    ));
}

#[test]
fn execute_with_diagnostics_returns_events() {
    setup_schema();

    let saver = SaveExecutor::<TestEntity>::new(DB, false);
    let entity = TestEntity {
        id: Ref::new(Ulid::from_u128(123)),
        name: "alpha".to_string(),
    };
    saver.insert(entity).unwrap();

    let query = Query::<TestEntity>::new(ReadConsistency::MissingOk);
    let session = DbSession::new(DB);

    let (response, diagnostics) = session
        .execute_with_diagnostics::<TestEntity>(&query)
        .expect("execute");

    let plan = query.plan().expect("plan");
    assert_eq!(diagnostics.fingerprint, plan.fingerprint());
    assert_eq!(response.0.len(), 1);
    assert_eq!(
        diagnostics.events,
        vec![
            QueryTraceEvent::Start {
                fingerprint: diagnostics.fingerprint,
                executor: TraceExecutorKind::Load,
                access: Some(TraceAccess::FullScan),
            },
            QueryTraceEvent::Finish {
                fingerprint: diagnostics.fingerprint,
                executor: TraceExecutorKind::Load,
                access: Some(TraceAccess::FullScan),
                rows: 1,
            },
        ]
    );
}

#[test]
fn resolve_data_values_rejects_prefix_too_long() {
    setup_schema();

    let values = vec![
        Value::Text("alpha".to_string()),
        Value::Text("beta".to_string()),
    ];

    let err = TEST_INDEX_STORE
        .with_borrow(|store| store.resolve_data_values::<TestEntity>(&INDEX_MODEL, &values))
        .expect_err("expected error");
    assert_eq!(err.class, ErrorClass::Unsupported);
    assert_eq!(err.origin, ErrorOrigin::Index);
}

#[test]
fn load_orders_with_incomparable_primary_uses_secondary() {
    setup_schema();

    let saver = SaveExecutor::<OrderEntity>::new(DB, false);
    let entities = vec![
        OrderEntity {
            id: Ref::new(Ulid::from_u128(1)),
            primary: Value::None,
            secondary: 0,
        },
        OrderEntity {
            id: Ref::new(Ulid::from_u128(2)),
            primary: Value::None,
            secondary: 1,
        },
        OrderEntity {
            id: Ref::new(Ulid::from_u128(3)),
            primary: Value::None,
            secondary: 0,
        },
        OrderEntity {
            id: Ref::new(Ulid::from_u128(4)),
            primary: Value::None,
            secondary: 1,
        },
    ];

    for entity in entities {
        saver.insert(entity).unwrap();
    }

    let mut plan = LogicalPlan::new(AccessPath::FullScan, ReadConsistency::MissingOk);
    plan.order = Some(OrderSpec {
        fields: vec![
            ("primary".to_string(), OrderDirection::Asc),
            ("secondary".to_string(), OrderDirection::Asc),
        ],
    });

    let loader = LoadExecutor::<OrderEntity>::new(DB, false);
    let ordered = loader
        .execute(ExecutablePlan::new(plan))
        .unwrap()
        .entities();
    let ordered_ids = ordered.iter().map(|entity| entity.id).collect::<Vec<_>>();

    assert_eq!(
        ordered_ids,
        vec![
            Ref::new(Ulid::from_u128(1)),
            Ref::new(Ulid::from_u128(3)),
            Ref::new(Ulid::from_u128(2)),
            Ref::new(Ulid::from_u128(4)),
        ]
    );
}

#[test]
fn load_paginates_after_ordering() {
    setup_schema();

    let saver = SaveExecutor::<TestEntity>::new(DB, false);
    let entities = vec![
        TestEntity {
            id: Ref::new(Ulid::from_u128(1)),
            name: "delta".to_string(),
        },
        TestEntity {
            id: Ref::new(Ulid::from_u128(2)),
            name: "alpha".to_string(),
        },
        TestEntity {
            id: Ref::new(Ulid::from_u128(3)),
            name: "charlie".to_string(),
        },
        TestEntity {
            id: Ref::new(Ulid::from_u128(4)),
            name: "bravo".to_string(),
        },
    ];

    for entity in entities {
        saver.insert(entity).unwrap();
    }

    let loader = LoadExecutor::<TestEntity>::new(DB, false);
    let page = loader
        .execute(
            Query::<TestEntity>::new(ReadConsistency::MissingOk)
                .order_by("name")
                .offset(1)
                .limit(2)
                .plan()
                .unwrap(),
        )
        .unwrap()
        .entities();
    let names = page
        .iter()
        .map(|entity| entity.name.as_str())
        .collect::<Vec<_>>();
    assert_eq!(names, vec!["bravo", "charlie"]);

    let names = loader
        .execute(
            Query::<TestEntity>::new(ReadConsistency::MissingOk)
                .order_by("name")
                .limit(10)
                .plan()
                .unwrap(),
        )
        .unwrap()
        .entities()
        .into_iter()
        .map(|entity| entity.name)
        .collect::<Vec<_>>();
    assert_eq!(names, vec!["alpha", "bravo", "charlie", "delta"]);

    assert!(
        loader
            .execute(
                Query::<TestEntity>::new(ReadConsistency::MissingOk)
                    .order_by("name")
                    .offset(10)
                    .limit(2)
                    .plan()
                    .unwrap(),
            )
            .unwrap()
            .entities()
            .into_iter()
            .map(|entity| entity.name)
            .next()
            .is_none()
    );
}

#[test]
fn load_paginates_after_filtering_and_ordering() {
    setup_schema();

    let saver = SaveExecutor::<OrderEntity>::new(DB, false);
    let entities = vec![
        OrderEntity {
            id: Ref::new(Ulid::from_u128(1)),
            primary: Value::Int(10),
            secondary: 0,
        },
        OrderEntity {
            id: Ref::new(Ulid::from_u128(2)),
            primary: Value::Int(20),
            secondary: 1,
        },
        OrderEntity {
            id: Ref::new(Ulid::from_u128(3)),
            primary: Value::Int(30),
            secondary: 2,
        },
        OrderEntity {
            id: Ref::new(Ulid::from_u128(4)),
            primary: Value::Int(40),
            secondary: 3,
        },
    ];

    for entity in entities {
        saver.insert(entity).unwrap();
    }

    let loader = LoadExecutor::<OrderEntity>::new(DB, false);
    let rows = loader
        .execute(
            Query::<OrderEntity>::new(ReadConsistency::MissingOk)
                .filter(FieldRef::new("secondary").gt(0))
                .order_by(FieldRef::new("secondary"))
                .limit(2)
                .plan()
                .unwrap(),
        )
        .unwrap()
        .entities();

    let secondaries = rows
        .iter()
        .map(|entity| entity.secondary)
        .collect::<Vec<_>>();

    assert_eq!(secondaries, vec![1, 2]);
}

#[test]
fn load_pagination_handles_large_offset() {
    setup_schema();

    let saver = SaveExecutor::<TestEntity>::new(DB, false);
    saver
        .insert(TestEntity {
            id: Ref::new(Ulid::from_u128(1)),
            name: "alpha".to_string(),
        })
        .unwrap();

    let loader = LoadExecutor::<TestEntity>::new(DB, false);
    let rows = loader
        .execute(
            Query::<TestEntity>::new(ReadConsistency::MissingOk)
                .order_by("name")
                .offset(u32::MAX)
                .limit(1)
                .plan()
                .unwrap(),
        )
        .unwrap()
        .entities();

    assert!(rows.is_empty());
}

#[test]
fn ordering_does_not_break_ties_by_primary_key() {
    setup_schema();

    let saver = SaveExecutor::<OrderEntity>::new(DB, false);
    let entities = vec![
        OrderEntity {
            id: Ref::new(Ulid::from_u128(3)),
            primary: Value::Int(1),
            secondary: 1,
        },
        OrderEntity {
            id: Ref::new(Ulid::from_u128(1)),
            primary: Value::Int(1),
            secondary: 2,
        },
        OrderEntity {
            id: Ref::new(Ulid::from_u128(4)),
            primary: Value::Int(1),
            secondary: 3,
        },
        OrderEntity {
            id: Ref::new(Ulid::from_u128(2)),
            primary: Value::Int(1),
            secondary: 4,
        },
    ];

    for entity in entities {
        saver.insert(entity).unwrap();
    }

    let loader = LoadExecutor::<OrderEntity>::new(DB, false);
    let base_ids = loader
        .execute(ExecutablePlan::new(LogicalPlan::new(
            AccessPath::FullScan,
            ReadConsistency::MissingOk,
        )))
        .unwrap()
        .entities()
        .iter()
        .map(|entity| entity.id)
        .collect::<Vec<_>>();

    let mut plan = LogicalPlan::new(AccessPath::FullScan, ReadConsistency::MissingOk);
    plan.order = Some(OrderSpec {
        fields: vec![("primary".to_string(), OrderDirection::Asc)],
    });

    let ordered = loader
        .execute(ExecutablePlan::new(plan))
        .unwrap()
        .entities();
    let ordered_ids = ordered.iter().map(|entity| entity.id).collect::<Vec<_>>();

    assert_eq!(ordered_ids, base_ids);
}

#[test]
fn ordering_does_not_compare_incomparable_values() {
    setup_schema();

    let saver = SaveExecutor::<OrderEntity>::new(DB, false);
    let entities = vec![
        OrderEntity {
            id: Ref::new(Ulid::from_u128(10)),
            primary: Value::None,
            secondary: 1,
        },
        OrderEntity {
            id: Ref::new(Ulid::from_u128(11)),
            primary: Value::Int(1),
            secondary: 2,
        },
        OrderEntity {
            id: Ref::new(Ulid::from_u128(12)),
            primary: Value::None,
            secondary: 3,
        },
        OrderEntity {
            id: Ref::new(Ulid::from_u128(13)),
            primary: Value::Int(1),
            secondary: 4,
        },
    ];

    for entity in entities.clone() {
        saver.insert(entity).unwrap();
    }

    let mut plan = LogicalPlan::new(AccessPath::FullScan, ReadConsistency::MissingOk);
    plan.order = Some(OrderSpec {
        fields: vec![("primary".to_string(), OrderDirection::Asc)],
    });

    let loader = LoadExecutor::<OrderEntity>::new(DB, false);
    let ordered = loader
        .execute(ExecutablePlan::new(plan))
        .unwrap()
        .entities();
    let ordered_ids = ordered.iter().map(|entity| entity.id).collect::<Vec<_>>();
    let inserted_ids = entities.iter().map(|entity| entity.id).collect::<Vec<_>>();

    assert_eq!(ordered_ids, inserted_ids);
}

#[test]
fn save_direct_ref_missing_target_fails_no_commit() {
    setup_schema();

    let saver = SaveExecutor::<DirectRefEntity>::new(RI_DB, false);
    let entity = DirectRefEntity {
        id: Ref::new(Ulid::generate()),
        owner: Some(Ref::new(Ulid::generate())),
    };

    let err = saver.insert(entity.clone()).unwrap_err();
    assert_eq!(err.class, ErrorClass::Conflict);
    assert_eq!(err.origin, ErrorOrigin::Executor);
    assert_commit_marker_clear();
    assert_row_missing::<DirectRefEntity>(RI_DB, entity.id);
}

#[test]
fn save_optional_ref_none_succeeds() {
    setup_schema();

    let saver = SaveExecutor::<DirectRefEntity>::new(RI_DB, false);
    let entity = DirectRefEntity {
        id: Ref::new(Ulid::generate()),
        owner: None,
    };

    saver.insert(entity.clone()).unwrap();
    assert_commit_marker_clear();
    assert_row_present::<DirectRefEntity>(RI_DB, entity.id);
}

#[test]
fn save_nested_record_ref_missing_target_succeeds() {
    setup_schema();

    let saver = SaveExecutor::<RecordRefEntity>::new(RI_DB, false);
    let entity = RecordRefEntity {
        id: Ref::new(Ulid::generate()),
        profile: RecordRefPayload {
            owner: Ref::new(Ulid::generate()),
        },
    };

    saver.insert(entity.clone()).unwrap();
    assert_commit_marker_clear();
    assert_row_present::<RecordRefEntity>(RI_DB, entity.id);
}

#[test]
fn save_nested_enum_ref_missing_target_succeeds() {
    setup_schema();

    let saver = SaveExecutor::<EnumRefEntity>::new(RI_DB, false);
    let entity = EnumRefEntity {
        id: Ref::new(Ulid::generate()),
        status: RefEnum::Missing(Ref::new(Ulid::generate())),
    };

    saver.insert(entity.clone()).unwrap();
    assert_commit_marker_clear();
    assert_row_present::<EnumRefEntity>(RI_DB, entity.id);
}

#[test]
fn save_collection_refs_do_not_trigger_invariant_failure() {
    setup_schema();

    let saver = SaveExecutor::<CollectionRefEntity>::new(RI_DB, false);
    let entity = CollectionRefEntity {
        id: Ref::new(Ulid::generate()),
        owners: vec![Ref::new(Ulid::generate()), Ref::new(Ulid::generate())],
    };

    saver.insert(entity.clone()).unwrap();
    assert_commit_marker_clear();
    assert_row_present::<CollectionRefEntity>(RI_DB, entity.id);
}

#[test]
fn delete_ignores_references() {
    setup_schema();

    let owner = OwnerEntity {
        id: Ref::new(Ulid::generate()),
    };
    let owner_saver = SaveExecutor::<OwnerEntity>::new(RI_DB, false);
    owner_saver.insert(owner.clone()).unwrap();

    let ref_entity = DirectRefEntity {
        id: Ref::new(Ulid::generate()),
        owner: Some(owner.id),
    };
    let ref_saver = SaveExecutor::<DirectRefEntity>::new(RI_DB, false);
    ref_saver.insert(ref_entity.clone()).unwrap();

    let mut plan = LogicalPlan::new(AccessPath::FullScan, ReadConsistency::MissingOk);
    plan.mode = QueryMode::Delete(DeleteSpec::new());
    let deleter = DeleteExecutor::<OwnerEntity>::new(RI_DB, false);
    let response = deleter.execute(ExecutablePlan::new(plan)).unwrap();
    assert_eq!(response.0.len(), 1);

    assert_row_missing::<OwnerEntity>(RI_DB, owner.id);
    assert_row_present::<DirectRefEntity>(RI_DB, ref_entity.id);
    assert_commit_marker_clear();
}
