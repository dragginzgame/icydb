use crate::{
    model::{
        entity::EntityModel,
        field::{EntityFieldKind, EntityFieldModel},
        index::IndexModel,
    },
    traits::{
        CanisterKind, DataStoreKind, EntityKind, FieldValue, FieldValues, Path, SanitizeAuto,
        SanitizeCustom, ValidateAuto, ValidateCustom, View, Visitable,
    },
    types::{Ref, Ulid},
    value::Value,
};
use serde::{Deserialize, Serialize};

const CANISTER_PATH: &str = "traits_tests::TestCanister";
const STORE_PATH: &str = "traits_tests::TestStore";
const OWNER_PATH: &str = "traits_tests::OwnerEntity";
const REF_PATH: &str = "traits_tests::RefEntity";

const OWNER_KEY_KIND: EntityFieldKind = EntityFieldKind::Ulid;

const OWNER_FIELDS: [EntityFieldModel; 1] = [EntityFieldModel {
    name: "id",
    kind: EntityFieldKind::Ulid,
}];

const REF_FIELDS: [EntityFieldModel; 2] = [
    EntityFieldModel {
        name: "id",
        kind: EntityFieldKind::Ulid,
    },
    EntityFieldModel {
        name: "owner",
        kind: EntityFieldKind::Ref {
            target_path: OWNER_PATH,
            key_kind: &OWNER_KEY_KIND,
        },
    },
];
const COLLECTION_FIELDS: [EntityFieldModel; 2] = [
    EntityFieldModel {
        name: "id",
        kind: EntityFieldKind::Ulid,
    },
    EntityFieldModel {
        name: "owners",
        kind: EntityFieldKind::List(&EntityFieldKind::Ref {
            target_path: OWNER_PATH,
            key_kind: &OWNER_KEY_KIND,
        }),
    },
];

const INDEXES: [&IndexModel; 0] = [];

const OWNER_MODEL: EntityModel = EntityModel {
    path: OWNER_PATH,
    entity_name: "OwnerEntity",
    primary_key: &OWNER_FIELDS[0],
    fields: &OWNER_FIELDS,
    indexes: &INDEXES,
};

const REF_MODEL: EntityModel = EntityModel {
    path: REF_PATH,
    entity_name: "RefEntity",
    primary_key: &REF_FIELDS[0],
    fields: &REF_FIELDS,
    indexes: &INDEXES,
};
const COLLECTION_MODEL: EntityModel = EntityModel {
    path: "traits_tests::CollectionRefEntity",
    entity_name: "CollectionRefEntity",
    primary_key: &COLLECTION_FIELDS[0],
    fields: &COLLECTION_FIELDS,
    indexes: &INDEXES,
};

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
struct OwnerEntity {
    id: Ref<Self>,
}

impl Path for OwnerEntity {
    const PATH: &'static str = OWNER_PATH;
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
            "id" => Some(self.id.as_value()),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
struct RefEntity {
    id: Ref<Self>,
    owner: Option<Ref<OwnerEntity>>,
}

impl Path for RefEntity {
    const PATH: &'static str = REF_PATH;
}

impl View for RefEntity {
    type ViewType = Self;

    fn to_view(&self) -> Self::ViewType {
        self.clone()
    }

    fn from_view(view: Self::ViewType) -> Self {
        view
    }
}

impl SanitizeAuto for RefEntity {}
impl SanitizeCustom for RefEntity {}
impl ValidateAuto for RefEntity {}
impl ValidateCustom for RefEntity {}
impl Visitable for RefEntity {}

impl FieldValues for RefEntity {
    fn get_value(&self, field: &str) -> Option<Value> {
        match field {
            "id" => Some(self.id.to_value()),
            "owner" => Some(self.owner.to_value()),
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
    const PATH: &'static str = "traits_tests::CollectionRefEntity";
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

struct TestStore;

impl Path for TestStore {
    const PATH: &'static str = STORE_PATH;
}

impl DataStoreKind for TestStore {
    type Canister = TestCanister;
}

impl EntityKind for OwnerEntity {
    type PrimaryKey = Ref<Self>;
    type DataStore = TestStore;
    type Canister = TestCanister;

    const ENTITY_NAME: &'static str = "OwnerEntity";
    const PRIMARY_KEY: &'static str = "id";
    const FIELDS: &'static [&'static str] = &["id"];
    const INDEXES: &'static [&'static IndexModel] = &INDEXES;
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

impl EntityKind for RefEntity {
    type PrimaryKey = Ref<Self>;
    type DataStore = TestStore;
    type Canister = TestCanister;

    const ENTITY_NAME: &'static str = "RefEntity";
    const PRIMARY_KEY: &'static str = "id";
    const FIELDS: &'static [&'static str] = &["id", "owner"];
    const INDEXES: &'static [&'static IndexModel] = &INDEXES;
    const MODEL: &'static EntityModel = &REF_MODEL;

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
    type DataStore = TestStore;
    type Canister = TestCanister;

    const ENTITY_NAME: &'static str = "CollectionRefEntity";
    const PRIMARY_KEY: &'static str = "id";
    const FIELDS: &'static [&'static str] = &["id", "owners"];
    const INDEXES: &'static [&'static IndexModel] = &INDEXES;
    const MODEL: &'static EntityModel = &COLLECTION_MODEL;

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

#[test]
fn entity_refs_empty_for_non_reference_entity() {
    let owner = OwnerEntity {
        id: Ref::new(Ulid::generate()),
    };

    let refs = owner
        .entity_refs()
        .expect("reference extraction should succeed");

    assert!(refs.is_empty());
}

#[test]
fn entity_refs_collect_optional_reference() {
    let owner_id = Ulid::generate();
    let entity = RefEntity {
        id: Ref::new(Ulid::generate()),
        owner: Some(Ref::new(owner_id)),
    };

    let refs = entity
        .entity_refs()
        .expect("reference extraction should succeed");

    assert_eq!(refs.len(), 1);
    assert_eq!(refs[0].target_path, OWNER_PATH);
    assert_eq!(refs[0].value(), Ref::<OwnerEntity>::new(owner_id).raw());
}

#[test]
fn entity_refs_skip_reference_collections() {
    let entity = CollectionRefEntity {
        id: Ref::new(Ulid::generate()),
        owners: vec![Ref::new(Ulid::generate())],
    };

    let refs = entity
        .entity_refs()
        .expect("reference extraction should succeed");

    assert!(refs.is_empty());
}
