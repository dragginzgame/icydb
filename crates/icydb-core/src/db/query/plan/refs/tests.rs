/*
use crate::{
    traits::{
        CanisterKind, DataStoreKind, FieldValues, Path, SanitizeAuto, SanitizeCustom, ValidateAuto,
        ValidateCustom, View, Visitable,
    },
    types::{Ref, Ulid},
    value::Value,
};
use icydb_test_macros::test_entity;
use serde::{Deserialize, Serialize};

const CANISTER_PATH: &str = "traits_tests::TestCanister";
const STORE_PATH: &str = "traits_tests::TestStore";
const OWNER_PATH: &str = "traits_tests::OwnerEntity";

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[test_entity(
    crate = crate,
    entity_name = "OwnerEntity",
    path = "traits_tests::OwnerEntity",
    datastore = TestStore,
    canister = TestCanister,
    primary_key = id,
    fields = ["id"],
)]
struct OwnerEntity {
    id: Ref<Self>,
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
#[test_entity(
    crate = crate,
    entity_name = "RefEntity",
    path = "traits_tests::RefEntity",
    datastore = TestStore,
    canister = TestCanister,
    primary_key = id,
    fields = ["id", "owner"],
)]
struct RefEntity {
    id: Ref<Self>,
    owner: Option<Ref<OwnerEntity>>,
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
            "id" => Some(self.id.as_value()),
            "owner" => Some(self.owner.map_or(Value::None, |owner| owner.as_value())),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[test_entity(
    crate = crate,
    entity_name = "CollectionRefEntity",
    path = "traits_tests::CollectionRefEntity",
    datastore = TestStore,
    canister = TestCanister,
    primary_key = id,
    fields = ["id", "owners"],
)]
struct CollectionRefEntity {
    id: Ref<Self>,
    owners: Vec<Ref<OwnerEntity>>,
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
            "id" => Some(self.id.as_value()),
            "owners" => Some(Value::List(
                self.owners.iter().map(|owner| owner.as_value()).collect(),
            )),
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
    assert_eq!(refs[0].value(), Ref::<OwnerEntity>::new(owner_id).as_value());
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
*/
