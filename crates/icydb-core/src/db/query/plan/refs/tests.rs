/*
use crate::{
    traits::{
        FieldValues, SanitizeAuto, SanitizeCustom, ValidateAuto, ValidateCustom, View, Visitable,
    },
    types::{Ref, Ulid},
    value::Value,
};
use serde::{Deserialize, Serialize};

const OWNER_PATH: &str = "traits_tests::OwnerEntity";

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
struct OwnerEntity {
    id: Ulid,
}

crate::test_entity! {
    entity OwnerEntity {
        path: "traits_tests::OwnerEntity",
        pk: id: Ulid,

        fields { id: Ulid }
    }
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
            "id" => Some(Value::Ulid(self.id)),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
struct RefEntity {
    id: Ulid,
    owner: Option<Ref<OwnerEntity>>,
}

crate::test_entity! {
    entity RefEntity {
        path: "traits_tests::RefEntity",
        pk: id: Ulid,

        fields { id: Ulid, owner: Ref<OwnerEntity> }
    }
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
            "id" => Some(Value::Ulid(self.id)),
            "owner" => Some(self.owner.map_or(Value::None, |owner| owner.as_value())),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
struct CollectionRefEntity {
    id: Ulid,
    owners: Vec<Ref<OwnerEntity>>,
}

crate::test_entity! {
    entity CollectionRefEntity {
        path: "traits_tests::CollectionRefEntity",
        pk: id: Ulid,

        fields { id: Ulid, owners: List<Ref<OwnerEntity>> }
    }
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
            "id" => Some(Value::Ulid(self.id)),
            "owners" => Some(Value::List(
                self.owners.iter().map(|owner| owner.as_value()).collect(),
            )),
            _ => None,
        }
    }
}

#[test]
fn entity_refs_empty_for_non_reference_entity() {
    let owner = OwnerEntity {
        id: Ulid::generate(),
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
        id: Ulid::generate(),
        owner: Some(Ref::new(owner_id)),
    };

    let refs = entity
        .entity_refs()
        .expect("reference extraction should succeed");

    assert_eq!(refs.len(), 1);
    assert_eq!(refs[0].target_path, OWNER_PATH);
    assert_eq!(
        refs[0].value(),
        Ref::<OwnerEntity>::new(owner_id).as_value()
    );
}

#[test]
fn entity_refs_skip_reference_collections() {
    let entity = CollectionRefEntity {
        id: Ulid::generate(),
        owners: vec![Ref::new(Ulid::generate())],
    };

    let refs = entity
        .entity_refs()
        .expect("reference extraction should succeed");

    assert!(refs.is_empty());
}
*/
