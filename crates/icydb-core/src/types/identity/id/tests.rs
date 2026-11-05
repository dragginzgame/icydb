use super::Id;
use crate::{
    db::{EntityKey, PrimaryKeyEncode},
    value::Value,
};

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct TestEntity;

impl EntityKey for TestEntity {
    type Key = u64;
}

#[test]
fn into_value_for_owned_and_borrowed_id_match_as_value() {
    let id = Id::<TestEntity>::from_key(42);
    let expected = id.as_value();
    let borrowed = Value::from(&id);
    let owned = Value::from(id);

    assert_eq!(borrowed, expected);
    assert_eq!(owned, expected);
}

#[test]
fn primary_key_encoding_matches_the_wrapped_entity_key() {
    let id = Id::<TestEntity>::from_key(42);

    assert_eq!(
        id.to_primary_key_value()
            .expect("typed identity should encode"),
        42_u64
            .to_primary_key_value()
            .expect("raw entity key should encode"),
    );
}

#[test]
fn wire_type_is_owned_by_the_wrapped_entity_key() {
    assert_eq!(
        icydb_schema::wire::encode_one(Id::<TestEntity>::from_key(42))
            .expect("typed identity should encode"),
        icydb_schema::wire::encode_one(42_u64).expect("raw entity key should encode"),
    );
}

#[test]
fn subaccount_is_deterministic_for_same_id() {
    use crate::{
        db::EntityKey,
        types::{Id, Subaccount},
    };

    #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
    struct TestEntity;

    impl EntityKey for TestEntity {
        type Key = u64;
    }

    let id = Id::<TestEntity>::from_key(42);

    let a: Subaccount = id.subaccount().expect("valid key should project");
    let b: Subaccount = id.subaccount().expect("valid key should project");

    assert_eq!(a, b);
}
