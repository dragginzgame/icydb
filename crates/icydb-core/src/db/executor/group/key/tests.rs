use super::canonical_group_key_equals;
use crate::{
    db::executor::group::{CanonicalKey, GroupKey, GroupKeySet, KeyCanonicalError},
    types::Decimal,
    value::{MapValueError, Value, with_test_hash_override},
};

fn map_value(entries: Vec<(Value, Value)>) -> Value {
    Value::Map(entries)
}

#[test]
fn canonical_key_normalizes_decimal_scale() {
    let key = Value::Decimal(Decimal::new(100, 2))
        .canonical_key()
        .expect("canonical key");

    let Value::Decimal(normalized) = key.raw() else {
        panic!("canonical decimal value expected");
    };
    assert_eq!(normalized.scale(), 0);
}

#[test]
fn canonical_key_normalizes_map_order() {
    let left = map_value(vec![
        (Value::Text("z".to_string()), Value::Nat64(9)),
        (Value::Text("a".to_string()), Value::Nat64(1)),
    ]);
    let right = map_value(vec![
        (Value::Text("a".to_string()), Value::Nat64(1)),
        (Value::Text("z".to_string()), Value::Nat64(9)),
    ]);

    let left_key = left.canonical_key().expect("left canonical key");
    let right_key = right.canonical_key().expect("right canonical key");

    assert_eq!(left_key, right_key);
    assert_eq!(left_key.hash(), right_key.hash());
}

#[test]
fn canonical_key_rejects_duplicate_map_keys_after_normalization() {
    let value = map_value(vec![
        (Value::Text("a".to_string()), Value::Nat64(1)),
        (Value::Text("a".to_string()), Value::Nat64(2)),
    ]);

    let err = value
        .canonical_key()
        .expect_err("duplicate map keys should fail");
    std::assert_matches!(
        err,
        KeyCanonicalError::InvalidMapValue(MapValueError::DuplicateKey { .. })
    );
}

#[test]
fn group_key_set_deduplicates_canonical_equivalents() {
    let mut set = GroupKeySet::default();
    let first = Value::Decimal(Decimal::new(100, 2));
    let second = Value::Decimal(Decimal::new(1, 0));

    assert!(
        set.insert_value(&first).expect("insert"),
        "first insert should be new"
    );
    assert!(
        !set.insert_value(&second).expect("insert"),
        "second insert should be deduplicated by canonical key equality"
    );
}

#[test]
fn canonical_equal_keys_always_share_stable_hash() {
    let equivalent_pairs = vec![
        (
            Value::Decimal(Decimal::new(1000, 3)),
            Value::Decimal(Decimal::new(1, 0)),
        ),
        (
            Value::Map(vec![
                (Value::Text("z".to_string()), Value::Nat64(9)),
                (Value::Text("a".to_string()), Value::Nat64(1)),
            ]),
            Value::Map(vec![
                (Value::Text("a".to_string()), Value::Nat64(1)),
                (Value::Text("z".to_string()), Value::Nat64(9)),
            ]),
        ),
        (
            Value::List(vec![Value::Decimal(Decimal::new(10, 1)), Value::Nat64(4)]),
            Value::List(vec![Value::Decimal(Decimal::new(1, 0)), Value::Nat64(4)]),
        ),
        (
            Value::List(vec![
                Value::Map(vec![
                    (Value::Text("z".to_string()), Value::Nat64(9)),
                    (Value::Text("a".to_string()), Value::Nat64(1)),
                ]),
                Value::Decimal(Decimal::new(2500, 2)),
            ]),
            Value::List(vec![
                Value::Map(vec![
                    (Value::Text("a".to_string()), Value::Nat64(1)),
                    (Value::Text("z".to_string()), Value::Nat64(9)),
                ]),
                Value::Decimal(Decimal::new(25, 0)),
            ]),
        ),
    ];

    for (left_value, right_value) in equivalent_pairs {
        let left_key = left_value.canonical_key().expect("left canonical key");
        let right_key = right_value.canonical_key().expect("right canonical key");
        assert!(
            canonical_group_key_equals(&left_key, &right_key),
            "pair should be canonical-equal under group key contract",
        );
        assert_eq!(
            left_key.hash(),
            right_key.hash(),
            "canonical-equal keys must hash to the same stable hash",
        );
    }
}

#[test]
fn group_key_set_handles_hash_collisions_with_equality_check() {
    with_test_hash_override([0xAB; 16], || {
        let mut set = GroupKeySet::default();
        let first = Value::Text("alpha".to_string())
            .canonical_key()
            .expect("first canonical key");
        let second = Value::Text("beta".to_string())
            .canonical_key()
            .expect("second canonical key");

        assert_eq!(
            first.hash(),
            second.hash(),
            "test setup requires an artificial hash collision",
        );
        assert!(
            !canonical_group_key_equals(&first, &second),
            "collision pair must remain distinct by canonical equality",
        );
        assert!(
            set.insert_key(first.clone()),
            "first colliding key should insert as new",
        );
        assert!(
            set.insert_key(second.clone()),
            "second colliding key must not be dropped on hash match alone",
        );
        assert!(
            !set.insert_key(first),
            "re-inserting first key should dedupe by canonical equality",
        );
        assert!(
            !set.insert_key(second),
            "re-inserting second key should dedupe by canonical equality",
        );
    });
}

#[test]
fn group_key_from_single_group_value_matches_group_values_path() {
    let single = Value::Decimal(Decimal::new(100, 2));
    let single_owned =
        GroupKey::from_single_group_value(single.clone()).expect("single owned canonical key");
    let list_owned = GroupKey::from_group_values(vec![single]).expect("list owned canonical key");

    assert_eq!(single_owned, list_owned);
    assert_eq!(single_owned.hash(), list_owned.hash());
}

#[test]
fn group_key_from_prehashed_paths_match_unhashed_paths() {
    let group_values = vec![
        Value::Decimal(Decimal::new(100, 2)),
        Value::Text("alpha".to_string()),
    ];
    let borrowed_hash = Value::List(group_values.clone())
        .canonical_key()
        .expect("borrowed canonical key")
        .hash();
    let prehashed_multi =
        GroupKey::from_group_values_with_hash(group_values.clone(), borrowed_hash)
            .expect("prehashed multi key");
    let unhashed_multi = GroupKey::from_group_values(group_values).expect("unhashed multi key");

    assert_eq!(prehashed_multi, unhashed_multi);
    assert_eq!(prehashed_multi.hash(), unhashed_multi.hash());

    let single = Value::Decimal(Decimal::new(100, 2));
    let single_hash = Value::List(vec![single.clone()])
        .canonical_key()
        .expect("borrowed single canonical key")
        .hash();
    let prehashed_single = GroupKey::from_single_group_value_with_hash(single.clone(), single_hash)
        .expect("prehashed single key");
    let unhashed_single = GroupKey::from_single_group_value(single).expect("unhashed single key");

    assert_eq!(prehashed_single, unhashed_single);
    assert_eq!(prehashed_single.hash(), unhashed_single.hash());
}

#[test]
fn group_key_from_single_canonical_group_value_matches_hashed_single_path() {
    let single = Value::Nat64(7);
    let single_hash = Value::List(vec![single.clone()])
        .canonical_key()
        .expect("borrowed single canonical key")
        .hash();
    let canonical =
        GroupKey::from_single_canonical_group_value_with_hash(single.clone(), single_hash);
    let hashed = GroupKey::from_single_group_value_with_hash(single, single_hash)
        .expect("hashed single canonical key");

    assert_eq!(canonical, hashed);
    assert_eq!(canonical.hash(), hashed.hash());
}

#[test]
fn group_key_from_group_values_matches_borrowed_canonical_key_path() {
    let group_values = vec![
        Value::Decimal(Decimal::new(100, 2)),
        Value::Text("alpha".to_string()),
        map_value(vec![(Value::Text("z".to_string()), Value::Nat64(9))]),
    ];
    let borrowed = Value::List(group_values.clone())
        .canonical_key()
        .expect("borrowed canonical key");
    let owned = GroupKey::from_group_values(group_values).expect("owned canonical key");

    assert_eq!(borrowed, owned);
    assert_eq!(borrowed.hash(), owned.hash());
}
