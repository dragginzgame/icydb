use crate::{
    db::executor::group::{
        GroupKey, GroupKeySet, canonicalize_owned_value, compare_borrowed_group_values,
        validate_borrowed_canonical_value,
    },
    error::InternalError,
    types::{Decimal, Float32, Float64, U256},
    value::{
        CanonicalEnumBody, CanonicalEnumValue, EnumTypeId, EnumVariantId, Value, ValueEnum,
        with_test_hash_override,
    },
};

// Build a trusted owned key for comparison with borrowed admission probes.
fn canonical_key(value: &Value) -> Result<GroupKey, InternalError> {
    GroupKey::from_canonical_value(canonicalize_owned_value(value.clone())?)
}

fn map_value(entries: Vec<(Value, Value)>) -> Value {
    Value::Map(entries)
}

#[test]
fn borrowed_group_key_lookup_matches_owned_equality_under_collisions() {
    let entries = vec![
        (Value::Text("z".into()), Value::Blob(vec![7; 4096])),
        (
            Value::Text("a".into()),
            Value::Decimal(Decimal::new(100, 2)),
        ),
    ];
    let mut reversed = entries.clone();
    reversed.reverse();
    let enum_value = |entries| {
        Value::Enum(ValueEnum::from_canonical(CanonicalEnumValue::new(
            EnumTypeId::new(7).unwrap(),
            EnumVariantId::new(11).unwrap(),
            CanonicalEnumBody::Payload(Box::new(Value::Map(entries))),
        )))
    };
    let values = [
        Value::Null,
        Value::Unit,
        Value::Bool(false),
        Value::Int64(1),
        Value::Int128(1),
        Value::Nat64(1),
        Value::Nat128(1),
        Value::U256(U256::MAX),
        Value::Float32(Float32::try_new(-0.0).unwrap()),
        Value::Float32(Float32::try_new(0.0).unwrap()),
        Value::Float64(Float64::try_new(-0.0).unwrap()),
        Value::Float64(Float64::try_new(0.0).unwrap()),
        Value::Decimal(Decimal::new(1, 0)),
        Value::Decimal(Decimal::new(100, 2)),
        Value::Text("payload".repeat(1024)),
        Value::Text("different".into()),
        Value::Map(vec![]),
        Value::Map(entries.clone()),
        Value::Map(reversed.clone()),
        Value::List(vec![Value::Map(entries.clone())]),
        Value::List(vec![Value::Map(reversed.clone())]),
        enum_value(entries),
        enum_value(reversed),
    ];
    with_test_hash_override(Ok([17; 16]), || {
        for retained in &values {
            let retained_key = canonical_key(retained).unwrap();
            let mut set = GroupKeySet::new();
            set.insert_key(retained_key.clone()).unwrap();
            for probe in &values {
                let probe_key = canonical_key(probe).unwrap();
                assert_eq!(
                    set.contains_value(probe, probe_key.hash()),
                    retained_key == probe_key
                );
            }
            assert_eq!(set.len(), 1);
        }
    });
}

#[test]
fn canonical_key_preserves_hash_failure() {
    use crate::value::test_hash_budget_error;
    let value = Value::List(vec![Value::Nat64(7)]);
    with_test_hash_override(Err(test_hash_budget_error), || {
        let error = canonical_key(&value).unwrap_err();
        assert_eq!(error.diagnostic(), test_hash_budget_error().diagnostic());
        assert_eq!(
            error.diagnostic_facts(),
            test_hash_budget_error().diagnostic_facts()
        );
    });
    assert!(canonical_key(&value).is_ok());
}

#[test]
fn canonical_key_normalizes_decimal_scale() {
    let key = canonical_key(&Value::Decimal(Decimal::new(100, 2))).expect("canonical key");

    let Value::Decimal(normalized) = key.canonical_value() else {
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

    let left_key = canonical_key(&left).expect("left canonical key");
    let right_key = canonical_key(&right).expect("right canonical key");

    assert_eq!(left_key, right_key);
    assert_eq!(left_key.hash(), right_key.hash());
}

#[test]
fn canonical_key_rejects_duplicate_map_keys_after_normalization() {
    let value = map_value(vec![
        (Value::Text("a".to_string()), Value::Nat64(1)),
        (Value::Text("a".to_string()), Value::Nat64(2)),
    ]);

    let err = canonical_key(&value).expect_err("duplicate map keys should fail");
    assert_eq!(
        err.diagnostic(),
        crate::error::InternalError::executor_invariant().diagnostic()
    );
}

#[test]
fn group_key_set_deduplicates_canonical_equivalents() {
    let mut set = GroupKeySet::default();
    let first = Value::Decimal(Decimal::new(100, 2));
    let second = Value::Decimal(Decimal::new(1, 0));

    assert!(
        set.insert_key(canonical_key(&first).expect("first key should canonicalize"))
            .expect("first set insert"),
        "first insert should be new"
    );
    assert!(
        !set.insert_key(canonical_key(&second).expect("second key should canonicalize"))
            .expect("second set insert"),
        "second insert should be deduplicated by canonical key equality"
    );
}

#[test]
fn group_key_set_preserves_u256_identity() {
    let mut set = GroupKeySet::default();
    let maximum = Value::U256(U256::MAX);

    assert!(
        set.insert_key(canonical_key(&maximum).expect("maximum U256 key"))
            .expect("maximum U256 insert"),
        "first U256 insert should be new",
    );
    assert!(
        !set.insert_key(canonical_key(&maximum).expect("duplicate U256 key"))
            .expect("duplicate U256 insert"),
        "equal U256 values should deduplicate",
    );
    assert!(
        set.insert_key(
            canonical_key(&Value::U256(U256::from_words(u128::MAX, u128::MAX - 1)))
                .expect("distinct U256 key"),
        )
        .expect("distinct U256 insert"),
        "adjacent U256 values should remain distinct",
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
        let left_key = canonical_key(&left_value).expect("left canonical key");
        let right_key = canonical_key(&right_value).expect("right canonical key");
        assert_eq!(
            left_key, right_key,
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
    with_test_hash_override(Ok([0xAB; 16]), || {
        let mut set = GroupKeySet::default();
        let first = canonical_key(&Value::Text("alpha".to_string())).expect("first canonical key");
        let second = canonical_key(&Value::Text("beta".to_string())).expect("second canonical key");

        assert_eq!(
            first.hash(),
            second.hash(),
            "test setup requires an artificial hash collision",
        );
        assert_ne!(
            first, second,
            "collision pair must remain distinct by canonical equality",
        );
        assert!(
            set.insert_key(first.clone()).expect("first set insert"),
            "first colliding key should insert as new",
        );
        assert!(
            set.insert_key(second.clone()).expect("second set insert"),
            "second colliding key must not be dropped on hash match alone",
        );
        assert!(
            !set.insert_key(first).expect("first duplicate insert"),
            "re-inserting first key should dedupe by canonical equality",
        );
        assert!(
            !set.insert_key(second).expect("second duplicate insert"),
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
    let borrowed_hash = canonical_key(&Value::List(group_values.clone()))
        .expect("borrowed canonical key")
        .hash();
    let prehashed_multi =
        GroupKey::from_group_values_with_hash(group_values.clone(), borrowed_hash)
            .expect("prehashed multi key");
    let unhashed_multi = GroupKey::from_group_values(group_values).expect("unhashed multi key");

    assert_eq!(prehashed_multi, unhashed_multi);
    assert_eq!(prehashed_multi.hash(), unhashed_multi.hash());

    let single = Value::Decimal(Decimal::new(100, 2));
    let single_hash = canonical_key(&Value::List(vec![single.clone()]))
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
    let single_hash = canonical_key(&Value::List(vec![single.clone()]))
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
    let borrowed =
        canonical_key(&Value::List(group_values.clone())).expect("borrowed canonical key");
    let owned = GroupKey::from_group_values(group_values).expect("owned canonical key");

    assert_eq!(borrowed, owned);
    assert_eq!(borrowed.hash(), owned.hash());
}

#[test]
fn borrowed_group_order_matches_owned_canonical_trees() {
    let values = vec![
        Value::Null,
        Value::Unit,
        Value::Int64(7),
        Value::Nat64(7),
        Value::Decimal(Decimal::new(100, 2)),
        Value::Decimal(Decimal::new(1, 0)),
        Value::Text("large".repeat(1024)),
        Value::Blob(vec![9; 4096]),
        Value::List(Vec::new()),
        Value::List(vec![Value::Nat64(7), Value::Null]),
        Value::List(vec![Value::Int64(7), Value::Null]),
        Value::Map(Vec::new()),
        Value::Map(vec![(Value::Text("a".into()), Value::Nat64(1))]),
        Value::Map(vec![
            (Value::Text("z".into()), Value::Nat64(9)),
            (
                Value::Text("a".into()),
                Value::Decimal(Decimal::new(100, 2)),
            ),
        ]),
        Value::Map(vec![
            (Value::Text("a".into()), Value::Decimal(Decimal::new(1, 0))),
            (Value::Text("z".into()), Value::Nat64(9)),
        ]),
        Value::List(vec![Value::Map(vec![
            (
                Value::Decimal(Decimal::new(200, 2)),
                Value::List(vec![Value::Null]),
            ),
            (
                Value::Decimal(Decimal::new(100, 2)),
                Value::Text("first".into()),
            ),
        ])]),
    ];
    for left in &values {
        validate_borrowed_canonical_value(left).unwrap();
        let owned_left = canonicalize_owned_value(left.clone()).unwrap();
        for right in &values {
            let owned_right = canonicalize_owned_value(right.clone()).unwrap();
            let expected = Value::canonical_cmp(&owned_left, &owned_right);
            assert_eq!(compare_borrowed_group_values(left, right), expected);
            assert_eq!(compare_borrowed_group_values(&owned_left, right), expected);
        }
    }
}
