use crate::{
    db::index::fingerprint::hash_value,
    serialize::{deserialize, serialize},
    traits::NumFromPrimitive,
    types::{
        Account, Date, Decimal, Duration, E8s, E18s, Float32 as F32, Float64 as F64, Int, Int128,
        Nat, Nat128, Principal, Subaccount, Timestamp, Ulid,
    },
    value::{CoercionFamily, CoercionFamilyExt, SchemaInvariantError, TextMode, Value, ValueEnum},
};
use std::{cmp::Ordering, str::FromStr};

// ---- helpers -----------------------------------------------------------

fn v_f64(x: f64) -> Value {
    Value::Float64(F64::try_new(x).expect("finite f64"))
}
fn v_f32(x: f32) -> Value {
    Value::Float32(F32::try_new(x).expect("finite f32"))
}
fn v_i(x: i64) -> Value {
    Value::Int(x)
}
fn v_u(x: u64) -> Value {
    Value::Uint(x)
}
fn v_d_i(x: i64) -> Value {
    Value::Decimal(Decimal::from_i64(x).unwrap())
}
fn v_txt(s: &str) -> Value {
    Value::Text(s.to_string())
}

macro_rules! sample_value_for_scalar {
    (Account) => {
        Value::Account(Account::dummy(7))
    };
    (Blob) => {
        Value::Blob(vec![1u8, 2u8, 3u8])
    };
    (Bool) => {
        Value::Bool(true)
    };
    (Date) => {
        Value::Date(Date::new(2024, 1, 2))
    };
    (Decimal) => {
        Value::Decimal(Decimal::new(123, 2))
    };
    (Duration) => {
        Value::Duration(Duration::from_secs(1))
    };
    (Enum) => {
        Value::Enum(ValueEnum::loose("example"))
    };
    (E8s) => {
        Value::E8s(E8s::from_atomic(1))
    };
    (E18s) => {
        Value::E18s(E18s::from_atomic(1))
    };
    (Float32) => {
        Value::Float32(F32::try_new(1.25).expect("Float32 sample should be finite"))
    };
    (Float64) => {
        Value::Float64(F64::try_new(2.5).expect("Float64 sample should be finite"))
    };
    (Int) => {
        Value::Int(-7)
    };
    (Int128) => {
        Value::Int128(Int128::from(123i128))
    };
    (IntBig) => {
        Value::IntBig(Int::from(99i32))
    };
    (Principal) => {
        Value::Principal(Principal::from_slice(&[1u8, 2u8, 3u8]))
    };
    (Subaccount) => {
        Value::Subaccount(Subaccount::new([1u8; 32]))
    };
    (Text) => {
        Value::Text("example".to_string())
    };
    (Timestamp) => {
        Value::Timestamp(Timestamp::from_seconds(1))
    };
    (Uint) => {
        Value::Uint(7)
    };
    (Uint128) => {
        Value::Uint128(Nat128::from(9u128))
    };
    (UintBig) => {
        Value::UintBig(Nat::from(11u64))
    };
    (Ulid) => {
        Value::Ulid(Ulid::from_u128(42))
    };
    (Unit) => {
        Value::Unit
    };
}

/// Build scalar-backed values paired with their registry numeric flag.
fn registry_numeric_cases() -> Vec<(Value, bool)> {
    macro_rules! collect_cases {
        ( @entries $( ($scalar:ident, $coercion_family:expr, $value_pat:pat, is_numeric_value = $is_numeric:expr, supports_numeric_coercion = $supports_numeric_coercion:expr, supports_arithmetic = $supports_arithmetic:expr, supports_equality = $supports_equality:expr, supports_ordering = $supports_ordering:expr, is_keyable = $is_keyable:expr, is_storage_key_encodable = $is_storage_key_encodable:expr) ),* $(,)? ) => {
            vec![ $( (sample_value_for_scalar!($scalar), $is_numeric) ),* ]
        };
        ( @args $($ignore:tt)*; @entries $( ($scalar:ident, $coercion_family:expr, $value_pat:pat, is_numeric_value = $is_numeric:expr, supports_numeric_coercion = $supports_numeric_coercion:expr, supports_arithmetic = $supports_arithmetic:expr, supports_equality = $supports_equality:expr, supports_ordering = $supports_ordering:expr, is_keyable = $is_keyable:expr, is_storage_key_encodable = $is_storage_key_encodable:expr) ),* $(,)? ) => {
            vec![ $( (sample_value_for_scalar!($scalar), $is_numeric) ),* ]
        };
    }

    let cases = scalar_registry!(collect_cases);

    cases
}

/// Build scalar-backed values paired with their registry numeric-coercion flag.
fn registry_numeric_coercion_cases() -> Vec<(Value, bool)> {
    macro_rules! collect_cases {
        ( @entries $( ($scalar:ident, $coercion_family:expr, $value_pat:pat, is_numeric_value = $is_numeric:expr, supports_numeric_coercion = $supports_numeric_coercion:expr, supports_arithmetic = $supports_arithmetic:expr, supports_equality = $supports_equality:expr, supports_ordering = $supports_ordering:expr, is_keyable = $is_keyable:expr, is_storage_key_encodable = $is_storage_key_encodable:expr) ),* $(,)? ) => {
            vec![ $( (sample_value_for_scalar!($scalar), $supports_numeric_coercion) ),* ]
        };
        ( @args $($ignore:tt)*; @entries $( ($scalar:ident, $coercion_family:expr, $value_pat:pat, is_numeric_value = $is_numeric:expr, supports_numeric_coercion = $supports_numeric_coercion:expr, supports_arithmetic = $supports_arithmetic:expr, supports_equality = $supports_equality:expr, supports_ordering = $supports_ordering:expr, is_keyable = $is_keyable:expr, is_storage_key_encodable = $is_storage_key_encodable:expr) ),* $(,)? ) => {
            vec![ $( (sample_value_for_scalar!($scalar), $supports_numeric_coercion) ),* ]
        };
    }

    scalar_registry!(collect_cases)
}

/// Build scalar-backed values paired with their registry keyable flag.
fn registry_keyable_cases() -> Vec<(Value, bool)> {
    macro_rules! collect_cases {
        ( @entries $( ($scalar:ident, $coercion_family:expr, $value_pat:pat, is_numeric_value = $is_numeric:expr, supports_numeric_coercion = $supports_numeric_coercion:expr, supports_arithmetic = $supports_arithmetic:expr, supports_equality = $supports_equality:expr, supports_ordering = $supports_ordering:expr, is_keyable = $is_keyable:expr, is_storage_key_encodable = $is_storage_key_encodable:expr) ),* $(,)? ) => {
            vec![ $( (sample_value_for_scalar!($scalar), $is_keyable) ),* ]
        };
        ( @args $($ignore:tt)*; @entries $( ($scalar:ident, $coercion_family:expr, $value_pat:pat, is_numeric_value = $is_numeric:expr, supports_numeric_coercion = $supports_numeric_coercion:expr, supports_arithmetic = $supports_arithmetic:expr, supports_equality = $supports_equality:expr, supports_ordering = $supports_ordering:expr, is_keyable = $is_keyable:expr, is_storage_key_encodable = $is_storage_key_encodable:expr) ),* $(,)? ) => {
            vec![ $( (sample_value_for_scalar!($scalar), $is_keyable) ),* ]
        };
    }

    let cases = scalar_registry!(collect_cases);

    cases
}

/// Build scalar-backed values paired with their registry coercion family.
fn registry_coercion_family_cases() -> Vec<(Value, CoercionFamily)> {
    macro_rules! collect_cases {
        ( @entries $( ($scalar:ident, $coercion_family:expr, $value_pat:pat, is_numeric_value = $is_numeric:expr, supports_numeric_coercion = $supports_numeric_coercion:expr, supports_arithmetic = $supports_arithmetic:expr, supports_equality = $supports_equality:expr, supports_ordering = $supports_ordering:expr, is_keyable = $is_keyable:expr, is_storage_key_encodable = $is_storage_key_encodable:expr) ),* $(,)? ) => {
            vec![ $( (sample_value_for_scalar!($scalar), $coercion_family) ),* ]
        };
        ( @args $($ignore:tt)*; @entries $( ($scalar:ident, $coercion_family:expr, $value_pat:pat, is_numeric_value = $is_numeric:expr, supports_numeric_coercion = $supports_numeric_coercion:expr, supports_arithmetic = $supports_arithmetic:expr, supports_equality = $supports_equality:expr, supports_ordering = $supports_ordering:expr, is_keyable = $is_keyable:expr, is_storage_key_encodable = $is_storage_key_encodable:expr) ),* $(,)? ) => {
            vec![ $( (sample_value_for_scalar!($scalar), $coercion_family) ),* ]
        };
    }

    let cases = scalar_registry!(collect_cases);

    cases
}

// ---- keys --------------------------------------------------------------

#[test]
fn as_storage_key_some_for_keyable_variants() {
    assert!(Value::Int(7).as_storage_key().is_some());
    assert!(Value::Uint(7).as_storage_key().is_some());
    assert!(Value::Ulid(Ulid::MIN).as_storage_key().is_some());
    assert!(Value::Unit.as_storage_key().is_some());

    // Non-key / non-orderable variants
    assert!(v_txt("x").as_storage_key().is_none());
    assert!(
        Value::Decimal(Decimal::new(1, 0))
            .as_storage_key()
            .is_none()
    );
    assert!(Value::List(vec![]).as_storage_key().is_none());
    assert!(Value::Null.as_storage_key().is_none());
}

#[test]
fn storage_key_round_trips_through_value() {
    let values = [
        Value::Int(-9),
        Value::Uint(9),
        Value::Ulid(Ulid::MAX),
        Value::Unit,
    ];

    for v in values {
        let key = v
            .as_storage_key()
            .expect("value should be convertible to storage key");

        let back = key.as_value();

        assert_eq!(
            v, back,
            "Value <-> StorageKey round trip failed: {v:?} -> {key:?} -> {back:?}"
        );
    }
}

// ---- numeric coercion & comparison ------------------------------------

#[test]
fn value_is_numeric_matches_registry_flag() {
    for (value, expected) in registry_numeric_cases() {
        assert_eq!(value.is_numeric(), expected, "value: {value:?}");
    }
}

#[test]
fn value_supports_numeric_coercion_matches_registry_flag() {
    for (value, expected) in registry_numeric_coercion_cases() {
        assert_eq!(
            value.supports_numeric_coercion(),
            expected,
            "value: {value:?}"
        );
    }
}

#[test]
fn value_as_storage_key_matches_registry_flag() {
    for (value, is_keyable) in registry_keyable_cases() {
        assert_eq!(
            value.as_storage_key().is_some(),
            is_keyable,
            "value: {value:?}"
        );
    }
}

#[test]
fn value_coercion_family_matches_registry_flag() {
    for (value, expected_coercion_family) in registry_coercion_family_cases() {
        assert_eq!(
            value.coercion_family(),
            expected_coercion_family,
            "value: {value:?}"
        );
    }
}

#[test]
fn cmp_numeric_int_nat_eq_and_order() {
    assert_eq!(v_i(10).cmp_numeric(&v_u(10)), Some(Ordering::Equal));
    assert_eq!(v_i(9).cmp_numeric(&v_u(10)), Some(Ordering::Less));
    // negative int vs nat: not comparable via f64 path; decimal path handles it
    assert_eq!(v_i(-1).cmp_numeric(&v_u(0)), Some(Ordering::Less));
}

#[test]
fn cmp_numeric_int_float_eq() {
    assert_eq!(v_i(42).cmp_numeric(&v_f64(42.0)), Some(Ordering::Equal));
    assert_eq!(v_i(42).cmp_numeric(&v_f32(42.0)), Some(Ordering::Equal));
}

#[test]
fn cmp_numeric_decimal_int_and_float() {
    assert_eq!(v_d_i(10).cmp_numeric(&v_i(10)), Some(Ordering::Equal));
    assert_eq!(v_d_i(10).cmp_numeric(&v_f64(10.0)), Some(Ordering::Equal));
    assert_eq!(v_d_i(11).cmp_numeric(&v_f64(10.5)), Some(Ordering::Greater));
}

#[test]
#[allow(clippy::cast_precision_loss)]
fn cmp_numeric_safe_int_boundary() {
    // 2^53 is exactly representable in f64
    let safe: i64 = 9_007_199_254_740_992; // 1 << 53
    let int_safe = v_i(safe);
    let float_safe = v_f64(safe as f64);
    assert_eq!(int_safe.cmp_numeric(&float_safe), Some(Ordering::Equal));

    // one above 2^53 is not exactly representable; decimal path should see it as greater
    let int_unsafe = v_i(safe + 1);
    assert_eq!(int_unsafe.cmp_numeric(&float_safe), Some(Ordering::Greater));
}

#[test]
fn cmp_numeric_neg_zero_equals_zero() {
    let neg_zero = Value::Float64(F64::try_new(-0.0).unwrap());
    assert_eq!(neg_zero.cmp_numeric(&v_i(0)), Some(Ordering::Equal));
    let neg_zero32 = Value::Float32(F32::try_new(-0.0).unwrap());
    assert_eq!(neg_zero32.cmp_numeric(&v_i(0)), Some(Ordering::Equal));
}

#[test]
fn cmp_numeric_respects_registry_numeric_coercion_flag() {
    for (value, supports_numeric_coercion) in registry_numeric_coercion_cases() {
        let cmp = value.cmp_numeric(&value);
        if supports_numeric_coercion {
            assert_eq!(cmp, Some(Ordering::Equal), "value: {value:?}");
        } else {
            assert!(cmp.is_none(), "value: {value:?}");
        }
    }
}

#[test]
fn cmp_numeric_rejects_date_and_bigints() {
    let date = Value::Date(Date::new(2024, 1, 2));
    let int_big = Value::IntBig(Int::from(10i32));
    let uint_big = Value::UintBig(Nat::from(10u64));
    let one = Value::Int(1);

    assert!(!date.supports_numeric_coercion());
    assert!(!int_big.supports_numeric_coercion());
    assert!(!uint_big.supports_numeric_coercion());

    assert!(date.cmp_numeric(&one).is_none());
    assert!(int_big.cmp_numeric(&one).is_none());
    assert!(uint_big.cmp_numeric(&one).is_none());
}

#[test]
fn cmp_numeric_is_unreachable_for_non_numeric_coercible_values() {
    let left = Value::Date(Date::EPOCH);
    let right = Value::Int(0);

    assert!(!left.supports_numeric_coercion());
    assert!(left.cmp_numeric(&right).is_none());
    assert!(left.partial_cmp(&right).is_none());
}

#[test]
fn partial_ord_cross_variant_is_none() {
    // PartialOrd stays within same variant; cross-variant returns None
    assert!(v_i(1).partial_cmp(&v_f64(1.0)).is_none());
    assert!(v_txt("a").partial_cmp(&v_txt("b")).is_some());
}

#[test]
fn from_map_is_canonical_and_order_independent() {
    let map_a = Value::from_map(vec![
        (v_txt("c"), v_u(3)),
        (v_txt("a"), v_u(1)),
        (v_txt("b"), v_u(2)),
    ])
    .expect("map_a should normalize");
    let map_b = Value::from_map(vec![
        (v_txt("a"), v_u(1)),
        (v_txt("b"), v_u(2)),
        (v_txt("c"), v_u(3)),
    ])
    .expect("map_b should normalize");

    assert_eq!(map_a, map_b);

    let bytes_a = serialize(&map_a).expect("serialize map_a");
    let bytes_b = serialize(&map_b).expect("serialize map_b");
    assert_eq!(bytes_a, bytes_b);

    let hash_a = hash_value(&map_a).expect("hash map_a");
    let hash_b = hash_value(&map_b).expect("hash map_b");
    assert_eq!(hash_a, hash_b);
}

#[test]
fn try_from_map_vec_is_canonical_and_order_independent() {
    let map_a = Value::try_from(vec![
        (v_txt("c"), v_u(3)),
        (v_txt("a"), v_u(1)),
        (v_txt("b"), v_u(2)),
    ])
    .expect("map_a should normalize");
    let map_b = Value::try_from(vec![
        (v_txt("a"), v_u(1)),
        (v_txt("b"), v_u(2)),
        (v_txt("c"), v_u(3)),
    ])
    .expect("map_b should normalize");

    assert_eq!(map_a, map_b);
}

#[test]
fn try_from_map_vec_returns_schema_invariant_error() {
    let err = Value::try_from(vec![(v_txt("a"), v_u(1)), (v_txt("a"), v_u(2))])
        .expect_err("duplicate map keys should fail");

    assert!(matches!(
        err,
        SchemaInvariantError::InvalidMapValue(crate::value::MapValueError::DuplicateKey { .. })
    ));
}

#[test]
fn deserialize_normalizes_non_canonical_map_encoding() {
    let non_canonical = Value::Map(vec![(v_txt("z"), v_u(9)), (v_txt("a"), v_u(1))]);
    let bytes = serialize(&non_canonical).expect("serialize non-canonical map payload");
    let decoded = deserialize::<Value>(&bytes).expect("deserialization should normalize map");

    let expected = Value::from_map(vec![(v_txt("a"), v_u(1)), (v_txt("z"), v_u(9))])
        .expect("expected canonical map");
    assert_eq!(decoded, expected);
}

#[test]
fn canonical_cmp_key_is_total_for_enum_payloads() {
    let left = Value::Enum(
        ValueEnum::new("Any", Some("test::Enum")).with_payload(Value::from_slice(&[v_i(1)])),
    );
    let right = Value::Enum(
        ValueEnum::new("Any", Some("test::Enum"))
            .with_payload(Value::from_map(vec![(v_txt("k"), v_i(1))]).expect("map payload")),
    );

    let forward = Value::canonical_cmp_key(&left, &right);
    let reverse = Value::canonical_cmp_key(&right, &left);
    assert_eq!(forward, reverse.reverse());
    assert_ne!(forward, Ordering::Equal);
}

// ---- list membership ---------------------------------------------------

#[test]
fn list_contains_scalar() {
    let l = Value::from_slice(&[v_i(1), v_txt("a")]);
    assert_eq!(l.contains(&v_i(1)), Some(true));
    assert_eq!(l.contains(&v_i(2)), Some(false));
}

#[test]
fn list_contains_any_all_and_vacuous_truth() {
    let l = Value::from_slice(&[v_txt("x"), v_txt("y")]);
    let needles_any = Value::from_slice(&[v_txt("z"), v_txt("y")]);
    let needles_all = Value::from_slice(&[v_txt("x"), v_txt("y")]);
    let empty = Value::from_slice::<Value>(&[]);
    assert_eq!(l.contains_any(&needles_any), Some(true));
    assert_eq!(l.contains_all(&needles_all), Some(true));
    assert_eq!(l.contains_any(&empty), Some(false), "AnyIn([]) == false");
    assert_eq!(l.contains_all(&empty), Some(true), "AllIn([]) == true");
}

// ---- list any/all ------------------------------------------------------

#[test]
fn contains_any_list_vs_list() {
    let haystack = Value::from_slice(&[v_i(1), v_i(2), v_i(3)]);
    let needles = Value::from_slice(&[v_i(4), v_i(2)]);
    assert_eq!(haystack.contains_any(&needles), Some(true));

    let needles_none = Value::from_slice(&[v_i(4), v_i(5)]);
    assert_eq!(haystack.contains_any(&needles_none), Some(false));

    let empty = Value::from_slice::<Value>(&[]);
    assert_eq!(
        haystack.contains_any(&empty),
        Some(false),
        "AnyIn([]) == false"
    );
}

#[test]
fn contains_all_list_vs_list() {
    let haystack = Value::from_slice(&[v_txt("a"), v_txt("b"), v_txt("c")]);
    let needles = Value::from_slice(&[v_txt("a"), v_txt("c")]);
    assert_eq!(haystack.contains_all(&needles), Some(true));

    let needles_missing = Value::from_slice(&[v_txt("a"), v_txt("z")]);
    assert_eq!(haystack.contains_all(&needles_missing), Some(false));

    let empty = Value::from_slice::<Value>(&[]);
    assert_eq!(
        haystack.contains_all(&empty),
        Some(true),
        "AllIn([]) == true"
    );
}

#[test]
fn contains_any_list_vs_scalar() {
    let haystack = Value::from_slice(&[v_i(10), v_i(20)]);
    assert_eq!(haystack.contains_any(&v_i(20)), Some(true));
    assert_eq!(haystack.contains_any(&v_i(99)), Some(false));
}

#[test]
fn contains_all_list_vs_scalar() {
    let haystack = Value::from_slice(&[v_i(10), v_i(20)]);
    assert_eq!(haystack.contains_all(&v_i(20)), Some(true));
    assert_eq!(haystack.contains_all(&v_i(99)), Some(false));
}

#[test]
fn contains_any_scalar_vs_list() {
    let scalar = v_txt("hello");
    let needles_yes = Value::from_slice(&[v_txt("x"), v_txt("hello")]);
    let needles_no = Value::from_slice(&[v_txt("x"), v_txt("y")]);

    assert_eq!(scalar.contains_any(&needles_yes), Some(true));
    assert_eq!(scalar.contains_any(&needles_no), Some(false));
}

#[test]
fn contains_all_scalar_vs_list() {
    let scalar = v_txt("hello");
    let needles_yes = Value::from_slice(&[v_txt("hello")]);
    let needles_extra = Value::from_slice(&[v_txt("hello"), v_txt("world")]);
    let empty = Value::from_slice::<Value>(&[]);

    assert_eq!(scalar.contains_all(&needles_yes), Some(true));
    assert_eq!(scalar.contains_all(&needles_extra), Some(false));
    assert_eq!(
        scalar.contains_all(&empty),
        Some(false),
        "Scalar all-in empty should be false"
    );
}

#[test]
fn contains_any_scalar_vs_scalar() {
    let scalar = v_u(5);
    assert_eq!(scalar.contains_any(&v_u(5)), Some(true));
    assert_eq!(scalar.contains_any(&v_u(6)), Some(false));
}

#[test]
fn contains_all_scalar_vs_scalar() {
    let scalar = v_u(5);
    assert_eq!(scalar.contains_all(&v_u(5)), Some(true));
    assert_eq!(scalar.contains_all(&v_u(6)), Some(false));
}

#[test]
fn in_list_ci_text_vs_list() {
    let haystack = Value::from_slice(&[v_txt("Alpha"), v_txt("Beta")]);
    assert_eq!(v_txt("alpha").in_list_ci(&haystack), Some(true));
    assert_eq!(v_txt("BETA").in_list_ci(&haystack), Some(true));
    assert_eq!(v_txt("gamma").in_list_ci(&haystack), Some(false));
}

#[test]
fn list_contains_ci_scalar() {
    let list = Value::from_slice(&[v_txt("Foo"), v_txt("Bar")]);
    assert_eq!(list.contains_ci(&v_txt("foo")), Some(true));
    assert_eq!(list.contains_ci(&v_txt("BAR")), Some(true));
    assert_eq!(list.contains_ci(&v_txt("baz")), Some(false));
}

#[test]
fn list_contains_any_ci() {
    let haystack = Value::from_slice(&[v_txt("Apple"), v_txt("Banana")]);
    let needles_yes = Value::from_slice(&[v_txt("banana"), v_txt("Cherry")]);
    let needles_no = Value::from_slice(&[v_txt("pear"), v_txt("cherry")]);

    assert_eq!(haystack.contains_any_ci(&needles_yes), Some(true));
    assert_eq!(haystack.contains_any_ci(&needles_no), Some(false));
}

#[test]
fn list_contains_all_ci() {
    let haystack = Value::from_slice(&[v_txt("Dog"), v_txt("Cat"), v_txt("Bird")]);
    let needles_yes = Value::from_slice(&[v_txt("dog"), v_txt("cat")]);
    let needles_no = Value::from_slice(&[v_txt("dog"), v_txt("lion")]);

    assert_eq!(haystack.contains_all_ci(&needles_yes), Some(true));
    assert_eq!(haystack.contains_all_ci(&needles_no), Some(false));
}

#[test]
fn scalar_vs_list_ci() {
    let scalar = v_txt("Hello");
    let list = Value::from_slice(&[v_txt("HELLO"), v_txt("World")]);

    assert_eq!(scalar.in_list_ci(&list), Some(true));
    assert_eq!(scalar.contains_any_ci(&list), Some(true));

    let list2 = Value::from_slice(&[v_txt("World")]);
    assert_eq!(scalar.contains_any_ci(&list2), Some(false));
}

#[test]
fn ci_membership_with_empty_lists() {
    let empty = Value::from_slice::<Value>(&[]);
    let scalar = v_txt("alpha");

    assert_eq!(scalar.in_list_ci(&empty), Some(false));
    assert_eq!(scalar.contains_any_ci(&empty), Some(false));
    assert_eq!(scalar.contains_all_ci(&empty), Some(false));
}

#[test]
fn ci_equality_parses_identifier_text() {
    let ulid = Ulid::generate();
    let ulid_text = Value::Text(ulid.to_string());

    assert!(Value::Ulid(ulid).contains_ci(&ulid_text).unwrap());
    assert!(
        Value::Ulid(ulid)
            .in_list_ci(&Value::from_slice(&[ulid_text]))
            .unwrap()
    );
}

#[test]
fn ci_membership_handles_ulid_strings() {
    let target = Ulid::generate();
    let actual = Value::from_slice(&[Value::Ulid(target)]);
    let needles = Value::from_slice(&[Value::Text(target.to_string())]);

    assert_eq!(actual.contains_any_ci(&needles), Some(true));
    assert_eq!(actual.contains_all_ci(&needles), Some(true));
}

// ---- text CS/CI --------------------------------------------------------

#[test]
fn text_eq_cs_vs_ci() {
    let a = v_txt("Alpha");
    let b = v_txt("alpha");
    assert_eq!(a.text_eq(&b, TextMode::Cs), Some(false));
    assert_eq!(a.text_eq(&b, TextMode::Ci), Some(true));
}

#[test]
fn text_contains_starts_ends_cs_ci() {
    let a = v_txt("Hello Alpha World");
    assert_eq!(a.text_contains(&v_txt("alpha"), TextMode::Cs), Some(false));
    assert_eq!(a.text_contains(&v_txt("alpha"), TextMode::Ci), Some(true));

    assert_eq!(
        a.text_starts_with(&v_txt("hello"), TextMode::Cs),
        Some(false)
    );
    assert_eq!(
        a.text_starts_with(&v_txt("hello"), TextMode::Ci),
        Some(true)
    );

    assert_eq!(a.text_ends_with(&v_txt("WORLD"), TextMode::Cs), Some(false));
    assert_eq!(a.text_ends_with(&v_txt("WORLD"), TextMode::Ci), Some(true));
}

// ---- E8s / E18s <-> Decimal / Float cross-type tests -------------------

// helper constructors — ADAPT these to your actual API
fn v_e8(raw: u64) -> Value {
    // e.g., E8s::from_raw(raw) or E8s(raw)
    Value::E8s(E8s::from(raw)) // <-- change if needed
}
fn v_e18(raw: u128) -> Value {
    Value::E18s(E18s::from(raw)) // <-- change if needed
}
fn v_dec_str(s: &str) -> Value {
    Value::Decimal(Decimal::from_str(s).expect("valid decimal"))
}

#[test]
fn e8s_equals_decimal_when_scaled() {
    // 1.00 token == 100_000_000 e8s
    let one_token_e8s = v_e8(100_000_000);
    let one_token_dec = v_dec_str("1");
    assert_eq!(
        one_token_e8s.cmp_numeric(&one_token_dec),
        Some(Ordering::Equal)
    );

    // 12.34567890 tokens == 1_234_567_890 e8s
    let e8s = v_e8(1_234_567_890);
    let dec = v_dec_str("12.3456789");
    assert_eq!(e8s.cmp_numeric(&dec), Some(Ordering::Equal));
}

#[test]
fn e8s_orders_correctly_against_decimal() {
    let nine_tenths_e8s = v_e8(90_000_000);
    let one_dec = v_dec_str("1");
    assert_eq!(nine_tenths_e8s.cmp_numeric(&one_dec), Some(Ordering::Less));

    let eleven_tenths_e8s = v_e8(110_000_000);
    assert_eq!(
        eleven_tenths_e8s.cmp_numeric(&one_dec),
        Some(Ordering::Greater)
    );
}

#[test]
fn e8s_vs_float64_safe_eq() {
    // 2^53-safe region: exact in f64 when converted through Decimal or safe-int path
    let e8s = v_e8(200_000_000); // 2.0
    assert_eq!(e8s.cmp_numeric(&v_f64(2.0)), Some(Ordering::Equal));
}

#[test]
fn e18s_equals_decimal_when_scaled() {
    // 1.000000000000000000 == 1e18 e18s
    let one = v_e18(1_000_000_000_000_000_000);
    let one_dec = v_dec_str("1");
    assert_eq!(one.cmp_numeric(&one_dec), Some(Ordering::Equal));

    // 0.000000000000000123 == 123 e18s
    let tiny = v_e18(123);
    let tiny_dec = v_dec_str("0.000000000000000123");
    assert_eq!(tiny.cmp_numeric(&tiny_dec), Some(Ordering::Equal));
}

#[test]
fn e18s_ordering_and_float_cross_check() {
    let half = v_e18(500_000_000_000_000_000); // 0.5
    assert_eq!(half.cmp_numeric(&v_dec_str("0.4")), Some(Ordering::Greater));
    assert_eq!(half.cmp_numeric(&v_dec_str("0.6")), Some(Ordering::Less));
    assert_eq!(half.cmp_numeric(&v_f64(0.5)), Some(Ordering::Equal));
}

#[test]
fn e8s_e18s_text_and_list_do_not_compare() {
    // sanity: non-numeric shapes return None from cmp_numeric
    assert!(v_e8(1).partial_cmp(&v_txt("1")).is_none());
    assert!(
        v_e18(1)
            .partial_cmp(&Value::from_slice(&[v_i(1)]))
            .is_none()
    );
}

// ----------- eq and none

#[test]
fn eq_and_ne_none_semantics() {
    let some_val = v_i(42);
    let none_val = Value::Null;

    // eq(None) only true if both sides are None
    assert!(none_val == Value::Null);
    assert!(some_val != Value::Null);

    // ne(None) true if left is not None
    assert!(none_val == Value::Null);
    assert!(some_val != Value::Null);
}
