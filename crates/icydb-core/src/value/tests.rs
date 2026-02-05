use crate::{
    traits::NumFromPrimitive,
    types::{Decimal, E8s, E18s, Float32 as F32, Float64 as F64, Ulid},
    value::{TextMode, Value},
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
    assert!(Value::None.as_storage_key().is_none());
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
fn partial_ord_cross_variant_is_none() {
    // PartialOrd stays within same variant; cross-variant returns None
    assert!(v_i(1).partial_cmp(&v_f64(1.0)).is_none());
    assert!(v_txt("a").partial_cmp(&v_txt("b")).is_some());
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
    let none_val = Value::None;

    // eq(None) only true if both sides are None
    assert!(none_val == Value::None);
    assert!(some_val != Value::None);

    // ne(None) true if left is not None
    assert!(none_val == Value::None);
    assert!(some_val != Value::None);
}
