use crate::{
    db::{executor::coerce::*, primitives::Cmp},
    types::{Account, Int, Nat, Principal, Ulid},
    value::{Value, ValueEnum},
};

//
// ───────────────────────────────────────────────────────────────
// Basic helpers
// ───────────────────────────────────────────────────────────────
//

fn v<T: Into<Value>>(v: T) -> Value {
    v.into()
}

//
// ───────────────────────────────────────────────────────────────
// Numeric Coercion
// ───────────────────────────────────────────────────────────────
//

#[test]
fn numeric_numeric_eq() {
    assert_eq!(coerce_basic(&v(10_i64), &v(10_u64), Cmp::Eq), Some(true));
}

#[test]
fn numeric_numeric_neq() {
    assert_eq!(coerce_basic(&v(5_i64), &v(10_u64), Cmp::Ne), Some(true));
}

#[test]
fn numeric_numeric_ordering() {
    assert_eq!(coerce_basic(&v(3_i64), &v(10_u64), Cmp::Lt), Some(true));
}

#[test]
fn numeric_bigint_cross_compare() {
    let big = Int::from(123_i32);
    assert_eq!(
        coerce_basic(&v(big.clone()), &v(123_i64), Cmp::Eq),
        Some(true)
    );
    assert_eq!(coerce_basic(&v(big), &v(200_i64), Cmp::Lt), Some(true));
}

#[test]
fn numeric_bignat_cross_compare() {
    let big = Nat::from(500_i32);
    assert_eq!(
        coerce_basic(&v(big.clone()), &v(400_u64), Cmp::Gt),
        Some(true)
    );
    assert_eq!(coerce_basic(&v(big), &v(500_u64), Cmp::Eq), Some(true));
}

//
// ───────────────────────────────────────────────────────────────
// Textual Coercion
// ───────────────────────────────────────────────────────────────
//

#[test]
fn text_cs_eq() {
    assert_eq!(coerce_basic(&v("Fox"), &v("Fox"), Cmp::Eq), Some(true));
}

#[test]
fn text_ci_eq() {
    assert_eq!(coerce_basic(&v("Fox"), &v("fOx"), Cmp::EqCi), Some(true));
}

#[test]
fn text_contains() {
    assert_eq!(
        coerce_basic(&v("Hello World"), &v("World"), Cmp::Contains),
        Some(true)
    );
}

#[test]
fn text_starts_with() {
    assert_eq!(
        coerce_basic(&v("Hello"), &v("He"), Cmp::StartsWith),
        Some(true)
    );
}

//
// ───────────────────────────────────────────────────────────────
// Enum Coercion
// ───────────────────────────────────────────────────────────────
//

#[test]
fn enum_eq() {
    let a = Value::Enum(ValueEnum::new("A", Some("MyEnum")));
    let b = Value::Enum(ValueEnum::new("A", Some("MyEnum")));
    assert_eq!(coerce_basic(&a, &b, Cmp::Eq), Some(true));
}

#[test]
fn enum_ne() {
    let a = Value::Enum(ValueEnum::new("A", Some("MyEnum")));
    let b = Value::Enum(ValueEnum::new("B", Some("MyEnum")));
    assert_eq!(coerce_basic(&a, &b, Cmp::Ne), Some(true));
}

#[test]
fn enum_eq_is_case_insensitive_for_variant() {
    let a = Value::Enum(ValueEnum::new("Common", Some("MyEnum")));
    let b = Value::Enum(ValueEnum::new("common", Some("MyEnum")));
    assert_eq!(coerce_basic(&a, &b, Cmp::Eq), Some(true));
    assert_eq!(coerce_basic(&a, &b, Cmp::EqCi), Some(true));
}

#[test]
fn enum_different_paths() {
    let a = Value::Enum(ValueEnum::new("A", Some("E1")));
    let b = Value::Enum(ValueEnum::new("A", Some("E2")));
    assert_eq!(coerce_basic(&a, &b, Cmp::Eq), None);
}

#[test]
fn enum_loose_match_without_path_on_rhs() {
    let a = Value::Enum(ValueEnum::new("A", Some("MyEnum")));
    let b = Value::Enum(ValueEnum::new("A", None));
    assert_eq!(coerce_basic(&a, &b, Cmp::Eq), Some(true));
}

#[test]
fn enum_loose_mismatch_without_path_on_rhs() {
    let a = Value::Enum(ValueEnum::new("A", Some("MyEnum")));
    let b = Value::Enum(ValueEnum::new("B", None));
    assert_eq!(coerce_basic(&a, &b, Cmp::Eq), Some(false));
}

#[test]
fn enum_payload_must_match_for_equality() {
    let a = Value::Enum(ValueEnum::new("A", Some("MyEnum")).with_payload(Value::Uint(10)));
    let b = Value::Enum(ValueEnum::new("A", Some("MyEnum")).with_payload(Value::Uint(20)));
    assert_eq!(coerce_basic(&a, &b, Cmp::Eq), Some(false));
}

//
// ───────────────────────────────────────────────────────────────
// Identifier <-> Text (ULID, Principal, Subaccount)
// ───────────────────────────────────────────────────────────────
//

#[test]
fn ulid_text_eq() {
    let id = Ulid::from_parts(1, 2);
    let text = id.to_string();
    assert_eq!(coerce_basic(&v(id), &v(text), Cmp::Eq), Some(true));
}

#[test]
fn ulid_text_ne() {
    let id = Ulid::from_parts(1, 2);
    let text = "01ARZ3NDEKTSV4RRFFQ69G5FAV"; // Different ULID
    assert_eq!(coerce_basic(&v(id), &v(text), Cmp::Eq), Some(false));
}

#[test]
fn principal_text_eq() {
    let p = Principal::dummy(1);
    let text = p.to_string();
    assert_eq!(coerce_basic(&v(p), &v(text), Cmp::Eq), Some(true));
}

#[test]
fn account_text_eq() {
    let a = Account::dummy(12);
    let text = a.to_string();
    assert_eq!(coerce_basic(&v(a), &v(text), Cmp::Eq), Some(true));
}

#[test]
fn identifier_text_invalid_format_is_none() {
    let p = Principal::dummy(1);
    // malformed principal string should not coerce
    assert_eq!(coerce_basic(&v(p), &v("not-a-principal"), Cmp::Eq), None);
}

//
// ───────────────────────────────────────────────────────────────
// Collection Membership
// ───────────────────────────────────────────────────────────────
//

#[test]
fn collection_contains() {
    let list = v(Value::from_list(&[1, 2, 3]));
    assert_eq!(coerce_basic(&list, &v(2), Cmp::Contains), Some(true));
}

#[test]
fn collection_any_in() {
    let list = v(Value::from_list(&["a", "b"]));
    let needles = v(Value::from_list(&["x", "b"]));
    assert_eq!(coerce_basic(&list, &needles, Cmp::AnyIn), Some(true));
}

#[test]
fn collection_all_in() {
    let list = v(Value::from_list(&["a", "b", "c"]));
    let needles = v(Value::from_list(&["a", "c"]));
    assert_eq!(coerce_basic(&list, &needles, Cmp::AllIn), Some(true));
}

#[test]
fn collection_is_empty() {
    let empty = v(Value::from_list::<Value>(&[]));
    assert_eq!(coerce_basic(&empty, &v(()), Cmp::IsEmpty), Some(true));
}

#[test]
fn collection_not_empty() {
    let nonempty = v(Value::from_list(&[1]));
    assert_eq!(coerce_basic(&nonempty, &v(()), Cmp::IsNotEmpty), Some(true));
}

//
// ───────────────────────────────────────────────────────────────
// Cross-family fallback
// ───────────────────────────────────────────────────────────────
//

#[test]
fn fallback_strict_equality() {
    // Different families → fallback to strict equality
    assert_eq!(
        coerce_basic(&v(true), &v("true"), Cmp::Eq),
        None // let FilterEvaluator::final_fallback handle it
    );
}

#[test]
fn fallback_strict_neq() {
    assert_eq!(coerce_basic(&v(true), &v("no"), Cmp::Ne), None);
}
