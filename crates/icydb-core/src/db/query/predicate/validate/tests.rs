// NOTE: Invalid helpers remain only for intentionally invalid or non-queryable schemas.
use super::{
    model::{FieldType, ScalarType},
    rules::ensure_coercion,
    schema::ValidateError,
    validate_model,
};
use crate::{
    db::query::{
        builder::field::FieldRef,
        predicate::{CoercionId, CoercionSpec, CompareOp, ComparePredicate, Predicate},
    },
    model::field::{FieldKind, FieldModel},
    test_support::InvalidEntityModelBuilder,
    traits::{EntitySchema, FieldValue},
    types::{
        Account, Date, Decimal, Duration, Float32, Float64, Int, Int128, Nat, Nat128, Principal,
        Subaccount, Timestamp, Ulid,
    },
    value::{CoercionFamily, Value, ValueEnum},
};
use std::collections::BTreeSet;

/// Build a registry-driven list of all scalar variants.
fn registry_scalars() -> Vec<ScalarType> {
    macro_rules! collect_scalars {
        ( @entries $( ($scalar:ident, $coercion_family:expr, $value_pat:pat, is_numeric_value = $is_numeric:expr, supports_numeric_coercion = $supports_numeric_coercion:expr, supports_arithmetic = $supports_arithmetic:expr, supports_equality = $supports_equality:expr, supports_ordering = $supports_ordering:expr, is_keyable = $is_keyable:expr, is_storage_key_encodable = $is_storage_key_encodable:expr) ),* $(,)? ) => {
            vec![ $( ScalarType::$scalar ),* ]
        };
        ( @args $($ignore:tt)*; @entries $( ($scalar:ident, $coercion_family:expr, $value_pat:pat, is_numeric_value = $is_numeric:expr, supports_numeric_coercion = $supports_numeric_coercion:expr, supports_arithmetic = $supports_arithmetic:expr, supports_equality = $supports_equality:expr, supports_ordering = $supports_ordering:expr, is_keyable = $is_keyable:expr, is_storage_key_encodable = $is_storage_key_encodable:expr) ),* $(,)? ) => {
            vec![ $( ScalarType::$scalar ),* ]
        };
    }

    let scalars = scalar_registry!(collect_scalars);

    scalars
}

/// Returns the total count of ScalarType variants.
const SCALAR_TYPE_VARIANT_COUNT: usize = 21;

/// Map each ScalarType variant to a stable index.
fn scalar_index(scalar: ScalarType) -> usize {
    match scalar {
        ScalarType::Account => 0,
        ScalarType::Blob => 1,
        ScalarType::Bool => 2,
        ScalarType::Date => 3,
        ScalarType::Decimal => 4,
        ScalarType::Duration => 5,
        ScalarType::Enum => 6,
        ScalarType::Float32 => 7,
        ScalarType::Float64 => 8,
        ScalarType::Int => 9,
        ScalarType::Int128 => 10,
        ScalarType::IntBig => 11,
        ScalarType::Principal => 12,
        ScalarType::Subaccount => 13,
        ScalarType::Text => 14,
        ScalarType::Timestamp => 15,
        ScalarType::Uint => 16,
        ScalarType::Uint128 => 17,
        ScalarType::UintBig => 18,
        ScalarType::Ulid => 19,
        ScalarType::Unit => 20,
    }
}

/// Return every ScalarType variant by index, ensuring exhaustiveness.
fn scalar_from_index(index: usize) -> Option<ScalarType> {
    let scalar = match index {
        0 => ScalarType::Account,
        1 => ScalarType::Blob,
        2 => ScalarType::Bool,
        3 => ScalarType::Date,
        4 => ScalarType::Decimal,
        5 => ScalarType::Duration,
        6 => ScalarType::Enum,
        7 => ScalarType::Float32,
        8 => ScalarType::Float64,
        9 => ScalarType::Int,
        10 => ScalarType::Int128,
        11 => ScalarType::IntBig,
        12 => ScalarType::Principal,
        13 => ScalarType::Subaccount,
        14 => ScalarType::Text,
        15 => ScalarType::Timestamp,
        16 => ScalarType::Uint,
        17 => ScalarType::Uint128,
        18 => ScalarType::UintBig,
        19 => ScalarType::Ulid,
        20 => ScalarType::Unit,
        _ => return None,
    };

    Some(scalar)
}

/// Build a representative value for each scalar variant.
fn sample_value_for_scalar(scalar: ScalarType) -> Value {
    match scalar {
        ScalarType::Account => Value::Account(Account::dummy(1)),
        ScalarType::Blob => Value::Blob(vec![0u8, 1u8]),
        ScalarType::Bool => Value::Bool(true),
        ScalarType::Date => Value::Date(Date::EPOCH),
        ScalarType::Decimal => Value::Decimal(Decimal::ZERO),
        ScalarType::Duration => Value::Duration(Duration::ZERO),
        ScalarType::Enum => Value::Enum(ValueEnum::loose("example")),
        ScalarType::Float32 => {
            Value::Float32(Float32::try_new(0.0).expect("Float32 sample should be finite"))
        }
        ScalarType::Float64 => {
            Value::Float64(Float64::try_new(0.0).expect("Float64 sample should be finite"))
        }
        ScalarType::Int => Value::Int(0),
        ScalarType::Int128 => Value::Int128(Int128::from(0i128)),
        ScalarType::IntBig => Value::IntBig(Int::from(0i32)),
        ScalarType::Principal => Value::Principal(Principal::anonymous()),
        ScalarType::Subaccount => Value::Subaccount(Subaccount::dummy(2)),
        ScalarType::Text => Value::Text("text".to_string()),
        ScalarType::Timestamp => Value::Timestamp(Timestamp::EPOCH),
        ScalarType::Uint => Value::Uint(0),
        ScalarType::Uint128 => Value::Uint128(Nat128::from(0u128)),
        ScalarType::UintBig => Value::UintBig(Nat::from(0u64)),
        ScalarType::Ulid => Value::Ulid(Ulid::nil()),
        ScalarType::Unit => Value::Unit,
    }
}

fn field(name: &'static str, kind: FieldKind) -> FieldModel {
    FieldModel { name, kind }
}

crate::test_entity! {
    ident = ScalarPredicateEntity,
    id = Ulid,
    entity_name = "ScalarEntity",
    primary_key = "id",
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        ("email", FieldKind::Text),
        ("age", FieldKind::Uint),
        ("created_at", FieldKind::Timestamp),
        ("active", FieldKind::Bool),
    ],
    indexes = [],
}

crate::test_entity! {
    ident = CollectionPredicateEntity,
    id = Ulid,
    entity_name = "CollectionEntity",
    primary_key = "id",
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        ("tags", FieldKind::List(&FieldKind::Text)),
        ("principals", FieldKind::Set(&FieldKind::Principal)),
        (
            "attributes",
            FieldKind::Map {
                key: &FieldKind::Text,
                value: &FieldKind::Uint,
            }
        ),
    ],
    indexes = [],
}

crate::test_entity! {
    ident = NumericCoercionPredicateEntity,
    id = Ulid,
    entity_name = "NumericCoercionEntity",
    primary_key = "id",
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        ("date", FieldKind::Date),
        ("int_big", FieldKind::IntBig),
        ("uint_big", FieldKind::UintBig),
        ("int_small", FieldKind::Int),
        ("uint_small", FieldKind::Uint),
        ("decimal", FieldKind::Decimal { scale: 18 }),
        ("decimal_scaled", FieldKind::Decimal { scale: 8 }),
    ],
    indexes = [],
}

#[test]
fn validate_model_accepts_scalars_and_coercions() {
    let model = <ScalarPredicateEntity as EntitySchema>::MODEL;

    let predicate = Predicate::And(vec![
        FieldRef::new("id").eq(Ulid::nil()),
        FieldRef::new("email").text_eq_ci("User@example.com"),
        FieldRef::new("age").lt(30u32),
    ]);

    assert!(validate_model(model, &predicate).is_ok());
}

#[test]
fn validate_model_accepts_deterministic_set_predicates() {
    let model = <CollectionPredicateEntity as EntitySchema>::MODEL;

    let predicate = Predicate::Compare(ComparePredicate::with_coercion(
        "principals",
        CompareOp::Contains,
        Principal::anonymous().to_value(),
        CoercionId::Strict,
    ));

    assert!(validate_model(model, &predicate).is_ok());
}

#[test]
fn validate_model_rejects_non_queryable_fields() {
    let model = InvalidEntityModelBuilder::from_fields(
        vec![
            field("id", FieldKind::Ulid),
            field("broken", FieldKind::Structured { queryable: false }),
        ],
        0,
    );

    let predicate = FieldRef::new("broken").eq(1u64);

    assert!(matches!(
        validate_model(&model, &predicate),
        Err(ValidateError::NonQueryableFieldType { field }) if field == "broken"
    ));
}

#[test]
fn validate_model_accepts_text_contains() {
    let model = <ScalarPredicateEntity as EntitySchema>::MODEL;

    let predicate = FieldRef::new("email").text_contains("example");
    assert!(validate_model(model, &predicate).is_ok());

    let predicate = FieldRef::new("email").text_contains_ci("EXAMPLE");
    assert!(validate_model(model, &predicate).is_ok());
}

#[test]
fn validate_model_rejects_text_contains_on_non_text() {
    let model = <ScalarPredicateEntity as EntitySchema>::MODEL;

    let predicate = FieldRef::new("age").text_contains("1");
    assert!(matches!(
        validate_model(model, &predicate),
        Err(ValidateError::InvalidOperator { field, op })
            if field == "age" && op == "text_contains"
    ));
}

#[test]
fn validate_model_rejects_numeric_widen_for_registry_exclusions() {
    let model = <NumericCoercionPredicateEntity as EntitySchema>::MODEL;

    let date_pred = FieldRef::new("date").lt(1i64);
    assert!(matches!(
        validate_model(model, &date_pred),
        Err(ValidateError::InvalidCoercion { field, coercion })
            if field == "date" && coercion == CoercionId::NumericWiden
    ));

    let int_big_pred = FieldRef::new("int_big").lt(Int::from(1i32));
    assert!(matches!(
        validate_model(model, &int_big_pred),
        Err(ValidateError::InvalidCoercion { field, coercion })
            if field == "int_big" && coercion == CoercionId::NumericWiden
    ));

    let uint_big_pred = FieldRef::new("uint_big").lt(Nat::from(1u64));
    assert!(matches!(
        validate_model(model, &uint_big_pred),
        Err(ValidateError::InvalidCoercion { field, coercion })
            if field == "uint_big" && coercion == CoercionId::NumericWiden
    ));
}

#[test]
fn validate_model_accepts_numeric_widen_for_registry_allowed_scalars() {
    let model = <NumericCoercionPredicateEntity as EntitySchema>::MODEL;
    let predicate = Predicate::And(vec![
        FieldRef::new("int_small").lt(9u64),
        FieldRef::new("uint_small").lt(9i64),
        FieldRef::new("decimal").lt(9u64),
        FieldRef::new("decimal_scaled").lt(9u64),
    ]);

    assert!(validate_model(model, &predicate).is_ok());
}

#[test]
fn numeric_widen_authority_tracks_registry_flags() {
    for scalar in registry_scalars() {
        let field_type = FieldType::Scalar(scalar.clone());
        let literal = sample_value_for_scalar(scalar.clone());
        let expected = scalar.supports_numeric_coercion();
        let actual = ensure_coercion(
            "value",
            &field_type,
            &literal,
            &CoercionSpec::new(CoercionId::NumericWiden),
        )
        .is_ok();

        assert_eq!(
            actual, expected,
            "numeric widen drift for scalar {scalar:?}: expected {expected}, got {actual}"
        );
    }
}

#[test]
fn numeric_widen_is_not_inferred_from_coercion_family() {
    let mut numeric_family_with_no_numeric_widen = 0usize;

    for scalar in registry_scalars() {
        if scalar.coercion_family() != CoercionFamily::Numeric {
            continue;
        }

        let field_type = FieldType::Scalar(scalar.clone());
        let literal = sample_value_for_scalar(scalar.clone());
        let numeric_widen_allowed = ensure_coercion(
            "value",
            &field_type,
            &literal,
            &CoercionSpec::new(CoercionId::NumericWiden),
        )
        .is_ok();

        assert_eq!(
            numeric_widen_allowed,
            scalar.supports_numeric_coercion(),
            "numeric family must not imply numeric widen for scalar {scalar:?}"
        );

        if !scalar.supports_numeric_coercion() {
            numeric_family_with_no_numeric_widen =
                numeric_family_with_no_numeric_widen.saturating_add(1);
        }
    }

    assert!(
        numeric_family_with_no_numeric_widen > 0,
        "expected at least one numeric-family scalar without numeric widen support"
    );
}

#[test]
fn scalar_registry_covers_all_variants_exactly_once() {
    let scalars = registry_scalars();
    let mut names = BTreeSet::new();
    let mut seen = [false; SCALAR_TYPE_VARIANT_COUNT];

    for scalar in scalars {
        let index = scalar_index(scalar.clone());
        assert!(!seen[index], "duplicate scalar entry: {scalar:?}");
        seen[index] = true;

        let name = format!("{scalar:?}");
        assert!(names.insert(name.clone()), "duplicate scalar entry: {name}");
    }

    let mut missing = Vec::new();
    for (index, was_seen) in seen.iter().enumerate() {
        if !*was_seen {
            let scalar = scalar_from_index(index).expect("index is in range");
            missing.push(format!("{scalar:?}"));
        }
    }

    assert!(missing.is_empty(), "missing scalar entries: {missing:?}");
    assert_eq!(names.len(), SCALAR_TYPE_VARIANT_COUNT);
}

#[test]
fn scalar_keyability_matches_value_storage_key() {
    for scalar in registry_scalars() {
        let value = sample_value_for_scalar(scalar.clone());
        let scalar_keyable = scalar.is_keyable();
        let value_keyable = value.as_storage_key().is_some();

        assert_eq!(
            value_keyable, scalar_keyable,
            "Value::as_storage_key drift for scalar {scalar:?}"
        );
    }
}
