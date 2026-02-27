use super::normalize_enum_literals;
use crate::{
    db::contracts::{
        CoercionId, CompareOp, ComparePredicate, Predicate, SchemaInfo, ValidateError,
    },
    model::field::{FieldKind, FieldModel},
    testing::entity_model_from_static,
    types::Ulid,
    value::{Value, ValueEnum},
};

static ENUM_FIELDS: [FieldModel; 2] = [
    FieldModel {
        name: "id",
        kind: FieldKind::Ulid,
    },
    FieldModel {
        name: "stage",
        kind: FieldKind::Enum {
            path: "tests::Stage",
        },
    },
];
static ENUM_INDEXES: [&crate::model::index::IndexModel; 0] = [];
static ENUM_MODEL: crate::model::entity::EntityModel = entity_model_from_static(
    "tests::EnumEntity",
    "EnumEntity",
    &ENUM_FIELDS[0],
    &ENUM_FIELDS,
    &ENUM_INDEXES,
);
static MULTI_ENUM_FIELDS: [FieldModel; 3] = [
    FieldModel {
        name: "id",
        kind: FieldKind::Ulid,
    },
    FieldModel {
        name: "stage",
        kind: FieldKind::Enum {
            path: "tests::Stage",
        },
    },
    FieldModel {
        name: "status",
        kind: FieldKind::Enum {
            path: "tests::Status",
        },
    },
];
static MULTI_ENUM_INDEXES: [&crate::model::index::IndexModel; 0] = [];
static MULTI_ENUM_MODEL: crate::model::entity::EntityModel = entity_model_from_static(
    "tests::MultiEnumEntity",
    "MultiEnumEntity",
    &MULTI_ENUM_FIELDS[0],
    &MULTI_ENUM_FIELDS,
    &MULTI_ENUM_INDEXES,
);
static SET_FIELDS: [FieldModel; 3] = [
    FieldModel {
        name: "id",
        kind: FieldKind::Ulid,
    },
    FieldModel {
        name: "tags_set",
        kind: FieldKind::Set(&FieldKind::Text),
    },
    FieldModel {
        name: "tags_list",
        kind: FieldKind::List(&FieldKind::Text),
    },
];
static SET_INDEXES: [&crate::model::index::IndexModel; 0] = [];
static SET_MODEL: crate::model::entity::EntityModel = entity_model_from_static(
    "tests::SetEntity",
    "SetEntity",
    &SET_FIELDS[0],
    &SET_FIELDS,
    &SET_INDEXES,
);

fn schema() -> SchemaInfo {
    SchemaInfo::from_entity_model(&ENUM_MODEL).expect("enum test schema should be valid")
}

fn multi_enum_schema() -> SchemaInfo {
    SchemaInfo::from_entity_model(&MULTI_ENUM_MODEL)
        .expect("multi-enum test schema should be valid")
}

fn set_schema() -> SchemaInfo {
    SchemaInfo::from_entity_model(&SET_MODEL).expect("set test schema should be valid")
}

fn eq(value: Value) -> Predicate {
    Predicate::Compare(ComparePredicate::with_coercion(
        "stage",
        CompareOp::Eq,
        value,
        CoercionId::Strict,
    ))
}

#[test]
fn strict_filter_matches_strict_enum() {
    let predicate = eq(Value::Enum(ValueEnum::new("Active", Some("tests::Stage"))));
    let normalized = normalize_enum_literals(&schema(), &predicate).expect("strict enum");
    assert_eq!(normalized, predicate);
}

#[test]
fn loose_filter_resolves_enum_path() {
    let predicate = eq(Value::Enum(ValueEnum::loose("Active")));
    let normalized = normalize_enum_literals(&schema(), &predicate).expect("loose enum");
    assert_eq!(
        normalized,
        eq(Value::Enum(ValueEnum::new("Active", Some("tests::Stage"))))
    );
}

#[test]
fn strict_filter_with_wrong_path_fails() {
    let predicate = eq(Value::Enum(ValueEnum::new("Active", Some("wrong::Path"))));
    let err = normalize_enum_literals(&schema(), &predicate).expect_err("wrong enum path");
    assert!(matches!(err, ValidateError::InvalidLiteral { field, .. } if field == "stage"));
}

#[test]
fn stage_in_filter_resolves_loose_values() {
    let predicate = Predicate::Compare(ComparePredicate::with_coercion(
        "stage",
        CompareOp::In,
        Value::List(vec![
            Value::Enum(ValueEnum::loose("Draft")),
            Value::Enum(ValueEnum::new("Active", Some("tests::Stage"))),
        ]),
        CoercionId::Strict,
    ));

    let normalized = normalize_enum_literals(&schema(), &predicate).expect("enum list");
    let expected = Predicate::Compare(ComparePredicate::with_coercion(
        "stage",
        CompareOp::In,
        Value::List(vec![
            Value::Enum(ValueEnum::new("Draft", Some("tests::Stage"))),
            Value::Enum(ValueEnum::new("Active", Some("tests::Stage"))),
        ]),
        CoercionId::Strict,
    ));

    assert_eq!(normalized, expected);
}

#[test]
fn unknown_fields_are_left_for_schema_validation() {
    let predicate = Predicate::Compare(ComparePredicate::with_coercion(
        "unknown",
        CompareOp::Eq,
        Value::Ulid(Ulid::nil()),
        CoercionId::Strict,
    ));
    let normalized = normalize_enum_literals(&schema(), &predicate).expect("unknown field");
    assert_eq!(normalized, predicate);
}

#[test]
fn normalization_is_idempotent() {
    let predicate = eq(Value::Enum(ValueEnum::loose("Active")));

    let once = normalize_enum_literals(&schema(), &predicate).expect("first normalize");
    let twice = normalize_enum_literals(&schema(), &once).expect("second normalize");

    assert_eq!(once, twice);
}

#[test]
fn loose_resolution_is_field_scoped_for_shared_variant_names() {
    let predicate = Predicate::And(vec![
        Predicate::Compare(ComparePredicate::with_coercion(
            "stage",
            CompareOp::Eq,
            Value::Enum(ValueEnum::loose("Active")),
            CoercionId::Strict,
        )),
        Predicate::Compare(ComparePredicate::with_coercion(
            "status",
            CompareOp::Eq,
            Value::Enum(ValueEnum::loose("Active")),
            CoercionId::Strict,
        )),
    ]);

    let normalized = normalize_enum_literals(&multi_enum_schema(), &predicate)
        .expect("field-scoped normalization");
    let Predicate::And(children) = normalized else {
        panic!("expected AND predicate");
    };
    assert_eq!(children.len(), 2);

    let Predicate::Compare(stage_cmp) = &children[0] else {
        panic!("expected first compare predicate");
    };
    let Value::Enum(stage) = &stage_cmp.value else {
        panic!("expected first enum value");
    };
    assert_eq!(stage.path.as_deref(), Some("tests::Stage"));

    let Predicate::Compare(status_cmp) = &children[1] else {
        panic!("expected second compare predicate");
    };
    let Value::Enum(status) = &status_cmp.value else {
        panic!("expected second enum value");
    };
    assert_eq!(status.path.as_deref(), Some("tests::Status"));
}

#[test]
fn set_literals_are_sorted_and_deduplicated() {
    let predicate = Predicate::Compare(ComparePredicate::with_coercion(
        "tags_set",
        CompareOp::Eq,
        Value::List(vec![
            Value::Text("b".to_string()),
            Value::Text("a".to_string()),
            Value::Text("b".to_string()),
        ]),
        CoercionId::Strict,
    ));

    let normalized = normalize_enum_literals(&set_schema(), &predicate).expect("set normalize");
    let expected = Predicate::Compare(ComparePredicate::with_coercion(
        "tags_set",
        CompareOp::Eq,
        Value::List(vec![
            Value::Text("a".to_string()),
            Value::Text("b".to_string()),
        ]),
        CoercionId::Strict,
    ));
    assert_eq!(normalized, expected);
}

#[test]
fn list_literals_preserve_original_order_and_duplicates() {
    let predicate = Predicate::Compare(ComparePredicate::with_coercion(
        "tags_list",
        CompareOp::Eq,
        Value::List(vec![
            Value::Text("b".to_string()),
            Value::Text("a".to_string()),
            Value::Text("b".to_string()),
        ]),
        CoercionId::Strict,
    ));

    let normalized = normalize_enum_literals(&set_schema(), &predicate).expect("list normalize");
    assert_eq!(normalized, predicate);
}
