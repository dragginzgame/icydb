use crate::{
    db::query::{
        expr::{FilterExpr, FilterValue},
        plan::expr::{BinaryOp, Expr, FieldId},
    },
    model::{EntityModel, field::FieldKind, field::FieldModel},
    types::Ulid,
    value::Value,
};
use candid::types::{CandidType, Label, Type, TypeInner};

static FILTER_TEST_FIELDS: [FieldModel; 3] = [
    FieldModel::generated("id", FieldKind::Ulid),
    FieldModel::generated("rank", FieldKind::Nat64),
    FieldModel::generated("active", FieldKind::Bool),
];
static FILTER_TEST_MODEL: EntityModel = EntityModel::generated(
    "tests::FilterEntity",
    "FilterEntity",
    1,
    &FILTER_TEST_FIELDS[0],
    0,
    &FILTER_TEST_FIELDS,
    &[],
);

fn expect_record_fields(ty: Type) -> Vec<String> {
    match ty.as_ref() {
        TypeInner::Record(fields) => fields
            .iter()
            .map(|field| match field.id.as_ref() {
                Label::Named(name) => name.clone(),
                other => panic!("expected named record field, got {other:?}"),
            })
            .collect(),
        other => panic!("expected candid record, got {other:?}"),
    }
}

fn expect_variant_labels(ty: Type) -> Vec<String> {
    match ty.as_ref() {
        TypeInner::Variant(fields) => fields
            .iter()
            .map(|field| match field.id.as_ref() {
                Label::Named(name) => name.clone(),
                other => panic!("expected named variant label, got {other:?}"),
            })
            .collect(),
        other => panic!("expected candid variant, got {other:?}"),
    }
}

fn expect_variant_field_type(ty: Type, variant_name: &str) -> Type {
    match ty.as_ref() {
        TypeInner::Variant(fields) => fields
            .iter()
            .find_map(|field| match field.id.as_ref() {
                Label::Named(name) if name == variant_name => Some(field.ty.clone()),
                _ => None,
            })
            .unwrap_or_else(|| panic!("expected variant label `{variant_name}`")),
        other => panic!("expected candid variant, got {other:?}"),
    }
}

#[test]
fn filter_expr_eq_candid_payload_shape_is_stable() {
    let fields = expect_record_fields(expect_variant_field_type(FilterExpr::ty(), "Eq"));

    for field in ["field", "value"] {
        assert!(
            fields.iter().any(|candidate| candidate == field),
            "Eq payload must keep `{field}` field key in Candid shape",
        );
    }
}

#[test]
fn filter_value_variant_labels_are_stable() {
    let labels = expect_variant_labels(FilterValue::ty());

    for label in ["String", "Bool", "Null", "List"] {
        assert!(
            labels.iter().any(|candidate| candidate == label),
            "FilterValue must keep `{label}` variant label",
        );
    }
}

#[test]
fn filter_expr_and_candid_payload_shape_is_stable() {
    match expect_variant_field_type(FilterExpr::ty(), "And").as_ref() {
        TypeInner::Vec(_) => {}
        other => panic!("And payload must remain a Candid vec payload, got {other:?}"),
    }
}

#[test]
fn filter_expr_text_contains_ci_candid_payload_shape_is_stable() {
    let fields = expect_record_fields(expect_variant_field_type(
        FilterExpr::ty(),
        "TextContainsCi",
    ));

    for field in ["field", "value"] {
        assert!(
            fields.iter().any(|candidate| candidate == field),
            "TextContainsCi payload must keep `{field}` field key",
        );
    }
}

#[test]
fn filter_expr_not_payload_shape_is_stable() {
    match expect_variant_field_type(FilterExpr::ty(), "Not").as_ref() {
        TypeInner::Var(_) | TypeInner::Knot(_) | TypeInner::Variant(_) => {}
        other => panic!("Not payload must keep nested predicate payload, got {other:?}"),
    }
}

#[test]
fn filter_expr_variant_labels_are_stable() {
    let labels = expect_variant_labels(FilterExpr::ty());

    for label in ["Eq", "And", "Not", "TextContainsCi", "IsMissing"] {
        assert!(
            labels.iter().any(|candidate| candidate == label),
            "FilterExpr must keep `{label}` variant label",
        );
    }
}

#[test]
fn query_expr_fixture_constructors_stay_usable() {
    let expr = FilterExpr::and(vec![
        FilterExpr::is_null("deleted_at"),
        FilterExpr::not(FilterExpr::is_missing("name")),
    ]);

    match expr {
        FilterExpr::And(items) => assert_eq!(items.len(), 2),
        other => panic!("expected And fixture, got {other:?}"),
    }
}

#[test]
fn filter_expr_model_lowering_rehydrates_string_ulid_literal() {
    let ulid = Ulid::nil();
    let expr = FilterExpr::eq("id", ulid.to_string()).lower_bool_expr_for_model(&FILTER_TEST_MODEL);

    assert_eq!(
        expr,
        Expr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(Expr::Field(FieldId::new("id".to_string()))),
            right: Box::new(Expr::Literal(Value::Ulid(ulid))),
        }
    );
}

#[test]
fn filter_expr_model_lowering_rehydrates_numeric_membership_literals() {
    let expr =
        FilterExpr::in_list("rank", [7_u64, 9_u64]).lower_bool_expr_for_model(&FILTER_TEST_MODEL);

    assert_eq!(
        expr,
        Expr::Binary {
            op: BinaryOp::Or,
            left: Box::new(Expr::Binary {
                op: BinaryOp::Eq,
                left: Box::new(Expr::Field(FieldId::new("rank".to_string()))),
                right: Box::new(Expr::Literal(Value::Nat64(7))),
            }),
            right: Box::new(Expr::Binary {
                op: BinaryOp::Eq,
                left: Box::new(Expr::Field(FieldId::new("rank".to_string()))),
                right: Box::new(Expr::Literal(Value::Nat64(9))),
            }),
        }
    );
}
