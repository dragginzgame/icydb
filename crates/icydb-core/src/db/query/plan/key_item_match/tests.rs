use super::{
    eq_lookup_value_for_key_item, key_item_matches_field_and_coercion,
    key_item_supports_lookup_value, key_item_supports_starts_with_value,
};
use crate::{
    db::{
        access::{SemanticIndexExpression, SemanticIndexKeyItemRef},
        predicate::CoercionId,
        schema::PersistedIndexExpressionOp,
    },
    value::Value,
};

#[test]
fn lookup_capability_matches_materialization_for_fields_and_all_expression_kinds() {
    use PersistedIndexExpressionOp::{Date, Day, Lower, LowerTrim, Month, Trim, Upper, Year};

    let expressions = [Lower, Upper, Trim, LowerTrim, Date, Year, Month, Day]
        .map(|op| SemanticIndexExpression::new(op, "name".into()));
    let keys = std::iter::once(SemanticIndexKeyItemRef::Field("name")).chain(
        expressions
            .iter()
            .map(SemanticIndexKeyItemRef::AcceptedExpression),
    );
    let values = [
        Value::Null,
        Value::Nat64(1),
        Value::List(vec![Value::Text("A".into())]),
        Value::Text(String::new()),
        Value::Text("ASCII".into()),
        Value::Text("İΣ".into()),
        Value::Text(" \0 ".into()),
        Value::Text("İ".repeat(2048)),
    ];
    for key in keys {
        for field in ["name", "other"] {
            for coercion in [
                CoercionId::Strict,
                CoercionId::TextCasefold,
                CoercionId::NumericWiden,
                CoercionId::CollectionElement,
            ] {
                for compatible in [false, true] {
                    for value in &values {
                        let supported = compatible
                            && field == "name"
                            && match key {
                                SemanticIndexKeyItemRef::Field(_) => coercion == CoercionId::Strict,
                                SemanticIndexKeyItemRef::AcceptedExpression(expression) => {
                                    expression.op() == Lower
                                        && coercion == CoercionId::TextCasefold
                                        && matches!(value, Value::Text(_))
                                }
                            };
                        assert_eq!(
                            key_item_supports_lookup_value(key, field, value, coercion, compatible,),
                            supported
                        );
                        assert_eq!(
                            eq_lookup_value_for_key_item(key, field, value, coercion, compatible,)
                                .is_some(),
                            supported
                        );

                        let prefix_supported =
                            supported && matches!(value, Value::Text(text) if !text.is_empty());
                        assert_eq!(
                            key_item_supports_starts_with_value(
                                key, field, value, coercion, compatible,
                            ),
                            prefix_supported
                        );
                    }
                }
            }
        }
    }
}

#[test]
fn planner_expression_lookups_preserve_unicode_values_and_admission_gates() {
    let lower = SemanticIndexExpression::new(PersistedIndexExpressionOp::Lower, "name".into());
    let key_item = SemanticIndexKeyItemRef::AcceptedExpression(&lower);
    for (source, expected) in [
        ("", ""),
        ("ASCII", "ascii"),
        ("İΣ", "i\u{307}ς"),
        ("ΣΑ", "σα"),
        (" Straße\0 ", " straße\0 "),
    ] {
        let value = Value::Text(source.into());
        assert_eq!(
            eq_lookup_value_for_key_item(key_item, "name", &value, CoercionId::TextCasefold, true),
            Some(Value::Text(expected.into())),
        );
        assert_eq!(value, Value::Text(source.into()));
        for (field, coercion, compatible) in [
            ("other", CoercionId::TextCasefold, true),
            ("name", CoercionId::Strict, true),
            ("name", CoercionId::TextCasefold, false),
        ] {
            assert!(
                eq_lookup_value_for_key_item(key_item, field, &value, coercion, compatible)
                    .is_none()
            );
        }
    }
    for value in [
        Value::Null,
        Value::Nat64(1),
        Value::List(vec![Value::Text("A".into())]),
    ] {
        assert!(
            eq_lookup_value_for_key_item(key_item, "name", &value, CoercionId::TextCasefold, true)
                .is_none()
        );
    }
}

#[test]
fn upper_expression_keys_do_not_claim_text_casefold_lookup_compatibility() {
    let upper = SemanticIndexExpression::new(PersistedIndexExpressionOp::Upper, "name".to_string());
    let key_item = SemanticIndexKeyItemRef::AcceptedExpression(&upper);

    assert!(!key_item_matches_field_and_coercion(
        key_item,
        "name",
        CoercionId::TextCasefold,
    ));
    assert_eq!(
        eq_lookup_value_for_key_item(
            key_item,
            "name",
            &Value::Text("ßeta".to_string()),
            CoercionId::TextCasefold,
            true,
        ),
        None,
    );
}
