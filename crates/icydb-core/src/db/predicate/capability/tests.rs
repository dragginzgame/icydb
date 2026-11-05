use super::{
    IndexCompileTarget, IndexCompileTargetKind, classify_index_compare_target,
    lower_index_compare_literal_for_kind, lower_index_starts_with_prefix_for_target,
    value_is_index_literal,
};
use crate::{
    db::{
        predicate::{CoercionId, CoercionSpec, CompareOp, ExecutableComparePredicate},
        schema::PersistedIndexExpressionOp,
    },
    value::Value,
};
use std::borrow::Cow;

fn expression_target(op: PersistedIndexExpressionOp) -> IndexCompileTarget {
    IndexCompileTarget {
        component_index: 0,
        field_slot: 0,
        kind: IndexCompileTargetKind::Expression(op),
    }
}

#[test]
fn target_classification_agrees_with_canonical_literal_lowering() {
    let field = IndexCompileTarget {
        kind: IndexCompileTargetKind::Field,
        ..expression_target(PersistedIndexExpressionOp::Lower)
    };
    let expressions = [
        PersistedIndexExpressionOp::Lower,
        PersistedIndexExpressionOp::Upper,
        PersistedIndexExpressionOp::Trim,
        PersistedIndexExpressionOp::LowerTrim,
        PersistedIndexExpressionOp::Date,
        PersistedIndexExpressionOp::Year,
        PersistedIndexExpressionOp::Month,
        PersistedIndexExpressionOp::Day,
    ];
    let values = [
        Value::Text(String::new()),
        Value::Text(" \u{2003}İΣß\0 ".repeat(128)),
        Value::Null,
        Value::Unit,
        Value::Nat64(1),
        Value::List(vec![]),
        Value::List(vec![Value::Text(String::new()), Value::Text("ÄBC".into())]),
        Value::List(vec![Value::Text("ÄBC".into()), Value::Null]),
        Value::List(vec![Value::Text("ÄBC".into()), Value::Nat64(1)]),
        Value::List(vec![Value::List(vec![Value::Text("ÄBC".into())])]),
    ];
    for target in std::iter::once(field).chain(expressions.map(expression_target)) {
        for coercion in [
            CoercionId::Strict,
            CoercionId::TextCasefold,
            CoercionId::NumericWiden,
            CoercionId::CollectionElement,
        ] {
            let scalar_supported = |value| {
                lower_index_compare_literal_for_kind(target.kind, value, coercion)
                    .is_some_and(|value| value_is_index_literal(&value))
            };
            for op in [
                CompareOp::Eq,
                CompareOp::Ne,
                CompareOp::Lt,
                CompareOp::Lte,
                CompareOp::Gt,
                CompareOp::Gte,
                CompareOp::In,
                CompareOp::NotIn,
                CompareOp::StartsWith,
                CompareOp::EndsWith,
                CompareOp::Contains,
            ] {
                for value in &values {
                    let expected = match op {
                        CompareOp::Eq
                        | CompareOp::Ne
                        | CompareOp::Lt
                        | CompareOp::Lte
                        | CompareOp::Gt
                        | CompareOp::Gte => scalar_supported(value),
                        CompareOp::In | CompareOp::NotIn => match value {
                            Value::List(items) => {
                                !items.is_empty() && items.iter().all(scalar_supported)
                            }
                            _ => false,
                        },
                        CompareOp::StartsWith => {
                            lower_index_starts_with_prefix_for_target(target, value, coercion)
                                .is_some()
                        }
                        CompareOp::EndsWith | CompareOp::Contains => false,
                    };
                    let cmp = ExecutableComparePredicate::field_literal(
                        Some(target.field_slot),
                        op,
                        value.clone(),
                        CoercionSpec::new(coercion),
                    );
                    assert_eq!(
                        classify_index_compare_target(&cmp, &[target]),
                        expected.then_some(target),
                        "target={target:?}, coercion={coercion:?}, op={op:?}",
                    );
                }
            }
        }
    }
}

#[test]
fn text_casefold_literals_only_lower_through_matching_expression_keys() {
    let literal = Value::Text("ßeta".to_string());

    assert_eq!(
        lower_index_compare_literal_for_kind(
            expression_target(PersistedIndexExpressionOp::Lower).kind,
            &literal,
            CoercionId::TextCasefold,
        )
        .as_deref(),
        Some(&literal),
    );
    assert_eq!(
        lower_index_compare_literal_for_kind(
            expression_target(PersistedIndexExpressionOp::Upper).kind,
            &literal,
            CoercionId::TextCasefold,
        ),
        None,
    );
}

#[test]
fn identity_target_lowering_borrows_operands_and_prefixes() {
    let target = IndexCompileTarget {
        component_index: 0,
        field_slot: 0,
        kind: IndexCompileTargetKind::Field,
    };
    let values = [
        Value::Text("x".repeat(4096)),
        Value::Null,
        Value::List(vec![Value::Text("member".into())]),
    ];
    for value in &values {
        let lowered =
            lower_index_compare_literal_for_kind(target.kind, value, CoercionId::Strict).unwrap();
        let Cow::Borrowed(borrowed) = lowered else {
            panic!("identity lowering must borrow");
        };
        assert!(std::ptr::eq(borrowed, value));
        for coercion in [
            CoercionId::NumericWiden,
            CoercionId::TextCasefold,
            CoercionId::CollectionElement,
        ] {
            assert!(lower_index_compare_literal_for_kind(target.kind, value, coercion).is_none());
        }
    }
    let Value::Text(text) = &values[0] else {
        unreachable!();
    };
    let prefix =
        lower_index_starts_with_prefix_for_target(target, &values[0], CoercionId::Strict).unwrap();
    let Cow::Borrowed(prefix) = prefix else {
        panic!("identity prefix must borrow");
    };
    assert!(std::ptr::eq(prefix, text.as_str()));
    for value in [Value::Text(String::new()), Value::Nat64(1), Value::Null] {
        assert!(
            lower_index_starts_with_prefix_for_target(target, &value, CoercionId::Strict).is_none()
        );
    }
}

#[test]
fn expression_target_lowering_keeps_derived_values_owned() {
    let target = expression_target(PersistedIndexExpressionOp::Lower);
    let value = Value::Text("ÄBC".into());
    let lowered =
        lower_index_compare_literal_for_kind(target.kind, &value, CoercionId::TextCasefold)
            .unwrap();
    assert!(matches!(lowered, Cow::Owned(_)));
    assert_eq!(lowered.as_ref(), &Value::Text("äbc".into()));
    let prefix =
        lower_index_starts_with_prefix_for_target(target, &value, CoercionId::TextCasefold)
            .unwrap();
    assert!(matches!(prefix, Cow::Owned(_)));
    assert_eq!(prefix, "äbc");
    assert_eq!(value, Value::Text("ÄBC".into()));
}
