//! Module: query::plan::key_item_match
//! Responsibility: shared key-item matching/lowering rules used by planner and explain access-choice.
//! Does not own: index ranking policy or access-path shape construction.
//! Boundary: canonical field/expression key-item lookup compatibility and literal lowering.

#[cfg(test)]
mod admission_tests;

use crate::{
    db::{
        access::{SemanticIndexExpression, SemanticIndexKeyItemRef},
        predicate::{
            CoercionId, IndexCompileTargetKind, admit_index_compare_literal_for_kind,
            lower_index_compare_literal_for_kind,
        },
        query::construction::ConstructionBudget,
    },
    error::InternalError,
    value::Value,
};
use std::borrow::Cow;

/// Return whether one key-item can match a predicate field/coercion pair.
#[must_use]
pub(in crate::db::query::plan) fn key_item_matches_field_and_coercion<'a>(
    key_item: impl Into<SemanticIndexKeyItemRef<'a>>,
    field: &str,
    coercion: CoercionId,
) -> bool {
    match key_item.into() {
        SemanticIndexKeyItemRef::Field(key_field) => {
            key_field == field && coercion == CoercionId::Strict
        }
        SemanticIndexKeyItemRef::AcceptedExpression(expression) => {
            expression.field() == field
                && accepted_expression_supports_lookup_coercion(expression, coercion)
        }
    }
}

const fn accepted_expression_supports_lookup_coercion(
    expression: &SemanticIndexExpression,
    coercion: CoercionId,
) -> bool {
    match coercion {
        CoercionId::TextCasefold => expression.supports_text_casefold_lookup(),
        CoercionId::Strict | CoercionId::NumericWiden | CoercionId::CollectionElement => false,
    }
}

/// Check lookup eligibility without copying or transforming the literal.
#[must_use]
pub(in crate::db::query::plan) fn key_item_supports_lookup_value<'a>(
    key_item: impl Into<SemanticIndexKeyItemRef<'a>>,
    field: &str,
    value: &Value,
    coercion: CoercionId,
    literal_compatible: bool,
) -> bool {
    let key_item = key_item.into();
    literal_compatible
        && key_item_matches_field_and_coercion(key_item, field, coercion)
        // LOWER is the only admitted expression lookup. It accepts every text
        // value; deciding eligibility never needs the transformed output.
        && (!key_item.is_expression() || matches!(value, Value::Text(_)))
}

/// Check prefix eligibility without constructing the canonical prefix.
#[must_use]
pub(in crate::db::query::plan) fn key_item_supports_starts_with_value<'a>(
    key_item: impl Into<SemanticIndexKeyItemRef<'a>>,
    field: &str,
    value: &Value,
    coercion: CoercionId,
    literal_compatible: bool,
) -> bool {
    // Identity and LOWER both preserve emptiness, including Unicode text.
    matches!(value, Value::Text(prefix) if !prefix.is_empty())
        && key_item_supports_lookup_value(key_item, field, value, coercion, literal_compatible)
}

/// Try to lower one predicate literal into a canonical key-item lookup value.
#[cfg(test)]
#[must_use]
pub(in crate::db::query::plan) fn eq_lookup_value_for_key_item<'a>(
    key_item: impl Into<SemanticIndexKeyItemRef<'a>>,
    field: &str,
    value: &Value,
    coercion: CoercionId,
    literal_compatible: bool,
) -> Option<Value> {
    let kind = lookup_kind_for_key_item(key_item, field, value, coercion, literal_compatible)?;
    lower_index_compare_literal_for_kind(kind, value, coercion).map(Cow::into_owned)
}

/// Construct one retained lookup operand using the caller's existing budget.
/// Identity results are copied once; admitted expression results move directly.
pub(in crate::db::query::plan) fn copy_lookup_value_for_key_item<'a>(
    key_item: impl Into<SemanticIndexKeyItemRef<'a>>,
    field: &str,
    value: &Value,
    coercion: CoercionId,
    literal_compatible: bool,
    budget: &dyn ConstructionBudget,
) -> Result<Option<Value>, InternalError> {
    match lower_lookup_value_for_key_item(
        key_item,
        field,
        value,
        coercion,
        literal_compatible,
        budget,
    )? {
        Some(Cow::Borrowed(value)) => budget.copy_value(value).map(Some),
        Some(Cow::Owned(value)) => Ok(Some(value)),
        None => Ok(None),
    }
}

/// Admit expression conversion while borrowing unchanged literals for comparison.
/// Consumers own admission of any retained copy of a borrowed result.
pub(in crate::db::query::plan) fn lower_lookup_value_for_key_item<'key, 'value>(
    key_item: impl Into<SemanticIndexKeyItemRef<'key>>,
    field: &str,
    value: &'value Value,
    coercion: CoercionId,
    literal_compatible: bool,
    budget: &dyn ConstructionBudget,
) -> Result<Option<Cow<'value, Value>>, InternalError> {
    let Some(kind) = lookup_kind_for_key_item(key_item, field, value, coercion, literal_compatible)
    else {
        return Ok(None);
    };
    admit_index_compare_literal_for_kind(kind, value, coercion, budget)?;
    Ok(lower_index_compare_literal_for_kind(kind, value, coercion))
}

// Field/literal eligibility stays planner-owned; the resulting kind is enough
// for shared conversion, without fabricating component indexes or field slots.
fn lookup_kind_for_key_item<'a>(
    key_item: impl Into<SemanticIndexKeyItemRef<'a>>,
    field: &str,
    value: &Value,
    coercion: CoercionId,
    literal_compatible: bool,
) -> Option<IndexCompileTargetKind> {
    let key_item = key_item.into();
    if !key_item_supports_lookup_value(key_item, field, value, coercion, literal_compatible) {
        return None;
    }

    Some(match key_item {
        SemanticIndexKeyItemRef::Field(_) => IndexCompileTargetKind::Field,
        SemanticIndexKeyItemRef::AcceptedExpression(expression) => {
            IndexCompileTargetKind::Expression(expression.op())
        }
    })
}

#[cfg(test)]
mod tests {
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
                                    SemanticIndexKeyItemRef::Field(_) => {
                                        coercion == CoercionId::Strict
                                    }
                                    SemanticIndexKeyItemRef::AcceptedExpression(expression) => {
                                        expression.op() == Lower
                                            && coercion == CoercionId::TextCasefold
                                            && matches!(value, Value::Text(_))
                                    }
                                };
                            assert_eq!(
                                key_item_supports_lookup_value(
                                    key, field, value, coercion, compatible,
                                ),
                                supported
                            );
                            assert_eq!(
                                eq_lookup_value_for_key_item(
                                    key, field, value, coercion, compatible,
                                )
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
                eq_lookup_value_for_key_item(
                    key_item,
                    "name",
                    &value,
                    CoercionId::TextCasefold,
                    true
                ),
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
                eq_lookup_value_for_key_item(
                    key_item,
                    "name",
                    &value,
                    CoercionId::TextCasefold,
                    true
                )
                .is_none()
            );
        }
    }

    #[test]
    fn upper_expression_keys_do_not_claim_text_casefold_lookup_compatibility() {
        let upper =
            SemanticIndexExpression::new(PersistedIndexExpressionOp::Upper, "name".to_string());
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
}
