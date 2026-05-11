//! Module: query::plan::key_item_match
//! Responsibility: shared key-item matching/lowering rules used by planner and explain access-choice.
//! Does not own: index ranking policy or access-path shape construction.
//! Boundary: canonical field/expression key-item lookup compatibility and literal lowering.

use crate::{
    db::{
        access::{SemanticIndexExpression, SemanticIndexKeyItemRef},
        index::derive_index_expression_value,
        predicate::CoercionId,
        scalar_expr::{
            ScalarExprValue, derive_non_null_scalar_expression_value, scalar_expr_value_into_value,
        },
        schema::PersistedIndexExpressionOp,
    },
    model::index::{IndexExpression, IndexKeyItem},
    value::Value,
};

impl From<IndexKeyItem> for SemanticIndexKeyItemRef<'_> {
    fn from(value: IndexKeyItem) -> Self {
        match value {
            IndexKeyItem::Field(field) => Self::Field(field),
            IndexKeyItem::Expression(expression) => Self::Expression(expression),
        }
    }
}

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
        SemanticIndexKeyItemRef::Expression(expression) => {
            expression.field() == field && expression_supports_lookup_coercion(expression, coercion)
        }
        SemanticIndexKeyItemRef::AcceptedExpression(expression) => {
            expression.field() == field
                && accepted_expression_supports_lookup_coercion(expression, coercion)
        }
    }
}

const fn expression_supports_lookup_coercion(
    expression: IndexExpression,
    coercion: CoercionId,
) -> bool {
    match coercion {
        CoercionId::TextCasefold => expression.supports_text_casefold_lookup(),
        CoercionId::Strict | CoercionId::NumericWiden | CoercionId::CollectionElement => false,
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

/// Try to lower one predicate literal into a canonical key-item lookup value.
#[must_use]
pub(in crate::db::query::plan) fn eq_lookup_value_for_key_item<'a>(
    key_item: impl Into<SemanticIndexKeyItemRef<'a>>,
    field: &str,
    value: &Value,
    coercion: CoercionId,
    literal_compatible: bool,
) -> Option<Value> {
    lower_lookup_value_for_key_item(key_item.into(), field, value, coercion, literal_compatible)
}

// Lower one predicate literal into the canonical key-item value once so the
// equality and prefix lookup paths share the same field/coercion/literal gate.
fn lower_lookup_value_for_key_item(
    key_item: SemanticIndexKeyItemRef<'_>,
    field: &str,
    value: &Value,
    coercion: CoercionId,
    literal_compatible: bool,
) -> Option<Value> {
    match key_item {
        SemanticIndexKeyItemRef::Field(key_field) => {
            if key_field != field || coercion != CoercionId::Strict || !literal_compatible {
                return None;
            }

            Some(value.clone())
        }
        SemanticIndexKeyItemRef::Expression(expression) => {
            if expression.field() != field
                || !expression_supports_lookup_coercion(expression, coercion)
                || !literal_compatible
            {
                return None;
            }

            derive_index_expression_value(expression, value.clone())
                .ok()
                .flatten()
        }
        SemanticIndexKeyItemRef::AcceptedExpression(expression) => {
            if expression.field() != field
                || !accepted_expression_supports_lookup_coercion(expression, coercion)
                || !literal_compatible
            {
                return None;
            }

            derive_accepted_index_expression_value(expression, value.clone())
                .ok()
                .flatten()
        }
    }
}

fn derive_accepted_index_expression_value(
    expression: &SemanticIndexExpression,
    source: Value,
) -> Result<Option<Value>, &'static str> {
    match expression.op() {
        PersistedIndexExpressionOp::Lower
        | PersistedIndexExpressionOp::Upper
        | PersistedIndexExpressionOp::Trim
        | PersistedIndexExpressionOp::LowerTrim => {
            derive_accepted_text_expression_value(expression.op(), source)
        }
        PersistedIndexExpressionOp::Date
        | PersistedIndexExpressionOp::Year
        | PersistedIndexExpressionOp::Month
        | PersistedIndexExpressionOp::Day => {
            derive_accepted_temporal_expression_value(expression.op(), source)
        }
    }
}

fn derive_accepted_text_expression_value(
    op: PersistedIndexExpressionOp,
    source: Value,
) -> Result<Option<Value>, &'static str> {
    let source = match source {
        Value::Null => return Ok(None),
        Value::Text(value) => ScalarExprValue::Text(value.into()),
        _ => return Err("Text"),
    };

    derive_non_null_scalar_expression_value(accepted_expression_op(op), source)
        .map(scalar_expr_value_into_value)
        .map(Some)
}

fn derive_accepted_temporal_expression_value(
    op: PersistedIndexExpressionOp,
    source: Value,
) -> Result<Option<Value>, &'static str> {
    let source = match source {
        Value::Null => return Ok(None),
        Value::Date(value) => ScalarExprValue::Date(value),
        Value::Timestamp(value) => ScalarExprValue::Timestamp(value),
        _ => return Err("Date/Timestamp"),
    };

    derive_non_null_scalar_expression_value(accepted_expression_op(op), source)
        .map(scalar_expr_value_into_value)
        .map(Some)
}

const fn accepted_expression_op(
    op: PersistedIndexExpressionOp,
) -> crate::db::scalar_expr::ScalarIndexExpressionOp {
    match op {
        PersistedIndexExpressionOp::Lower => crate::db::scalar_expr::ScalarIndexExpressionOp::Lower,
        PersistedIndexExpressionOp::Upper => crate::db::scalar_expr::ScalarIndexExpressionOp::Upper,
        PersistedIndexExpressionOp::Trim => crate::db::scalar_expr::ScalarIndexExpressionOp::Trim,
        PersistedIndexExpressionOp::LowerTrim => {
            crate::db::scalar_expr::ScalarIndexExpressionOp::LowerTrim
        }
        PersistedIndexExpressionOp::Date => crate::db::scalar_expr::ScalarIndexExpressionOp::Date,
        PersistedIndexExpressionOp::Year => crate::db::scalar_expr::ScalarIndexExpressionOp::Year,
        PersistedIndexExpressionOp::Month => crate::db::scalar_expr::ScalarIndexExpressionOp::Month,
        PersistedIndexExpressionOp::Day => crate::db::scalar_expr::ScalarIndexExpressionOp::Day,
    }
}

/// Try to lower one starts-with predicate literal into a canonical key-item prefix value.
#[must_use]
pub(in crate::db::query::plan) fn starts_with_lookup_value_for_key_item<'a>(
    key_item: impl Into<SemanticIndexKeyItemRef<'a>>,
    field: &str,
    value: &Value,
    coercion: CoercionId,
    literal_compatible: bool,
) -> Option<String> {
    let lowered = lower_lookup_value_for_key_item(
        key_item.into(),
        field,
        value,
        coercion,
        literal_compatible,
    )?;
    let Value::Text(prefix) = lowered else {
        return None;
    };
    if prefix.is_empty() {
        return None;
    }

    Some(prefix)
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::{
        db::{
            predicate::CoercionId,
            query::plan::key_item_match::{
                eq_lookup_value_for_key_item, key_item_matches_field_and_coercion,
                starts_with_lookup_value_for_key_item,
            },
        },
        model::index::{IndexExpression, IndexKeyItem},
        value::Value,
    };

    #[test]
    fn key_item_match_supports_only_declared_expression_lookup_matrix() {
        assert!(key_item_matches_field_and_coercion(
            IndexKeyItem::Expression(IndexExpression::Lower("email")),
            "email",
            CoercionId::TextCasefold,
        ));
        assert!(key_item_matches_field_and_coercion(
            IndexKeyItem::Expression(IndexExpression::Upper("email")),
            "email",
            CoercionId::TextCasefold,
        ));
        assert!(!key_item_matches_field_and_coercion(
            IndexKeyItem::Expression(IndexExpression::LowerTrim("email")),
            "email",
            CoercionId::TextCasefold,
        ));
    }

    #[test]
    fn eq_lookup_value_rejects_expression_not_in_lookup_matrix() {
        let lowered = eq_lookup_value_for_key_item(
            IndexKeyItem::Expression(IndexExpression::Lower("email")),
            "email",
            &Value::Text("ALICE@Example.Com".to_string()),
            CoercionId::TextCasefold,
            true,
        );
        assert_eq!(lowered, Some(Value::Text("alice@example.com".to_string())));

        let unsupported = eq_lookup_value_for_key_item(
            IndexKeyItem::Expression(IndexExpression::Upper("email")),
            "email",
            &Value::Text("ALICE@Example.Com".to_string()),
            CoercionId::TextCasefold,
            true,
        );
        assert_eq!(
            unsupported,
            Some(Value::Text("ALICE@EXAMPLE.COM".to_string()))
        );

        let unsupported = eq_lookup_value_for_key_item(
            IndexKeyItem::Expression(IndexExpression::LowerTrim("email")),
            "email",
            &Value::Text("ALICE@Example.Com".to_string()),
            CoercionId::TextCasefold,
            true,
        );
        assert_eq!(unsupported, None);
    }

    #[test]
    fn starts_with_lookup_value_lowers_text_casefold_expression_prefix() {
        let lowered = starts_with_lookup_value_for_key_item(
            IndexKeyItem::Expression(IndexExpression::Lower("email")),
            "email",
            &Value::Text("ALICE".to_string()),
            CoercionId::TextCasefold,
            true,
        );
        assert_eq!(lowered, Some("alice".to_string()));

        let unsupported = starts_with_lookup_value_for_key_item(
            IndexKeyItem::Expression(IndexExpression::Upper("email")),
            "email",
            &Value::Text("ALICE".to_string()),
            CoercionId::TextCasefold,
            true,
        );
        assert_eq!(unsupported, Some("ALICE".to_string()));

        let unsupported = starts_with_lookup_value_for_key_item(
            IndexKeyItem::Expression(IndexExpression::LowerTrim("email")),
            "email",
            &Value::Text("ALICE".to_string()),
            CoercionId::TextCasefold,
            true,
        );
        assert_eq!(unsupported, None);
    }

    #[test]
    fn starts_with_lookup_value_rejects_empty_prefix() {
        let lowered = starts_with_lookup_value_for_key_item(
            IndexKeyItem::Field("email"),
            "email",
            &Value::Text(String::new()),
            CoercionId::Strict,
            true,
        );

        assert_eq!(lowered, None);

        let lowered_expression = starts_with_lookup_value_for_key_item(
            IndexKeyItem::Expression(IndexExpression::Lower("email")),
            "email",
            &Value::Text(String::new()),
            CoercionId::TextCasefold,
            true,
        );

        assert_eq!(lowered_expression, None);
    }
}
