//! Module: query::plan::key_item_match
//! Responsibility: shared key-item matching/lowering rules used by planner and explain access-choice.
//! Does not own: index ranking policy or access-path shape construction.
//! Boundary: canonical field/expression key-item lookup compatibility and literal lowering.

use crate::{
    db::predicate::CoercionId,
    model::index::{IndexExpression, IndexKeyItem, IndexKeyItemsRef, IndexModel},
    value::Value,
};

/// Return the canonical key-item count for one index model.
#[must_use]
pub(in crate::db::query::plan) const fn index_key_item_count(index: &IndexModel) -> usize {
    match index.key_items() {
        IndexKeyItemsRef::Fields(fields) => fields.len(),
        IndexKeyItemsRef::Items(items) => items.len(),
    }
}

/// Return the canonical leading key-item for one index model.
#[must_use]
pub(in crate::db::query::plan) const fn leading_index_key_item(
    index: &IndexModel,
) -> Option<IndexKeyItem> {
    match index.key_items() {
        IndexKeyItemsRef::Fields(fields) => {
            if fields.is_empty() {
                None
            } else {
                Some(IndexKeyItem::Field(fields[0]))
            }
        }
        IndexKeyItemsRef::Items(items) => {
            if items.is_empty() {
                None
            } else {
                Some(items[0])
            }
        }
    }
}

/// Return whether one key-item can match a predicate field/coercion pair.
#[must_use]
pub(in crate::db::query::plan) fn key_item_matches_field_and_coercion(
    key_item: IndexKeyItem,
    field: &str,
    coercion: CoercionId,
) -> bool {
    match key_item {
        IndexKeyItem::Field(key_field) => key_field == field && coercion == CoercionId::Strict,
        IndexKeyItem::Expression(IndexExpression::Lower(key_field)) => {
            key_field == field && coercion == CoercionId::TextCasefold
        }
        IndexKeyItem::Expression(_) => false,
    }
}

fn fold_ci_text(input: &str) -> String {
    if input.is_ascii() {
        input.to_ascii_lowercase()
    } else {
        input.to_lowercase()
    }
}

/// Try to lower one predicate literal into a canonical key-item lookup value.
#[must_use]
pub(in crate::db::query::plan) fn eq_lookup_value_for_key_item(
    key_item: IndexKeyItem,
    field: &str,
    value: &Value,
    coercion: CoercionId,
    literal_compatible: bool,
) -> Option<Value> {
    match key_item {
        IndexKeyItem::Field(key_field) => {
            if key_field != field || coercion != CoercionId::Strict || !literal_compatible {
                return None;
            }

            Some(value.clone())
        }
        IndexKeyItem::Expression(IndexExpression::Lower(key_field)) => {
            if key_field != field || coercion != CoercionId::TextCasefold || !literal_compatible {
                return None;
            }
            let Value::Text(text) = value else {
                return None;
            };

            Some(Value::Text(fold_ci_text(text)))
        }
        IndexKeyItem::Expression(_) => None,
    }
}
