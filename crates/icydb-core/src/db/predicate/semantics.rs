//! Module: predicate::semantics
//! Responsibility: value comparison semantics under explicit coercion policies.
//! Does not own: predicate AST normalization or schema legality checks.
//! Boundary: runtime predicate evaluation delegates compare behavior here.

use crate::{
    db::{
        numeric::{compare_numeric_eq, compare_numeric_order},
        predicate::{
            coercion::{CoercionId, CoercionSpec},
            model::CompareOp,
        },
    },
    value::{TextMode, Value},
};
use std::{cmp::Ordering, mem::discriminant};

///
/// TextOp
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum TextOp {
    StartsWith,
    EndsWith,
}

/// Perform equality comparison under an explicit coercion policy.
#[must_use]
pub(in crate::db) fn compare_eq(
    left: &Value,
    right: &Value,
    coercion: &CoercionSpec,
) -> Option<bool> {
    // Equality semantics are coercion-policy dependent.
    match coercion.id {
        CoercionId::Strict | CoercionId::CollectionElement => {
            same_variant(left, right).then_some(left == right)
        }
        CoercionId::NumericWiden => compare_numeric_eq(left, right),
        CoercionId::TextCasefold => compare_casefold(left, right),
    }
}

/// Perform ordering comparison under an explicit coercion policy.
#[must_use]
pub(in crate::db) fn compare_order(
    left: &Value,
    right: &Value,
    coercion: &CoercionSpec,
) -> Option<Ordering> {
    // Ordering semantics are coercion-policy dependent.
    match coercion.id {
        CoercionId::Strict | CoercionId::CollectionElement => {
            if !same_variant(left, right) {
                return None;
            }
            Value::strict_order_cmp(left, right)
        }
        CoercionId::NumericWiden => compare_numeric_order(left, right),
        CoercionId::TextCasefold => {
            let left = casefold_value(left)?;
            let right = casefold_value(right)?;
            Some(left.cmp(&right))
        }
    }
}

/// Canonical total ordering for database predicate semantics.
#[must_use]
pub(in crate::db) fn canonical_cmp(left: &Value, right: &Value) -> Ordering {
    if let Some(ordering) = Value::strict_order_cmp(left, right) {
        return ordering;
    }

    left.canonical_rank().cmp(&right.canonical_rank())
}

/// Perform text-specific comparison operations.
#[must_use]
pub(in crate::db) fn compare_text(
    left: &Value,
    right: &Value,
    coercion: &CoercionSpec,
    op: TextOp,
) -> Option<bool> {
    if !matches!(left, Value::Text(_)) || !matches!(right, Value::Text(_)) {
        return None;
    }

    let mode = match coercion.id {
        CoercionId::Strict => TextMode::Cs,
        CoercionId::TextCasefold => TextMode::Ci,
        _ => return None,
    };

    match op {
        TextOp::StartsWith => left.text_starts_with(right, mode),
        TextOp::EndsWith => left.text_ends_with(right, mode),
    }
}

/// Return whether grouped HAVING supports this compare operator in grouped v1.
#[must_use]
pub(in crate::db) const fn grouped_having_compare_op_supported(op: CompareOp) -> bool {
    grouped_having_compare_kind(op).is_some()
}

/// Evaluate one grouped HAVING comparison under grouped-v1 predicate semantics.
///
/// Returns `None` when `op` is outside grouped HAVING v1 support.
#[must_use]
pub(in crate::db) fn evaluate_grouped_having_compare_v1(
    actual: &Value,
    op: CompareOp,
    expected: &Value,
) -> Option<bool> {
    let kind = grouped_having_compare_kind(op)?;

    // Keep grouped NULL checks explicit so `IS NULL`/`IS NOT NULL` style
    // HAVING comparisons remain deterministic and do not depend on
    // variant-mismatch coercion fallback behavior.
    if matches!(expected, Value::Null) {
        return Some(match kind {
            GroupedHavingCompareKind::Eq => matches!(actual, Value::Null),
            GroupedHavingCompareKind::Ne => !matches!(actual, Value::Null),
            GroupedHavingCompareKind::Lt
            | GroupedHavingCompareKind::Lte
            | GroupedHavingCompareKind::Gt
            | GroupedHavingCompareKind::Gte => false,
        });
    }

    let numeric = CoercionSpec::new(CoercionId::NumericWiden);
    let strict = CoercionSpec::default();
    let coercion = if actual.supports_numeric_coercion() || expected.supports_numeric_coercion() {
        &numeric
    } else {
        &strict
    };

    Some(match kind {
        GroupedHavingCompareKind::Eq => compare_eq(actual, expected, coercion).unwrap_or(false),
        GroupedHavingCompareKind::Ne => {
            compare_eq(actual, expected, coercion).is_some_and(|equal| !equal)
        }
        GroupedHavingCompareKind::Lt => {
            compare_order(actual, expected, coercion).is_some_and(std::cmp::Ordering::is_lt)
        }
        GroupedHavingCompareKind::Lte => {
            compare_order(actual, expected, coercion).is_some_and(std::cmp::Ordering::is_le)
        }
        GroupedHavingCompareKind::Gt => {
            compare_order(actual, expected, coercion).is_some_and(std::cmp::Ordering::is_gt)
        }
        GroupedHavingCompareKind::Gte => {
            compare_order(actual, expected, coercion).is_some_and(std::cmp::Ordering::is_ge)
        }
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GroupedHavingCompareKind {
    Eq,
    Ne,
    Lt,
    Lte,
    Gt,
    Gte,
}

const fn grouped_having_compare_kind(op: CompareOp) -> Option<GroupedHavingCompareKind> {
    match op {
        CompareOp::Eq => Some(GroupedHavingCompareKind::Eq),
        CompareOp::Ne => Some(GroupedHavingCompareKind::Ne),
        CompareOp::Lt => Some(GroupedHavingCompareKind::Lt),
        CompareOp::Lte => Some(GroupedHavingCompareKind::Lte),
        CompareOp::Gt => Some(GroupedHavingCompareKind::Gt),
        CompareOp::Gte => Some(GroupedHavingCompareKind::Gte),
        CompareOp::In
        | CompareOp::NotIn
        | CompareOp::Contains
        | CompareOp::StartsWith
        | CompareOp::EndsWith => None,
    }
}

fn same_variant(left: &Value, right: &Value) -> bool {
    discriminant(left) == discriminant(right)
}

fn compare_casefold(left: &Value, right: &Value) -> Option<bool> {
    let left = casefold_value(left)?;
    let right = casefold_value(right)?;
    Some(left == right)
}

fn casefold_value(value: &Value) -> Option<String> {
    match value {
        Value::Text(text) => Some(casefold(text)),
        _ => None,
    }
}

fn casefold(input: &str) -> String {
    if input.is_ascii() {
        return input.to_ascii_lowercase();
    }

    input.to_lowercase()
}
