//! Module: db::numeric
//! Responsibility: shared numeric capability classification for planning/execution.
//! Does not own: numeric expression evaluation or aggregate fold arithmetic.
//! Boundary: centralizes numeric field-kind domain predicates to reduce drift.

use crate::model::field::FieldKind;
use crate::types::Decimal;
use crate::value::Value;
use std::cmp::Ordering;

/// Return true when one field kind is accepted by planner expression arithmetic.
///
/// This intentionally mirrors the current expression-spine bootstrap domain.
#[must_use]
pub(in crate::db) const fn field_kind_supports_expr_numeric(kind: &FieldKind) -> bool {
    matches!(
        kind,
        FieldKind::Int
            | FieldKind::Int128
            | FieldKind::IntBig
            | FieldKind::Uint
            | FieldKind::Uint128
            | FieldKind::UintBig
            | FieldKind::Duration
            | FieldKind::Timestamp
            | FieldKind::Float32
            | FieldKind::Float64
            | FieldKind::Decimal { .. }
    )
}

/// Return true when one field kind is accepted by numeric aggregate terminals.
///
/// Relation key kinds recurse so relation-backed numeric keys remain eligible.
#[must_use]
pub(in crate::db) const fn field_kind_supports_aggregate_numeric(kind: &FieldKind) -> bool {
    match kind {
        FieldKind::Decimal { .. }
        | FieldKind::Duration
        | FieldKind::Float32
        | FieldKind::Float64
        | FieldKind::Int
        | FieldKind::Int128
        | FieldKind::IntBig
        | FieldKind::Timestamp
        | FieldKind::Uint
        | FieldKind::Uint128
        | FieldKind::UintBig => true,
        FieldKind::Relation { key_kind, .. } => field_kind_supports_aggregate_numeric(key_kind),
        FieldKind::Account
        | FieldKind::Blob
        | FieldKind::Bool
        | FieldKind::Date
        | FieldKind::Enum { .. }
        | FieldKind::List(_)
        | FieldKind::Map { .. }
        | FieldKind::Principal
        | FieldKind::Set(_)
        | FieldKind::Structured { .. }
        | FieldKind::Subaccount
        | FieldKind::Text
        | FieldKind::Ulid
        | FieldKind::Unit => false,
    }
}

/// Coerce one value into decimal under the shared numeric coercion contract.
///
/// Returns `None` when the value is outside numeric coercion domain or cannot
/// be represented as a decimal under current runtime numeric rules.
#[must_use]
pub(in crate::db) fn coerce_numeric_decimal(value: &Value) -> Option<Decimal> {
    if !value.supports_numeric_coercion() {
        return None;
    }

    value.to_numeric_decimal()
}

/// Compare two values under numeric-widen coercion semantics.
///
/// Returns `None` when either side is outside numeric coercion domain or when
/// conversion cannot produce a comparable numeric representation.
#[must_use]
pub(in crate::db) fn compare_numeric_order(left: &Value, right: &Value) -> Option<Ordering> {
    let left = coerce_numeric_decimal(left)?;
    let right = coerce_numeric_decimal(right)?;
    left.partial_cmp(&right)
}

/// Compare two values for numeric equality under numeric-widen semantics.
#[must_use]
pub(in crate::db) fn compare_numeric_eq(left: &Value, right: &Value) -> Option<bool> {
    compare_numeric_order(left, right).map(|ordering| ordering == Ordering::Equal)
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::{
        db::numeric::{
            coerce_numeric_decimal, compare_numeric_eq, compare_numeric_order,
            field_kind_supports_aggregate_numeric, field_kind_supports_expr_numeric,
        },
        model::field::FieldKind,
        types::Int,
        value::Value,
    };
    use std::cmp::Ordering;

    #[test]
    fn expr_numeric_domain_matches_bootstrap_contract() {
        assert!(field_kind_supports_expr_numeric(&FieldKind::Int));
        assert!(field_kind_supports_expr_numeric(&FieldKind::Uint));
        assert!(field_kind_supports_expr_numeric(&FieldKind::Float64));
        assert!(field_kind_supports_expr_numeric(&FieldKind::Decimal {
            scale: 2
        }));
        assert!(field_kind_supports_expr_numeric(&FieldKind::Timestamp));
        assert!(field_kind_supports_expr_numeric(&FieldKind::Duration));
        assert!(!field_kind_supports_expr_numeric(&FieldKind::Text));
    }

    #[test]
    fn aggregate_numeric_domain_keeps_duration_and_timestamp() {
        assert!(field_kind_supports_aggregate_numeric(&FieldKind::Int));
        assert!(field_kind_supports_aggregate_numeric(&FieldKind::Duration));
        assert!(field_kind_supports_aggregate_numeric(&FieldKind::Timestamp));
        assert!(!field_kind_supports_aggregate_numeric(&FieldKind::Text));
    }

    #[test]
    fn numeric_compare_helpers_follow_numeric_widen_domain() {
        assert_eq!(
            compare_numeric_order(&Value::Int(2), &Value::Uint(2)),
            Some(Ordering::Equal)
        );
        assert_eq!(
            compare_numeric_eq(&Value::Int(2), &Value::Uint(2)),
            Some(true)
        );
        assert_eq!(
            compare_numeric_order(&Value::Text("x".to_string()), &Value::Text("x".to_string())),
            None
        );
    }

    #[test]
    fn numeric_decimal_coercion_rejects_non_coercible_variants() {
        assert!(coerce_numeric_decimal(&Value::Int(4)).is_some());
        assert!(coerce_numeric_decimal(&Value::Text("x".to_string())).is_none());
        assert!(coerce_numeric_decimal(&Value::IntBig(Int::from(4i32))).is_none());
    }
}
