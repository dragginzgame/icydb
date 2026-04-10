//! Module: executor::aggregate::field
//! Responsibility: aggregate field-slot resolution and field-value extraction/comparison helpers.
//! Does not own: aggregate route planning decisions.
//! Boundary: field-target aggregate helper surface used by aggregate executors.

#[cfg(test)]
use crate::model::field::FieldModel;
use crate::{
    db::{
        direction::Direction,
        executor::aggregate::capability::{
            field_kind_supports_aggregate_ordering, field_kind_supports_numeric_aggregation,
        },
        numeric::{coerce_numeric_decimal, compare_numeric_or_strict_order},
        query::plan::FieldSlot as PlannedFieldSlot,
    },
    error::InternalError,
    model::field::FieldKind,
    types::Decimal,
    value::Value,
};
use std::cmp::Ordering;
use thiserror::Error as ThisError;

///
/// AggregateFieldValueError
///
/// Typed field-aggregate extraction/comparison errors used by aggregate
/// field-value helpers. These remain internal while field aggregates are scaffolded.
///

#[derive(Clone, Debug, ThisError)]
pub(in crate::db::executor) enum AggregateFieldValueError {
    #[error("unknown aggregate target field: {field}")]
    UnknownField { field: String },

    #[error("aggregate target field does not support ordering: {field} kind={kind:?}")]
    UnsupportedFieldKind { field: String, kind: FieldKind },

    #[error("aggregate target field value missing on entity: {field}")]
    MissingFieldValue { field: String },

    #[error("aggregate target field value type mismatch: {field} kind={kind:?} value={value:?}")]
    FieldValueTypeMismatch {
        field: String,
        kind: FieldKind,
        value: Box<Value>,
    },

    #[error(
        "aggregate target field values are incomparable under strict ordering: {field} left={left:?} right={right:?}"
    )]
    IncomparableFieldValues {
        field: String,
        left: Box<Value>,
        right: Box<Value>,
    },
}

impl AggregateFieldValueError {
    // Map field-target extraction/comparison failures into taxonomy-correct
    // execution errors.
    pub(in crate::db::executor) fn into_internal_error(self) -> InternalError {
        let message = self.to_string();
        match self {
            Self::UnknownField { .. } | Self::UnsupportedFieldKind { .. } => {
                InternalError::executor_unsupported(message)
            }
            Self::MissingFieldValue { .. }
            | Self::FieldValueTypeMismatch { .. }
            | Self::IncomparableFieldValues { .. } => {
                InternalError::query_executor_invariant(message)
            }
        }
    }
}

// Resolve one field model entry by name and return its stable slot index.
#[cfg(test)]
fn field_model_with_index<'a>(
    fields: &'a [FieldModel],
    field: &str,
) -> Option<(usize, &'a FieldModel)> {
    fields
        .iter()
        .enumerate()
        .find(|(_, candidate)| candidate.name() == field)
}

///
/// FieldSlot
///
/// Stable aggregate field projection descriptor resolved once at setup.
///
#[derive(Clone, Copy, Debug)]
pub(in crate::db::executor) struct FieldSlot {
    pub(in crate::db::executor) index: usize,
    pub(in crate::db::executor) kind: FieldKind,
}

// Return true when one runtime value matches the declared field kind shape.
fn field_kind_matches_value(kind: &FieldKind, value: &Value) -> bool {
    match (kind, value) {
        (FieldKind::Account, Value::Account(_))
        | (FieldKind::Blob, Value::Blob(_))
        | (FieldKind::Bool, Value::Bool(_))
        | (FieldKind::Date, Value::Date(_))
        | (FieldKind::Decimal { .. }, Value::Decimal(_))
        | (FieldKind::Duration, Value::Duration(_))
        | (FieldKind::Enum { .. }, Value::Enum(_))
        | (FieldKind::Float32, Value::Float32(_))
        | (FieldKind::Float64, Value::Float64(_))
        | (FieldKind::Int, Value::Int(_))
        | (FieldKind::Int128, Value::Int128(_))
        | (FieldKind::IntBig, Value::IntBig(_))
        | (FieldKind::Principal, Value::Principal(_))
        | (FieldKind::Subaccount, Value::Subaccount(_))
        | (FieldKind::Text, Value::Text(_))
        | (FieldKind::Timestamp, Value::Timestamp(_))
        | (FieldKind::Uint, Value::Uint(_))
        | (FieldKind::Uint128, Value::Uint128(_))
        | (FieldKind::UintBig, Value::UintBig(_))
        | (FieldKind::Ulid, Value::Ulid(_))
        | (FieldKind::Unit, Value::Unit)
        | (FieldKind::Structured { .. }, Value::List(_) | Value::Map(_)) => true,
        (FieldKind::Relation { key_kind, .. }, value) => field_kind_matches_value(key_kind, value),
        (FieldKind::List(inner) | FieldKind::Set(inner), Value::List(items)) => items
            .iter()
            .all(|item| field_kind_matches_value(inner, item)),
        (FieldKind::Map { key, value }, Value::Map(entries)) => {
            entries.iter().all(|(entry_key, entry_value)| {
                field_kind_matches_value(key, entry_key)
                    && field_kind_matches_value(value, entry_value)
            })
        }
        _ => false,
    }
}

// Compare exact declared field/value pairs directly before falling back to the
// wider numeric-or-strict comparator stack.
fn direct_compare_orderable_field_values(
    kind: &FieldKind,
    left: &Value,
    right: &Value,
) -> Option<Ordering> {
    match (kind, left, right) {
        (FieldKind::Decimal { .. }, Value::Decimal(left), Value::Decimal(right)) => {
            left.partial_cmp(right)
        }
        (FieldKind::Float32, Value::Float32(left), Value::Float32(right)) => {
            left.get().partial_cmp(&right.get())
        }
        (FieldKind::Float64, Value::Float64(left), Value::Float64(right)) => {
            left.get().partial_cmp(&right.get())
        }
        (FieldKind::Int, Value::Int(left), Value::Int(right)) => Some(left.cmp(right)),
        (FieldKind::Int128, Value::Int128(left), Value::Int128(right)) => {
            Some(left.get().cmp(&right.get()))
        }
        (FieldKind::Uint, Value::Uint(left), Value::Uint(right)) => Some(left.cmp(right)),
        (FieldKind::Uint128, Value::Uint128(left), Value::Uint128(right)) => {
            Some(left.get().cmp(&right.get()))
        }
        (FieldKind::Relation { key_kind, .. }, left, right) => {
            direct_compare_orderable_field_values(key_kind, left, right)
        }
        _ => None,
    }
}

/// Resolve one orderable aggregate target field into a stable projection slot using structural model data.
#[cfg(test)]
pub(in crate::db::executor) fn resolve_orderable_aggregate_target_slot_from_fields(
    fields: &[FieldModel],
    target_field: &str,
) -> Result<FieldSlot, AggregateFieldValueError> {
    let Some((index, field)) = field_model_with_index(fields, target_field) else {
        return Err(AggregateFieldValueError::UnknownField {
            field: target_field.to_string(),
        });
    };
    if !field_kind_supports_aggregate_ordering(&field.kind()) {
        return Err(AggregateFieldValueError::UnsupportedFieldKind {
            field: target_field.to_string(),
            kind: field.kind(),
        });
    }

    Ok(FieldSlot {
        index,
        kind: field.kind(),
    })
}

/// Resolve one planner field slot into one orderable aggregate projection slot using planner-frozen field metadata.
pub(in crate::db::executor) fn resolve_orderable_aggregate_target_slot_from_planner_slot(
    field_slot: &PlannedFieldSlot,
) -> Result<FieldSlot, AggregateFieldValueError> {
    let target_field = field_slot.field();
    let Some(kind) = field_slot.kind() else {
        return Err(AggregateFieldValueError::UnknownField {
            field: target_field.to_string(),
        });
    };
    if !field_kind_supports_aggregate_ordering(&kind) {
        return Err(AggregateFieldValueError::UnsupportedFieldKind {
            field: target_field.to_string(),
            kind,
        });
    }

    Ok(FieldSlot {
        index: field_slot.index(),
        kind,
    })
}

/// Resolve one aggregate target field into a stable projection slot using structural model data.
#[cfg(test)]
pub(in crate::db::executor) fn resolve_any_aggregate_target_slot_from_fields(
    fields: &[FieldModel],
    target_field: &str,
) -> Result<FieldSlot, AggregateFieldValueError> {
    let Some((index, field)) = field_model_with_index(fields, target_field) else {
        return Err(AggregateFieldValueError::UnknownField {
            field: target_field.to_string(),
        });
    };

    Ok(FieldSlot {
        index,
        kind: field.kind(),
    })
}

/// Resolve one planner field slot into one aggregate projection slot using planner-frozen field metadata.
pub(in crate::db::executor) fn resolve_any_aggregate_target_slot_from_planner_slot(
    field_slot: &PlannedFieldSlot,
) -> Result<FieldSlot, AggregateFieldValueError> {
    let target_field = field_slot.field();
    let Some(kind) = field_slot.kind() else {
        return Err(AggregateFieldValueError::UnknownField {
            field: target_field.to_string(),
        });
    };

    Ok(FieldSlot {
        index: field_slot.index(),
        kind,
    })
}

/// Resolve one numeric aggregate target field into a stable projection slot using structural model data.
#[cfg(test)]
pub(in crate::db::executor) fn resolve_numeric_aggregate_target_slot_from_fields(
    fields: &[FieldModel],
    target_field: &str,
) -> Result<FieldSlot, AggregateFieldValueError> {
    let Some((index, field)) = field_model_with_index(fields, target_field) else {
        return Err(AggregateFieldValueError::UnknownField {
            field: target_field.to_string(),
        });
    };
    if !field_kind_supports_numeric_aggregation(&field.kind()) {
        return Err(AggregateFieldValueError::UnsupportedFieldKind {
            field: target_field.to_string(),
            kind: field.kind(),
        });
    }

    Ok(FieldSlot {
        index,
        kind: field.kind(),
    })
}

/// Resolve one planner field slot into one numeric aggregate projection slot using planner-frozen field metadata.
pub(in crate::db::executor) fn resolve_numeric_aggregate_target_slot_from_planner_slot(
    field_slot: &PlannedFieldSlot,
) -> Result<FieldSlot, AggregateFieldValueError> {
    let target_field = field_slot.field();
    let Some(kind) = field_slot.kind() else {
        return Err(AggregateFieldValueError::UnknownField {
            field: target_field.to_string(),
        });
    };
    if !field_kind_supports_numeric_aggregation(&kind) {
        return Err(AggregateFieldValueError::UnsupportedFieldKind {
            field: target_field.to_string(),
            kind,
        });
    }

    Ok(FieldSlot {
        index: field_slot.index(),
        kind,
    })
}

/// Extract one field value from a slot reader and enforce the declared runtime field kind.
pub(in crate::db::executor) fn extract_orderable_field_value_with_slot_reader(
    target_field: &str,
    field_slot: FieldSlot,
    read_slot: &mut dyn FnMut(usize) -> Option<Value>,
) -> Result<Value, AggregateFieldValueError> {
    let Some(value) = read_slot(field_slot.index) else {
        return Err(AggregateFieldValueError::MissingFieldValue {
            field: target_field.to_string(),
        });
    };
    if !field_kind_matches_value(&field_slot.kind, &value) {
        return Err(AggregateFieldValueError::FieldValueTypeMismatch {
            field: target_field.to_string(),
            kind: field_slot.kind,
            value: Box::new(value),
        });
    }

    Ok(value)
}

// Extract one field value from one already-decoded retained slot and enforce
// the declared runtime field kind without rebuilding a slot-reader closure at
// each retained-slot callsite.
pub(in crate::db::executor) fn extract_orderable_field_value_from_decoded_slot(
    target_field: &str,
    field_slot: FieldSlot,
    decoded_value: Option<Value>,
) -> Result<Value, AggregateFieldValueError> {
    let mut decoded_value = decoded_value;

    extract_orderable_field_value_with_slot_reader(target_field, field_slot, &mut |_| {
        decoded_value.take()
    })
}

/// Extract one numeric field value as `Decimal` from a slot reader for aggregate arithmetic.
pub(in crate::db::executor) fn extract_numeric_field_decimal_with_slot_reader(
    target_field: &str,
    field_slot: FieldSlot,
    read_slot: &mut dyn FnMut(usize) -> Option<Value>,
) -> Result<Decimal, AggregateFieldValueError> {
    let value =
        extract_orderable_field_value_with_slot_reader(target_field, field_slot, read_slot)?;
    let Some(decimal) = coerce_numeric_decimal(&value) else {
        return Err(AggregateFieldValueError::FieldValueTypeMismatch {
            field: target_field.to_string(),
            kind: field_slot.kind,
            value: Box::new(value),
        });
    };

    Ok(decimal)
}

// Extract one numeric field value as `Decimal` from one already-decoded
// retained slot without rebuilding a one-shot slot-reader closure at each
// retained-slot numeric callsite.
pub(in crate::db::executor) fn extract_numeric_field_decimal_from_decoded_slot(
    target_field: &str,
    field_slot: FieldSlot,
    decoded_value: Option<Value>,
) -> Result<Decimal, AggregateFieldValueError> {
    let value =
        extract_orderable_field_value_from_decoded_slot(target_field, field_slot, decoded_value)?;
    let Some(decimal) = coerce_numeric_decimal(&value) else {
        return Err(AggregateFieldValueError::FieldValueTypeMismatch {
            field: target_field.to_string(),
            kind: field_slot.kind,
            value: Box::new(value),
        });
    };

    Ok(decimal)
}

/// Compare two extracted field values using shared numeric ordering semantics
/// first, then strict same-variant ordering fallback.
pub(in crate::db::executor) fn compare_orderable_field_values(
    target_field: &str,
    left: &Value,
    right: &Value,
) -> Result<Ordering, AggregateFieldValueError> {
    let Some(ordering) = compare_numeric_or_strict_order(left, right) else {
        return Err(AggregateFieldValueError::IncomparableFieldValues {
            field: target_field.to_string(),
            left: Box::new(left.clone()),
            right: Box::new(right.clone()),
        });
    };

    Ok(ordering)
}

/// Compare two extracted field values using the declared field slot first,
/// then fall back to the shared numeric-widen and strict-ordering contract.
pub(in crate::db::executor) fn compare_orderable_field_values_with_slot(
    target_field: &str,
    field_slot: FieldSlot,
    left: &Value,
    right: &Value,
) -> Result<Ordering, AggregateFieldValueError> {
    if let Some(ordering) = direct_compare_orderable_field_values(&field_slot.kind, left, right) {
        return Ok(ordering);
    }

    compare_orderable_field_values(target_field, left, right)
}

/// Apply aggregate direction to one base ordering result.
#[must_use]
pub(in crate::db::executor) const fn apply_aggregate_direction(
    ordering: Ordering,
    direction: Direction,
) -> Ordering {
    match direction {
        Direction::Asc => ordering,
        Direction::Desc => ordering.reverse(),
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests;
