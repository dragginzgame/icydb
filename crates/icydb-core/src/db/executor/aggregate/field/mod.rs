//! Module: executor::aggregate::field
//! Responsibility: aggregate field-slot resolution and field-value extraction/comparison helpers.
//! Does not own: aggregate route planning decisions.
//! Boundary: field-target aggregate helper surface used by aggregate executors.

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
    model::{
        entity::EntityModel,
        field::{FieldKind, FieldModel},
    },
    traits::{EntityKind, EntityValue},
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
                crate::db::error::executor_unsupported(message)
            }
            Self::MissingFieldValue { .. }
            | Self::FieldValueTypeMismatch { .. }
            | Self::IncomparableFieldValues { .. } => {
                crate::db::error::query_executor_invariant(message)
            }
        }
    }
}

// Resolve one field model entry by name and return its stable slot index.
fn field_model_with_index<'a>(
    model: &'a EntityModel,
    field: &str,
) -> Option<(usize, &'a FieldModel)> {
    model
        .fields
        .iter()
        .enumerate()
        .find(|(_, candidate)| candidate.name == field)
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

/// Resolve one orderable aggregate target field into a stable projection slot.
pub(in crate::db::executor) fn resolve_orderable_aggregate_target_slot<E: EntityKind>(
    target_field: &str,
) -> Result<FieldSlot, AggregateFieldValueError> {
    let Some((index, field)) = field_model_with_index(E::MODEL, target_field) else {
        return Err(AggregateFieldValueError::UnknownField {
            field: target_field.to_string(),
        });
    };
    if !field_kind_supports_aggregate_ordering(&field.kind) {
        return Err(AggregateFieldValueError::UnsupportedFieldKind {
            field: target_field.to_string(),
            kind: field.kind,
        });
    }

    Ok(FieldSlot {
        index,
        kind: field.kind,
    })
}

/// Resolve one planner field slot into one orderable aggregate projection slot.
pub(in crate::db::executor) fn resolve_orderable_aggregate_target_slot_from_planner_slot<
    E: EntityKind,
>(
    field_slot: &PlannedFieldSlot,
) -> Result<FieldSlot, AggregateFieldValueError> {
    let target_field = field_slot.field();
    let Some(field) = E::MODEL.fields.get(field_slot.index()) else {
        return Err(AggregateFieldValueError::UnknownField {
            field: target_field.to_string(),
        });
    };
    if field.name != target_field {
        return Err(AggregateFieldValueError::UnknownField {
            field: target_field.to_string(),
        });
    }
    if !field_kind_supports_aggregate_ordering(&field.kind) {
        return Err(AggregateFieldValueError::UnsupportedFieldKind {
            field: target_field.to_string(),
            kind: field.kind,
        });
    }

    Ok(FieldSlot {
        index: field_slot.index(),
        kind: field.kind,
    })
}

/// Resolve one aggregate target field into a stable projection slot.
#[cfg(test)]
pub(in crate::db::executor) fn resolve_any_aggregate_target_slot<E: EntityKind>(
    target_field: &str,
) -> Result<FieldSlot, AggregateFieldValueError> {
    resolve_any_aggregate_target_slot_with_model(E::MODEL, target_field)
}

/// Resolve one aggregate target field into a stable projection slot using structural model data.
pub(in crate::db::executor) fn resolve_any_aggregate_target_slot_with_model(
    model: &'static EntityModel,
    target_field: &str,
) -> Result<FieldSlot, AggregateFieldValueError> {
    let Some((index, field)) = field_model_with_index(model, target_field) else {
        return Err(AggregateFieldValueError::UnknownField {
            field: target_field.to_string(),
        });
    };

    Ok(FieldSlot {
        index,
        kind: field.kind,
    })
}

/// Resolve one planner field slot into one aggregate projection slot.
pub(in crate::db::executor) fn resolve_any_aggregate_target_slot_from_planner_slot<
    E: EntityKind,
>(
    field_slot: &PlannedFieldSlot,
) -> Result<FieldSlot, AggregateFieldValueError> {
    let target_field = field_slot.field();
    let Some(field) = E::MODEL.fields.get(field_slot.index()) else {
        return Err(AggregateFieldValueError::UnknownField {
            field: target_field.to_string(),
        });
    };
    if field.name != target_field {
        return Err(AggregateFieldValueError::UnknownField {
            field: target_field.to_string(),
        });
    }

    Ok(FieldSlot {
        index: field_slot.index(),
        kind: field.kind,
    })
}

/// Resolve one numeric aggregate target field into a stable projection slot.
#[cfg(test)]
pub(in crate::db::executor) fn resolve_numeric_aggregate_target_slot<E: EntityKind>(
    target_field: &str,
) -> Result<FieldSlot, AggregateFieldValueError> {
    resolve_numeric_aggregate_target_slot_with_model(E::MODEL, target_field)
}

/// Resolve one numeric aggregate target field into a stable projection slot using structural model data.
pub(in crate::db::executor) fn resolve_numeric_aggregate_target_slot_with_model(
    model: &'static EntityModel,
    target_field: &str,
) -> Result<FieldSlot, AggregateFieldValueError> {
    let Some((index, field)) = field_model_with_index(model, target_field) else {
        return Err(AggregateFieldValueError::UnknownField {
            field: target_field.to_string(),
        });
    };
    if !field_kind_supports_numeric_aggregation(&field.kind) {
        return Err(AggregateFieldValueError::UnsupportedFieldKind {
            field: target_field.to_string(),
            kind: field.kind,
        });
    }

    Ok(FieldSlot {
        index,
        kind: field.kind,
    })
}

/// Resolve one planner field slot into one numeric aggregate projection slot.
pub(in crate::db::executor) fn resolve_numeric_aggregate_target_slot_from_planner_slot<
    E: EntityKind,
>(
    field_slot: &PlannedFieldSlot,
) -> Result<FieldSlot, AggregateFieldValueError> {
    let target_field = field_slot.field();
    let Some(field) = E::MODEL.fields.get(field_slot.index()) else {
        return Err(AggregateFieldValueError::UnknownField {
            field: target_field.to_string(),
        });
    };
    if field.name != target_field {
        return Err(AggregateFieldValueError::UnknownField {
            field: target_field.to_string(),
        });
    }
    if !field_kind_supports_numeric_aggregation(&field.kind) {
        return Err(AggregateFieldValueError::UnsupportedFieldKind {
            field: target_field.to_string(),
            kind: field.kind,
        });
    }

    Ok(FieldSlot {
        index: field_slot.index(),
        kind: field.kind,
    })
}

/// Extract one field value from an entity and enforce the declared runtime field kind.
pub(in crate::db::executor) fn extract_orderable_field_value<E: EntityKind + EntityValue>(
    entity: &E,
    target_field: &str,
    field_slot: FieldSlot,
) -> Result<Value, AggregateFieldValueError> {
    extract_orderable_field_value_with_slot_reader(target_field, field_slot, &mut |index| {
        entity.get_value_by_index(index)
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

/// Extract one numeric field value as `Decimal` for aggregate arithmetic.
pub(in crate::db::executor) fn extract_numeric_field_decimal<E: EntityKind + EntityValue>(
    entity: &E,
    target_field: &str,
    field_slot: FieldSlot,
) -> Result<Decimal, AggregateFieldValueError> {
    extract_numeric_field_decimal_with_slot_reader(target_field, field_slot, &mut |index| {
        entity.get_value_by_index(index)
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

/// Compare two entities by one orderable aggregate field and return base ascending ordering.
pub(in crate::db::executor) fn compare_entities_by_orderable_field<E: EntityKind + EntityValue>(
    left: &E,
    right: &E,
    target_field: &str,
    field_slot: FieldSlot,
) -> Result<Ordering, AggregateFieldValueError> {
    let left_value = extract_orderable_field_value(left, target_field, field_slot)?;
    let right_value = extract_orderable_field_value(right, target_field, field_slot)?;

    compare_orderable_field_values(target_field, &left_value, &right_value)
}

/// Compare two entities for field-extrema selection with deterministic tie-break semantics.
///
/// Contract:
/// - primary comparison follows aggregate `direction` over the target field value.
/// - ties always break on canonical primary-key ascending order.
pub(in crate::db::executor) fn compare_entities_for_field_extrema<E: EntityKind + EntityValue>(
    left: &E,
    right: &E,
    target_field: &str,
    field_slot: FieldSlot,
    direction: Direction,
) -> Result<Ordering, AggregateFieldValueError> {
    let field_order = compare_entities_by_orderable_field(left, right, target_field, field_slot)?;
    let directional_field_order = apply_aggregate_direction(field_order, direction);
    if directional_field_order != Ordering::Equal {
        return Ok(directional_field_order);
    }

    let left_id = left.id().as_value();
    let right_id = right.id().as_value();

    compare_orderable_field_values(E::MODEL.primary_key.name, &left_id, &right_id)
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
