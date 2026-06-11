//! Module: executor::aggregate::field
//! Responsibility: aggregate field-slot resolution and field-value extraction/comparison helpers.
//! Does not own: aggregate route planning decisions.
//! Boundary: field-target aggregate helper surface used by aggregate executors.

use super::contracts::FieldSlot as PlannedFieldSlot;
#[cfg(test)]
use crate::model::field::FieldModel;
use crate::{
    db::{
        direction::Direction,
        executor::aggregate::capability::{
            field_kind_supports_aggregate_ordering, field_kind_supports_numeric_aggregation,
        },
        numeric::{coerce_numeric_decimal, compare_numeric_or_strict_order},
    },
    error::InternalError,
    model::field::FieldKind,
    types::Decimal,
    value::Value,
};
use std::cmp::Ordering;

///
/// AggregateFieldValueError
///
/// Typed field-aggregate extraction/comparison errors used by aggregate
/// field-value helpers. These remain internal while field aggregates are scaffolded.
///

#[derive(Clone, Debug)]
pub(in crate::db::executor) enum AggregateFieldValueError {
    UnknownField,

    UnsupportedFieldKind {
        slot_index: usize,
        kind: AggregateFieldKindCode,
    },

    MissingFieldValue {
        slot_index: usize,
    },

    FieldValueTypeMismatch {
        slot_index: usize,
        expected: AggregateFieldKindCode,
        found: AggregateValueKindCode,
    },

    IncomparableFieldValues {
        left: AggregateValueKindCode,
        right: AggregateValueKindCode,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) struct AggregateFieldKindCode(u8);

impl AggregateFieldKindCode {
    pub(in crate::db::executor) const ACCOUNT: Self = Self(0);
    pub(in crate::db::executor) const BLOB: Self = Self(1);
    pub(in crate::db::executor) const BOOL: Self = Self(2);
    pub(in crate::db::executor) const DATE: Self = Self(3);
    pub(in crate::db::executor) const DECIMAL: Self = Self(4);
    pub(in crate::db::executor) const DURATION: Self = Self(5);
    pub(in crate::db::executor) const ENUM: Self = Self(6);
    pub(in crate::db::executor) const FLOAT32: Self = Self(7);
    pub(in crate::db::executor) const FLOAT64: Self = Self(8);
    pub(in crate::db::executor) const INT8: Self = Self(9);
    pub(in crate::db::executor) const INT16: Self = Self(10);
    pub(in crate::db::executor) const INT32: Self = Self(11);
    pub(in crate::db::executor) const INT64: Self = Self(12);
    pub(in crate::db::executor) const INT128: Self = Self(13);
    pub(in crate::db::executor) const INT_BIG: Self = Self(14);
    pub(in crate::db::executor) const PRINCIPAL: Self = Self(15);
    pub(in crate::db::executor) const SUBACCOUNT: Self = Self(16);
    pub(in crate::db::executor) const TEXT: Self = Self(17);
    pub(in crate::db::executor) const TIMESTAMP: Self = Self(18);
    pub(in crate::db::executor) const NAT8: Self = Self(19);
    pub(in crate::db::executor) const NAT16: Self = Self(20);
    pub(in crate::db::executor) const NAT32: Self = Self(21);
    pub(in crate::db::executor) const NAT64: Self = Self(22);
    pub(in crate::db::executor) const NAT128: Self = Self(23);
    pub(in crate::db::executor) const NAT_BIG: Self = Self(24);
    pub(in crate::db::executor) const ULID: Self = Self(25);
    pub(in crate::db::executor) const UNIT: Self = Self(26);
    pub(in crate::db::executor) const RELATION: Self = Self(27);
    pub(in crate::db::executor) const LIST: Self = Self(28);
    pub(in crate::db::executor) const SET: Self = Self(29);
    pub(in crate::db::executor) const MAP: Self = Self(30);
    pub(in crate::db::executor) const STRUCTURED: Self = Self(31);

    const fn from_field_kind(kind: &FieldKind) -> Self {
        match kind {
            FieldKind::Account => Self::ACCOUNT,
            FieldKind::Blob { .. } => Self::BLOB,
            FieldKind::Bool => Self::BOOL,
            FieldKind::Date => Self::DATE,
            FieldKind::Decimal { .. } => Self::DECIMAL,
            FieldKind::Duration => Self::DURATION,
            FieldKind::Enum { .. } => Self::ENUM,
            FieldKind::Float32 => Self::FLOAT32,
            FieldKind::Float64 => Self::FLOAT64,
            FieldKind::Int8 => Self::INT8,
            FieldKind::Int16 => Self::INT16,
            FieldKind::Int32 => Self::INT32,
            FieldKind::Int64 => Self::INT64,
            FieldKind::Int128 => Self::INT128,
            FieldKind::IntBig { .. } => Self::INT_BIG,
            FieldKind::Principal => Self::PRINCIPAL,
            FieldKind::Subaccount => Self::SUBACCOUNT,
            FieldKind::Text { .. } => Self::TEXT,
            FieldKind::Timestamp => Self::TIMESTAMP,
            FieldKind::Nat8 => Self::NAT8,
            FieldKind::Nat16 => Self::NAT16,
            FieldKind::Nat32 => Self::NAT32,
            FieldKind::Nat64 => Self::NAT64,
            FieldKind::Nat128 => Self::NAT128,
            FieldKind::NatBig { .. } => Self::NAT_BIG,
            FieldKind::Ulid => Self::ULID,
            FieldKind::Unit => Self::UNIT,
            FieldKind::Relation { .. } => Self::RELATION,
            FieldKind::List(_) => Self::LIST,
            FieldKind::Set(_) => Self::SET,
            FieldKind::Map { .. } => Self::MAP,
            FieldKind::Structured { .. } => Self::STRUCTURED,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) struct AggregateValueKindCode(u8);

impl AggregateValueKindCode {
    pub(in crate::db::executor) const ACCOUNT: Self = Self(0);
    pub(in crate::db::executor) const BLOB: Self = Self(1);
    pub(in crate::db::executor) const BOOL: Self = Self(2);
    pub(in crate::db::executor) const DATE: Self = Self(3);
    pub(in crate::db::executor) const DECIMAL: Self = Self(4);
    pub(in crate::db::executor) const DURATION: Self = Self(5);
    pub(in crate::db::executor) const ENUM: Self = Self(6);
    pub(in crate::db::executor) const FLOAT32: Self = Self(7);
    pub(in crate::db::executor) const FLOAT64: Self = Self(8);
    pub(in crate::db::executor) const INT64: Self = Self(9);
    pub(in crate::db::executor) const INT128: Self = Self(10);
    pub(in crate::db::executor) const INT_BIG: Self = Self(11);
    pub(in crate::db::executor) const LIST: Self = Self(12);
    pub(in crate::db::executor) const MAP: Self = Self(13);
    pub(in crate::db::executor) const NULL: Self = Self(14);
    pub(in crate::db::executor) const PRINCIPAL: Self = Self(15);
    pub(in crate::db::executor) const SUBACCOUNT: Self = Self(16);
    pub(in crate::db::executor) const TEXT: Self = Self(17);
    pub(in crate::db::executor) const TIMESTAMP: Self = Self(18);
    pub(in crate::db::executor) const NAT64: Self = Self(19);
    pub(in crate::db::executor) const NAT128: Self = Self(20);
    pub(in crate::db::executor) const NAT_BIG: Self = Self(21);
    pub(in crate::db::executor) const ULID: Self = Self(22);
    pub(in crate::db::executor) const UNIT: Self = Self(23);

    const fn from_value(value: &Value) -> Self {
        match value {
            Value::Account(_) => Self::ACCOUNT,
            Value::Blob(_) => Self::BLOB,
            Value::Bool(_) => Self::BOOL,
            Value::Date(_) => Self::DATE,
            Value::Decimal(_) => Self::DECIMAL,
            Value::Duration(_) => Self::DURATION,
            Value::Enum(_) => Self::ENUM,
            Value::Float32(_) => Self::FLOAT32,
            Value::Float64(_) => Self::FLOAT64,
            Value::Int64(_) => Self::INT64,
            Value::Int128(_) => Self::INT128,
            Value::IntBig(_) => Self::INT_BIG,
            Value::List(_) => Self::LIST,
            Value::Map(_) => Self::MAP,
            Value::Null => Self::NULL,
            Value::Principal(_) => Self::PRINCIPAL,
            Value::Subaccount(_) => Self::SUBACCOUNT,
            Value::Text(_) => Self::TEXT,
            Value::Timestamp(_) => Self::TIMESTAMP,
            Value::Nat64(_) => Self::NAT64,
            Value::Nat128(_) => Self::NAT128,
            Value::NatBig(_) => Self::NAT_BIG,
            Value::Ulid(_) => Self::ULID,
            Value::Unit => Self::UNIT,
        }
    }
}

impl AggregateFieldValueError {
    pub(in crate::db::executor) const fn field_value_type_mismatch(
        field_slot: FieldSlot,
        found: &Value,
    ) -> Self {
        Self::FieldValueTypeMismatch {
            slot_index: field_slot.index,
            expected: AggregateFieldKindCode::from_field_kind(&field_slot.kind),
            found: AggregateValueKindCode::from_value(found),
        }
    }

    // Map field-target extraction/comparison failures into taxonomy-correct
    // execution errors.
    pub(in crate::db::executor) fn into_internal_error(self) -> InternalError {
        match self {
            Self::UnknownField => InternalError::executor_unsupported(),
            Self::UnsupportedFieldKind { slot_index, kind } => {
                let _ = (slot_index, kind);
                InternalError::executor_unsupported()
            }
            Self::MissingFieldValue { slot_index } => {
                let _ = slot_index;
                InternalError::query_executor_invariant()
            }
            Self::FieldValueTypeMismatch {
                slot_index,
                expected,
                found,
            } => {
                let _ = (slot_index, expected, found);
                InternalError::query_executor_invariant()
            }
            Self::IncomparableFieldValues { left, right } => {
                let _ = (left, right);
                InternalError::query_executor_invariant()
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
/// Stable aggregate field projection slot resolved once at setup.
///
#[derive(Clone, Copy, Debug)]
pub(in crate::db::executor) struct FieldSlot {
    pub(in crate::db::executor) index: usize,
    pub(in crate::db::executor) kind: FieldKind,
}

// Build the canonical unknown-field error for aggregate field-slot resolution.
const fn unknown_aggregate_target_field(_target_field: &str) -> AggregateFieldValueError {
    AggregateFieldValueError::UnknownField
}

// Resolve one final field slot from already-known index/kind metadata and
// optionally enforce one capability gate over the declared field kind.
fn resolve_aggregate_target_slot(
    index: usize,
    _target_field: &str,
    kind: FieldKind,
    supports_kind: Option<fn(&FieldKind) -> bool>,
) -> Result<FieldSlot, AggregateFieldValueError> {
    if let Some(supports_kind) = supports_kind
        && !supports_kind(&kind)
    {
        return Err(AggregateFieldValueError::UnsupportedFieldKind {
            slot_index: index,
            kind: AggregateFieldKindCode::from_field_kind(&kind),
        });
    }

    Ok(FieldSlot { index, kind })
}

// Coerce one already-validated aggregate field payload into Decimal while
// preserving the canonical type-mismatch error shape for numeric terminals.
fn coerce_numeric_field_decimal_owned(
    _target_field: &str,
    field_slot: FieldSlot,
    value: Value,
) -> Result<Decimal, AggregateFieldValueError> {
    let Some(decimal) = coerce_numeric_decimal(&value) else {
        return Err(AggregateFieldValueError::field_value_type_mismatch(
            field_slot, &value,
        ));
    };

    Ok(decimal)
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
        (FieldKind::Int64, Value::Int64(left), Value::Int64(right)) => Some(left.cmp(right)),
        (FieldKind::Int128, Value::Int128(left), Value::Int128(right)) => Some(left.cmp(right)),
        (FieldKind::Nat64, Value::Nat64(left), Value::Nat64(right)) => Some(left.cmp(right)),
        (FieldKind::Nat128, Value::Nat128(left), Value::Nat128(right)) => Some(left.cmp(right)),
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
        return Err(unknown_aggregate_target_field(target_field));
    };

    resolve_aggregate_target_slot(
        index,
        target_field,
        field.kind(),
        Some(field_kind_supports_aggregate_ordering),
    )
}

/// Resolve one planner field slot into one orderable aggregate projection slot using planner-frozen field metadata.
pub(in crate::db::executor) fn resolve_orderable_aggregate_target_slot_from_planner_slot(
    field_slot: &PlannedFieldSlot,
) -> Result<FieldSlot, AggregateFieldValueError> {
    let target_field = field_slot.field();
    let Some(kind) = field_slot.kind() else {
        return Err(unknown_aggregate_target_field(target_field));
    };

    resolve_aggregate_target_slot(
        field_slot.index(),
        target_field,
        kind,
        Some(field_kind_supports_aggregate_ordering),
    )
}

/// Resolve one aggregate target field into a stable projection slot using structural model data.
#[cfg(test)]
pub(in crate::db::executor) fn resolve_any_aggregate_target_slot_from_fields(
    fields: &[FieldModel],
    target_field: &str,
) -> Result<FieldSlot, AggregateFieldValueError> {
    let Some((index, field)) = field_model_with_index(fields, target_field) else {
        return Err(unknown_aggregate_target_field(target_field));
    };

    resolve_aggregate_target_slot(index, target_field, field.kind(), None)
}

/// Resolve one planner field slot into one aggregate projection slot using planner-frozen field metadata.
pub(in crate::db::executor) fn resolve_any_aggregate_target_slot_from_planner_slot(
    field_slot: &PlannedFieldSlot,
) -> Result<FieldSlot, AggregateFieldValueError> {
    let target_field = field_slot.field();
    let Some(kind) = field_slot.kind() else {
        return Err(unknown_aggregate_target_field(target_field));
    };

    resolve_aggregate_target_slot(field_slot.index(), target_field, kind, None)
}

/// Resolve one numeric aggregate target field into a stable projection slot using structural model data.
#[cfg(test)]
pub(in crate::db::executor) fn resolve_numeric_aggregate_target_slot_from_fields(
    fields: &[FieldModel],
    target_field: &str,
) -> Result<FieldSlot, AggregateFieldValueError> {
    let Some((index, field)) = field_model_with_index(fields, target_field) else {
        return Err(unknown_aggregate_target_field(target_field));
    };

    resolve_aggregate_target_slot(
        index,
        target_field,
        field.kind(),
        Some(field_kind_supports_numeric_aggregation),
    )
}

/// Resolve one planner field slot into one numeric aggregate projection slot using planner-frozen field metadata.
pub(in crate::db::executor) fn resolve_numeric_aggregate_target_slot_from_planner_slot(
    field_slot: &PlannedFieldSlot,
) -> Result<FieldSlot, AggregateFieldValueError> {
    let target_field = field_slot.field();
    let Some(kind) = field_slot.kind() else {
        return Err(unknown_aggregate_target_field(target_field));
    };

    resolve_aggregate_target_slot(
        field_slot.index(),
        target_field,
        kind,
        Some(field_kind_supports_numeric_aggregation),
    )
}

/// Extract one field value from a slot reader and enforce the declared runtime field kind.
pub(in crate::db::executor) fn extract_orderable_field_value_with_slot_reader(
    _target_field: &str,
    field_slot: FieldSlot,
    read_slot: &mut dyn FnMut(usize) -> Option<Value>,
) -> Result<Value, AggregateFieldValueError> {
    let Some(value) = read_slot(field_slot.index) else {
        return Err(AggregateFieldValueError::MissingFieldValue {
            slot_index: field_slot.index,
        });
    };
    if !field_slot.kind.accepts_value(&value) {
        return Err(AggregateFieldValueError::field_value_type_mismatch(
            field_slot, &value,
        ));
    }

    Ok(value)
}

/// Extract one borrowed field value from a slot reader and enforce the
/// declared runtime field kind without cloning the underlying slot payload.
pub(in crate::db::executor) fn extract_orderable_field_value_with_slot_ref_reader<'a>(
    _target_field: &str,
    field_slot: FieldSlot,
    read_slot: &mut dyn FnMut(usize) -> Option<&'a Value>,
) -> Result<&'a Value, AggregateFieldValueError> {
    let Some(value) = read_slot(field_slot.index) else {
        return Err(AggregateFieldValueError::MissingFieldValue {
            slot_index: field_slot.index,
        });
    };
    if !field_slot.kind.accepts_value(value) {
        return Err(AggregateFieldValueError::field_value_type_mismatch(
            field_slot, value,
        ));
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
#[cfg(test)]
pub(in crate::db::executor) fn extract_numeric_field_decimal_with_slot_reader(
    target_field: &str,
    field_slot: FieldSlot,
    read_slot: &mut dyn FnMut(usize) -> Option<Value>,
) -> Result<Decimal, AggregateFieldValueError> {
    let value =
        extract_orderable_field_value_with_slot_reader(target_field, field_slot, read_slot)?;

    coerce_numeric_field_decimal_owned(target_field, field_slot, value)
}

/// Extract one numeric field value as `Decimal` from a borrowed slot reader
/// so aggregate streaming paths avoid cloning validated slot payloads.
pub(in crate::db::executor) fn extract_numeric_field_decimal_with_slot_ref_reader<'a>(
    target_field: &str,
    field_slot: FieldSlot,
    read_slot: &mut dyn FnMut(usize) -> Option<&'a Value>,
) -> Result<Decimal, AggregateFieldValueError> {
    let value =
        extract_orderable_field_value_with_slot_ref_reader(target_field, field_slot, read_slot)?;

    coerce_numeric_field_decimal_owned(target_field, field_slot, value.clone())
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

    coerce_numeric_field_decimal_owned(target_field, field_slot, value)
}

/// Compare two extracted field values using shared numeric ordering semantics
/// first, then strict same-variant ordering fallback.
pub(in crate::db::executor) fn compare_orderable_field_values(
    _target_field: &str,
    left: &Value,
    right: &Value,
) -> Result<Ordering, AggregateFieldValueError> {
    let Some(ordering) = compare_numeric_or_strict_order(left, right) else {
        return Err(AggregateFieldValueError::IncomparableFieldValues {
            left: AggregateValueKindCode::from_value(left),
            right: AggregateValueKindCode::from_value(right),
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
