//! Module: executor::aggregate::field
//! Responsibility: aggregate field-slot resolution and field-value extraction/comparison helpers.
//! Does not own: aggregate route planning decisions.
//! Boundary: field-target aggregate helper surface used by aggregate executors.

use super::contracts::{AggregateKind, FieldSlot as PlannedFieldSlot};
#[cfg(test)]
use crate::model::field::{FieldKind, FieldModel};
use crate::{
    db::{
        direction::Direction,
        executor::aggregate::capability::{
            accepted_field_kind_supports_aggregate_ordering,
            accepted_field_kind_supports_numeric_aggregation,
        },
        numeric::{coerce_numeric_decimal, compare_numeric_or_strict_order},
        schema::AcceptedFieldKind,
    },
    error::InternalError,
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

    AcceptedContractUnavailable {
        slot_index: usize,
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

// Compact runtime representation selected from accepted schema authority.
// Full bounds, recursive shape, and enum-ID validation already happen at the
// accepted row boundary; aggregate execution only guards the decoded top-level
// representation and retains the direct comparison strategy it needs.
#[derive(Clone, Copy, Debug)]
enum AggregateRuntimeValueShape {
    Exact(AggregateValueKindCode),
    Structured,
}

impl AggregateRuntimeValueShape {
    fn accepts_value(self, value: &Value) -> bool {
        match (self, value) {
            (Self::Exact(expected), value) => expected == AggregateValueKindCode::from_value(value),
            (Self::Structured, Value::List(_) | Value::Map(_)) => true,
            _ => false,
        }
    }

    fn direct_compare(self, left: &Value, right: &Value) -> Option<Ordering> {
        match (self, left, right) {
            (
                Self::Exact(AggregateValueKindCode::DECIMAL),
                Value::Decimal(left),
                Value::Decimal(right),
            ) => left.partial_cmp(right),
            (
                Self::Exact(AggregateValueKindCode::FLOAT32),
                Value::Float32(left),
                Value::Float32(right),
            ) => left.get().partial_cmp(&right.get()),
            (
                Self::Exact(AggregateValueKindCode::FLOAT64),
                Value::Float64(left),
                Value::Float64(right),
            ) => left.get().partial_cmp(&right.get()),
            (
                Self::Exact(AggregateValueKindCode::INT64),
                Value::Int64(left),
                Value::Int64(right),
            ) => Some(left.cmp(right)),
            (
                Self::Exact(AggregateValueKindCode::INT128),
                Value::Int128(left),
                Value::Int128(right),
            ) => Some(left.cmp(right)),
            (
                Self::Exact(AggregateValueKindCode::NAT64),
                Value::Nat64(left),
                Value::Nat64(right),
            ) => Some(left.cmp(right)),
            (
                Self::Exact(AggregateValueKindCode::NAT128),
                Value::Nat128(left),
                Value::Nat128(right),
            ) => Some(left.cmp(right)),
            _ => None,
        }
    }
}

// Executor-owned projection of one accepted field contract. Keeping this
// projection copyable avoids cloning recursive accepted kinds into every
// per-group reducer state.
#[derive(Clone, Copy, Debug)]
struct AggregateFieldValueContract {
    diagnostic_kind: AggregateFieldKindCode,
    runtime_shape: AggregateRuntimeValueShape,
}

impl AggregateFieldValueContract {
    const fn exact(
        diagnostic_kind: AggregateFieldKindCode,
        runtime_kind: AggregateValueKindCode,
    ) -> Self {
        Self {
            diagnostic_kind,
            runtime_shape: AggregateRuntimeValueShape::Exact(runtime_kind),
        }
    }

    fn from_accepted_field_kind(kind: &AcceptedFieldKind) -> Self {
        use AcceptedFieldKind as Accepted;
        use AggregateFieldKindCode as Field;
        use AggregateValueKindCode as Runtime;

        match kind {
            Accepted::Account => Self::exact(Field::ACCOUNT, Runtime::ACCOUNT),
            Accepted::Blob { .. } => Self::exact(Field::BLOB, Runtime::BLOB),
            Accepted::Bool => Self::exact(Field::BOOL, Runtime::BOOL),
            Accepted::Date => Self::exact(Field::DATE, Runtime::DATE),
            Accepted::Decimal { .. } => Self::exact(Field::DECIMAL, Runtime::DECIMAL),
            Accepted::Duration => Self::exact(Field::DURATION, Runtime::DURATION),
            Accepted::Enum { .. } => Self::exact(Field::ENUM, Runtime::ENUM),
            Accepted::Float32 => Self::exact(Field::FLOAT32, Runtime::FLOAT32),
            Accepted::Float64 => Self::exact(Field::FLOAT64, Runtime::FLOAT64),
            Accepted::Int8 => Self::exact(Field::INT8, Runtime::INT64),
            Accepted::Int16 => Self::exact(Field::INT16, Runtime::INT64),
            Accepted::Int32 => Self::exact(Field::INT32, Runtime::INT64),
            Accepted::Int64 => Self::exact(Field::INT64, Runtime::INT64),
            Accepted::Int128 => Self::exact(Field::INT128, Runtime::INT128),
            Accepted::IntBig { .. } => Self::exact(Field::INT_BIG, Runtime::INT_BIG),
            Accepted::Principal => Self::exact(Field::PRINCIPAL, Runtime::PRINCIPAL),
            Accepted::Subaccount => Self::exact(Field::SUBACCOUNT, Runtime::SUBACCOUNT),
            Accepted::Text { .. } => Self::exact(Field::TEXT, Runtime::TEXT),
            Accepted::Timestamp => Self::exact(Field::TIMESTAMP, Runtime::TIMESTAMP),
            Accepted::Nat8 => Self::exact(Field::NAT8, Runtime::NAT64),
            Accepted::Nat16 => Self::exact(Field::NAT16, Runtime::NAT64),
            Accepted::Nat32 => Self::exact(Field::NAT32, Runtime::NAT64),
            Accepted::Nat64 => Self::exact(Field::NAT64, Runtime::NAT64),
            Accepted::Nat128 => Self::exact(Field::NAT128, Runtime::NAT128),
            Accepted::NatBig { .. } => Self::exact(Field::NAT_BIG, Runtime::NAT_BIG),
            Accepted::Ulid => Self::exact(Field::ULID, Runtime::ULID),
            Accepted::Unit => Self::exact(Field::UNIT, Runtime::UNIT),
            Accepted::Relation { key_kind, .. } => {
                let key_contract = Self::from_accepted_field_kind(key_kind);
                Self {
                    diagnostic_kind: Field::RELATION,
                    runtime_shape: key_contract.runtime_shape,
                }
            }
            Accepted::List(_) => Self::exact(Field::LIST, Runtime::LIST),
            Accepted::Set(_) => Self::exact(Field::SET, Runtime::LIST),
            Accepted::Map { .. } => Self::exact(Field::MAP, Runtime::MAP),
            Accepted::Composite { .. } => Self {
                diagnostic_kind: Field::STRUCTURED,
                runtime_shape: AggregateRuntimeValueShape::Structured,
            },
        }
    }

    fn accepts_value(self, value: &Value) -> bool {
        self.runtime_shape.accepts_value(value)
    }
}

impl AggregateFieldValueError {
    pub(in crate::db::executor) const fn field_value_type_mismatch(
        field_slot: FieldSlot,
        found: &Value,
    ) -> Self {
        Self::FieldValueTypeMismatch {
            slot_index: field_slot.index,
            expected: field_slot.contract.diagnostic_kind,
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
            Self::MissingFieldValue { slot_index }
            | Self::AcceptedContractUnavailable { slot_index } => {
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
    contract: AggregateFieldValueContract,
}

#[cfg(test)]
impl FieldSlot {
    pub(in crate::db::executor) fn from_test_model_kind(index: usize, kind: FieldKind) -> Self {
        let accepted = AcceptedFieldKind::from_model_kind(kind);
        Self {
            index,
            contract: AggregateFieldValueContract::from_accepted_field_kind(&accepted),
        }
    }

    const fn diagnostic_kind(self) -> AggregateFieldKindCode {
        self.contract.diagnostic_kind
    }
}

// Build the canonical unknown-field error for aggregate field-slot resolution.
const fn unknown_aggregate_target_field() -> AggregateFieldValueError {
    AggregateFieldValueError::UnknownField
}

// Require accepted authority for a known planner slot while preserving the
// unsupported-field taxonomy for an unresolved slot.
fn accepted_kind_from_planner_slot(
    field_slot: &PlannedFieldSlot,
) -> Result<&AcceptedFieldKind, AggregateFieldValueError> {
    field_slot.accepted_kind().ok_or_else(|| {
        if field_slot.is_unresolved() {
            unknown_aggregate_target_field()
        } else {
            AggregateFieldValueError::AcceptedContractUnavailable {
                slot_index: field_slot.index(),
            }
        }
    })
}

// Resolve one final field slot from already-known index/kind metadata and
// optionally enforce one capability gate over the declared field kind.
fn resolve_aggregate_target_slot(
    index: usize,
    accepted_kind: &AcceptedFieldKind,
    supports_kind: Option<fn(&AcceptedFieldKind) -> bool>,
) -> Result<FieldSlot, AggregateFieldValueError> {
    let contract = AggregateFieldValueContract::from_accepted_field_kind(accepted_kind);
    if let Some(supports_kind) = supports_kind
        && !supports_kind(accepted_kind)
    {
        return Err(AggregateFieldValueError::UnsupportedFieldKind {
            slot_index: index,
            kind: contract.diagnostic_kind,
        });
    }

    Ok(FieldSlot { index, contract })
}

// Coerce one already-validated aggregate field payload into Decimal while
// preserving the canonical type-mismatch error shape for numeric terminals.
fn coerce_numeric_field_decimal_owned(
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

/// Resolve one orderable aggregate target field into a stable projection slot using structural model data.
#[cfg(test)]
pub(in crate::db::executor) fn resolve_orderable_aggregate_target_slot_from_fields(
    fields: &[FieldModel],
    target_field: &str,
) -> Result<FieldSlot, AggregateFieldValueError> {
    let Some((index, field)) = field_model_with_index(fields, target_field) else {
        return Err(unknown_aggregate_target_field());
    };

    resolve_aggregate_target_slot(
        index,
        &AcceptedFieldKind::from_model_kind(field.kind()),
        Some(accepted_field_kind_supports_aggregate_ordering),
    )
}

/// Resolve one planner field slot into one orderable aggregate projection slot using planner-frozen field metadata.
pub(in crate::db::executor) fn resolve_orderable_aggregate_target_slot_from_planner_slot(
    field_slot: &PlannedFieldSlot,
) -> Result<FieldSlot, AggregateFieldValueError> {
    let accepted_kind = accepted_kind_from_planner_slot(field_slot)?;

    resolve_aggregate_target_slot(
        field_slot.index(),
        accepted_kind,
        Some(accepted_field_kind_supports_aggregate_ordering),
    )
}

/// Resolve one aggregate target field into a stable projection slot using structural model data.
#[cfg(test)]
pub(in crate::db::executor) fn resolve_any_aggregate_target_slot_from_fields(
    fields: &[FieldModel],
    target_field: &str,
) -> Result<FieldSlot, AggregateFieldValueError> {
    let Some((index, field)) = field_model_with_index(fields, target_field) else {
        return Err(unknown_aggregate_target_field());
    };

    resolve_aggregate_target_slot(
        index,
        &AcceptedFieldKind::from_model_kind(field.kind()),
        None,
    )
}

/// Resolve one planner field slot into one aggregate projection slot using planner-frozen field metadata.
pub(in crate::db::executor) fn resolve_any_aggregate_target_slot_from_planner_slot(
    field_slot: &PlannedFieldSlot,
) -> Result<FieldSlot, AggregateFieldValueError> {
    let accepted_kind = accepted_kind_from_planner_slot(field_slot)?;

    resolve_aggregate_target_slot(field_slot.index(), accepted_kind, None)
}

/// Resolve one numeric aggregate target field into a stable projection slot using structural model data.
#[cfg(test)]
pub(in crate::db::executor) fn resolve_numeric_aggregate_target_slot_from_fields(
    fields: &[FieldModel],
    target_field: &str,
) -> Result<FieldSlot, AggregateFieldValueError> {
    let Some((index, field)) = field_model_with_index(fields, target_field) else {
        return Err(unknown_aggregate_target_field());
    };

    resolve_aggregate_target_slot(
        index,
        &AcceptedFieldKind::from_model_kind(field.kind()),
        Some(accepted_field_kind_supports_numeric_aggregation),
    )
}

/// Resolve one planner field slot into one numeric aggregate projection slot using planner-frozen field metadata.
pub(in crate::db::executor) fn resolve_numeric_aggregate_target_slot_from_planner_slot(
    field_slot: &PlannedFieldSlot,
) -> Result<FieldSlot, AggregateFieldValueError> {
    let accepted_kind = accepted_kind_from_planner_slot(field_slot)?;

    resolve_aggregate_target_slot(
        field_slot.index(),
        accepted_kind,
        Some(accepted_field_kind_supports_numeric_aggregation),
    )
}

/// Resolve one planner field slot through the capability required by its
/// aggregate family.
pub(in crate::db::executor) fn resolve_aggregate_target_slot_from_planner_slot(
    kind: AggregateKind,
    field_slot: &PlannedFieldSlot,
) -> Result<FieldSlot, AggregateFieldValueError> {
    match kind {
        AggregateKind::Sum | AggregateKind::Avg => {
            resolve_numeric_aggregate_target_slot_from_planner_slot(field_slot)
        }
        AggregateKind::Min | AggregateKind::Max => {
            resolve_orderable_aggregate_target_slot_from_planner_slot(field_slot)
        }
        AggregateKind::Count
        | AggregateKind::Exists
        | AggregateKind::First
        | AggregateKind::Last => resolve_any_aggregate_target_slot_from_planner_slot(field_slot),
    }
}

/// Extract one field value from a slot reader and enforce the declared runtime field kind.
pub(in crate::db::executor) fn extract_orderable_field_value_with_slot_reader(
    field_slot: FieldSlot,
    read_slot: &mut dyn FnMut(usize) -> Option<Value>,
) -> Result<Value, AggregateFieldValueError> {
    let Some(value) = read_slot(field_slot.index) else {
        return Err(AggregateFieldValueError::MissingFieldValue {
            slot_index: field_slot.index,
        });
    };
    if !field_slot.contract.accepts_value(&value) {
        return Err(AggregateFieldValueError::field_value_type_mismatch(
            field_slot, &value,
        ));
    }

    Ok(value)
}

/// Extract one borrowed field value from a slot reader and enforce the
/// declared runtime field kind without cloning the underlying slot payload.
pub(in crate::db::executor) fn extract_orderable_field_value_with_slot_ref_reader<'a>(
    field_slot: FieldSlot,
    read_slot: &mut dyn FnMut(usize) -> Option<&'a Value>,
) -> Result<&'a Value, AggregateFieldValueError> {
    let Some(value) = read_slot(field_slot.index) else {
        return Err(AggregateFieldValueError::MissingFieldValue {
            slot_index: field_slot.index,
        });
    };
    if !field_slot.contract.accepts_value(value) {
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
    field_slot: FieldSlot,
    decoded_value: Option<Value>,
) -> Result<Value, AggregateFieldValueError> {
    let mut decoded_value = decoded_value;

    extract_orderable_field_value_with_slot_reader(field_slot, &mut |_| decoded_value.take())
}

/// Extract one numeric field value as `Decimal` from a slot reader for aggregate arithmetic.
#[cfg(test)]
pub(in crate::db::executor) fn extract_numeric_field_decimal_with_slot_reader(
    field_slot: FieldSlot,
    read_slot: &mut dyn FnMut(usize) -> Option<Value>,
) -> Result<Decimal, AggregateFieldValueError> {
    let value = extract_orderable_field_value_with_slot_reader(field_slot, read_slot)?;

    coerce_numeric_field_decimal_owned(field_slot, value)
}

/// Extract one numeric field value as `Decimal` from a borrowed slot reader
/// so aggregate streaming paths avoid cloning validated slot payloads.
pub(in crate::db::executor) fn extract_numeric_field_decimal_with_slot_ref_reader<'a>(
    field_slot: FieldSlot,
    read_slot: &mut dyn FnMut(usize) -> Option<&'a Value>,
) -> Result<Decimal, AggregateFieldValueError> {
    let value = extract_orderable_field_value_with_slot_ref_reader(field_slot, read_slot)?;

    coerce_numeric_field_decimal_owned(field_slot, value.clone())
}

// Extract one numeric field value as `Decimal` from one already-decoded
// retained slot without rebuilding a one-shot slot-reader closure at each
// retained-slot numeric callsite.
pub(in crate::db::executor) fn extract_numeric_field_decimal_from_decoded_slot(
    field_slot: FieldSlot,
    decoded_value: Option<Value>,
) -> Result<Decimal, AggregateFieldValueError> {
    let value = extract_orderable_field_value_from_decoded_slot(field_slot, decoded_value)?;

    coerce_numeric_field_decimal_owned(field_slot, value)
}

/// Compare two extracted field values using shared numeric ordering semantics
/// first, then strict same-variant ordering fallback.
pub(in crate::db::executor) fn compare_orderable_field_values(
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
    field_slot: FieldSlot,
    left: &Value,
    right: &Value,
) -> Result<Ordering, AggregateFieldValueError> {
    if let Some(ordering) = field_slot
        .contract
        .runtime_shape
        .direct_compare(left, right)
    {
        return Ok(ordering);
    }

    compare_orderable_field_values(left, right)
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
