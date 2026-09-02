//! Module: executor::aggregate::field
//! Responsibility: aggregate field-slot resolution and field-value extraction/comparison helpers.
//! Does not own: aggregate route planning decisions.
//! Boundary: field-target aggregate helper surface used by aggregate executors.

use super::contracts::{AggregateKind, FieldSlot as PlannedFieldSlot};
use crate::{
    db::{
        executor::aggregate::capability::{
            accepted_field_kind_supports_aggregate_ordering, accepted_field_kind_supports_average,
            accepted_field_kind_supports_sum,
        },
        numeric::compare_numeric_or_strict_order,
        schema::AcceptedFieldKind,
    },
    error::InternalError,
    value::{Value, ValueTag},
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
        found: ValueTag,
    },

    IncomparableFieldValues {
        left: ValueTag,
        right: ValueTag,
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
    pub(in crate::db::executor) const U256: Self = Self(32);
}

// Compact runtime representation selected from accepted schema authority.
// Full bounds, recursive shape, and enum-ID validation already happen at the
// accepted row boundary; aggregate execution only guards the decoded top-level
// representation and retains the direct comparison strategy it needs.
#[derive(Clone, Copy, Debug)]
enum AggregateRuntimeValueShape {
    Exact(ValueTag),
    Structured,
}

impl AggregateRuntimeValueShape {
    fn accepts_value(self, value: &Value) -> bool {
        match (self, value) {
            (Self::Exact(expected), value) => expected == value.canonical_tag(),
            (Self::Structured, Value::List(_) | Value::Map(_)) => true,
            _ => false,
        }
    }

    fn direct_compare(self, left: &Value, right: &Value) -> Option<Ordering> {
        match (self, left, right) {
            (Self::Exact(ValueTag::Decimal), Value::Decimal(left), Value::Decimal(right)) => {
                left.partial_cmp(right)
            }
            (Self::Exact(ValueTag::Float32), Value::Float32(left), Value::Float32(right)) => {
                left.get().partial_cmp(&right.get())
            }
            (Self::Exact(ValueTag::Float64), Value::Float64(left), Value::Float64(right)) => {
                left.get().partial_cmp(&right.get())
            }
            (Self::Exact(ValueTag::Int64), Value::Int64(left), Value::Int64(right)) => {
                Some(left.cmp(right))
            }
            (Self::Exact(ValueTag::Int128), Value::Int128(left), Value::Int128(right)) => {
                Some(left.cmp(right))
            }
            (Self::Exact(ValueTag::Nat64), Value::Nat64(left), Value::Nat64(right)) => {
                Some(left.cmp(right))
            }
            (Self::Exact(ValueTag::Nat128), Value::Nat128(left), Value::Nat128(right)) => {
                Some(left.cmp(right))
            }
            (Self::Exact(ValueTag::U256), Value::U256(left), Value::U256(right)) => {
                Some(left.cmp(right))
            }
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
    const fn exact(diagnostic_kind: AggregateFieldKindCode, runtime_kind: ValueTag) -> Self {
        Self {
            diagnostic_kind,
            runtime_shape: AggregateRuntimeValueShape::Exact(runtime_kind),
        }
    }

    fn from_accepted_field_kind(kind: &AcceptedFieldKind) -> Self {
        use AcceptedFieldKind as Accepted;
        use AggregateFieldKindCode as Field;
        use ValueTag as Runtime;

        match kind {
            Accepted::Account => Self::exact(Field::ACCOUNT, Runtime::Account),
            Accepted::Blob { .. } => Self::exact(Field::BLOB, Runtime::Blob),
            Accepted::Bool => Self::exact(Field::BOOL, Runtime::Bool),
            Accepted::Date => Self::exact(Field::DATE, Runtime::Date),
            Accepted::Decimal { .. } => Self::exact(Field::DECIMAL, Runtime::Decimal),
            Accepted::Duration => Self::exact(Field::DURATION, Runtime::Duration),
            Accepted::Enum { .. } => Self::exact(Field::ENUM, Runtime::Enum),
            Accepted::Float32 => Self::exact(Field::FLOAT32, Runtime::Float32),
            Accepted::Float64 => Self::exact(Field::FLOAT64, Runtime::Float64),
            Accepted::Int8 => Self::exact(Field::INT8, Runtime::Int64),
            Accepted::Int16 => Self::exact(Field::INT16, Runtime::Int64),
            Accepted::Int32 => Self::exact(Field::INT32, Runtime::Int64),
            Accepted::Int64 => Self::exact(Field::INT64, Runtime::Int64),
            Accepted::Int128 => Self::exact(Field::INT128, Runtime::Int128),
            Accepted::IntBig { .. } => Self::exact(Field::INT_BIG, Runtime::IntBig),
            Accepted::Principal => Self::exact(Field::PRINCIPAL, Runtime::Principal),
            Accepted::Subaccount => Self::exact(Field::SUBACCOUNT, Runtime::Subaccount),
            Accepted::Text { .. } => Self::exact(Field::TEXT, Runtime::Text),
            Accepted::Timestamp => Self::exact(Field::TIMESTAMP, Runtime::Timestamp),
            Accepted::Nat8 => Self::exact(Field::NAT8, Runtime::Nat64),
            Accepted::Nat16 => Self::exact(Field::NAT16, Runtime::Nat64),
            Accepted::Nat32 => Self::exact(Field::NAT32, Runtime::Nat64),
            Accepted::Nat64 => Self::exact(Field::NAT64, Runtime::Nat64),
            Accepted::Nat128 => Self::exact(Field::NAT128, Runtime::Nat128),
            Accepted::NatBig { .. } => Self::exact(Field::NAT_BIG, Runtime::NatBig),
            Accepted::Ulid => Self::exact(Field::ULID, Runtime::Ulid),
            Accepted::Unit => Self::exact(Field::UNIT, Runtime::Unit),
            Accepted::U256 => Self::exact(Field::U256, Runtime::U256),
            Accepted::Relation { key_kind, .. } => {
                let key_contract = Self::from_accepted_field_kind(key_kind);
                Self {
                    diagnostic_kind: Field::RELATION,
                    runtime_shape: key_contract.runtime_shape,
                }
            }
            Accepted::List(_) => Self::exact(Field::LIST, Runtime::List),
            Accepted::Set(_) => Self::exact(Field::SET, Runtime::List),
            Accepted::Map { .. } => Self::exact(Field::MAP, Runtime::Map),
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
            found: found.canonical_tag(),
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

/// Resolve one planner field slot into one aggregate projection slot using planner-frozen field metadata.
pub(in crate::db::executor) fn resolve_any_aggregate_target_slot_from_planner_slot(
    field_slot: &PlannedFieldSlot,
) -> Result<FieldSlot, AggregateFieldValueError> {
    let accepted_kind = accepted_kind_from_planner_slot(field_slot)?;

    resolve_aggregate_target_slot(field_slot.index(), accepted_kind, None)
}

/// Resolve one planner field slot into one SUM projection slot using planner-frozen field metadata.
pub(in crate::db::executor) fn resolve_sum_aggregate_target_slot_from_planner_slot(
    field_slot: &PlannedFieldSlot,
) -> Result<FieldSlot, AggregateFieldValueError> {
    let accepted_kind = accepted_kind_from_planner_slot(field_slot)?;

    resolve_aggregate_target_slot(
        field_slot.index(),
        accepted_kind,
        Some(accepted_field_kind_supports_sum),
    )
}

/// Resolve one planner field slot into one AVG projection slot using planner-frozen field metadata.
pub(in crate::db::executor) fn resolve_average_aggregate_target_slot_from_planner_slot(
    field_slot: &PlannedFieldSlot,
) -> Result<FieldSlot, AggregateFieldValueError> {
    let accepted_kind = accepted_kind_from_planner_slot(field_slot)?;

    resolve_aggregate_target_slot(
        field_slot.index(),
        accepted_kind,
        Some(accepted_field_kind_supports_average),
    )
}

/// Resolve one planner field slot through the capability required by its
/// aggregate family.
pub(in crate::db::executor) fn resolve_aggregate_target_slot_from_planner_slot(
    kind: AggregateKind,
    field_slot: &PlannedFieldSlot,
) -> Result<FieldSlot, AggregateFieldValueError> {
    match kind {
        AggregateKind::Sum => resolve_sum_aggregate_target_slot_from_planner_slot(field_slot),
        AggregateKind::Avg => resolve_average_aggregate_target_slot_from_planner_slot(field_slot),
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

// Extract one field value from one already-decoded retained slot and enforce
// the declared runtime field kind without rebuilding a slot-reader closure at
// each retained-slot callsite.

// Extract one numeric field value as `Decimal` from one already-decoded
// retained slot without rebuilding a one-shot slot-reader closure at each
// retained-slot numeric callsite.

/// Compare two extracted field values using shared numeric ordering semantics
/// first, then strict same-variant ordering fallback.
pub(in crate::db::executor) fn compare_orderable_field_values(
    left: &Value,
    right: &Value,
) -> Result<Ordering, AggregateFieldValueError> {
    let Some(ordering) = compare_numeric_or_strict_order(left, right) else {
        return Err(AggregateFieldValueError::IncomparableFieldValues {
            left: left.canonical_tag(),
            right: right.canonical_tag(),
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
