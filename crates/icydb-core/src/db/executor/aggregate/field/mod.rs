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
/// field-value helpers.
///

#[derive(Clone, Debug)]
pub(in crate::db::executor) enum AggregateFieldValueError {
    UnknownField,
    UnsupportedFieldKind,
    MissingFieldValue,
    FieldValueTypeMismatch,
    IncomparableFieldValues,
    AcceptedContractUnavailable,
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
    runtime_shape: AggregateRuntimeValueShape,
}

impl AggregateFieldValueContract {
    const fn exact(runtime_kind: ValueTag) -> Self {
        Self {
            runtime_shape: AggregateRuntimeValueShape::Exact(runtime_kind),
        }
    }

    fn from_accepted_field_kind(kind: &AcceptedFieldKind) -> Self {
        use AcceptedFieldKind as Accepted;
        use ValueTag as Runtime;

        match kind {
            Accepted::Account => Self::exact(Runtime::Account),
            Accepted::Blob { .. } => Self::exact(Runtime::Blob),
            Accepted::Bool => Self::exact(Runtime::Bool),
            Accepted::Date => Self::exact(Runtime::Date),
            Accepted::Decimal { .. } => Self::exact(Runtime::Decimal),
            Accepted::Duration => Self::exact(Runtime::Duration),
            Accepted::Enum { .. } => Self::exact(Runtime::Enum),
            Accepted::Float32 => Self::exact(Runtime::Float32),
            Accepted::Float64 => Self::exact(Runtime::Float64),
            Accepted::Int8 | Accepted::Int16 | Accepted::Int32 | Accepted::Int64 => {
                Self::exact(Runtime::Int64)
            }
            Accepted::Int128 => Self::exact(Runtime::Int128),
            Accepted::IntBig { .. } => Self::exact(Runtime::IntBig),
            Accepted::Principal => Self::exact(Runtime::Principal),
            Accepted::Subaccount => Self::exact(Runtime::Subaccount),
            Accepted::Text { .. } => Self::exact(Runtime::Text),
            Accepted::Timestamp => Self::exact(Runtime::Timestamp),
            Accepted::Nat8 | Accepted::Nat16 | Accepted::Nat32 | Accepted::Nat64 => {
                Self::exact(Runtime::Nat64)
            }
            Accepted::Nat128 => Self::exact(Runtime::Nat128),
            Accepted::NatBig { .. } => Self::exact(Runtime::NatBig),
            Accepted::Ulid => Self::exact(Runtime::Ulid),
            Accepted::Unit => Self::exact(Runtime::Unit),
            Accepted::U256 => Self::exact(Runtime::U256),
            Accepted::Relation { key_kind, .. } => Self::from_accepted_field_kind(key_kind),
            Accepted::List(_) | Accepted::Set(_) => Self::exact(Runtime::List),
            Accepted::Map { .. } => Self::exact(Runtime::Map),
            Accepted::Composite { .. } => Self {
                runtime_shape: AggregateRuntimeValueShape::Structured,
            },
        }
    }

    fn accepts_value(self, value: &Value) -> bool {
        self.runtime_shape.accepts_value(value)
    }
}

impl AggregateFieldValueError {
    // Preserve unsupported-target versus execution-invariant classification.
    pub(in crate::db::executor) fn into_internal_error(self) -> InternalError {
        match self {
            Self::UnknownField | Self::UnsupportedFieldKind => {
                InternalError::executor_unsupported()
            }
            Self::MissingFieldValue
            | Self::AcceptedContractUnavailable
            | Self::FieldValueTypeMismatch
            | Self::IncomparableFieldValues => InternalError::query_executor_invariant(),
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
            AggregateFieldValueError::AcceptedContractUnavailable
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
        return Err(AggregateFieldValueError::UnsupportedFieldKind);
    }

    Ok(FieldSlot { index, contract })
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

/// Extract one non-NULL aggregate input and enforce its declared runtime field kind.
/// Accepted row validation owns nullability; NULL contributes no aggregate value.
pub(in crate::db::executor) fn extract_non_null_aggregate_field_value_with_slot_reader(
    field_slot: FieldSlot,
    read_slot: &mut dyn FnMut(usize) -> Option<Value>,
) -> Result<Option<Value>, AggregateFieldValueError> {
    let Some(value) = read_slot(field_slot.index) else {
        return Err(AggregateFieldValueError::MissingFieldValue);
    };
    if matches!(value, Value::Null) {
        return Ok(None);
    }
    if !field_slot.contract.accepts_value(&value) {
        return Err(AggregateFieldValueError::FieldValueTypeMismatch);
    }

    Ok(Some(value))
}

/// Compare two extracted field values using shared numeric ordering semantics
/// first, then strict same-variant ordering fallback.
pub(in crate::db::executor) fn compare_orderable_field_values(
    left: &Value,
    right: &Value,
) -> Result<Ordering, AggregateFieldValueError> {
    let Some(ordering) = compare_numeric_or_strict_order(left, right) else {
        return Err(AggregateFieldValueError::IncomparableFieldValues);
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

// Exhaustive cache-retention coverage; new owned fields require accounting.
crate::retained::retained_copy!(FieldSlot);
