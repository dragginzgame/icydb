//! Module: index::key::ordered
//! Responsibility: canonical component encoding where byte order matches value order.
//! Does not own: full index-key framing or index-store traversal.
//! Boundary: used by index-key build/predicate compile/range lowering.

mod error;
mod normalize;
mod segments;
mod semantics;

#[cfg(test)]
mod admission_tests;

#[cfg(test)]
use crate::db::key_taxonomy::PrimaryKeyComponent;
#[cfg(test)]
use crate::db::numeric::compare_numeric_or_strict_order;
use crate::{
    db::{
        index::key::ordered::semantics::OrderedEncode, query::construction::ConstructionBudget,
        schema::UNIT_ENUM_EQUALITY_KEY_BYTES,
    },
    error::InternalError,
    value::{Value, ValueTag},
};
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;
#[cfg(test)]
use std::cmp::Ordering;

pub(crate) use error::OrderedValueEncodeError;

const NEGATIVE_MARKER: u8 = 0x00;
const ZERO_MARKER: u8 = 0x01;
const POSITIVE_MARKER: u8 = 0x02;

///
/// EncodedValue
///
/// Cached canonical index-component bytes for one logical `Value`. This wrapper
/// stores only the encoded bytes so planning/execution callsites can avoid
/// retaining cloned semantic values after lowering.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct EncodedValue {
    encoded: Vec<u8>,
}

impl EncodedValue {
    /// Wrap bytes produced by another accepted canonical component encoder.
    #[must_use]
    pub(in crate::db) const fn from_canonical_bytes(encoded: Vec<u8>) -> Self {
        Self { encoded }
    }

    /// Encode a value once into canonical index-component bytes.
    pub(crate) fn try_new(raw: &Value) -> Result<Self, OrderedValueEncodeError> {
        let encoded = encode_canonical_index_component(raw)?;

        Ok(Self { encoded })
    }

    /// Encode a borrowed value into this cached wrapper.
    pub(crate) fn try_from_ref(raw: &Value) -> Result<Self, OrderedValueEncodeError> {
        Self::try_new(raw)
    }

    #[must_use]
    pub(crate) const fn encoded(&self) -> &[u8] {
        self.encoded.as_slice()
    }

    /// Consume this cached wrapper into the owned encoded component bytes.
    pub(crate) fn into_bytes(self) -> Vec<u8> {
        self.encoded
    }
}

impl AsRef<[u8]> for EncodedValue {
    fn as_ref(&self) -> &[u8] {
        self.encoded()
    }
}

/// Compare two semantic index-component values under the index ordering contract.
///
/// Contract:
/// - same-variant component values delegate to shared numeric-or-strict
///   comparator authority
/// - mixed-variant values fall back to canonical key ordering for deterministic
///   cross-kind ordering in test/support surfaces
#[must_use]
#[cfg(test)]
pub(crate) fn compare_index_component_values(left: &Value, right: &Value) -> Ordering {
    if std::mem::discriminant(left) == std::mem::discriminant(right)
        && let Some(ordering) = compare_numeric_or_strict_order(left, right)
    {
        return ordering;
    }

    Value::canonical_cmp_key(left, right)
}

/// Encode one scalar index component so lexicographic byte order matches
/// canonical `Value` order for supported primitive variants.
pub(crate) fn encode_canonical_index_component(
    value: &Value,
) -> Result<Vec<u8>, OrderedValueEncodeError> {
    let capacity = component_capacity(value)?;
    // Reserve once, then emit the canonical tag and unchanged ordered payload.
    let mut out = Vec::with_capacity(capacity);
    out.push(value.canonical_tag().to_u8());
    encode_component_payload(&mut out, value)?;

    Ok(out)
}

/// Admit query operand construction without installing new write/replay limits.
/// Both consumers use the same capacity and byte encoder, never a second format.
pub(in crate::db) fn admit_query_index_component(
    value: &Value,
    budget: &dyn ConstructionBudget,
) -> Result<(), InternalError> {
    let capacity = if matches!(value, Value::Enum(_)) {
        // Accepted unit-enum bytes have their own catalog-native encoder.
        // Its validation remains authoritative; this only admits output backing.
        UNIT_ENUM_EQUALITY_KEY_BYTES
    } else {
        match component_capacity(value) {
            Ok(capacity) => capacity,
            // Preserve unsupported/invalid-value handling at the caller's
            // semantic boundary. The encoder rejects before allocating.
            Err(_) => return Ok(()),
        }
    };
    budget.charge(Resource::TemporaryBytes, capacity as u64)?;
    budget.charge(Resource::PredicateExpressionSteps, capacity as u64)
}

// Capacity discovery reads only scalar metadata, not text bytes or bigint limbs.
// Escaping and decimal digits use conservative bounds; no sizing buffer is built.
fn component_capacity(value: &Value) -> Result<usize, OrderedValueEncodeError> {
    let payload = match value {
        Value::Unit => 0,
        Value::Bool(_) => 1,
        Value::Date(_) | Value::Float32(_) => 4,
        Value::Duration(_)
        | Value::Timestamp(_)
        | Value::Int64(_)
        | Value::Nat64(_)
        | Value::Float64(_) => 8,
        Value::Int128(_) | Value::Nat128(_) | Value::Ulid(_) => 16,
        Value::Subaccount(_) | Value::U256(_) => 32,
        Value::Account(_) => segments::ACCOUNT_PAYLOAD_BYTES,
        Value::Decimal(_) => normalize::DECIMAL_PAYLOAD_MAX_BYTES,
        Value::Text(text) => text
            .len()
            .checked_mul(2)
            .and_then(|len| len.checked_add(2))
            .ok_or(OrderedValueEncodeError::SegmentTooLarge)?,
        Value::Principal(principal) => principal.as_slice().len() * 2 + 2,
        Value::IntBig(value) => {
            let bytes = value.magnitude_bits().div_ceil(8);
            let bytes =
                u16::try_from(bytes).map_err(|_| OrderedValueEncodeError::SegmentTooLarge)?;
            if bytes == 0 {
                1
            } else {
                usize::from(bytes) + 3
            }
        }
        Value::NatBig(value) => {
            let bytes = u16::try_from(value.magnitude_bits().div_ceil(8))
                .map_err(|_| OrderedValueEncodeError::SegmentTooLarge)?;
            usize::from(bytes) + 2
        }
        Value::Null => return Err(OrderedValueEncodeError::NullNotIndexable),
        Value::Blob(_) | Value::Enum(_) | Value::List(_) | Value::Map(_) => {
            return Err(OrderedValueEncodeError::UnsupportedValueKind);
        }
    };
    payload
        .checked_add(1)
        .ok_or(OrderedValueEncodeError::SegmentTooLarge)
}

/// Decode the canonical signed-integer component shared by covering and
/// metadata-only execution.
pub(in crate::db) fn decode_canonical_index_int64_component(
    component: &[u8],
) -> Result<i64, InternalError> {
    let (&tag, payload) = component
        .split_first()
        .ok_or_else(InternalError::bytes_covering_component_payload_empty)?;
    if tag != ValueTag::Int64.to_u8() {
        return Err(InternalError::query_executor_invariant());
    }
    let Ok(bytes) = <[u8; 8]>::try_from(payload) else {
        return Err(InternalError::bytes_covering_component_payload_invalid_length());
    };
    Ok(i64::from_be_bytes(
        (u64::from_be_bytes(bytes) ^ (1_u64 << 63)).to_be_bytes(),
    ))
}

/// Encode one decoded primary-key value into canonical index-component bytes without
/// materializing an owned runtime `Value`.
#[cfg(test)]
pub(crate) fn encode_canonical_index_component_from_primary_key_value(
    value: PrimaryKeyComponent,
) -> Result<Vec<u8>, OrderedValueEncodeError> {
    let mut out = Vec::new();
    out.push(value.as_runtime_value().canonical_tag().to_u8());

    match value {
        PrimaryKeyComponent::Account(value) => {
            segments::push_account_payload(&mut out, &value)?;
            Ok(out)
        }
        PrimaryKeyComponent::Int64(value) => {
            out.extend_from_slice(&semantics::ordered_i64_bytes(value));
            Ok(out)
        }
        PrimaryKeyComponent::Int128(value) => {
            value.encode_ordered(&mut out)?;
            Ok(out)
        }
        PrimaryKeyComponent::Principal(value) => {
            segments::push_terminated_bytes(&mut out, value.as_slice());
            Ok(out)
        }
        PrimaryKeyComponent::Subaccount(value) => {
            out.extend_from_slice(&value.to_bytes());
            Ok(out)
        }
        PrimaryKeyComponent::Timestamp(value) => {
            value.encode_ordered(&mut out)?;
            Ok(out)
        }
        PrimaryKeyComponent::Nat64(value) => {
            out.extend_from_slice(&value.to_be_bytes());
            Ok(out)
        }
        PrimaryKeyComponent::Nat128(value) => {
            value.encode_ordered(&mut out)?;
            Ok(out)
        }
        PrimaryKeyComponent::Ulid(value) => {
            out.extend_from_slice(&value.to_bytes());
            Ok(out)
        }
        PrimaryKeyComponent::Unit => Ok(out),
        PrimaryKeyComponent::U256(value) => {
            out.extend_from_slice(&value.to_be_bytes());
            Ok(out)
        }
    }
}

/// Encode the variant-local payload after the canonical variant tag.
fn encode_component_payload(
    out: &mut Vec<u8>,
    value: &Value,
) -> Result<(), OrderedValueEncodeError> {
    match value {
        Value::Account(v) => segments::push_account_payload(out, v),
        Value::Blob(_) | Value::List(_) | Value::Map(_) => {
            Err(OrderedValueEncodeError::UnsupportedValueKind)
        }
        Value::Bool(v) => {
            out.push(u8::from(*v));
            Ok(())
        }
        Value::Date(v) => v.encode_ordered(out),
        Value::Decimal(v) => normalize::push_decimal_payload(out, *v),
        Value::Duration(v) => v.encode_ordered(out),
        Value::Enum(_) => Err(OrderedValueEncodeError::UnsupportedValueKind),
        Value::Float32(v) => {
            out.extend_from_slice(&semantics::ordered_f32_bytes(v.get()));
            Ok(())
        }
        Value::Float64(v) => {
            out.extend_from_slice(&semantics::ordered_f64_bytes(v.get()));
            Ok(())
        }
        Value::Int64(v) => {
            out.extend_from_slice(&semantics::ordered_i64_bytes(*v));
            Ok(())
        }
        Value::Int128(v) => v.encode_ordered(out),
        Value::IntBig(v) => normalize::push_signed_big_integer_payload(out, v),
        Value::Null => Err(OrderedValueEncodeError::NullNotIndexable),
        Value::Principal(v) => {
            segments::push_terminated_bytes(out, v.as_slice());
            Ok(())
        }
        Value::Subaccount(v) => {
            out.extend_from_slice(&v.to_bytes());
            Ok(())
        }
        Value::Text(v) => {
            segments::push_terminated_bytes(out, v.as_bytes());
            Ok(())
        }
        Value::Timestamp(v) => v.encode_ordered(out),
        Value::Nat64(v) => {
            out.extend_from_slice(&v.to_be_bytes());
            Ok(())
        }
        Value::Nat128(v) => v.encode_ordered(out),
        Value::NatBig(v) => normalize::push_unsigned_big_integer_payload(out, v),
        Value::Ulid(v) => {
            out.extend_from_slice(&v.to_bytes());
            Ok(())
        }
        // Unit intentionally has no payload; tag-only encoding is canonical.
        Value::Unit => Ok(()),
        Value::U256(v) => {
            out.extend_from_slice(&v.to_be_bytes());
            Ok(())
        }
    }
}
