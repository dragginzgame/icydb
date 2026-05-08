//! Module: index::key::ordered
//! Responsibility: canonical component encoding where byte order matches value order.
//! Does not own: full index-key framing or index-store traversal.
//! Boundary: used by index-key build/predicate compile/range lowering.

mod error;
mod normalize;
mod parts;
mod semantics;

#[cfg(test)]
use crate::db::numeric::compare_numeric_or_strict_order;
use crate::{
    db::index::key::ordered::semantics::OrderedEncode,
    types::{Account, Principal, Subaccount, Timestamp, Ulid},
    value::{StorageKey, Value},
};
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
    /// Encode a value once into canonical index-component bytes.
    pub(crate) fn try_new(raw: &Value) -> Result<Self, OrderedValueEncodeError> {
        let encoded = encode_canonical_index_component(raw)?;

        Ok(Self { encoded })
    }

    /// Encode a borrowed value into this cached wrapper.
    pub(crate) fn try_from_ref(raw: &Value) -> Result<Self, OrderedValueEncodeError> {
        Self::try_new(raw)
    }

    /// Encode all values in order into cached wrappers.
    pub(crate) fn try_encode_all(values: &[Value]) -> Result<Vec<Self>, OrderedValueEncodeError> {
        values.iter().map(Self::try_from_ref).collect()
    }

    #[must_use]
    pub(crate) const fn encoded(&self) -> &[u8] {
        self.encoded.as_slice()
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
    // Phase 1: emit canonical value tag to establish cross-kind ordering.
    let mut out = Vec::new();
    out.push(value.canonical_tag().to_u8());

    // Phase 2: encode kind-specific payload preserving in-kind ordering.
    encode_component_payload(&mut out, value)?;

    Ok(out)
}

/// Encode one storage-key value into canonical index-component bytes without
/// materializing an owned runtime `Value`.
pub(crate) fn encode_canonical_index_component_from_storage_key(
    value: StorageKey,
) -> Result<Vec<u8>, OrderedValueEncodeError> {
    let mut out = Vec::new();
    out.push(match value {
        StorageKey::Account(_) => {
            Value::Account(Account::from_parts(Principal::from_slice(&[]), None))
                .canonical_tag()
                .to_u8()
        }
        StorageKey::Int(_) => Value::Int(0).canonical_tag().to_u8(),
        StorageKey::Principal(_) => Value::Principal(Principal::default())
            .canonical_tag()
            .to_u8(),
        StorageKey::Subaccount(_) => Value::Subaccount(Subaccount::default())
            .canonical_tag()
            .to_u8(),
        StorageKey::Timestamp(_) => Value::Timestamp(Timestamp::EPOCH).canonical_tag().to_u8(),
        StorageKey::Uint(_) => Value::Uint(0).canonical_tag().to_u8(),
        StorageKey::Ulid(_) => Value::Ulid(Ulid::nil()).canonical_tag().to_u8(),
        StorageKey::Unit => Value::Unit.canonical_tag().to_u8(),
    });

    match value {
        StorageKey::Account(value) => {
            parts::push_account_payload(&mut out, &value)?;
            Ok(out)
        }
        StorageKey::Int(value) => {
            out.extend_from_slice(&semantics::ordered_i64_bytes(value));
            Ok(out)
        }
        StorageKey::Principal(value) => {
            parts::push_terminated_bytes(&mut out, value.as_slice());
            Ok(out)
        }
        StorageKey::Subaccount(value) => {
            out.extend_from_slice(&value.to_bytes());
            Ok(out)
        }
        StorageKey::Timestamp(value) => {
            value.encode_ordered(&mut out)?;
            Ok(out)
        }
        StorageKey::Uint(value) => {
            out.extend_from_slice(&value.to_be_bytes());
            Ok(out)
        }
        StorageKey::Ulid(value) => {
            out.extend_from_slice(&value.to_bytes());
            Ok(out)
        }
        StorageKey::Unit => Ok(out),
    }
}

/// Encode the variant-local payload after the canonical variant tag.
fn encode_component_payload(
    out: &mut Vec<u8>,
    value: &Value,
) -> Result<(), OrderedValueEncodeError> {
    match value {
        Value::Account(v) => parts::push_account_payload(out, v),
        Value::Blob(_) | Value::List(_) | Value::Map(_) => {
            Err(OrderedValueEncodeError::UnsupportedValueKind {
                kind: value.canonical_tag().label(),
            })
        }
        Value::Bool(v) => {
            out.push(u8::from(*v));
            Ok(())
        }
        Value::Date(v) => v.encode_ordered(out),
        Value::Decimal(v) => normalize::push_decimal_payload(out, *v),
        Value::Duration(v) => v.encode_ordered(out),
        Value::Enum(v) => parts::push_enum_payload(out, v),
        Value::Float32(v) => {
            out.extend_from_slice(&semantics::ordered_f32_bytes(v.get()));
            Ok(())
        }
        Value::Float64(v) => {
            out.extend_from_slice(&semantics::ordered_f64_bytes(v.get()));
            Ok(())
        }
        Value::Int(v) => {
            out.extend_from_slice(&semantics::ordered_i64_bytes(*v));
            Ok(())
        }
        Value::Int128(v) => v.encode_ordered(out),
        Value::IntBig(v) => normalize::push_signed_bigint_payload(out, v),
        Value::Null => Err(OrderedValueEncodeError::NullNotIndexable),
        Value::Principal(v) => {
            parts::push_terminated_bytes(out, v.as_slice());
            Ok(())
        }
        Value::Subaccount(v) => {
            out.extend_from_slice(&v.to_bytes());
            Ok(())
        }
        Value::Text(v) => {
            parts::push_terminated_bytes(out, v.as_bytes());
            Ok(())
        }
        Value::Timestamp(v) => v.encode_ordered(out),
        Value::Uint(v) => {
            out.extend_from_slice(&v.to_be_bytes());
            Ok(())
        }
        Value::Uint128(v) => v.encode_ordered(out),
        Value::UintBig(v) => normalize::push_unsigned_bigint_payload(out, v),
        Value::Ulid(v) => {
            out.extend_from_slice(&v.to_bytes());
            Ok(())
        }
        // Unit intentionally has no payload; tag-only encoding is canonical.
        Value::Unit => Ok(()),
    }
}
