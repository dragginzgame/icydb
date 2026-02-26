use crate::{db::index::key::ordered::semantics::OrderedEncode, value::Value};

mod error;
mod normalize;
mod parts;
mod semantics;

pub(crate) use error::OrderedValueEncodeError;

const NEGATIVE_MARKER: u8 = 0x00;
const ZERO_MARKER: u8 = 0x01;
const POSITIVE_MARKER: u8 = 0x02;

///
/// EncodedValue
///
/// Cached canonical index-component bytes for one logical `Value`.
/// This wrapper keeps value + bytes together so planning/execution callsites
/// can avoid re-encoding the same literal repeatedly.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct EncodedValue {
    raw: Value,
    encoded: Vec<u8>,
}

impl EncodedValue {
    /// Encode a value once into canonical index-component bytes.
    pub(crate) fn try_new(raw: Value) -> Result<Self, OrderedValueEncodeError> {
        let encoded = encode_canonical_index_component(&raw)?;

        Ok(Self { raw, encoded })
    }

    /// Encode a borrowed value by cloning it into this cached wrapper.
    pub(crate) fn try_from_ref(raw: &Value) -> Result<Self, OrderedValueEncodeError> {
        Self::try_new(raw.clone())
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

/// Encode one scalar index component so lexicographic byte order matches
/// canonical `Value` order for supported primitive variants.
pub(crate) fn encode_canonical_index_component(
    value: &Value,
) -> Result<Vec<u8>, OrderedValueEncodeError> {
    let mut out = Vec::new();

    out.push(value.canonical_tag().to_u8());
    encode_component_payload(&mut out, value)?;

    Ok(out)
}

// Encode the variant-local payload after the canonical variant tag.
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
