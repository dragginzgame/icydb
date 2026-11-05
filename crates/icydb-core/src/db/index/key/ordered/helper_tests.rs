//! Test-only helpers for the parent module.

use super::*;

/// Compare two semantic index-component values under the index ordering contract.
///
/// Contract:
/// - same-variant component values delegate to shared numeric-or-strict
///   comparator authority
/// - mixed-variant values fall back to canonical key ordering for deterministic
///   cross-kind ordering in test/support surfaces
#[must_use]
pub(crate) fn compare_index_component_values(left: &Value, right: &Value) -> Ordering {
    if std::mem::discriminant(left) == std::mem::discriminant(right)
        && let Some(ordering) = compare_numeric_or_strict_order(left, right)
    {
        return ordering;
    }

    Value::canonical_cmp_key(left, right)
}

/// Encode one decoded primary-key value into canonical index-component bytes without
/// materializing an owned runtime `Value`.
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
            segments::push_principal_payload(&mut out, &value)?;
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
