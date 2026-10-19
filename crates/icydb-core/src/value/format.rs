//! Compact value labels with incremental, fallible output for nested containers.

#[cfg(test)]
mod tests;

use crate::value::Value;
use std::fmt;

/// Preserve compact Value Debug spelling, not alternate/pretty Rust formatting.
/// The caller owns structural admission and output limits.
pub(crate) fn write_value_debug(
    value: &Value,
    out: &mut (impl std::fmt::Write + ?Sized),
) -> fmt::Result {
    match value {
        Value::List(values) => {
            out.write_str("List([")?;
            for (index, value) in values.iter().enumerate() {
                if index != 0 {
                    out.write_str(", ")?;
                }
                write_value_debug(value, out)?;
            }
            out.write_str("])")
        }
        Value::Map(entries) => {
            out.write_str("Map([")?;
            for (index, (key, value)) in entries.iter().enumerate() {
                if index != 0 {
                    out.write_str(", ")?;
                }
                out.write_char('(')?;
                write_value_debug(key, out)?;
                out.write_str(", ")?;
                write_value_debug(value, out)?;
                out.write_char(')')?;
            }
            out.write_str("])")
        }
        Value::Enum(value) => {
            write!(
                out,
                "Enum(ValueEnum(CanonicalEnumValue {{ type_id: {:?}, variant_id: {:?}, body: ",
                value.type_id(),
                value.variant_id(),
            )?;
            if let Some(payload) = value.payload() {
                out.write_str("Payload(")?;
                write_value_debug(payload, out)?;
                out.write_char(')')?;
            } else {
                out.write_str("Unit")?;
            }
            out.write_str(" }))")
        }
        // Exhaustive scalar fallback: new containers must choose their
        // recursion owner explicitly.
        Value::Account(_)
        | Value::Blob(_)
        | Value::Bool(_)
        | Value::Date(_)
        | Value::Decimal(_)
        | Value::Duration(_)
        | Value::Float32(_)
        | Value::Float64(_)
        | Value::Int64(_)
        | Value::Int128(_)
        | Value::Null
        | Value::Principal(_)
        | Value::Subaccount(_)
        | Value::Text(_)
        | Value::Timestamp(_)
        | Value::Nat64(_)
        | Value::Nat128(_)
        | Value::Ulid(_)
        | Value::Unit
        | Value::U256(_) => write!(out, "{value:?}"),
    }
}
