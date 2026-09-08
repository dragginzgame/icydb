//! Iterative disposal of already-owned runtime value trees at input boundaries.

use crate::value::{CanonicalEnumBody, Value};

/// Detach recursive children before dropping each parent; no clone or encoding.
pub(crate) fn clear_value(value: Value) {
    let mut pending = Vec::new();
    let mut current = Some(value);
    while let Some(value) = current.take().or_else(|| pending.pop()) {
        match value {
            Value::List(values) => pending.extend(values),
            Value::Map(entries) => {
                for (key, value) in entries {
                    pending.push(key);
                    pending.push(value);
                }
            }
            Value::Enum(value) => {
                if let CanonicalEnumBody::Payload(value) = value.into_body() {
                    current = Some(*value);
                }
            }
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
            | Value::IntBig(_)
            | Value::Nat64(_)
            | Value::Nat128(_)
            | Value::NatBig(_)
            | Value::Null
            | Value::Principal(_)
            | Value::Subaccount(_)
            | Value::Text(_)
            | Value::Timestamp(_)
            | Value::U256(_)
            | Value::Ulid(_)
            | Value::Unit => {}
        }
    }
}
