use crate::value::{StorageKey, StorageKeyEncodeError, Value};

// Name one runtime `Value` kind for storage-key bridge diagnostics.
const fn runtime_value_kind_label(value: &Value) -> &'static str {
    match value {
        Value::Account(_) => "Account",
        Value::Blob(_) => "Blob",
        Value::Bool(_) => "Bool",
        Value::Date(_) => "Date",
        Value::Decimal(_) => "Decimal",
        Value::Duration(_) => "Duration",
        Value::Enum(_) => "Enum",
        Value::Float32(_) => "Float32",
        Value::Float64(_) => "Float64",
        Value::Int(_) => "Int",
        Value::Int128(_) => "Int128",
        Value::IntBig(_) => "IntBig",
        Value::List(_) => "List",
        Value::Map(_) => "Map",
        Value::Null => "Null",
        Value::Principal(_) => "Principal",
        Value::Subaccount(_) => "Subaccount",
        Value::Text(_) => "Text",
        Value::Timestamp(_) => "Timestamp",
        Value::Uint(_) => "Uint",
        Value::Uint128(_) => "Uint128",
        Value::UintBig(_) => "UintBig",
        Value::Ulid(_) => "Ulid",
        Value::Unit => "Unit",
    }
}

/// Convert one storage-normalized key into a runtime `Value`.
///
/// This bridge is runtime-only. Persistence and indexing must keep working on
/// `StorageKey` directly rather than routing back through `Value`.
#[must_use]
pub(crate) const fn storage_key_as_runtime_value(key: &StorageKey) -> Value {
    match key {
        StorageKey::Account(v) => Value::Account(*v),
        StorageKey::Int(v) => Value::Int(*v),
        StorageKey::Principal(v) => Value::Principal(*v),
        StorageKey::Subaccount(v) => Value::Subaccount(*v),
        StorageKey::Timestamp(v) => Value::Timestamp(*v),
        StorageKey::Uint(v) => Value::Uint(*v),
        StorageKey::Ulid(v) => Value::Ulid(*v),
        StorageKey::Unit => Value::Unit,
    }
}

/// Bridge one runtime `Value` into `StorageKey`.
///
/// This quarantine helper exists only for runtime/structural surfaces that
/// still traffic in `Value`. Typed persistence and indexing must use
/// `StorageKeyCodec` instead of routing through this bridge.
pub(crate) const fn storage_key_from_runtime_value(
    value: &Value,
) -> Result<StorageKey, StorageKeyEncodeError> {
    // Storage encodability is a persistent compatibility contract.
    // Changing admission is a breaking change and may require index migration.
    // This bridge is intentionally separate from typed key ownership.
    match value {
        Value::Account(v) => Ok(StorageKey::Account(*v)),
        Value::Int(v) => Ok(StorageKey::Int(*v)),
        Value::Principal(v) => Ok(StorageKey::Principal(*v)),
        Value::Subaccount(v) => Ok(StorageKey::Subaccount(*v)),
        Value::Timestamp(v) => Ok(StorageKey::Timestamp(*v)),
        Value::Uint(v) => Ok(StorageKey::Uint(*v)),
        Value::Ulid(v) => Ok(StorageKey::Ulid(*v)),
        Value::Unit => Ok(StorageKey::Unit),
        _ => Err(StorageKeyEncodeError::UnsupportedValueKind {
            kind: runtime_value_kind_label(value),
        }),
    }
}
