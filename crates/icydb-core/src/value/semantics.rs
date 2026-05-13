//! Module: value::semantics
//!
//! Responsibility: semantic classification for dynamic `Value` variants.
//! Does not own: operator execution, map normalization, or numeric conversion.
//! Boundary: lightweight capability and coercion-family classification.

use crate::value::{CoercionFamily, CoercionFamilyExt, Value};

/// Returns true if the value is one of the numeric-like variants supported by
/// numeric comparison/ordering.
#[must_use]
pub const fn is_numeric(value: &Value) -> bool {
    matches!(
        value,
        Value::Decimal(_)
            | Value::Duration(_)
            | Value::Float32(_)
            | Value::Float64(_)
            | Value::Int(_)
            | Value::Int128(_)
            | Value::Timestamp(_)
            | Value::Nat(_)
            | Value::Nat128(_)
    )
}

/// Returns true when numeric coercion/comparison is explicitly allowed.
#[must_use]
pub const fn supports_numeric_coercion(value: &Value) -> bool {
    matches!(
        value,
        Value::Decimal(_)
            | Value::Duration(_)
            | Value::Float32(_)
            | Value::Float64(_)
            | Value::Int(_)
            | Value::Int128(_)
            | Value::Timestamp(_)
            | Value::Nat(_)
            | Value::Nat128(_)
    )
}

/// Returns the coercion-routing family for this value.
#[must_use]
pub const fn coercion_family(value: &Value) -> CoercionFamily {
    match value {
        Value::Account(_) | Value::Principal(_) | Value::Ulid(_) => CoercionFamily::Identifier,
        Value::Blob(_) | Value::Subaccount(_) => CoercionFamily::Blob,
        Value::Bool(_) => CoercionFamily::Bool,
        Value::Date(_)
        | Value::Decimal(_)
        | Value::Duration(_)
        | Value::Float32(_)
        | Value::Float64(_)
        | Value::Int(_)
        | Value::Int128(_)
        | Value::IntBig(_)
        | Value::Timestamp(_)
        | Value::Nat(_)
        | Value::Nat128(_)
        | Value::NatBig(_) => CoercionFamily::Numeric,
        Value::Enum(_) => CoercionFamily::Enum,
        Value::List(_) | Value::Map(_) => CoercionFamily::Collection,
        Value::Null => CoercionFamily::Null,
        Value::Text(_) => CoercionFamily::Textual,
        Value::Unit => CoercionFamily::Unit,
    }
}

impl Value {
    /// Returns true if the value is one of the numeric-like variants
    /// supported by numeric comparison/ordering.
    #[must_use]
    pub const fn is_numeric(&self) -> bool {
        is_numeric(self)
    }

    /// Returns true when numeric coercion/comparison is explicitly allowed.
    #[must_use]
    pub const fn supports_numeric_coercion(&self) -> bool {
        supports_numeric_coercion(self)
    }
}

impl CoercionFamilyExt for Value {
    /// Returns the coercion-routing family for this value.
    ///
    /// NOTE:
    /// This does NOT imply numeric, arithmetic, ordering, or keyability support.
    /// All scalar capabilities are registry-driven.
    fn coercion_family(&self) -> CoercionFamily {
        coercion_family(self)
    }
}
