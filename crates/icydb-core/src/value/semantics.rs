//! Module: value::semantics
//!
//! Responsibility: semantic classification for dynamic `Value` variants.
//! Does not own: operator execution, map normalization, or numeric conversion.
//! Boundary: lightweight capability and coercion-family classification.

use crate::value::{CoercionFamily, Value};

// Preserve independent capability flags through const matches generated from
// the canonical registry. Collections and null have no scalar capability.
macro_rules! numeric_capabilities_from_registry {
    ( @args $value:expr; @entries $( ($scalar:ident, $coercion_family:expr, $value_pat:pat, is_numeric_value = $is_numeric:expr, supports_numeric_coercion = $supports_numeric_coercion:expr, supports_arithmetic = $supports_arithmetic:expr, supports_equality = $supports_equality:expr, supports_ordering = $supports_ordering:expr, is_keyable = $is_keyable:expr, is_primary_key_component_encodable = $is_primary_key_component_encodable:expr) ),* $(,)? ) => {
        match $value {
            $( $value_pat => ($is_numeric, $supports_numeric_coercion), )*
            Value::List(_) | Value::Map(_) | Value::Null => (false, false),
        }
    };
}

#[must_use]
const fn is_numeric(value: &Value) -> bool {
    scalar_registry!(numeric_capabilities_from_registry, value).0
}

#[must_use]
pub(crate) const fn supports_numeric_coercion(value: &Value) -> bool {
    scalar_registry!(numeric_capabilities_from_registry, value).1
}

/// Returns the coercion-routing family for this value.
#[must_use]
const fn coercion_family(value: &Value) -> CoercionFamily {
    match value {
        Value::Account(_) | Value::Principal(_) | Value::Ulid(_) => CoercionFamily::Identifier,
        Value::Blob(_) | Value::Subaccount(_) => CoercionFamily::Blob,
        Value::Bool(_) => CoercionFamily::Bool,
        Value::Date(_)
        | Value::Decimal(_)
        | Value::Duration(_)
        | Value::Float32(_)
        | Value::Float64(_)
        | Value::Int64(_)
        | Value::Int128(_)
        | Value::IntBig(_)
        | Value::Timestamp(_)
        | Value::Nat64(_)
        | Value::Nat128(_)
        | Value::NatBig(_)
        | Value::U256(_) => CoercionFamily::Numeric,
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

    /// Returns the coercion-routing family for this value.
    ///
    /// NOTE:
    /// This does NOT imply numeric, arithmetic, ordering, or keyability support.
    /// All scalar capabilities are registry-driven.
    #[must_use]
    pub const fn coercion_family(&self) -> CoercionFamily {
        coercion_family(self)
    }
}
