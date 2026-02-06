///
/// Scalar Registry
///
/// Single source of truth for scalar metadata shared across the core.
///

// NOTE: Numeric exclusions are intentional.
// Date / IntBig / UintBig are Numeric by family but MUST remain non-numeric
// for Value::is_numeric and numeric coercion.
// CoercionFamily is a routing category only.
// Scalar capabilities are defined separately and MUST NOT be inferred.
// Do not infer numeric-ness from CoercionFamily.
// NOTE: `supports_numeric_coercion` is the only gate for numeric widening.
// NOTE: Floats are numeric but do NOT support arithmetic traits in schema-derive.
// TODO(breaking): consider whether Decimal / Text should ever be
// storage-key encodable, with explicit canonical encoding.
#[doc(hidden)]
#[macro_export]
macro_rules! scalar_registry_entries {
    ($macro:ident $(, @args $($args:tt)+ )?) => {
        $macro! {
            $(
                @args $($args)+;
            )?
            @entries
            (
                Account,
                $crate::value::CoercionFamily::Identifier,
                $crate::value::Value::Account(_),
                is_numeric_value = false,
                supports_numeric_coercion = false,
                supports_arithmetic = false,
                supports_equality = true,
                supports_ordering = true,
                is_keyable = true,
                is_storage_key_encodable = true
            ),
            (
                Blob,
                $crate::value::CoercionFamily::Blob,
                $crate::value::Value::Blob(_),
                is_numeric_value = false,
                supports_numeric_coercion = false,
                supports_arithmetic = false,
                supports_equality = true,
                supports_ordering = false,
                is_keyable = false,
                is_storage_key_encodable = false
            ),
            (
                Bool,
                $crate::value::CoercionFamily::Bool,
                $crate::value::Value::Bool(_),
                is_numeric_value = false,
                supports_numeric_coercion = false,
                supports_arithmetic = false,
                supports_equality = true,
                supports_ordering = true,
                is_keyable = false,
                is_storage_key_encodable = false
            ),
            (
                Date,
                $crate::value::CoercionFamily::Numeric,
                $crate::value::Value::Date(_),
                is_numeric_value = false,
                supports_numeric_coercion = false,
                supports_arithmetic = false,
                supports_equality = true,
                supports_ordering = true,
                is_keyable = false,
                is_storage_key_encodable = false
            ),
            (
                Decimal,
                $crate::value::CoercionFamily::Numeric,
                $crate::value::Value::Decimal(_),
                is_numeric_value = true,
                supports_numeric_coercion = true,
                supports_arithmetic = true,
                supports_equality = true,
                supports_ordering = true,
                is_keyable = false,
                is_storage_key_encodable = false
            ),
            (
                Duration,
                $crate::value::CoercionFamily::Numeric,
                $crate::value::Value::Duration(_),
                is_numeric_value = true,
                supports_numeric_coercion = true,
                supports_arithmetic = false,
                supports_equality = true,
                supports_ordering = true,
                is_keyable = false,
                is_storage_key_encodable = false
            ),
            (
                Enum,
                $crate::value::CoercionFamily::Enum,
                $crate::value::Value::Enum(_),
                is_numeric_value = false,
                supports_numeric_coercion = false,
                supports_arithmetic = false,
                supports_equality = true,
                supports_ordering = true,
                is_keyable = false,
                is_storage_key_encodable = false
            ),
            (
                E8s,
                $crate::value::CoercionFamily::Numeric,
                $crate::value::Value::E8s(_),
                is_numeric_value = true,
                supports_numeric_coercion = true,
                supports_arithmetic = true,
                supports_equality = true,
                supports_ordering = true,
                is_keyable = false,
                is_storage_key_encodable = false
            ),
            (
                E18s,
                $crate::value::CoercionFamily::Numeric,
                $crate::value::Value::E18s(_),
                is_numeric_value = true,
                supports_numeric_coercion = true,
                supports_arithmetic = true,
                supports_equality = true,
                supports_ordering = true,
                is_keyable = false,
                is_storage_key_encodable = false
            ),
            (
                Float32,
                $crate::value::CoercionFamily::Numeric,
                $crate::value::Value::Float32(_),
                is_numeric_value = true,
                supports_numeric_coercion = true,
                supports_arithmetic = false,
                supports_equality = true,
                supports_ordering = true,
                is_keyable = false,
                is_storage_key_encodable = false
            ),
            (
                Float64,
                $crate::value::CoercionFamily::Numeric,
                $crate::value::Value::Float64(_),
                is_numeric_value = true,
                supports_numeric_coercion = true,
                supports_arithmetic = false,
                supports_equality = true,
                supports_ordering = true,
                is_keyable = false,
                is_storage_key_encodable = false
            ),
            (
                Int,
                $crate::value::CoercionFamily::Numeric,
                $crate::value::Value::Int(_),
                is_numeric_value = true,
                supports_numeric_coercion = true,
                supports_arithmetic = true,
                supports_equality = true,
                supports_ordering = true,
                is_keyable = true,
                is_storage_key_encodable = true
            ),
            (
                Int128,
                $crate::value::CoercionFamily::Numeric,
                $crate::value::Value::Int128(_),
                is_numeric_value = true,
                supports_numeric_coercion = true,
                supports_arithmetic = true,
                supports_equality = true,
                supports_ordering = true,
                is_keyable = false,
                is_storage_key_encodable = false
            ),
            (
                IntBig,
                $crate::value::CoercionFamily::Numeric,
                $crate::value::Value::IntBig(_),
                is_numeric_value = false,
                supports_numeric_coercion = false,
                // IntBig participates in arithmetic trait emission for schema newtypes,
                // but is intentionally excluded from numeric widening/coercion.
                supports_arithmetic = true,
                supports_equality = true,
                supports_ordering = true,
                is_keyable = false,
                is_storage_key_encodable = false
            ),
            (
                Principal,
                $crate::value::CoercionFamily::Identifier,
                $crate::value::Value::Principal(_),
                is_numeric_value = false,
                supports_numeric_coercion = false,
                supports_arithmetic = false,
                supports_equality = true,
                supports_ordering = true,
                is_keyable = true,
                is_storage_key_encodable = true
            ),
            (
                Subaccount,
                $crate::value::CoercionFamily::Blob,
                $crate::value::Value::Subaccount(_),
                is_numeric_value = false,
                supports_numeric_coercion = false,
                supports_arithmetic = false,
                supports_equality = true,
                supports_ordering = true,
                is_keyable = true,
                is_storage_key_encodable = true
            ),
            (
                Text,
                $crate::value::CoercionFamily::Textual,
                $crate::value::Value::Text(_),
                is_numeric_value = false,
                supports_numeric_coercion = false,
                supports_arithmetic = false,
                supports_equality = true,
                supports_ordering = true,
                is_keyable = false,
                is_storage_key_encodable = false
            ),
            (
                Timestamp,
                $crate::value::CoercionFamily::Numeric,
                $crate::value::Value::Timestamp(_),
                is_numeric_value = true,
                supports_numeric_coercion = true,
                supports_arithmetic = false,
                supports_equality = true,
                supports_ordering = true,
                is_keyable = true,
                is_storage_key_encodable = true
            ),
            (
                Uint,
                $crate::value::CoercionFamily::Numeric,
                $crate::value::Value::Uint(_),
                is_numeric_value = true,
                supports_numeric_coercion = true,
                supports_arithmetic = true,
                supports_equality = true,
                supports_ordering = true,
                is_keyable = true,
                is_storage_key_encodable = true
            ),
            (
                Uint128,
                $crate::value::CoercionFamily::Numeric,
                $crate::value::Value::Uint128(_),
                is_numeric_value = true,
                supports_numeric_coercion = true,
                supports_arithmetic = true,
                supports_equality = true,
                supports_ordering = true,
                is_keyable = false,
                is_storage_key_encodable = false
            ),
            (
                UintBig,
                $crate::value::CoercionFamily::Numeric,
                $crate::value::Value::UintBig(_),
                is_numeric_value = false,
                supports_numeric_coercion = false,
                // UintBig participates in arithmetic trait emission for schema newtypes,
                // but is intentionally excluded from numeric widening/coercion.
                supports_arithmetic = true,
                supports_equality = true,
                supports_ordering = true,
                is_keyable = false,
                is_storage_key_encodable = false
            ),
            (
                Ulid,
                $crate::value::CoercionFamily::Identifier,
                $crate::value::Value::Ulid(_),
                is_numeric_value = false,
                supports_numeric_coercion = false,
                supports_arithmetic = false,
                supports_equality = true,
                supports_ordering = true,
                is_keyable = true,
                is_storage_key_encodable = true
            ),
            (
                Unit,
                $crate::value::CoercionFamily::Unit,
                $crate::value::Value::Unit,
                is_numeric_value = false,
                supports_numeric_coercion = false,
                supports_arithmetic = false,
                supports_equality = true,
                supports_ordering = false,
                is_keyable = true,
                is_storage_key_encodable = true
            ),
        }
    };
}

#[macro_export]
macro_rules! scalar_registry {
    ($macro:ident) => {
        $crate::scalar_registry_entries!($macro)
    };
    ($macro:ident, $($args:tt)+) => {
        $crate::scalar_registry_entries!($macro, @args $($args)+)
    };
}
