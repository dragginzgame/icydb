///
/// Scalar Registry
///
/// Single source of truth for scalar metadata shared across the core.
///

// NOTE: Numeric exclusions are intentional.
// Date / IntBig / UintBig are Numeric by family but MUST remain non-numeric
// for Value::is_numeric and numeric coercion.
// Do not infer numeric-ness from ValueFamily.
// NOTE: Floats are numeric but do NOT support arithmetic traits in schema-derive.
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
                $crate::value::ValueFamily::Identifier,
                $crate::value::Value::Account(_),
                is_numeric_value = false,
                supports_arithmetic = false,
                supports_equality = true,
                supports_ordering = true,
                is_keyable = true
            ),
            (
                Blob,
                $crate::value::ValueFamily::Blob,
                $crate::value::Value::Blob(_),
                is_numeric_value = false,
                supports_arithmetic = false,
                supports_equality = true,
                supports_ordering = false,
                is_keyable = false
            ),
            (
                Bool,
                $crate::value::ValueFamily::Bool,
                $crate::value::Value::Bool(_),
                is_numeric_value = false,
                supports_arithmetic = false,
                supports_equality = true,
                supports_ordering = true,
                is_keyable = false
            ),
            (
                Date,
                $crate::value::ValueFamily::Numeric,
                $crate::value::Value::Date(_),
                is_numeric_value = false,
                supports_arithmetic = false,
                supports_equality = true,
                supports_ordering = true,
                is_keyable = false
            ),
            (
                Decimal,
                $crate::value::ValueFamily::Numeric,
                $crate::value::Value::Decimal(_),
                is_numeric_value = true,
                supports_arithmetic = true,
                supports_equality = true,
                supports_ordering = true,
                is_keyable = false
            ),
            (
                Duration,
                $crate::value::ValueFamily::Numeric,
                $crate::value::Value::Duration(_),
                is_numeric_value = true,
                supports_arithmetic = false,
                supports_equality = true,
                supports_ordering = true,
                is_keyable = false
            ),
            (
                Enum,
                $crate::value::ValueFamily::Enum,
                $crate::value::Value::Enum(_),
                is_numeric_value = false,
                supports_arithmetic = false,
                supports_equality = true,
                supports_ordering = true,
                is_keyable = false
            ),
            (
                E8s,
                $crate::value::ValueFamily::Numeric,
                $crate::value::Value::E8s(_),
                is_numeric_value = true,
                supports_arithmetic = true,
                supports_equality = true,
                supports_ordering = true,
                is_keyable = false
            ),
            (
                E18s,
                $crate::value::ValueFamily::Numeric,
                $crate::value::Value::E18s(_),
                is_numeric_value = true,
                supports_arithmetic = true,
                supports_equality = true,
                supports_ordering = true,
                is_keyable = false
            ),
            (
                Float32,
                $crate::value::ValueFamily::Numeric,
                $crate::value::Value::Float32(_),
                is_numeric_value = true,
                supports_arithmetic = false,
                supports_equality = true,
                supports_ordering = true,
                is_keyable = false
            ),
            (
                Float64,
                $crate::value::ValueFamily::Numeric,
                $crate::value::Value::Float64(_),
                is_numeric_value = true,
                supports_arithmetic = false,
                supports_equality = true,
                supports_ordering = true,
                is_keyable = false
            ),
            (
                Int,
                $crate::value::ValueFamily::Numeric,
                $crate::value::Value::Int(_),
                is_numeric_value = true,
                supports_arithmetic = true,
                supports_equality = true,
                supports_ordering = true,
                is_keyable = true
            ),
            (
                Int128,
                $crate::value::ValueFamily::Numeric,
                $crate::value::Value::Int128(_),
                is_numeric_value = true,
                supports_arithmetic = true,
                supports_equality = true,
                supports_ordering = true,
                is_keyable = false
            ),
            (
                IntBig,
                $crate::value::ValueFamily::Numeric,
                $crate::value::Value::IntBig(_),
                is_numeric_value = false,
                supports_arithmetic = true,
                supports_equality = true,
                supports_ordering = true,
                is_keyable = false
            ),
            (
                Principal,
                $crate::value::ValueFamily::Identifier,
                $crate::value::Value::Principal(_),
                is_numeric_value = false,
                supports_arithmetic = false,
                supports_equality = true,
                supports_ordering = true,
                is_keyable = true
            ),
            (
                Subaccount,
                $crate::value::ValueFamily::Blob,
                $crate::value::Value::Subaccount(_),
                is_numeric_value = false,
                supports_arithmetic = false,
                supports_equality = true,
                supports_ordering = true,
                is_keyable = true
            ),
            (
                Text,
                $crate::value::ValueFamily::Textual,
                $crate::value::Value::Text(_),
                is_numeric_value = false,
                supports_arithmetic = false,
                supports_equality = true,
                supports_ordering = true,
                is_keyable = false
            ),
            (
                Timestamp,
                $crate::value::ValueFamily::Numeric,
                $crate::value::Value::Timestamp(_),
                is_numeric_value = true,
                supports_arithmetic = false,
                supports_equality = true,
                supports_ordering = true,
                is_keyable = true
            ),
            (
                Uint,
                $crate::value::ValueFamily::Numeric,
                $crate::value::Value::Uint(_),
                is_numeric_value = true,
                supports_arithmetic = true,
                supports_equality = true,
                supports_ordering = true,
                is_keyable = true
            ),
            (
                Uint128,
                $crate::value::ValueFamily::Numeric,
                $crate::value::Value::Uint128(_),
                is_numeric_value = true,
                supports_arithmetic = true,
                supports_equality = true,
                supports_ordering = true,
                is_keyable = false
            ),
            (
                UintBig,
                $crate::value::ValueFamily::Numeric,
                $crate::value::Value::UintBig(_),
                is_numeric_value = false,
                supports_arithmetic = true,
                supports_equality = true,
                supports_ordering = true,
                is_keyable = false
            ),
            (
                Ulid,
                $crate::value::ValueFamily::Identifier,
                $crate::value::Value::Ulid(_),
                is_numeric_value = false,
                supports_arithmetic = false,
                supports_equality = true,
                supports_ordering = true,
                is_keyable = true
            ),
            (
                Unit,
                $crate::value::ValueFamily::Unit,
                $crate::value::Value::Unit,
                is_numeric_value = false,
                supports_arithmetic = false,
                supports_equality = true,
                supports_ordering = false,
                is_keyable = true
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
