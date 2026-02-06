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
macro_rules! scalar_registry_entries {
    ($macro:ident $(, @args $($args:tt)+ )?) => {
        $macro! {
            $(
                @args $($args)+;
            )?
            @entries
            (
                Account,
                ValueFamily::Identifier,
                Value::Account(_),
                is_numeric_value = false,
                supports_arithmetic = false,
                supports_equality = true,
                supports_ordering = true,
                is_keyable = true
            ),
            (
                Blob,
                ValueFamily::Blob,
                Value::Blob(_),
                is_numeric_value = false,
                supports_arithmetic = false,
                supports_equality = true,
                supports_ordering = false,
                is_keyable = false
            ),
            (
                Bool,
                ValueFamily::Bool,
                Value::Bool(_),
                is_numeric_value = false,
                supports_arithmetic = false,
                supports_equality = true,
                supports_ordering = true,
                is_keyable = false
            ),
            (
                Date,
                ValueFamily::Numeric,
                Value::Date(_),
                is_numeric_value = false,
                supports_arithmetic = false,
                supports_equality = true,
                supports_ordering = true,
                is_keyable = false
            ),
            (
                Decimal,
                ValueFamily::Numeric,
                Value::Decimal(_),
                is_numeric_value = true,
                supports_arithmetic = true,
                supports_equality = true,
                supports_ordering = true,
                is_keyable = false
            ),
            (
                Duration,
                ValueFamily::Numeric,
                Value::Duration(_),
                is_numeric_value = true,
                supports_arithmetic = false,
                supports_equality = true,
                supports_ordering = true,
                is_keyable = false
            ),
            (
                Enum,
                ValueFamily::Enum,
                Value::Enum(_),
                is_numeric_value = false,
                supports_arithmetic = false,
                supports_equality = true,
                supports_ordering = true,
                is_keyable = false
            ),
            (
                E8s,
                ValueFamily::Numeric,
                Value::E8s(_),
                is_numeric_value = true,
                supports_arithmetic = true,
                supports_equality = true,
                supports_ordering = true,
                is_keyable = false
            ),
            (
                E18s,
                ValueFamily::Numeric,
                Value::E18s(_),
                is_numeric_value = true,
                supports_arithmetic = true,
                supports_equality = true,
                supports_ordering = true,
                is_keyable = false
            ),
            (
                Float32,
                ValueFamily::Numeric,
                Value::Float32(_),
                is_numeric_value = true,
                supports_arithmetic = false,
                supports_equality = true,
                supports_ordering = true,
                is_keyable = false
            ),
            (
                Float64,
                ValueFamily::Numeric,
                Value::Float64(_),
                is_numeric_value = true,
                supports_arithmetic = false,
                supports_equality = true,
                supports_ordering = true,
                is_keyable = false
            ),
            (
                Int,
                ValueFamily::Numeric,
                Value::Int(_),
                is_numeric_value = true,
                supports_arithmetic = true,
                supports_equality = true,
                supports_ordering = true,
                is_keyable = true
            ),
            (
                Int128,
                ValueFamily::Numeric,
                Value::Int128(_),
                is_numeric_value = true,
                supports_arithmetic = true,
                supports_equality = true,
                supports_ordering = true,
                is_keyable = false
            ),
            (
                IntBig,
                ValueFamily::Numeric,
                Value::IntBig(_),
                is_numeric_value = false,
                supports_arithmetic = true,
                supports_equality = true,
                supports_ordering = true,
                is_keyable = false
            ),
            (
                Principal,
                ValueFamily::Identifier,
                Value::Principal(_),
                is_numeric_value = false,
                supports_arithmetic = false,
                supports_equality = true,
                supports_ordering = true,
                is_keyable = true
            ),
            (
                Subaccount,
                ValueFamily::Blob,
                Value::Subaccount(_),
                is_numeric_value = false,
                supports_arithmetic = false,
                supports_equality = true,
                supports_ordering = true,
                is_keyable = true
            ),
            (
                Text,
                ValueFamily::Textual,
                Value::Text(_),
                is_numeric_value = false,
                supports_arithmetic = false,
                supports_equality = true,
                supports_ordering = true,
                is_keyable = false
            ),
            (
                Timestamp,
                ValueFamily::Numeric,
                Value::Timestamp(_),
                is_numeric_value = true,
                supports_arithmetic = false,
                supports_equality = true,
                supports_ordering = true,
                is_keyable = true
            ),
            (
                Uint,
                ValueFamily::Numeric,
                Value::Uint(_),
                is_numeric_value = true,
                supports_arithmetic = true,
                supports_equality = true,
                supports_ordering = true,
                is_keyable = true
            ),
            (
                Uint128,
                ValueFamily::Numeric,
                Value::Uint128(_),
                is_numeric_value = true,
                supports_arithmetic = true,
                supports_equality = true,
                supports_ordering = true,
                is_keyable = false
            ),
            (
                UintBig,
                ValueFamily::Numeric,
                Value::UintBig(_),
                is_numeric_value = false,
                supports_arithmetic = true,
                supports_equality = true,
                supports_ordering = true,
                is_keyable = false
            ),
            (
                Ulid,
                ValueFamily::Identifier,
                Value::Ulid(_),
                is_numeric_value = false,
                supports_arithmetic = false,
                supports_equality = true,
                supports_ordering = true,
                is_keyable = true
            ),
            (
                Unit,
                ValueFamily::Unit,
                Value::Unit,
                is_numeric_value = false,
                supports_arithmetic = false,
                supports_equality = true,
                supports_ordering = false,
                is_keyable = true
            ),
        }
    };
}

macro_rules! scalar_registry {
    ($macro:ident) => {
        scalar_registry_entries!($macro)
    };
    ($macro:ident, $($args:tt)+) => {
        scalar_registry_entries!($macro, @args $($args)+)
    };
}
