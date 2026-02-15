///
/// Scalar Registry
///
/// Adapter macros that project shared `icydb-primitives` scalar metadata into
/// core `Value`/`CoercionFamily`-aware entries.
///

#[doc(hidden)]
#[macro_export]
macro_rules! scalar_registry_family {
    (Numeric) => {
        $crate::value::CoercionFamily::Numeric
    };
    (Textual) => {
        $crate::value::CoercionFamily::Textual
    };
    (Identifier) => {
        $crate::value::CoercionFamily::Identifier
    };
    (Enum) => {
        $crate::value::CoercionFamily::Enum
    };
    (Blob) => {
        $crate::value::CoercionFamily::Blob
    };
    (Bool) => {
        $crate::value::CoercionFamily::Bool
    };
    (Unit) => {
        $crate::value::CoercionFamily::Unit
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! scalar_registry_value_pat {
    (Account) => {
        $crate::value::Value::Account(_)
    };
    (Blob) => {
        $crate::value::Value::Blob(_)
    };
    (Bool) => {
        $crate::value::Value::Bool(_)
    };
    (Date) => {
        $crate::value::Value::Date(_)
    };
    (Decimal) => {
        $crate::value::Value::Decimal(_)
    };
    (Duration) => {
        $crate::value::Value::Duration(_)
    };
    (Enum) => {
        $crate::value::Value::Enum(_)
    };
    (E8s) => {
        $crate::value::Value::E8s(_)
    };
    (E18s) => {
        $crate::value::Value::E18s(_)
    };
    (Float32) => {
        $crate::value::Value::Float32(_)
    };
    (Float64) => {
        $crate::value::Value::Float64(_)
    };
    (Int) => {
        $crate::value::Value::Int(_)
    };
    (Int128) => {
        $crate::value::Value::Int128(_)
    };
    (IntBig) => {
        $crate::value::Value::IntBig(_)
    };
    (Principal) => {
        $crate::value::Value::Principal(_)
    };
    (Subaccount) => {
        $crate::value::Value::Subaccount(_)
    };
    (Text) => {
        $crate::value::Value::Text(_)
    };
    (Timestamp) => {
        $crate::value::Value::Timestamp(_)
    };
    (Uint) => {
        $crate::value::Value::Uint(_)
    };
    (Uint128) => {
        $crate::value::Value::Uint128(_)
    };
    (UintBig) => {
        $crate::value::Value::UintBig(_)
    };
    (Ulid) => {
        $crate::value::Value::Ulid(_)
    };
    (Unit) => {
        $crate::value::Value::Unit
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! scalar_registry_entries_from_primitives {
    ( @args $consumer:ident; @entries $( ($scalar:ident, $family:ident, is_numeric_value = $is_numeric:expr, supports_numeric_coercion = $supports_numeric_coercion:expr, supports_arithmetic = $supports_arithmetic:expr, supports_equality = $supports_equality:expr, supports_ordering = $supports_ordering:expr, is_keyable = $is_keyable:tt, is_storage_key_encodable = $is_storage_key_encodable:tt) ),* $(,)? ) => {
        $consumer! {
            @entries
            $(
                (
                    $scalar,
                    $crate::scalar_registry_family!($family),
                    $crate::scalar_registry_value_pat!($scalar),
                    is_numeric_value = $is_numeric,
                    supports_numeric_coercion = $supports_numeric_coercion,
                    supports_arithmetic = $supports_arithmetic,
                    supports_equality = $supports_equality,
                    supports_ordering = $supports_ordering,
                    is_keyable = $is_keyable,
                    is_storage_key_encodable = $is_storage_key_encodable
                )
            ),*
        }
    };
    ( @args $consumer:ident, ($($consumer_args:tt)+); @entries $( ($scalar:ident, $family:ident, is_numeric_value = $is_numeric:expr, supports_numeric_coercion = $supports_numeric_coercion:expr, supports_arithmetic = $supports_arithmetic:expr, supports_equality = $supports_equality:expr, supports_ordering = $supports_ordering:expr, is_keyable = $is_keyable:tt, is_storage_key_encodable = $is_storage_key_encodable:tt) ),* $(,)? ) => {
        $consumer! {
            @args $($consumer_args)+;
            @entries
            $(
                (
                    $scalar,
                    $crate::scalar_registry_family!($family),
                    $crate::scalar_registry_value_pat!($scalar),
                    is_numeric_value = $is_numeric,
                    supports_numeric_coercion = $supports_numeric_coercion,
                    supports_arithmetic = $supports_arithmetic,
                    supports_equality = $supports_equality,
                    supports_ordering = $supports_ordering,
                    is_keyable = $is_keyable,
                    is_storage_key_encodable = $is_storage_key_encodable
                )
            ),*
        }
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! scalar_registry_entries {
    ($macro:ident) => {
        icydb_primitives::scalar_kind_registry!(scalar_registry_entries_from_primitives, $macro)
    };
    ($macro:ident, @args $($args:tt)+) => {
        icydb_primitives::scalar_kind_registry!(
            scalar_registry_entries_from_primitives,
            $macro,
            ($($args)+)
        )
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
