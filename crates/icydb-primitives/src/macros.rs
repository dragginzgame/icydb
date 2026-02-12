#[macro_export]
macro_rules! scalar_kind_registry_entries {
    ($macro:ident $(, @args $($args:tt)+ )?) => {
        $macro! {
            $(
                @args $($args)+;
            )?
            @entries
            (
                Account,
                Identifier,
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
                Blob,
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
                Bool,
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
                Numeric,
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
                Numeric,
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
                Numeric,
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
                Enum,
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
                Numeric,
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
                Numeric,
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
                Numeric,
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
                Numeric,
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
                Numeric,
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
                Numeric,
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
                Numeric,
                is_numeric_value = false,
                supports_numeric_coercion = false,
                supports_arithmetic = true,
                supports_equality = true,
                supports_ordering = true,
                is_keyable = false,
                is_storage_key_encodable = false
            ),
            (
                Principal,
                Identifier,
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
                Blob,
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
                Textual,
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
                Numeric,
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
                Numeric,
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
                Numeric,
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
                Numeric,
                is_numeric_value = false,
                supports_numeric_coercion = false,
                supports_arithmetic = true,
                supports_equality = true,
                supports_ordering = true,
                is_keyable = false,
                is_storage_key_encodable = false
            ),
            (
                Ulid,
                Identifier,
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
                Unit,
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
macro_rules! scalar_kind_registry {
    ($macro:ident) => {
        $crate::scalar_kind_registry_entries!($macro)
    };
    ($macro:ident, $($args:tt)+) => {
        $crate::scalar_kind_registry_entries!($macro, @args $($args)+)
    };
}

macro_rules! metadata_from_registry {
    ( @args $kind:expr; @entries $( ($scalar:ident, $family:ident, is_numeric_value = $is_numeric:expr, supports_numeric_coercion = $supports_numeric_coercion:expr, supports_arithmetic = $supports_arithmetic:expr, supports_equality = $supports_equality:expr, supports_ordering = $supports_ordering:expr, is_keyable = $is_keyable:expr, is_storage_key_encodable = $is_storage_key_encodable:expr) ),* $(,)? ) => {
        match $kind {
            $(
                $crate::ScalarKind::$scalar => $crate::ScalarMetadata {
                    family: $crate::ScalarCoercionFamily::$family,
                    is_numeric_value: $is_numeric,
                    supports_numeric_coercion: $supports_numeric_coercion,
                    supports_arithmetic: $supports_arithmetic,
                    supports_equality: $supports_equality,
                    supports_ordering: $supports_ordering,
                    is_keyable: $is_keyable,
                    is_storage_key_encodable: $is_storage_key_encodable,
                },
            )*
        }
    };
}

macro_rules! all_kinds_from_registry {
    ( @entries $( ($scalar:ident, $family:ident, is_numeric_value = $is_numeric:expr, supports_numeric_coercion = $supports_numeric_coercion:expr, supports_arithmetic = $supports_arithmetic:expr, supports_equality = $supports_equality:expr, supports_ordering = $supports_ordering:expr, is_keyable = $is_keyable:expr, is_storage_key_encodable = $is_storage_key_encodable:expr) ),* $(,)? ) => {
        [ $( $crate::ScalarKind::$scalar ),* ]
    };
    ( @args $($ignore:tt)*; @entries $( ($scalar:ident, $family:ident, is_numeric_value = $is_numeric:expr, supports_numeric_coercion = $supports_numeric_coercion:expr, supports_arithmetic = $supports_arithmetic:expr, supports_equality = $supports_equality:expr, supports_ordering = $supports_ordering:expr, is_keyable = $is_keyable:expr, is_storage_key_encodable = $is_storage_key_encodable:expr) ),* $(,)? ) => {
        [ $( $crate::ScalarKind::$scalar ),* ]
    };
}
