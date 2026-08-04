//! Module: macros
//! Responsibility: scalar-kind and authoring-primitive registry macro definitions.
//! Does not own: scalar runtime behavior, query coercion, or storage encoding.
//! Boundary: expands canonical scalar catalogs into generated metadata helpers.

#[doc(hidden)]
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
                is_primary_key_component_encodable = true
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
                is_primary_key_component_encodable = false
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
                is_primary_key_component_encodable = false
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
                is_primary_key_component_encodable = false
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
                is_primary_key_component_encodable = false
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
                is_primary_key_component_encodable = false
            ),
            (
                Enum,
                Enum,
                is_numeric_value = false,
                supports_numeric_coercion = false,
                supports_arithmetic = false,
                supports_equality = true,
                supports_ordering = false,
                is_keyable = false,
                is_primary_key_component_encodable = false
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
                is_primary_key_component_encodable = false
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
                is_primary_key_component_encodable = false
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
                is_primary_key_component_encodable = true
            ),
            (
                Int128,
                Numeric,
                is_numeric_value = true,
                supports_numeric_coercion = true,
                supports_arithmetic = true,
                supports_equality = true,
                supports_ordering = true,
                is_keyable = true,
                is_primary_key_component_encodable = true
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
                is_primary_key_component_encodable = false
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
                is_primary_key_component_encodable = true
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
                is_primary_key_component_encodable = true
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
                is_primary_key_component_encodable = false
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
                is_primary_key_component_encodable = true
            ),
            (
                Nat,
                Numeric,
                is_numeric_value = true,
                supports_numeric_coercion = true,
                supports_arithmetic = true,
                supports_equality = true,
                supports_ordering = true,
                is_keyable = true,
                is_primary_key_component_encodable = true
            ),
            (
                Nat128,
                Numeric,
                is_numeric_value = true,
                supports_numeric_coercion = true,
                supports_arithmetic = true,
                supports_equality = true,
                supports_ordering = true,
                is_keyable = true,
                is_primary_key_component_encodable = true
            ),
            (
                NatBig,
                Numeric,
                is_numeric_value = false,
                supports_numeric_coercion = false,
                supports_arithmetic = true,
                supports_equality = true,
                supports_ordering = true,
                is_keyable = false,
                is_primary_key_component_encodable = false
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
                is_primary_key_component_encodable = true
            ),
            (
                Unit,
                Unit,
                is_numeric_value = false,
                supports_numeric_coercion = false,
                supports_arithmetic = false,
                supports_equality = true,
                supports_ordering = true,
                is_keyable = true,
                is_primary_key_component_encodable = true
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

#[doc(hidden)]
#[macro_export]
/// Expand the width-preserving application-authoring primitive catalog.
///
/// Each entry pairs an authoring primitive with its canonical runtime
/// [`ScalarKind`](crate::ScalarKind). The registry deliberately excludes
/// named enums because they are authored as named types rather than primitive
/// Rust fields.
macro_rules! authoring_primitive_registry {
    ($macro:ident) => {
        $macro! {
            @entries
            (Account, Account),
            (Blob, Blob),
            (Bool, Bool),
            (Date, Date),
            (Decimal, Decimal),
            (Duration, Duration),
            (Float32, Float32),
            (Float64, Float64),
            (Int8, Int),
            (Int16, Int),
            (Int32, Int),
            (Int64, Int),
            (Int128, Int128),
            (IntBig, IntBig),
            (Nat8, Nat),
            (Nat16, Nat),
            (Nat32, Nat),
            (Nat64, Nat),
            (Nat128, Nat128),
            (NatBig, NatBig),
            (Principal, Principal),
            (Subaccount, Subaccount),
            (Text, Text),
            (Timestamp, Timestamp),
            (Ulid, Ulid),
            (Unit, Unit),
        }
    };
}

macro_rules! metadata_from_registry {
    ( @args $kind:expr; @entries $( ($scalar:ident, $family:ident, is_numeric_value = $is_numeric:expr, supports_numeric_coercion = $supports_numeric_coercion:expr, supports_arithmetic = $supports_arithmetic:expr, supports_equality = $supports_equality:expr, supports_ordering = $supports_ordering:expr, is_keyable = $is_keyable:expr, is_primary_key_component_encodable = $is_primary_key_component_encodable:expr) ),* $(,)? ) => {
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
                    is_primary_key_component_encodable: $is_primary_key_component_encodable,
                },
            )*
        }
    };
}

macro_rules! all_kinds_from_registry {
    ( @entries $( ($scalar:ident, $family:ident, is_numeric_value = $is_numeric:expr, supports_numeric_coercion = $supports_numeric_coercion:expr, supports_arithmetic = $supports_arithmetic:expr, supports_equality = $supports_equality:expr, supports_ordering = $supports_ordering:expr, is_keyable = $is_keyable:expr, is_primary_key_component_encodable = $is_primary_key_component_encodable:expr) ),* $(,)? ) => {
        [ $( $crate::ScalarKind::$scalar ),* ]
    };
    ( @args $($ignore:tt)*; @entries $( ($scalar:ident, $family:ident, is_numeric_value = $is_numeric:expr, supports_numeric_coercion = $supports_numeric_coercion:expr, supports_arithmetic = $supports_arithmetic:expr, supports_equality = $supports_equality:expr, supports_ordering = $supports_ordering:expr, is_keyable = $is_keyable:expr, is_primary_key_component_encodable = $is_primary_key_component_encodable:expr) ),* $(,)? ) => {
        [ $( $crate::ScalarKind::$scalar ),* ]
    };
}
