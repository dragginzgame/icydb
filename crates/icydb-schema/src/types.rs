use crate::prelude::*;
use candid::CandidType;
use darling::FromMeta;
use derive_more::{Display, FromStr};
use proc_macro2::TokenStream;
use quote::{ToTokens, format_ident, quote};

///
/// Cardinality
///

#[derive(
    CandidType, Clone, Copy, Default, Debug, Deserialize, Display, Eq, FromStr, PartialEq, Serialize,
)]
pub enum Cardinality {
    #[default]
    One,
    Opt,
    Many,
}

impl FromMeta for Cardinality {
    fn from_string(s: &str) -> Result<Self, darling::Error> {
        s.parse::<Self>()
            .map_err(|_| darling::Error::unknown_value(s))
    }
}

impl ToTokens for Cardinality {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let ident = format_ident!("{self}");

        tokens.extend(quote!(::icydb::schema::types::Cardinality::#ident));
    }
}

///
/// Primitive
///

#[derive(
    CandidType, Clone, Copy, Debug, Deserialize, Display, Eq, PartialEq, FromStr, Serialize,
)]
#[remain::sorted]
pub enum Primitive {
    Account,
    Blob,
    Bool,
    Date,
    Decimal,
    Duration,
    E8s,
    E18s,
    Float32,
    Float64,
    Int,
    Int8,
    Int16,
    Int32,
    Int64,
    Int128,
    Nat,
    Nat8,
    Nat16,
    Nat32,
    Nat64,
    Nat128,
    Principal,
    Subaccount,
    Text,
    Timestamp,
    Ulid,
    Unit,
}

// Local helper to map scalar registry variants to schema primitive variants.
macro_rules! primitive_matches_scalar {
    ( $primitive:expr, Int ) => {
        matches!(
            $primitive,
            Primitive::Int8 | Primitive::Int16 | Primitive::Int32 | Primitive::Int64
        )
    };
    ( $primitive:expr, Uint ) => {
        matches!(
            $primitive,
            Primitive::Nat8 | Primitive::Nat16 | Primitive::Nat32 | Primitive::Nat64
        )
    };
    ( $primitive:expr, Enum ) => {
        false
    };
    ( $primitive:expr, IntBig ) => {
        matches!($primitive, Primitive::Int)
    };
    ( $primitive:expr, UintBig ) => {
        matches!($primitive, Primitive::Nat)
    };
    ( $primitive:expr, Uint128 ) => {
        matches!($primitive, Primitive::Nat128)
    };
    ( $primitive:expr, $scalar:ident ) => {
        matches!($primitive, Primitive::$scalar)
    };
}

macro_rules! primitive_supports_arithmetic_from_registry {
    ( @args $primitive:expr; @entries $( ($scalar:ident, $family:expr, $value_pat:pat, is_numeric_value = $is_numeric:expr, supports_arithmetic = $supports_arithmetic:expr, supports_equality = $supports_equality:expr, supports_ordering = $supports_ordering:expr, is_keyable = $is_keyable:expr) ),* $(,)? ) => {
        false $( || (primitive_matches_scalar!($primitive, $scalar) && $supports_arithmetic) )*
    };
}

impl Primitive {
    #[must_use]
    pub const fn supports_arithmetic(self) -> bool {
        scalar_registry!(primitive_supports_arithmetic_from_registry, self)
    }

    #[must_use]
    pub const fn supports_remainder(self) -> bool {
        matches!(
            self,
            Self::Decimal
                | Self::Int8
                | Self::Int16
                | Self::Int32
                | Self::Int64
                | Self::Int128
                | Self::Nat8
                | Self::Nat16
                | Self::Nat32
                | Self::Nat64
                | Self::Nat128
        )
    }

    #[must_use]
    pub const fn supports_copy(self) -> bool {
        !matches!(self, Self::Blob | Self::Int | Self::Nat | Self::Text)
    }

    #[must_use]
    pub const fn supports_hash(self) -> bool {
        !matches!(self, Self::Blob | Self::Unit)
    }

    // Int and Nat are unbounded integers so have no native representation
    #[must_use]
    pub const fn supports_num_cast(self) -> bool {
        matches!(
            self,
            Self::Date
                | Self::Decimal
                | Self::Duration
                | Self::E8s
                | Self::E18s
                | Self::Int8
                | Self::Int16
                | Self::Int32
                | Self::Int64
                | Self::Float32
                | Self::Float64
                | Self::Nat8
                | Self::Nat16
                | Self::Nat32
                | Self::Nat64
                | Self::Timestamp
        )
    }

    // both Ord and PartialOrd
    #[must_use]
    pub const fn supports_ord(self) -> bool {
        !matches!(self, Self::Blob | Self::Unit)
    }

    //
    // grouped helpers
    //

    #[must_use]
    pub const fn is_decimal(self) -> bool {
        matches!(self, Self::Decimal)
    }

    // is_numeric
    // Includes ints, floats, fixed‑point (E8s/E18s), and Decimal.
    #[must_use]
    pub const fn is_numeric(self) -> bool {
        self.is_int() || self.is_float() || self.is_fixed_point() || self.is_decimal()
    }

    #[must_use]
    pub const fn is_float(self) -> bool {
        matches!(self, Self::Float32 | Self::Float64)
    }

    #[must_use]
    pub const fn is_signed_int(self) -> bool {
        matches!(
            self,
            Self::Int | Self::Int8 | Self::Int16 | Self::Int32 | Self::Int64 | Self::Int128
        )
    }

    #[must_use]
    pub const fn is_unsigned_int(self) -> bool {
        matches!(
            self,
            Self::Nat | Self::Nat8 | Self::Nat16 | Self::Nat32 | Self::Nat64 | Self::Nat128
        )
    }

    #[must_use]
    pub const fn is_int(self) -> bool {
        self.is_signed_int() || self.is_unsigned_int()
    }

    #[must_use]
    pub const fn is_fixed_point(self) -> bool {
        matches!(self, Self::E8s | Self::E18s)
    }

    #[must_use]
    pub fn as_type(self) -> TokenStream {
        let ident = format_ident!("{self}");

        quote!(::icydb::types::#ident)
    }

    ///
    /// Returns the numeric cast function suffix for supported primitives.
    /// Emits a structured error for non-numeric primitives.
    ///
    pub fn num_cast_fn(self) -> Result<&'static str, darling::Error> {
        match self {
            Self::E18s => Ok("u128"),
            Self::Float32 => Ok("f32"),
            Self::Float64 | Self::Decimal => Ok("f64"),
            Self::Int8 => Ok("i8"),
            Self::Int16 => Ok("i16"),
            Self::Int32 | Self::Date => Ok("i32"),
            Self::Int64 => Ok("i64"),
            Self::Nat8 => Ok("u8"),
            Self::Nat16 => Ok("u16"),
            Self::Nat32 => Ok("u32"),
            Self::Nat64 | Self::Duration | Self::E8s | Self::Timestamp => Ok("u64"),
            _ => Err(darling::Error::custom(format!(
                "numeric cast is unsupported for primitive {self}"
            ))),
        }
    }
}

impl FromMeta for Primitive {
    fn from_string(s: &str) -> Result<Self, darling::Error> {
        s.parse::<Self>()
            .map_err(|_| darling::Error::unknown_value(s))
    }
}

impl ToTokens for Primitive {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let ident = format_ident!("{self}");

        tokens.extend(quote!(::icydb::schema::types::Primitive::#ident));
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::Primitive;

    const ALL_PRIMITIVES: [Primitive; 28] = [
        Primitive::Account,
        Primitive::Blob,
        Primitive::Bool,
        Primitive::Date,
        Primitive::Decimal,
        Primitive::Duration,
        Primitive::E8s,
        Primitive::E18s,
        Primitive::Float32,
        Primitive::Float64,
        Primitive::Int,
        Primitive::Int8,
        Primitive::Int16,
        Primitive::Int32,
        Primitive::Int64,
        Primitive::Int128,
        Primitive::Nat,
        Primitive::Nat8,
        Primitive::Nat16,
        Primitive::Nat32,
        Primitive::Nat64,
        Primitive::Nat128,
        Primitive::Principal,
        Primitive::Subaccount,
        Primitive::Text,
        Primitive::Timestamp,
        Primitive::Ulid,
        Primitive::Unit,
    ];

    fn legacy_supports_arithmetic(primitive: Primitive) -> bool {
        primitive.is_int() || primitive.is_fixed_point() || primitive.is_decimal()
    }

    #[test]
    fn supports_arithmetic_matches_legacy() {
        for primitive in ALL_PRIMITIVES {
            let expected = legacy_supports_arithmetic(primitive);

            assert_eq!(primitive.supports_arithmetic(), expected, "{primitive:?}");
        }
    }
}
