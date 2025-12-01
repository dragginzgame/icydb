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
        let sp = paths().schema;

        tokens.extend(quote!(#sp::types::Cardinality::#ident));
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

impl Primitive {
    #[must_use]
    pub const fn supports_arithmetic(self) -> bool {
        self.is_int() || self.is_fixed_point() || self.is_decimal()
    }

    #[must_use]
    pub const fn supports_copy(self) -> bool {
        !matches!(self, Self::Blob | Self::Int | Self::Nat | Self::Text)
    }

    #[must_use]
    pub const fn supports_display(self) -> bool {
        !matches!(self, Self::Blob | Self::Unit)
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
            Self::Int | Self::Int8 | Self::Int16 | Self::Int32 | Self::Int64
        )
    }

    #[must_use]
    pub const fn is_unsigned_int(self) -> bool {
        matches!(
            self,
            Self::Nat | Self::Nat8 | Self::Nat16 | Self::Nat32 | Self::Nat64
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
        let cp = paths().core;

        quote!(#cp::types::#ident)
    }

    #[must_use]
    pub fn num_cast_fn(self) -> String {
        match self {
            Self::E18s => "u128",
            Self::Float32 => "f32",
            Self::Float64 | Self::Decimal => "f64",
            Self::Int8 => "i8",
            Self::Int16 => "i16",
            Self::Int32 | Self::Date => "i32",
            Self::Int64 => "i64",
            Self::Nat8 => "u8",
            Self::Nat16 => "u16",
            Self::Nat32 => "u32",
            Self::Nat64 | Self::Duration | Self::E8s | Self::Timestamp => "u64",
            _ => panic!("unexpected primitive type"),
        }
        .into()
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
        let sp = paths().schema;

        tokens.extend(quote!(#sp::types::Primitive::#ident));
    }
}

///
/// StoreType
///

#[derive(CandidType, Clone, Copy, Debug, Deserialize, Display, FromStr, Serialize)]
pub enum StoreType {
    Data,
    Index,
}

impl FromMeta for StoreType {
    fn from_string(s: &str) -> Result<Self, darling::Error> {
        s.parse::<Self>()
            .map_err(|_| darling::Error::unknown_value(s))
    }
}

impl ToTokens for StoreType {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let ident = format_ident!("{self}");
        let sp = paths().schema;

        tokens.extend(quote!(#sp::types::StoreType::#ident));
    }
}
