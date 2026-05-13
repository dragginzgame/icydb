use crate::prelude::*;
use candid::CandidType;
use darling::FromMeta;
use icydb_primitives::ScalarKind;
use proc_macro2::TokenStream;
use quote::{ToTokens, format_ident, quote};
use std::str::FromStr;

//
// Cardinality
//
// Schema-level multiplicity marker used by codegen and validation passes.
// `One` means a required single value.
// `Opt` means an optional slot (nullable / absent is valid).
// `Many` means repeated values (for list/set-like shapes).
//

#[derive(CandidType, Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub enum Cardinality {
    #[default]
    One,
    Opt,
    Many,
}

impl FromStr for Cardinality {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "One" => Ok(Self::One),
            "Opt" => Ok(Self::Opt),
            "Many" => Ok(Self::Many),
            _ => Err("unknown Cardinality"),
        }
    }
}

impl FromMeta for Cardinality {
    fn from_string(s: &str) -> Result<Self, darling::Error> {
        s.parse::<Self>()
            .map_err(|_| darling::Error::unknown_value(s))
    }
}

impl ToTokens for Cardinality {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let ident = format_ident!("{self:?}");

        tokens.extend(quote!(::icydb::schema::types::Cardinality::#ident));
    }
}

//
// Primitive
//
// Scalar primitive catalog used by schema macros and generated runtime wiring.
// This enum is the canonical source for primitive capability checks
// (ordering, arithmetic, casting, key-encoding, and hashing support).
//

#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[remain::sorted]
pub enum Primitive {
    Account,
    Blob,
    Bool,
    Date,
    Decimal,
    Duration,
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

impl FromStr for Primitive {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Account" => Ok(Self::Account),
            "Blob" => Ok(Self::Blob),
            "Bool" => Ok(Self::Bool),
            "Date" => Ok(Self::Date),
            "Decimal" => Ok(Self::Decimal),
            "Duration" => Ok(Self::Duration),
            "Float32" => Ok(Self::Float32),
            "Float64" => Ok(Self::Float64),
            "Int" => Ok(Self::Int),
            "Int8" => Ok(Self::Int8),
            "Int16" => Ok(Self::Int16),
            "Int32" => Ok(Self::Int32),
            "Int64" => Ok(Self::Int64),
            "Int128" => Ok(Self::Int128),
            "Nat" => Ok(Self::Nat),
            "Nat8" => Ok(Self::Nat8),
            "Nat16" => Ok(Self::Nat16),
            "Nat32" => Ok(Self::Nat32),
            "Nat64" => Ok(Self::Nat64),
            "Nat128" => Ok(Self::Nat128),
            "Principal" => Ok(Self::Principal),
            "Subaccount" => Ok(Self::Subaccount),
            "Text" => Ok(Self::Text),
            "Timestamp" => Ok(Self::Timestamp),
            "Ulid" => Ok(Self::Ulid),
            "Unit" => Ok(Self::Unit),
            _ => Err("unknown Primitive"),
        }
    }
}

const fn primitive_scalar_kind(primitive: Primitive) -> ScalarKind {
    match primitive {
        Primitive::Account => ScalarKind::Account,
        Primitive::Blob => ScalarKind::Blob,
        Primitive::Bool => ScalarKind::Bool,
        Primitive::Date => ScalarKind::Date,
        Primitive::Decimal => ScalarKind::Decimal,
        Primitive::Duration => ScalarKind::Duration,
        Primitive::Float32 => ScalarKind::Float32,
        Primitive::Float64 => ScalarKind::Float64,
        Primitive::Int => ScalarKind::IntBig,
        Primitive::Int8 | Primitive::Int16 | Primitive::Int32 | Primitive::Int64 => ScalarKind::Int,
        Primitive::Int128 => ScalarKind::Int128,
        Primitive::Nat => ScalarKind::NatBig,
        Primitive::Nat8 | Primitive::Nat16 | Primitive::Nat32 | Primitive::Nat64 => ScalarKind::Nat,
        Primitive::Nat128 => ScalarKind::Nat128,
        Primitive::Principal => ScalarKind::Principal,
        Primitive::Subaccount => ScalarKind::Subaccount,
        Primitive::Text => ScalarKind::Text,
        Primitive::Timestamp => ScalarKind::Timestamp,
        Primitive::Ulid => ScalarKind::Ulid,
        Primitive::Unit => ScalarKind::Unit,
    }
}

impl Primitive {
    #[must_use]
    pub const fn supports_arithmetic(self) -> bool {
        primitive_scalar_kind(self).supports_arithmetic()
    }

    #[must_use]
    pub const fn is_storage_key_encodable(self) -> bool {
        primitive_scalar_kind(self).is_storage_key_encodable()
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

    // NumericValue can fallibly route all numeric-like primitives through Decimal.
    #[must_use]
    pub const fn supports_numeric_value(self) -> bool {
        matches!(
            self,
            Self::Date
                | Self::Decimal
                | Self::Duration
                | Self::Int
                | Self::Int8
                | Self::Int16
                | Self::Int32
                | Self::Int64
                | Self::Int128
                | Self::Float32
                | Self::Float64
                | Self::Nat
                | Self::Nat8
                | Self::Nat16
                | Self::Nat32
                | Self::Nat64
                | Self::Nat128
                | Self::Timestamp
        )
    }

    // both Ord and PartialOrd
    #[must_use]
    pub const fn supports_ord(self) -> bool {
        primitive_scalar_kind(self).supports_ordering()
    }

    //
    // grouped helpers
    //

    #[must_use]
    pub const fn is_decimal(self) -> bool {
        matches!(self, Self::Decimal)
    }

    // is_numeric
    // Includes ints, floats, and Decimal.
    #[must_use]
    pub const fn is_numeric(self) -> bool {
        self.is_int() || self.is_float() || self.is_decimal()
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
    pub fn as_type(self) -> TokenStream {
        let ident = format_ident!("{self:?}");

        quote!(::icydb::types::#ident)
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
        let ident = format_ident!("{self:?}");

        tokens.extend(quote!(::icydb::schema::types::Primitive::#ident));
    }
}
