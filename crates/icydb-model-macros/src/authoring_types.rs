use crate::prelude::*;
use darling::FromMeta;
use icydb_schema::ScalarKind;
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

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
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

        tokens.extend(quote!(::icydb_model::types::Cardinality::#ident));
    }
}

//
// Primitive
//
// Scalar primitive catalog used by schema macros and generated runtime wiring.
// This enum owns database/query and representation capabilities. Generated
// Rust wrapper traits are selected separately by the newtype emitter from the
// operations supported by each concrete wrapped Rust type.
//

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
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
    Int8,
    Int16,
    Int32,
    Int64,
    Int128,
    IntBig,
    Nat8,
    Nat16,
    Nat32,
    Nat64,
    Nat128,
    NatBig,
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
            "Int8" => Ok(Self::Int8),
            "Int16" => Ok(Self::Int16),
            "Int32" => Ok(Self::Int32),
            "Int64" => Ok(Self::Int64),
            "Int128" => Ok(Self::Int128),
            "IntBig" => Ok(Self::IntBig),
            "Nat8" => Ok(Self::Nat8),
            "Nat16" => Ok(Self::Nat16),
            "Nat32" => Ok(Self::Nat32),
            "Nat64" => Ok(Self::Nat64),
            "Nat128" => Ok(Self::Nat128),
            "NatBig" => Ok(Self::NatBig),
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
        Primitive::Int8 | Primitive::Int16 | Primitive::Int32 | Primitive::Int64 => ScalarKind::Int,
        Primitive::Int128 => ScalarKind::Int128,
        Primitive::IntBig => ScalarKind::IntBig,
        Primitive::Nat8 | Primitive::Nat16 | Primitive::Nat32 | Primitive::Nat64 => ScalarKind::Nat,
        Primitive::Nat128 => ScalarKind::Nat128,
        Primitive::NatBig => ScalarKind::NatBig,
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
    pub const fn is_primary_key_encodable(self) -> bool {
        primitive_scalar_kind(self).is_primary_key_component_encodable()
    }

    #[must_use]
    pub const fn supports_copy(self) -> bool {
        !matches!(self, Self::Blob | Self::IntBig | Self::NatBig | Self::Text)
    }

    // NumericValue can fallibly route all numeric-like primitives through Decimal.
    #[must_use]
    pub const fn supports_numeric_value(self) -> bool {
        matches!(
            self,
            Self::Date
                | Self::Decimal
                | Self::Duration
                | Self::Int8
                | Self::Int16
                | Self::Int32
                | Self::Int64
                | Self::Int128
                | Self::IntBig
                | Self::Float32
                | Self::Float64
                | Self::Nat8
                | Self::Nat16
                | Self::Nat32
                | Self::Nat64
                | Self::Nat128
                | Self::NatBig
                | Self::Timestamp
        )
    }

    // both Ord and PartialOrd
    #[must_use]
    pub const fn supports_ord(self) -> bool {
        primitive_scalar_kind(self).supports_ordering()
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

        tokens.extend(quote!(::icydb_model::types::Primitive::#ident));
    }
}
