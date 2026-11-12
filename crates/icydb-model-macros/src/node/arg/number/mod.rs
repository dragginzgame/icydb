//! Module: node::arg::number
//! Responsibility: derive-side node parsing.
//! Does not own: runtime schema semantics.
//! Boundary: macro metadata to node models.

use crate::prelude::*;
use darling::{Error as DarlingError, FromMeta};
use syn::Lit;

///
/// ArgNumber
///

#[derive(Clone, Debug)]
pub enum ArgNumber {
    Float32(f32),
    Float64(f64),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Int128(i128),
    Nat8(u8),
    Nat16(u16),
    Nat32(u32),
    Nat64(u64),
    Nat128(u128),
}

macro_rules! impl_from_for_numeric_value {
    ($($t:ty => $variant:ident),*) => {
        $(
            impl From<$t> for ArgNumber {
                fn from(n: $t) -> Self {
                    Self::$variant(n)
                }
            }
        )*
    }
}

impl_from_for_numeric_value! {
    i8 => Int8,
    i16 => Int16,
    i32 => Int32,
    i64 => Int64,
    i128 => Int128,
    u8 => Nat8,
    u16 => Nat16,
    u32 => Nat32,
    u64 => Nat64,
    u128 => Nat128
}

impl ArgNumber {
    // parse_numeric_string
    pub fn parse_numeric_string(s: &str) -> Result<Self, DarlingError> {
        let s = s.replace('_', "");

        let suffixes = [
            "f32", "f64", "i8", "i16", "i32", "i64", "i128", "u8", "u16", "u32", "u64", "u128",
        ];

        // 1. Handle suffixed values
        for &suffix in &suffixes {
            if s.ends_with(suffix) {
                let num_part = s.trim_end_matches(suffix);

                let result: Result<Self, DarlingError> =
                    match suffix {
                        "f32" => num_part.parse::<f32>().map(Self::Float32).map_err(|_| {
                            DarlingError::custom(format!("invalid float literal '{s}'"))
                        }),
                        "f64" => num_part.parse::<f64>().map(Self::Float64).map_err(|_| {
                            DarlingError::custom(format!("invalid float literal '{s}'"))
                        }),
                        "i8" => num_part.parse::<i8>().map(Self::Int8).map_err(|_| {
                            DarlingError::custom(format!("invalid numeric literal '{s}'"))
                        }),
                        "i16" => num_part.parse::<i16>().map(Self::Int16).map_err(|_| {
                            DarlingError::custom(format!("invalid numeric literal '{s}'"))
                        }),
                        "i32" => num_part.parse::<i32>().map(Self::Int32).map_err(|_| {
                            DarlingError::custom(format!("invalid numeric literal '{s}'"))
                        }),
                        "i64" => num_part.parse::<i64>().map(Self::Int64).map_err(|_| {
                            DarlingError::custom(format!("invalid numeric literal '{s}'"))
                        }),
                        "i128" => num_part.parse::<i128>().map(Self::Int128).map_err(|_| {
                            DarlingError::custom(format!("invalid numeric literal '{s}'"))
                        }),
                        "u8" => num_part.parse::<u8>().map(Self::Nat8).map_err(|_| {
                            DarlingError::custom(format!("invalid numeric literal '{s}'"))
                        }),
                        "u16" => num_part.parse::<u16>().map(Self::Nat16).map_err(|_| {
                            DarlingError::custom(format!("invalid numeric literal '{s}'"))
                        }),
                        "u32" => num_part.parse::<u32>().map(Self::Nat32).map_err(|_| {
                            DarlingError::custom(format!("invalid numeric literal '{s}'"))
                        }),
                        "u64" => num_part.parse::<u64>().map(Self::Nat64).map_err(|_| {
                            DarlingError::custom(format!("invalid numeric literal '{s}'"))
                        }),
                        "u128" => num_part.parse::<u128>().map(Self::Nat128).map_err(|_| {
                            DarlingError::custom(format!("invalid numeric literal '{s}'"))
                        }),
                        _ => Err(DarlingError::custom(format!(
                            "invalid numeric literal '{s}'"
                        ))),
                    };

                return result;
            }
        }

        // 2. Unsuffixed: first try integers
        if s.contains('.') {
            // 3. Unsuffixed float -> f64
            return s
                .parse::<f64>()
                .map(Self::Float64)
                .map_err(|_| DarlingError::custom(format!("invalid numeric literal '{s}'")));
        }

        macro_rules! try_parse {
                ($($ty:ty => $variant:ident),*) => {
                    $(
                        if let Ok(value) = s.parse::<$ty>() {
                            return Ok(Self::$variant(value));
                        }
                    )*
                };
            }

        // Try smallest fitting signed int, then unsigned
        try_parse!(
            i32 => Int32,
            i64 => Int64,
            i128 => Int128,
            u32 => Nat32,
            u64 => Nat64,
            u128 => Nat128
        );

        // Return error if no match found
        Err(DarlingError::custom(format!(
            "invalid or unsupported numeric literal '{s}'"
        )))
    }
}

impl FromMeta for ArgNumber {
    fn from_value(value: &Lit) -> Result<Self, DarlingError> {
        match value {
            // Int
            Lit::Int(lit) => {
                let s = lit.to_string();
                Self::parse_numeric_string(&s)
            }

            // Float
            Lit::Float(lit) => {
                let s = lit.to_string();
                Self::parse_numeric_string(&s)
            }

            // Support string form: "-3", "1.5f32"
            Lit::Str(s) => {
                let inner = s.value();
                Self::parse_numeric_string(&inner)
            }

            _ => Err(DarlingError::custom("expected numeric literal")),
        }
    }
}

impl PartialEq for ArgNumber {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Float64(a), Self::Float64(b)) => a.to_bits() == b.to_bits(),
            (Self::Float32(a), Self::Float32(b)) => a.to_bits() == b.to_bits(),
            (Self::Int8(a), Self::Int8(b)) => a == b,
            (Self::Int16(a), Self::Int16(b)) => a == b,
            (Self::Int32(a), Self::Int32(b)) => a == b,
            (Self::Int64(a), Self::Int64(b)) => a == b,
            (Self::Int128(a), Self::Int128(b)) => a == b,
            (Self::Nat8(a), Self::Nat8(b)) => a == b,
            (Self::Nat16(a), Self::Nat16(b)) => a == b,
            (Self::Nat32(a), Self::Nat32(b)) => a == b,
            (Self::Nat64(a), Self::Nat64(b)) => a == b,
            (Self::Nat128(a), Self::Nat128(b)) => a == b,
            _ => false,
        }
    }
}

impl HasSchemaPart for ArgNumber {
    fn schema_part(&self, paths: &CratePaths) -> TokenStream {
        let model = &paths.model;
        match self {
            Self::Float32(v) => quote!(#model::node::ArgNumber::Float32(#v)),
            Self::Float64(v) => quote!(#model::node::ArgNumber::Float64(#v)),
            Self::Int8(v) => quote!(#model::node::ArgNumber::Int8(#v)),
            Self::Int16(v) => quote!(#model::node::ArgNumber::Int16(#v)),
            Self::Int32(v) => quote!(#model::node::ArgNumber::Int32(#v)),
            Self::Int64(v) => quote!(#model::node::ArgNumber::Int64(#v)),
            Self::Int128(v) => quote!(#model::node::ArgNumber::Int128(#v)),
            Self::Nat8(v) => quote!(#model::node::ArgNumber::Nat8(#v)),
            Self::Nat16(v) => quote!(#model::node::ArgNumber::Nat16(#v)),
            Self::Nat32(v) => quote!(#model::node::ArgNumber::Nat32(#v)),
            Self::Nat64(v) => quote!(#model::node::ArgNumber::Nat64(#v)),
            Self::Nat128(v) => quote!(#model::node::ArgNumber::Nat128(#v)),
        }
    }
}

impl ToTokens for ArgNumber {
    // this has to be done in this way so
    // we get the _u8 suffix
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let q = match self {
            Self::Float32(v) => quote!(#v),
            Self::Float64(v) => quote!(#v),
            Self::Int8(v) => quote!(#v),
            Self::Int16(v) => quote!(#v),
            Self::Int32(v) => quote!(#v),
            Self::Int64(v) => quote!(#v),
            Self::Int128(v) => quote!(#v),
            Self::Nat8(v) => quote!(#v),
            Self::Nat16(v) => quote!(#v),
            Self::Nat32(v) => quote!(#v),
            Self::Nat64(v) => quote!(#v),
            Self::Nat128(v) => quote!(#v),
        };

        tokens.extend(q);
    }
}

///
/// TESTS
///

#[cfg(test)]
mod number_tests;
