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
// Width-preserving scalar vocabulary used by schema macros and generated Rust
// wiring. `icydb-schema` owns the catalog and its canonical `ScalarKind`
// mapping. Generated wrapper traits remain proc-macro policy because they
// describe concrete Rust types rather than database capabilities.
//

macro_rules! define_primitive {
    ( @entries $( ($primitive:ident, $scalar:ident) ),* $(,)? ) => {
        #[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
        #[remain::sorted]
        pub enum Primitive {
            $( $primitive, )*
        }

        impl FromStr for Primitive {
            type Err = &'static str;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                match value {
                    $( stringify!($primitive) => Ok(Self::$primitive), )*
                    _ => Err("unknown Primitive"),
                }
            }
        }

        const fn primitive_scalar_kind(primitive: Primitive) -> ScalarKind {
            match primitive {
                $( Primitive::$primitive => ScalarKind::$scalar, )*
            }
        }
    };
}

icydb_schema::authoring_primitive_registry!(define_primitive);

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

#[cfg(test)]
mod tests {
    use super::{Primitive, primitive_scalar_kind};
    use icydb_schema::ScalarKind;

    macro_rules! define_primitive_cases {
        ( @entries $( ($primitive:ident, $scalar:ident) ),* $(,)? ) => {
            const PRIMITIVE_CASES: &[(&str, Primitive, ScalarKind)] = &[
                $(
                    (
                        stringify!($primitive),
                        Primitive::$primitive,
                        ScalarKind::$scalar,
                    ),
                )*
            ];
        };
    }

    icydb_schema::authoring_primitive_registry!(define_primitive_cases);

    #[test]
    fn primitive_catalog_parses_and_maps_to_scalar_kinds() {
        for &(name, primitive, scalar_kind) in PRIMITIVE_CASES {
            assert_eq!(name.parse(), Ok(primitive));
            assert_eq!(primitive_scalar_kind(primitive), scalar_kind);
        }

        assert_eq!("Unknown".parse::<Primitive>(), Err("unknown Primitive"));
    }
}
