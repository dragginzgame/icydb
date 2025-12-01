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
    // as_type
    pub fn as_type(&self) -> TokenStream {
        match &self {
            Self::Float32(_) => quote!(f32),
            Self::Float64(_) => quote!(f64),
            Self::Int8(_) => quote!(i8),
            Self::Int16(_) => quote!(i16),
            Self::Int32(_) => quote!(i32),
            Self::Int64(_) => quote!(i64),
            Self::Int128(_) => quote!(i128),
            Self::Nat8(_) => quote!(u8),
            Self::Nat16(_) => quote!(u16),
            Self::Nat32(_) => quote!(u32),
            Self::Nat64(_) => quote!(u64),
            Self::Nat128(_) => quote!(u128),
        }
    }

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

                let result: Result<Self, DarlingError> = if num_part.contains('.') {
                    match suffix {
                        "f32" => num_part.parse::<f32>().map(Self::Float32),
                        "f64" => num_part.parse::<f64>().map(Self::Float64),
                        _ => unreachable!(),
                    }
                    .map_err(|_| DarlingError::custom(format!("invalid float literal '{s}'")))
                } else {
                    match suffix {
                        "i8" => num_part.parse::<i8>().map(Self::Int8),
                        "i16" => num_part.parse::<i16>().map(Self::Int16),
                        "i32" => num_part.parse::<i32>().map(Self::Int32),
                        "i64" => num_part.parse::<i64>().map(Self::Int64),
                        "i128" => num_part.parse::<i128>().map(Self::Int128),
                        "u8" => num_part.parse::<u8>().map(Self::Nat8),
                        "u16" => num_part.parse::<u16>().map(Self::Nat16),
                        "u32" => num_part.parse::<u32>().map(Self::Nat32),
                        "u64" => num_part.parse::<u64>().map(Self::Nat64),
                        "u128" => num_part.parse::<u128>().map(Self::Nat128),
                        _ => unreachable!(),
                    }
                    .map_err(|_| DarlingError::custom(format!("invalid numeric literal '{s}'")))
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
            u32 => Nat32,
            u64 => Nat64
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
    fn schema_part(&self) -> TokenStream {
        let sp = paths().schema;

        match self {
            Self::Float32(v) => quote!(#sp::node::ArgNumber::Float32(#v)),
            Self::Float64(v) => quote!(#sp::node::ArgNumber::Float64(#v)),
            Self::Int8(v) => quote!(#sp::node::ArgNumber::Int8(#v)),
            Self::Int16(v) => quote!(#sp::node::ArgNumber::Int16(#v)),
            Self::Int32(v) => quote!(#sp::node::ArgNumber::Int32(#v)),
            Self::Int64(v) => quote!(#sp::node::ArgNumber::Int64(#v)),
            Self::Int128(v) => quote!(#sp::node::ArgNumber::Int128(#v)),
            Self::Nat8(v) => quote!(#sp::node::ArgNumber::Nat8(#v)),
            Self::Nat16(v) => quote!(#sp::node::ArgNumber::Nat16(#v)),
            Self::Nat32(v) => quote!(#sp::node::ArgNumber::Nat32(#v)),
            Self::Nat64(v) => quote!(#sp::node::ArgNumber::Nat64(#v)),
            Self::Nat128(v) => quote!(#sp::node::ArgNumber::Nat128(#v)),
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
mod number_tests {
    use super::*;

    #[test]
    fn test_integer_literals() {
        assert_eq!(
            ArgNumber::parse_numeric_string("42i8").unwrap(),
            ArgNumber::Int8(42)
        );
        assert_eq!(
            ArgNumber::parse_numeric_string("-100").unwrap(),
            ArgNumber::Int32(-100)
        );
        assert_eq!(
            ArgNumber::parse_numeric_string("1000i16").unwrap(),
            ArgNumber::Int16(1000)
        );
        assert_eq!(
            ArgNumber::parse_numeric_string("-30000i32").unwrap(),
            ArgNumber::Int32(-30000)
        );
        assert_eq!(
            ArgNumber::parse_numeric_string("500000i64").unwrap(),
            ArgNumber::Int64(500_000)
        );
        assert_eq!(
            ArgNumber::parse_numeric_string("42u8").unwrap(),
            ArgNumber::Nat8(42)
        );
        assert_eq!(
            ArgNumber::parse_numeric_string("65535u16").unwrap(),
            ArgNumber::Nat16(65535)
        );
        assert_eq!(
            ArgNumber::parse_numeric_string("4000000000u32").unwrap(),
            ArgNumber::Nat32(4_000_000_000)
        );
        assert_eq!(
            ArgNumber::parse_numeric_string("-10_i8").unwrap(),
            ArgNumber::Int8(-10)
        );
    }

    #[test]
    fn test_integer_uscores() {
        assert_eq!(
            ArgNumber::parse_numeric_string("10_000").unwrap(),
            ArgNumber::Int32(10_000)
        );
        assert_eq!(
            ArgNumber::parse_numeric_string("10_000_u64").unwrap(),
            ArgNumber::Nat64(10_000)
        );
        assert_eq!(
            ArgNumber::parse_numeric_string("10_000_i64").unwrap(),
            ArgNumber::Int64(10_000)
        );
    }

    #[test]
    fn test_float_literals() {
        assert_eq!(
            ArgNumber::parse_numeric_string("3.12_f32").unwrap(),
            ArgNumber::Float32(3.12)
        );
        assert_eq!(
            ArgNumber::parse_numeric_string("3.13_f64").unwrap(),
            ArgNumber::Float64(3.13)
        );
        assert_eq!(
            ArgNumber::parse_numeric_string("3.15").unwrap(),
            ArgNumber::Float64(3.15)
        );
    }

    #[test]
    fn test_large_integers() {
        // Should overflow i32, fall back to i64
        assert_eq!(
            ArgNumber::parse_numeric_string("5000000000").unwrap(),
            ArgNumber::Int64(5_000_000_000)
        );

        // Should fit in u64
        assert_eq!(
            ArgNumber::parse_numeric_string("18446744073709551615u64").unwrap(),
            ArgNumber::Nat64(u64::MAX)
        );
    }

    #[test]
    fn test_negative_with_suffix() {
        assert_eq!(
            ArgNumber::parse_numeric_string("-128i8").unwrap(),
            ArgNumber::Int8(-128)
        );
    }

    #[test]
    fn test_invalid_literals() {
        assert!(ArgNumber::parse_numeric_string("hello").is_err());
        assert!(ArgNumber::parse_numeric_string("42x").is_err());
        assert!(ArgNumber::parse_numeric_string("4.2.5_f32").is_err());
    }

    #[test]
    fn test_to_tokens_integer() {
        let num = ArgNumber::parse_numeric_string("10").unwrap();
        let tokens = quote!(#num);
        assert_eq!(tokens.to_string(), "10i32");
    }

    #[test]
    fn test_to_tokens_float() {
        let num = ArgNumber::parse_numeric_string("3.14").unwrap();
        let tokens = quote!(#num);
        assert_eq!(tokens.to_string(), "3.14f64");

        let num = ArgNumber::parse_numeric_string("3.14_f64").unwrap();
        let tokens = quote!(#num);
        assert_eq!(tokens.to_string(), "3.14f64");
    }
}
