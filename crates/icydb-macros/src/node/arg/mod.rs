mod number;

pub use number::*;

use crate::prelude::*;
use darling::{Error as DarlingError, FromMeta, ast::NestedMeta};
use derive_more::Deref;
use syn::{Lit, LitStr, Path};

///
/// Arg
///
/// Note: `String` literals are accepted, but may be confused for Path/Number.
///       If arguments need to be true strings, keep them quoted.
///

#[derive(Clone, Debug)]
pub enum Arg {
    Bool(bool),
    Char(char),
    Number(ArgNumber),
    ConstPath(Path), // e.g. MY_CONST, Self::CONST, crate::FOO::BAR
    FuncPath(Path),  // e.g. my_fun, path::to::fun
    String(LitStr),
}

impl Arg {
    pub fn as_type(&self) -> TokenStream {
        match &self {
            Self::Bool(_) => quote!(bool),
            Self::Char(_) => quote!(char),
            Self::Number(n) => n.as_type(),
            _ => {
                quote!(::core::compile_error!("invalid arg"))
            }
        }
    }
}

impl FromMeta for Arg {
    fn from_value(value: &Lit) -> Result<Self, DarlingError> {
        match value {
            Lit::Bool(lit) => Ok(Self::Bool(lit.value)),
            Lit::Char(lit) => Ok(Self::Char(lit.value())),
            Lit::Int(_) | Lit::Float(_) => ArgNumber::from_value(value).map(Self::Number),
            Lit::Str(lit) => {
                if lit.value().contains("::") {
                    let path: Path = syn::parse_str(&lit.value())
                        .map_err(|_| DarlingError::custom("Failed to parse path"))?;

                    let last = path.segments.last().unwrap().ident.to_string();

                    if last.chars().next().is_some_and(char::is_uppercase) {
                        Ok(Self::ConstPath(path))
                    } else {
                        Ok(Self::FuncPath(path))
                    }
                } else {
                    Ok(Self::String(lit.clone()))
                }
            }
            _ => Err(DarlingError::custom(format!(
                "Unsupported literal type: {value:?}"
            ))),
        }
    }

    fn from_nested_meta(item: &NestedMeta) -> Result<Self, DarlingError> {
        match item {
            NestedMeta::Lit(lit) => Self::from_value(lit),
            NestedMeta::Meta(syn::Meta::Path(path)) => {
                // bare path like CONST or my_func
                let last = path.segments.last().unwrap().ident.to_string();

                if last.chars().next().is_some_and(char::is_uppercase) {
                    Ok(Self::ConstPath(path.clone()))
                } else {
                    Ok(Self::FuncPath(path.clone()))
                }
            }
            NestedMeta::Meta(syn::Meta::NameValue(nv)) => Err(DarlingError::custom(format!(
                "NameValue not supported here: {nv:?}"
            ))),
            NestedMeta::Meta(syn::Meta::List(list)) => Err(DarlingError::custom(format!(
                "Nested list not supported here: {list:?}"
            ))),
        }
    }
}

impl HasSchemaPart for Arg {
    fn schema_part(&self) -> TokenStream {
        match self {
            Self::Bool(v) => quote!(::icydb::schema::node::Arg::Bool(#v)),
            Self::Char(v) => quote!(::icydb::schema::node::Arg::Char(#v)),
            Self::Number(v) => {
                let num = quote_one(v, ArgNumber::schema_part);
                quote!(::icydb::schema::node::Arg::Number(#num))
            }
            Self::ConstPath(p) => {
                let path = quote_one(p, to_str_lit);
                quote!(::icydb::schema::node::Arg::ConstPath(#path))
            }
            Self::FuncPath(p) => {
                let path = quote_one(p, to_str_lit);
                quote!(::icydb::schema::node::Arg::FuncPath(#path))
            }
            Self::String(v) => quote!(::icydb::schema::node::Arg::String(#v)),
        }
    }
}

impl ToTokens for Arg {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let q = match self {
            Self::Bool(v) => quote!(#v),
            Self::Char(v) => quote!(#v),       //
            Self::Number(v) => quote!(#v),     // already prints `1u8`, `42i32`, etc.
            Self::ConstPath(p) => quote!(#p),  // plain constant
            Self::FuncPath(p) => quote!(#p()), // function call
            Self::String(v) => quote!(#v),
        };

        tokens.extend(q);
    }
}
///
/// Args
/// Generic re-useable list of arguments
///

#[derive(Clone, Debug, Default, Deref)]
pub struct Args(Vec<Arg>);

impl Args {
    #[must_use]
    pub const fn none() -> Self {
        Self(vec![])
    }
}

impl FromMeta for Args {
    fn from_list(items: &[NestedMeta]) -> Result<Self, DarlingError> {
        let mut args = Vec::new();

        for item in items {
            args.push(Arg::from_nested_meta(item)?);
        }

        Ok(Self(args))
    }
}

impl HasSchemaPart for Args {
    fn schema_part(&self) -> TokenStream {
        let args = quote_slice(&self.0, Arg::schema_part);

        quote! {
            ::icydb::schema::node::Args(#args)
        }
    }
}

///
/// TESTS
///

#[cfg(test)]
mod arg_tests {
    use super::*;
    use syn::parse_quote;

    #[test]
    fn test_bool_parsing() {
        let lit = parse_quote!(true);
        if let Ok(Arg::Bool(b)) = Arg::from_value(&lit) {
            assert!(b, "Parsed boolean should be true");
        } else {
            panic!("Expected Bool variant");
        }
    }

    #[test]
    fn test_char_parsing() {
        let lit = parse_quote!('a');
        if let Ok(Arg::Char(c)) = Arg::from_value(&lit) {
            assert_eq!(c, 'a', "Parsed char should be 'a'");
        } else {
            panic!("Expected Char variant");
        }
    }

    #[test]
    fn test_number_parsing() {
        let lit = parse_quote!(42);
        if let Ok(Arg::Number(num)) = Arg::from_value(&lit) {
            assert_eq!(num, ArgNumber::Int32(42), "Parsed number does not match");
        } else {
            panic!("Expected Number variant");
        }
    }

    #[test]
    fn test_const_path_parsing() {
        let arg: Arg = Arg::from_nested_meta(&parse_quote!(MY_CONST)).unwrap();
        match arg {
            Arg::ConstPath(path) => {
                assert_eq!(path.segments.last().unwrap().ident.to_string(), "MY_CONST");
            }
            _ => panic!("Expected ConstPath variant"),
        }
    }

    #[test]
    fn test_func_path_parsing() {
        let arg: Arg = Arg::from_nested_meta(&parse_quote!(my_func)).unwrap();
        match arg {
            Arg::FuncPath(path) => {
                assert_eq!(path.segments.last().unwrap().ident.to_string(), "my_func");
            }
            _ => panic!("Expected FuncPath variant"),
        }
    }

    #[test]
    fn test_invalid_input() {
        let lit = parse_quote!(b"invalid");
        assert!(
            Arg::from_value(&lit).is_err(),
            "Expected an error for unsupported literal type."
        );
    }
}
