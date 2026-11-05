//! Module: node::arg
//! Responsibility: derive-side node parsing.
//! Does not own: runtime schema semantics.
//! Boundary: macro metadata to node models.

mod number;

use crate::prelude::*;
use darling::{Error as DarlingError, FromMeta, ast::NestedMeta};
use syn::{Expr, Lit, LitStr, Path};

pub use number::*;

///
/// Arg
///
/// Quoted strings are literals; bare paths name constants or functions.
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

impl FromMeta for Arg {
    fn from_value(value: &Lit) -> Result<Self, DarlingError> {
        match value {
            Lit::Bool(lit) => Ok(Self::Bool(lit.value)),
            Lit::Char(lit) => Ok(Self::Char(lit.value())),
            Lit::Int(_) | Lit::Float(_) => ArgNumber::from_value(value).map(Self::Number),
            Lit::Str(lit) => Ok(Self::String(lit.clone())),
            _ => Err(DarlingError::custom(format!(
                "Unsupported literal type: {value:?}"
            ))),
        }
    }

    fn from_expr(expr: &Expr) -> Result<Self, DarlingError> {
        match expr {
            Expr::Lit(lit) => Self::from_value(&lit.lit),
            Expr::Group(group) => Self::from_expr(&group.expr),
            Expr::Path(path) if path.qself.is_none() => {
                Self::from_nested_meta(&NestedMeta::Meta(syn::Meta::Path(path.path.clone())))
            }
            _ => Err(DarlingError::unexpected_expr_type(expr)),
        }
        .map_err(|error| error.with_span(expr))
    }

    fn from_nested_meta(item: &NestedMeta) -> Result<Self, DarlingError> {
        match item {
            NestedMeta::Lit(lit) => Self::from_value(lit),
            NestedMeta::Meta(syn::Meta::Path(path)) => {
                // bare path like CONST or my_func
                let last = path
                    .segments
                    .last()
                    .ok_or_else(|| DarlingError::custom("path has no segments"))?
                    .ident
                    .to_string();

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
            NestedMeta::NameValueInvalidExpr(value) => Self::from_invalid_expr(value),
        }
    }
}

impl HasSchemaPart for Arg {
    fn schema_part(&self, paths: &CratePaths) -> TokenStream {
        let model = &paths.model;
        match self {
            Self::Bool(v) => quote!(#model::node::Arg::Bool(#v)),
            Self::Char(v) => quote!(#model::node::Arg::Char(#v)),
            Self::Number(v) => {
                let num = quote_one(v, |value| ArgNumber::schema_part(value, paths));
                quote!(#model::node::Arg::Number(#num))
            }
            Self::ConstPath(p) => {
                let path = quote_one(p, to_str_lit);
                quote!(#model::node::Arg::ConstPath(#path))
            }
            Self::FuncPath(p) => {
                let path = quote_one(p, to_str_lit);
                quote!(#model::node::Arg::FuncPath(#path))
            }
            Self::String(v) => quote!(#model::node::Arg::String(#v)),
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

#[derive(Clone, Debug, Default)]
pub struct Args(Vec<Arg>);

impl Args {
    #[must_use]
    pub(crate) const fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub(crate) fn iter(&self) -> std::slice::Iter<'_, Arg> {
        self.0.iter()
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
    fn schema_part(&self, paths: &CratePaths) -> TokenStream {
        let model = &paths.model;
        let args = quote_slice(&self.0, |value| Arg::schema_part(value, paths));

        quote! {
            #model::node::Args(#args)
        }
    }
}

///
/// TESTS
///

#[cfg(test)]
mod arg_tests;
