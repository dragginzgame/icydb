//! Module: node::type
//! Responsibility: derive-side node parsing.
//! Does not own: runtime schema semantics.
//! Boundary: macro metadata to node models.

use crate::prelude::*;

///
/// Type
///

#[derive(Clone, Debug, Default, FromMeta)]
pub struct Type {
    #[darling(multiple, rename = "normalizer")]
    pub(crate) normalizers: Vec<TypeNormalizer>,

    #[darling(multiple, rename = "validator")]
    pub(crate) validators: Vec<TypeValidator>,
}

impl HasSchemaPart for Type {
    fn schema_part(&self) -> TokenStream {
        let normalizers = quote_slice(&self.normalizers, TypeNormalizer::schema_part);
        let validators = quote_slice(&self.validators, TypeValidator::schema_part);

        // quote
        quote! {
            ::icydb::schema::node::Type::new(#normalizers, #validators)
        }
    }
}

///
/// TypeNormalizer
///

#[derive(Clone, Debug, FromMeta)]
pub struct TypeNormalizer {
    pub(crate) path: Path,

    #[darling(default)]
    pub(crate) args: Args,
}

impl TypeNormalizer {
    pub fn quote_constructor(&self) -> TokenStream {
        let path = &self.path;
        let args = self.args.iter();

        if self.args.is_empty() {
            quote! { #path }
        } else {
            quote! { #path::new(#(#args),*) }
        }
    }
}

impl HasSchemaPart for TypeNormalizer {
    fn schema_part(&self) -> TokenStream {
        let path = quote_one(&self.path, to_path);
        let args = &self.args.schema_part();

        // quote
        quote! {
            ::icydb::schema::node::TypeNormalizer::new(#path, #args)
        }
    }
}

///
/// TypeValidator
///

#[derive(Clone, Debug, FromMeta)]
pub struct TypeValidator {
    pub(crate) path: Path,

    #[darling(default)]
    pub(crate) args: Args,
}

impl TypeValidator {
    pub fn quote_constructor(&self) -> TokenStream {
        let path = &self.path;
        let args = self.args.iter();

        if self.args.is_empty() {
            quote! { #path }
        } else {
            quote! { #path::new(#(#args),*) }
        }
    }
}

impl HasSchemaPart for TypeValidator {
    fn schema_part(&self) -> TokenStream {
        let path = quote_one(&self.path, to_path);
        let args = &self.args.schema_part();

        // quote
        quote! {
            ::icydb::schema::node::TypeValidator::new(#path, #args)
        }
    }
}
