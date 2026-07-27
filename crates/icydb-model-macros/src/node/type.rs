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

    #[darling(multiple, rename = "rule")]
    pub(crate) rules: Vec<SourceRule>,
}

impl HasSchemaPart for Type {
    fn schema_part(&self) -> TokenStream {
        let normalizers = quote_slice(&self.normalizers, TypeNormalizer::schema_part);
        let validators = quote_slice(&self.validators, TypeValidator::schema_part);
        let rules = quote_slice(&self.rules, SourceRule::schema_part);

        // quote
        quote! {
            ::icydb_model::node::Type::new(#normalizers, #validators, #rules)
        }
    }
}

///
/// SourceRule
///

#[derive(Clone, Debug, FromMeta)]
pub struct SourceRule {
    pub(crate) source_key: LitStr,
    pub(crate) kind: SourceRuleKind,

    #[darling(default)]
    pub(crate) args: Args,
}

impl HasSchemaPart for SourceRule {
    fn schema_part(&self) -> TokenStream {
        let source_key = &self.source_key;
        let kind = self.kind.schema_part();
        let args = self.args.schema_part();

        quote! {
            ::icydb_model::node::SourceRule::new(#source_key, #kind, #args)
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum SourceRuleKind {
    LengthRange,
    NumericMinimum,
    NumericRange,
}

impl FromMeta for SourceRuleKind {
    fn from_string(value: &str) -> Result<Self, DarlingError> {
        match value {
            "length_range_inclusive" => Ok(Self::LengthRange),
            "numeric_minimum_inclusive" => Ok(Self::NumericMinimum),
            "numeric_range_inclusive" => Ok(Self::NumericRange),
            _ => Err(DarlingError::unknown_value(value)),
        }
    }
}

impl HasSchemaPart for SourceRuleKind {
    fn schema_part(&self) -> TokenStream {
        let variant = match self {
            Self::LengthRange => quote!(LengthRange),
            Self::NumericMinimum => quote!(NumericMinimum),
            Self::NumericRange => quote!(NumericRange),
        };
        quote! {
            ::icydb_model::node::SourceRuleKind::#variant
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
            ::icydb_model::node::TypeNormalizer::new(#path, #args)
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
            ::icydb_model::node::TypeValidator::new(#path, #args)
        }
    }
}
