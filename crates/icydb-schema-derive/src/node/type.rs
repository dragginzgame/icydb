use crate::prelude::*;

///
/// Type
///

#[derive(Clone, Debug, Default, FromMeta)]
pub struct Type {
    #[darling(multiple, rename = "sanitizer")]
    pub(crate) sanitizers: Vec<TypeSanitizer>,

    #[darling(multiple, rename = "validator")]
    pub(crate) validators: Vec<TypeValidator>,
}

impl HasSchemaPart for Type {
    fn schema_part(&self) -> TokenStream {
        let sanitizers = quote_slice(&self.sanitizers, TypeSanitizer::schema_part);
        let validators = quote_slice(&self.validators, TypeValidator::schema_part);

        // quote
        quote! {
            ::icydb::schema::node::Type::new(#sanitizers, #validators)
        }
    }
}

///
/// TypeSanitizer
///

#[derive(Clone, Debug, FromMeta)]
pub struct TypeSanitizer {
    pub(crate) path: Path,

    #[darling(default)]
    pub(crate) args: Args,
}

impl TypeSanitizer {
    /// Creates a new TypeSanitizer from a string path.
    pub fn new(path: &str, args: Args) -> Self {
        Self {
            path: syn::parse_str(path).unwrap(),
            args,
        }
    }

    pub fn quote_constructor(&self) -> TokenStream {
        let path = &self.path;
        let args = &self.args;

        if args.is_empty() {
            quote! { #path }
        } else {
            quote! { #path::new(#(#args),*) }
        }
    }
}

impl HasSchemaPart for TypeSanitizer {
    fn schema_part(&self) -> TokenStream {
        let path = quote_one(&self.path, to_path);
        let args = &self.args.schema_part();

        // quote
        quote! {
            ::icydb::schema::node::TypeSanitizer::new(#path, #args)
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
        let args = &self.args;

        if args.is_empty() {
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
