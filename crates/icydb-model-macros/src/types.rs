//! Module: types
//! Responsibility: authored model type metadata.
//! Does not own: runtime schema semantics.
//! Boundary: macro input to generated tokens.

use crate::prelude::*;

///
/// TraitStrategy
///

#[derive(Debug, Default)]
pub struct TraitStrategy {
    pub(crate) derive: Option<TraitKind>,
    pub(crate) imp: Option<TokenStream>,
}

impl TraitStrategy {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_derive(t: TraitKind) -> Self {
        Self::new().with_derive(t)
    }

    pub fn from_impl(tokens: TokenStream) -> Self {
        Self::new().with_impl(tokens)
    }

    pub const fn with_derive(mut self, t: TraitKind) -> Self {
        self.derive = Some(t);
        self
    }

    pub fn with_impl(mut self, tokens: TokenStream) -> Self {
        self.imp = Some(tokens);
        self
    }
}

pub(crate) fn primitive_type_tokens(primitive: Primitive) -> TokenStream {
    match primitive {
        Primitive::Bool => quote!(bool),
        Primitive::Int8 => quote!(i8),
        Primitive::Int16 => quote!(i16),
        Primitive::Int32 => quote!(i32),
        Primitive::Int64 => quote!(i64),
        Primitive::Int128 => quote!(i128),
        Primitive::Nat8 => quote!(u8),
        Primitive::Nat16 => quote!(u16),
        Primitive::Nat32 => quote!(u32),
        Primitive::Nat64 => quote!(u64),
        Primitive::Nat128 => quote!(u128),
        Primitive::Text => quote!(::std::string::String),
        _ => {
            let ident = format_ident!("{primitive:?}");
            quote!(::icydb_model::schema::#ident)
        }
    }
}
