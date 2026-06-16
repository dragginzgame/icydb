//! Module: types
//! Responsibility: schema derive support.
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
        Primitive::Int128 => return quote!(i128),
        Primitive::IntBig => return quote!(::icydb::types::IntBig),
        Primitive::Nat128 => return quote!(u128),
        Primitive::NatBig => return quote!(::icydb::types::NatBig),
        _ => {}
    }

    let ident = format_ident!("{primitive:?}");

    quote!(::icydb::types::#ident)
}
