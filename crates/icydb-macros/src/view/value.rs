use crate::{
    prelude::*,
    view::{ItemFilter, ItemUpdate, ItemView, traits::ViewExpr},
};

///
/// ValueView
///

pub struct ValueView<'a>(pub &'a Value);

impl ViewExpr for ValueView<'_> {
    fn expr(&self) -> Option<TokenStream> {
        let node = self.0;
        let item = ItemView(&node.item).expr();

        match node.cardinality() {
            Cardinality::One => quote!(#item),
            Cardinality::Opt => quote!(Option<#item>),
            Cardinality::Many => quote!(Vec<#item>),
        }
        .into()
    }
}

///
/// ValueUpdate
///

pub struct ValueUpdate<'a>(pub &'a Value);

impl ViewExpr for ValueUpdate<'_> {
    fn expr(&self) -> Option<TokenStream> {
        let node = self.0;
        let item = ItemUpdate(&node.item).expr();
        let cp = paths().core;

        match node.cardinality() {
            Cardinality::One => quote!(#item),
            Cardinality::Opt => quote!(Option<#item>),
            Cardinality::Many => quote!(Vec<#cp::view::ListPatch<#item>>),
        }
        .into()
    }
}

///
/// ValueFilter
///

pub struct ValueFilter<'a>(pub &'a Value);

impl ViewExpr for ValueFilter<'_> {
    fn expr(&self) -> Option<TokenStream> {
        let node = self.0;

        // The Rust type of the field’s VALUE type (String, u64, Decimal, etc.)
        let ty = node.item.target().type_expr();

        // The scalar filter payload: <<T as Filterable>::Filter as FilterKind>::Payload
        let scalar_payload = ItemFilter(&node.item).expr()?;

        // quote
        let cp = paths().core;
        let q = match node.cardinality() {
            Cardinality::One => {
                // Just the scalar filter payload
                quote!(#scalar_payload)
            }
            Cardinality::Opt => {
                // Still scalar payload – NOT Option<T> !!
                quote!(#scalar_payload)
            }
            Cardinality::Many => {
                quote!(
                    <<#ty as #cp::traits::Filterable>::ListFilter
                        as #cp::db::primitives::filter::FilterKind>::Payload
                )
            }
        };

        Some(q)
    }
}
