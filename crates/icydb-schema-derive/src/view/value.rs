use crate::{
    prelude::*,
    view::{ItemUpdate, ItemView, traits::ViewExpr},
};
use quote::quote;

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

        match node.cardinality() {
            Cardinality::One => quote!(#item),
            Cardinality::Opt => quote!(Option<#item>),
            Cardinality::Many => quote!(Vec<::icydb::patch::ListPatch<#item>>),
        }
        .into()
    }
}
