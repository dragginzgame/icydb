use crate::{
    prelude::*,
    view::{
        ItemUpdate, ItemView,
        traits::{View, ViewExpr},
    },
};

///
/// ListView
///

pub struct ListView<'a>(pub &'a List);

impl View for ListView<'_> {
    fn generate(&self) -> TokenStream {
        let node = self.0;
        let view_ident = node.view_ident();
        let view_expr = ItemView(&node.item).expr();

        quote! {
            pub type #view_ident = Vec<#view_expr>;
        }
    }
}

impl ToTokens for ListView<'_> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        tokens.extend(self.generate());
    }
}

///
/// ListUpdate
///

pub struct ListUpdate<'a>(pub &'a List);

impl View for ListUpdate<'_> {
    fn generate(&self) -> TokenStream {
        let node = self.0;
        let update_ident = node.update_ident();
        let item_update = ItemUpdate(&node.item).expr();

        // quote
        let cp = paths().core;
        quote! {
            pub type #update_ident =
                Vec<#cp::view::ListPatch<#item_update>>;
        }
    }
}

impl ToTokens for ListUpdate<'_> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        tokens.extend(self.generate());
    }
}
