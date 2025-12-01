use crate::{
    prelude::*,
    view::{
        ItemUpdate, ItemView,
        traits::{View, ViewExpr},
    },
};

///
/// SetView
///

pub struct SetView<'a>(pub &'a Set);

impl View for SetView<'_> {
    fn generate(&self) -> TokenStream {
        let node = self.0;
        let view_ident = node.view_ident();
        let view_expr = ItemView(&node.item).expr();

        quote! {
            pub type #view_ident = Vec<#view_expr>;
        }
    }
}

impl ToTokens for SetView<'_> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        tokens.extend(self.generate());
    }
}

///
/// SetUpdate
///

pub struct SetUpdate<'a>(pub &'a Set);

impl View for SetUpdate<'_> {
    fn generate(&self) -> TokenStream {
        let node = self.0;
        let update_ident = node.update_ident();
        let item_update = ItemUpdate(&node.item).expr();

        // quote
        let cp = paths().core;
        quote! {
            pub type #update_ident =
                Vec<#cp::view::SetPatch<#item_update>>;
        }
    }
}

impl ToTokens for SetUpdate<'_> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        tokens.extend(self.generate());
    }
}
