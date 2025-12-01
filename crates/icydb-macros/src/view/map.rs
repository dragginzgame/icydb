use crate::{
    prelude::*,
    view::{
        ItemUpdate, ItemView, ValueUpdate, ValueView,
        traits::{View, ViewExpr},
    },
};

///
/// MapView
///

pub struct MapView<'a>(pub &'a Map);

impl View for MapView<'_> {
    fn generate(&self) -> TokenStream {
        let node = self.0;
        let view_ident = node.view_ident();
        let key_view = ItemView(&node.key).expr();
        let value_view = ValueView(&node.value).expr();

        quote! {
            pub type #view_ident = Vec<(#key_view, #value_view)>;
        }
    }
}

impl ToTokens for MapView<'_> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        tokens.extend(self.generate());
    }
}

///
/// MapUpdate
///

pub struct MapUpdate<'a>(pub &'a Map);

impl View for MapUpdate<'_> {
    fn generate(&self) -> TokenStream {
        let node = self.0;
        let update_ident = node.update_ident();
        let key_update = ItemUpdate(&node.key).expr();
        let value_update = ValueUpdate(&node.value).expr();

        // quote
        let cp = paths().core;
        quote! {
            pub type #update_ident = Vec<
                #cp::view::MapPatch<
                    #key_update,
                    #value_update
                >
            >;
        }
    }
}

impl ToTokens for MapUpdate<'_> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        tokens.extend(self.generate());
    }
}
