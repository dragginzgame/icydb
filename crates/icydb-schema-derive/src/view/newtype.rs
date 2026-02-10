use crate::{prelude::*, view::traits::View};

///
/// NewtypeView
///

pub struct NewtypeView<'a>(pub &'a Newtype);

impl View for NewtypeView<'_> {
    fn generate(&self) -> TokenStream {
        let node = self.0;
        let view_ident = node.view_ident();
        let view_type = node.item.type_expr();

        // quote
        quote! {
            pub type #view_ident = <#view_type as ::icydb::__internal::core::traits::AsView>::ViewType;
        }
    }
}

impl ToTokens for NewtypeView<'_> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        tokens.extend(self.generate());
    }
}

///
/// NewtypeUpdate
///

pub struct NewtypeUpdate<'a>(pub &'a Newtype);

impl View for NewtypeUpdate<'_> {
    fn generate(&self) -> TokenStream {
        let node = self.0;
        let update_ident = node.update_ident();
        let update_type = node.item.type_expr();

        // quote
        quote! {
            pub type #update_ident = <#update_type as ::icydb::__internal::core::traits::UpdateView>::UpdateViewType;
        }
    }
}

impl ToTokens for NewtypeUpdate<'_> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        tokens.extend(self.generate());
    }
}
