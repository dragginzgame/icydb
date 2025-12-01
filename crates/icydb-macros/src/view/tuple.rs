use crate::{
    prelude::*,
    view::{
        ValueUpdate, ValueView,
        traits::{View, ViewExpr},
    },
};

///
/// TupleView
///

pub struct TupleView<'a>(pub &'a Tuple);

impl View for TupleView<'_> {
    fn generate(&self) -> TokenStream {
        let node = self.0;
        let view_ident = node.view_ident();
        let view_values = node.values.iter().map(|v| ValueView(v).expr());

        quote! {
            pub type #view_ident = (#(#view_values),*);
        }
    }
}

impl ToTokens for TupleView<'_> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        tokens.extend(self.generate());
    }
}

///
/// TupleUpdate
///

pub struct TupleUpdate<'a>(pub &'a Tuple);

impl View for TupleUpdate<'_> {
    fn generate(&self) -> TokenStream {
        let node = self.0;
        let update_ident = node.update_ident();

        // Each element gets Option<ValueUpdateType>
        let update_values = node.values.iter().map(|v| {
            let ty = ValueUpdate(v).expr();
            quote!(Option<#ty>)
        });

        quote! {
            pub type #update_ident = (#(#update_values),*);
        }
    }
}

impl ToTokens for TupleUpdate<'_> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        tokens.extend(self.generate());
    }
}
