use crate::{
    node::Enum,
    prelude::*,
    view::{
        ValueView,
        traits::{View, ViewExpr},
    },
};

///
/// EnumView
///

pub struct EnumView<'a>(pub &'a Enum);

impl View for EnumView<'_> {
    fn generate(&self) -> TokenStream {
        let node = self.0;
        let view_ident = node.view_ident();

        // each variant uses ValueView to produce its payload expression
        let view_variants = node.variants.iter().map(|variant| {
            let vi = variant.effective_ident();
            if let Some(value) = &variant.value {
                let ve = ValueView(value).expr();

                quote!(#vi(#ve))
            } else {
                quote!(#vi)
            }
        });

        // traits
        let derives = self.traits();

        // handle default manually
        let default_impl = self.view_default_impl();

        quote! {
            #derives
            pub enum #view_ident {
                #(#view_variants),*
            }

            #default_impl
        }
    }
}

impl EnumView<'_> {
    fn view_default_impl(&self) -> Option<TokenStream> {
        let node = self.0;
        let view_ident = node.view_ident();
        let default_variant = node.default_variant()?;
        let variant_ident = default_variant.effective_ident();

        // Handle payloads
        let value_expr = if default_variant.value.is_some() {
            quote!((Default::default()))
        } else {
            quote!()
        };

        Some(quote! {
            impl Default for #view_ident {
                fn default() -> Self {
                    Self::#variant_ident #value_expr
                }
            }
        })
    }
}

impl ToTokens for EnumView<'_> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        tokens.extend(self.generate());
    }
}

///
/// EnumUpdate
/// just the same as the view ident
///

pub struct EnumUpdate<'a>(pub &'a Enum);

impl View for EnumUpdate<'_> {
    fn generate(&self) -> TokenStream {
        let node = self.0;
        let update_ident = node.update_ident();
        let view_ident = node.view_ident();

        quote! {
            pub type #update_ident = #view_ident;
        }
    }
}

impl ToTokens for EnumUpdate<'_> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        tokens.extend(self.generate());
    }
}
