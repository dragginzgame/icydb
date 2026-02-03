use crate::newtype::parse_newtype;
use proc_macro2::TokenStream;
use quote::quote;

// derive_inner
pub fn derive_inner(input: TokenStream) -> TokenStream {
    let input = match parse_newtype(input, "Inner") {
        Ok(input) => input,
        Err(err) => return err.to_compile_error(),
    };

    let ident = &input.ident;
    let inner = &input.inner;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    quote! {
        impl #impl_generics ::icydb::traits::Inner<#inner> for #ident #ty_generics #where_clause {
            fn inner(&self) -> &#inner {
                &self.0
            }

            fn into_inner(self) -> #inner {
                self.0
            }
        }
    }
}
