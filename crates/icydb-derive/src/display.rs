use crate::newtype;
use proc_macro2::TokenStream;
use quote::quote;
use syn::parse_quote;

pub fn derive_display(input: TokenStream) -> TokenStream {
    let newtype = match newtype::parse_newtype(input, "Display") {
        Ok(newtype) => newtype,
        Err(err) => return err.to_compile_error(),
    };

    let ident = newtype.ident;
    let inner = newtype.inner;
    let mut generics = newtype.generics;
    generics
        .make_where_clause()
        .predicates
        .push(parse_quote!(#inner: ::std::fmt::Display));

    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    let self_ty = quote!(#ident #ty_generics);

    quote! {
        impl #impl_generics ::std::fmt::Display for #self_ty #where_clause {
            fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                ::std::fmt::Display::fmt(&self.0, f)
            }
        }
    }
}
