//! Module: helper_deref
//! Responsibility: generate standard-library dereference implementations for wrappers.
//! Does not own: application-model schema semantics or wrapper validation.
//! Boundary: validated one-field wrapper input to `Deref`/`DerefMut` implementations.

use crate::helper_newtype::parse_newtype;
use proc_macro2::TokenStream;
use quote::quote;

pub(crate) fn derive_deref(input: TokenStream) -> TokenStream {
    let input = match parse_newtype(input, "Deref") {
        Ok(input) => input,
        Err(err) => return err.to_compile_error(),
    };
    let ident = &input.ident;
    let inner = &input.inner;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    quote! {
        impl #impl_generics ::core::ops::Deref for #ident #ty_generics #where_clause {
            type Target = #inner;

            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }
    }
}

pub(crate) fn derive_deref_mut(input: TokenStream) -> TokenStream {
    let input = match parse_newtype(input, "DerefMut") {
        Ok(input) => input,
        Err(err) => return err.to_compile_error(),
    };
    let ident = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    quote! {
        impl #impl_generics ::core::ops::DerefMut for #ident #ty_generics #where_clause {
            fn deref_mut(&mut self) -> &mut Self::Target {
                &mut self.0
            }
        }
    }
}
