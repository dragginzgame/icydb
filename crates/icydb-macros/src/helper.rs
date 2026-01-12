use proc_macro2::TokenStream;
use quote::{ToTokens, format_ident, quote};
use syn::Ident;

// Quoting helpers

/// Quote a single value after transforming it into tokens.
pub fn quote_one<T, F>(t: &T, transform: F) -> TokenStream
where
    F: FnOnce(&T) -> TokenStream,
{
    let transformed = transform(t);
    quote!(#transformed)
}

/// Quote an `Option`, applying the transform to the inner value when present.
pub fn quote_option<T, F>(opt: Option<&T>, transform: F) -> TokenStream
where
    F: FnOnce(&T) -> TokenStream,
{
    if let Some(v) = opt {
        let transformed = transform(v);
        quote!(Some(#transformed))
    } else {
        quote!(None)
    }
}

/// Quote a slice by transforming each element and returning a token array.
pub fn quote_slice<T, F>(vec: &[T], transform: F) -> TokenStream
where
    F: Fn(&T) -> TokenStream,
{
    let items: Vec<TokenStream> = vec
        .iter()
        .map(transform)
        .filter(|ts| !ts.is_empty())
        .collect();

    quote! {
        &[#(#items),*]
    }
}

// Transform helpers

/// Pass through a tokenizable value unchanged (useful for comments).
pub fn as_tokens<T: ToTokens>(t: &T) -> TokenStream {
    quote!(#t)
}

/// Convert a tokenizable value into a string literal token.
pub fn to_str_lit<T: ToTokens>(t: &T) -> TokenStream {
    let s = quote!(#t).to_string();

    quote!(#s)
}

/// Resolve a `Path` implementor to its associated `PATH` constant.
pub fn to_path<T: ToTokens>(t: &T) -> TokenStream {
    quote! { <#t as ::icydb::traits::Path>::PATH }
}

#[must_use]
/// Split a comma-separated list into idents for Darling parsing.
pub fn split_idents(s: String) -> Vec<Ident> {
    s.split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(|item| format_ident!("{item}"))
        .collect()
}
