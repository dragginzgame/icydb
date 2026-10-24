//! Resolve the consumer's runtime dependency before emitting actor code.

use proc_macro_crate::{FoundCrate, crate_name};
use proc_macro2::TokenStream;
use quote::quote;

const ICYDB_PACKAGE: &str = "icydb";

pub(super) fn resolve_path(explicit: Option<&str>) -> TokenStream {
    if let Some(path) = explicit {
        let path = syn::parse_str::<syn::Path>(path)
            .expect("explicit IcyDB actor crate path must be a valid Rust path");
        return quote!(#path);
    }

    match crate_name(ICYDB_PACKAGE).expect("actor generation requires a direct `icydb` dependency")
    {
        FoundCrate::Itself => quote!(crate),
        FoundCrate::Name(name) => {
            let ident = syn::Ident::new(name.as_str(), proc_macro2::Span::call_site());
            quote!(::#ident)
        }
    }
}
