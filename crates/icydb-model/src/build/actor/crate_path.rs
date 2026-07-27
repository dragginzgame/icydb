//! Generated actor crate-path resolution.
//!
//! Actor output belongs to the consuming canister, so it must name that
//! package's direct `icydb` dependency rather than assuming its Cargo key.

use proc_macro_crate::{FoundCrate, crate_name};
use proc_macro2::{Group, TokenStream, TokenTree};
use quote::quote;

const ICYDB_PACKAGE: &str = "icydb";

/// Rewrite the `::icydb` sentinel used by actor token builders.
///
/// # Panics
///
/// Panics when an explicit override is not a valid Rust path or when Cargo
/// cannot resolve a direct `icydb` dependency for the consuming package.
pub(super) fn rewrite_icydb_path(tokens: TokenStream, explicit: Option<&str>) -> TokenStream {
    if !contains_ident(&tokens, "icydb") {
        return tokens;
    }

    let replacement = resolve_path(explicit);
    rewrite_stream(tokens, &replacement)
}

fn resolve_path(explicit: Option<&str>) -> TokenStream {
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

fn contains_ident(tokens: &TokenStream, needle: &str) -> bool {
    tokens.clone().into_iter().any(|token| match token {
        TokenTree::Group(group) => contains_ident(&group.stream(), needle),
        TokenTree::Ident(ident) => ident == needle,
        TokenTree::Literal(_) | TokenTree::Punct(_) => false,
    })
}

fn rewrite_stream(tokens: TokenStream, replacement: &TokenStream) -> TokenStream {
    let input = tokens.into_iter().collect::<Vec<_>>();
    let mut output = TokenStream::new();
    let mut index = 0;

    while index < input.len() {
        if index + 2 < input.len()
            && is_colon(&input[index])
            && is_colon(&input[index + 1])
            && matches!(&input[index + 2], TokenTree::Ident(ident) if ident == "icydb")
        {
            output.extend(replacement.clone());
            index += 3;
            continue;
        }

        match &input[index] {
            TokenTree::Group(group) => {
                let mut rewritten = Group::new(
                    group.delimiter(),
                    rewrite_stream(group.stream(), replacement),
                );
                rewritten.set_span(group.span());
                output.extend([TokenTree::Group(rewritten)]);
            }
            TokenTree::Ident(ident) if ident == "icydb" => {
                output.extend(replacement.clone());
            }
            TokenTree::Ident(_) | TokenTree::Literal(_) | TokenTree::Punct(_) => {
                output.extend([input[index].clone()]);
            }
        }
        index += 1;
    }

    output
}

fn is_colon(token: &TokenTree) -> bool {
    matches!(token, TokenTree::Punct(punct) if punct.as_char() == ':')
}

#[cfg(test)]
mod tests {
    use quote::quote;

    use super::rewrite_icydb_path;

    #[test]
    fn explicit_dependency_path_rewrites_nested_actor_tokens() {
        let rewritten = rewrite_icydb_path(
            quote! {
                fn dispatch() -> ::icydb::Error {
                    nested!(::icydb::__macro::Path);
                }
            },
            Some("runtime_api"),
        )
        .to_string();

        assert!(rewritten.contains("runtime_api :: Error"));
        assert!(rewritten.contains("runtime_api :: __macro :: Path"));
        assert!(!rewritten.contains(":: icydb ::"));
    }

    #[test]
    fn unrelated_generated_identifiers_are_not_rewritten() {
        let rewritten = rewrite_icydb_path(
            quote!(
                fn icydb_snapshot() {}
            ),
            Some("runtime_api"),
        )
        .to_string();

        assert!(rewritten.contains("icydb_snapshot"));
        assert!(!rewritten.contains("runtime_api"));
    }
}
