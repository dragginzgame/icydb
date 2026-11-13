use quote::quote;

use super::SchemaSurfaceTokens;

fn compact_tokens(tokens: &proc_macro2::TokenStream) -> String {
    tokens
        .to_string()
        .chars()
        .filter(|character| !character.is_whitespace())
        .collect()
}

#[test]
fn generated_schema_capability_resolves_current_entity_names_privately() {
    let mut surface_tokens = SchemaSurfaceTokens::empty(quote!(::icydb));
    surface_tokens.push_entity("Character");

    let surface = compact_tokens(&quote!(#surface_tokens));

    assert!(surface.contains("fn__icydb_endpoint_handler_schema("));
    assert!(surface.contains("try_describe_entity_by_source_key(\"Character\")"));
    assert!(surface.contains("__macro::into_deliverable_schema_reply("));
    assert!(!surface.contains("crate::Character"));
    assert!(!surface.contains("icydb_schema"));
    assert!(!surface.contains("ic_cdk::query"));
}

#[test]
fn generated_schema_private_handler_cannot_export_a_method() {
    let mut surface_tokens = SchemaSurfaceTokens::empty(quote!(::icydb));
    surface_tokens.push_entity("Character");
    let file = syn::parse2::<syn::File>(quote!(#surface_tokens))
        .expect("generated schema surface should remain valid Rust syntax");
    let handler = file
        .items
        .into_iter()
        .find_map(|item| match item {
            syn::Item::Fn(function) if function.sig.ident == "__icydb_endpoint_handler_schema" => {
                Some(function)
            }
            _ => None,
        })
        .expect("generated schema surface should contain its private handler");

    assert!(matches!(handler.vis, syn::Visibility::Restricted(_)));

    assert!(
        handler
            .attrs
            .iter()
            .all(|attribute| attribute.path().is_ident("allow"))
    );
}
