use quote::quote;

use super::SqlSurfaceTokens;

fn compact_tokens(tokens: &proc_macro2::TokenStream) -> String {
    tokens
        .to_string()
        .chars()
        .filter(|character| !character.is_whitespace())
        .collect()
}

#[test]
fn generated_sql_capability_contains_every_private_handler_and_no_export() {
    let mut surface = SqlSurfaceTokens::empty(quote!(::icydb));
    surface.push_entity("Character");
    let surface = compact_tokens(&quote!(#surface));

    for handler in [
        "sql_query_public",
        "sql_query_trusted",
        "sql_ddl",
        "fixtures_reset",
        "fixtures_load",
        "sql_update_primary_key",
        "sql_update_bounded",
        "sql_integrity",
    ] {
        assert!(surface.contains(format!("__icydb_endpoint_handler_{handler}").as_str()));
    }
    for forbidden in [
        "ic_cdk::query",
        "ic_cdk::update",
        "export_name",
        "no_mangle",
    ] {
        assert!(!surface.contains(forbidden));
    }
}

#[test]
fn generated_sql_capability_keeps_both_sealed_update_admissions() {
    let mut surface = SqlSurfaceTokens::empty(quote!(::icydb));
    surface.push_entity("Character");
    let surface = compact_tokens(&quote!(#surface));

    assert!(surface.contains("execute_sql_public_primary_key_update(&dispatch)"));
    assert!(surface.contains("execute_sql_public_bounded_update(&dispatch)"));
    assert!(surface.contains("__icydb_sql_surface_update_primary_key_dispatch"));
    assert!(surface.contains("__icydb_sql_surface_update_bounded_dispatch"));
}

#[test]
fn generated_sql_capability_uses_trusted_query_and_admin_ddl() {
    let mut surface = SqlSurfaceTokens::empty(quote!(::icydb));
    surface.push_entity("Character");
    let surface = compact_tokens(&quote!(#surface));

    assert!(surface.contains("execute_trusted_sql_query_dispatch(&dispatch,&[])"));
    assert!(surface.contains("execute_public_sql_query_page_dispatch(&dispatch,&[],continuation)"));
    assert!(surface.contains("into_deliverable_query_reply()"));
    assert!(surface.contains("execute_admin_sql_ddl_dispatch(&dispatch)"));
}

#[test]
fn generated_sql_query_capability_keeps_introspection_gate() {
    let mut surface = SqlSurfaceTokens::empty(quote!(::icydb));
    surface.push_entity("Character");
    let surface = compact_tokens(&quote!(#surface));

    assert!(surface.contains("if!INTROSPECTION&&dispatch.requires_introspection()"));
    assert!(surface.contains("SqlIntrospectionDisabled"));
}

#[test]
fn generated_fixture_load_reuses_reset_then_calls_handler_once() {
    let surface = SqlSurfaceTokens::empty(quote!(::icydb));
    let handler = compact_tokens(&surface.handler_tokens());
    let reset = handler
        .find("__icydb_endpoint_handler_fixtures_reset()?")
        .expect("fixture load should propagate reset failure");
    let load = handler
        .find("handler()")
        .expect("fixture load should invoke the checked handler");

    assert!(reset < load);
    assert_eq!(handler.matches("handler()").count(), 1);
}

#[test]
fn empty_and_entity_sql_capabilities_keep_exact_dispatch_boundaries() {
    let empty_surface = SqlSurfaceTokens::empty(quote!(::icydb));
    let empty = compact_tokens(&quote!(#empty_surface));
    let mut entity = SqlSurfaceTokens::empty(quote!(::icydb));
    entity.push_entity("Character");
    let entity = compact_tokens(&quote!(#entity));

    assert!(empty.contains("SqlQueryNoConfiguredEntities"));
    assert!(!empty.contains("execute_trusted_sql_query_dispatch"));
    assert!(entity.contains("execute_trusted_sql_query_dispatch"));
    assert!(entity.contains("DELETEFROMCharacter"));
}
