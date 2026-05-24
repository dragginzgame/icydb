mod schema;
mod sql;
mod store;

use crate::ActorBuilder;
use proc_macro2::TokenStream;
use quote::quote;
use schema::SchemaSurfaceTokens;
use sql::SqlSurfaceTokens;
use syn::parse_str;

/// Render the generated store/session wiring for one canister actor.
#[must_use]
pub fn generate(builder: &ActorBuilder) -> TokenStream {
    let canister_path_source = builder.canister_path();
    let canister_path: syn::Path = parse_str(&canister_path_source)
        .unwrap_or_else(|_| panic!("invalid canister path: {canister_path_source}"));
    let entity_runtime_hooks = entity_runtime_hooks(builder, &canister_path);

    store::generate_store_wiring(builder, &canister_path, entity_runtime_hooks)
}

/// Emit the entity runtime hook table for all entities bound to this canister.
fn entity_runtime_hooks(builder: &ActorBuilder, canister_path: &syn::Path) -> TokenStream {
    let mut hook_inits = quote!();
    let mut sql_surface = builder.options.sql_enabled().then(|| {
        SqlSurfaceTokens::empty(
            builder.options.sql_readonly_enabled(),
            builder.options.sql_ddl_enabled(),
            builder.options.sql_fixtures_enabled(),
        )
    });
    let mut schema_surface = builder
        .options
        .schema_enabled()
        .then(SchemaSurfaceTokens::empty);
    let entities = builder.get_entities();

    for (entity_path, _) in entities {
        let entity_ty: syn::Path = parse_str(&entity_path)
            .unwrap_or_else(|_| panic!("invalid entity path: {entity_path}"));
        hook_inits.extend(quote! {
            ::icydb::__macro::EntityRuntimeHooks::<#canister_path>::for_entity::<#entity_ty>(),
        });
        if let Some(sql_surface) = sql_surface.as_mut() {
            sql_surface.push_entity(&entity_ty);
        }
        if let Some(schema_surface) = schema_surface.as_mut() {
            schema_surface.push_entity(&entity_ty);
        }
    }
    let sql_surface = sql_surface.map_or_else(TokenStream::new, |sql_surface| quote!(#sql_surface));
    let schema_surface =
        schema_surface.map_or_else(TokenStream::new, |schema_surface| quote!(#schema_surface));

    quote! {
        static ENTITY_RUNTIME_HOOKS: &[
            ::icydb::__macro::EntityRuntimeHooks<#canister_path>
        ] = &[
            #hook_inits
        ];

        #sql_surface
        #schema_surface
    }
}
