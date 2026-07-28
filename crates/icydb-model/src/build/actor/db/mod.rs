//! Module: db
//! Responsibility: generated database session wiring for one canister actor.
//! Does not own: schema validation, store runtime semantics, or SQL planning.
//! Boundary: assembles store, SQL, and schema-surface token bundles.

mod schema;
mod sql;
mod store;

use crate::build::actor::ActorBuilder;
use schema::SchemaSurfaceTokens;
use sql::SqlSurfaceTokens;

use proc_macro2::TokenStream;
use quote::quote;
/// Render the generated store/session wiring for one canister actor.
#[must_use]
pub fn generate(builder: &ActorBuilder) -> TokenStream {
    let frontend_surfaces = frontend_surfaces(builder);

    store::generate_store_wiring(builder, frontend_surfaces)
}

/// Emit optional SQL and accepted-schema frontend surfaces.
fn frontend_surfaces(builder: &ActorBuilder) -> TokenStream {
    let mut sql_surface = builder.options.sql_enabled().then(|| {
        SqlSurfaceTokens::empty(
            builder.options.sql_surface_flags(),
            builder.options.sql_update_policy(),
        )
    });
    let mut schema_surface = builder
        .options
        .schema_enabled()
        .then(SchemaSurfaceTokens::empty);
    let entities = builder.get_entities();

    for (_, entity) in entities {
        let entity_source_key = entity.source_key();
        let entity_name = entity.resolved_name();
        if let Some(sql_surface) = sql_surface.as_mut() {
            sql_surface.push_entity(entity_name);
        }
        if let Some(schema_surface) = schema_surface.as_mut() {
            schema_surface.push_entity(entity_source_key);
        }
    }
    let sql_surface = sql_surface.map_or_else(TokenStream::new, |sql_surface| quote!(#sql_surface));
    let schema_surface =
        schema_surface.map_or_else(TokenStream::new, |schema_surface| quote!(#schema_surface));

    quote! {
        #sql_surface
        #schema_surface
    }
}
