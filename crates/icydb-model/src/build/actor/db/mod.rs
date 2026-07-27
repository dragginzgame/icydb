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
    let entity_registrations = entity_registrations(builder);

    store::generate_store_wiring(builder, entity_registrations)
}

/// Emit proposal/runtime registration pairs for entities bound to this canister.
fn entity_registrations(builder: &ActorBuilder) -> TokenStream {
    let mut registration_inits = quote!();
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

    for (entity_path, entity) in entities {
        let entity_source_key = entity.source_key();
        let store_path = entity.store();
        let entity_name = entity.resolved_name();
        registration_inits.extend(quote! {
            ::icydb::__macro::EntityRegistration::<__IcydbGeneratedCanister>::new(
                #entity_source_key,
                #entity_path,
                #store_path,
            ),
        });
        if let Some(sql_surface) = sql_surface.as_mut() {
            sql_surface.push_entity(entity_path.as_str(), entity_name);
        }
        if let Some(schema_surface) = schema_surface.as_mut() {
            schema_surface.push_entity(entity_path.as_str(), entity_name);
        }
    }
    let sql_surface = sql_surface.map_or_else(TokenStream::new, |sql_surface| quote!(#sql_surface));
    let schema_surface =
        schema_surface.map_or_else(TokenStream::new, |schema_surface| quote!(#schema_surface));

    quote! {
        static ENTITY_REGISTRATIONS: &[
            ::icydb::__macro::EntityRegistration<__IcydbGeneratedCanister>
        ] = &[
            #registration_inits
        ];

        #sql_surface
        #schema_surface
    }
}
