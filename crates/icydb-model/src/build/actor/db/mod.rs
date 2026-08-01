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
    let private_surfaces = private_surfaces(builder);

    store::generate_store_wiring(builder, private_surfaces)
}

/// Emit private SQL and accepted-schema capabilities without public methods.
fn private_surfaces(builder: &ActorBuilder) -> TokenStream {
    let mut sql_surface = SqlSurfaceTokens::empty();
    let mut schema_surface = SchemaSurfaceTokens::empty();
    let entities = builder.get_entities();

    for (_, entity) in entities {
        let entity_name = entity.name();
        sql_surface.push_entity(entity_name);
        schema_surface.push_entity(entity_name);
    }

    quote! {
        #sql_surface
        #schema_surface
    }
}
