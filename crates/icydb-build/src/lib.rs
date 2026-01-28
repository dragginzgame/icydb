mod db;
mod macros;
mod metrics;

use icydb_schema::{
    build::get_schema,
    node::{Canister, Entity, Schema, Store},
};
use proc_macro2::TokenStream;
use quote::quote;
use std::sync::Arc;

// generate
#[must_use]
/// Generate canister actor code for the given schema path.
pub fn generate(canister_path: &str) -> String {
    // load schema and get the specified canister
    let schema = get_schema().expect("schema must be valid before codegen");

    // filter by name
    let canister = schema.cast_node::<Canister>(canister_path).unwrap();

    // create the ActorBuilder and generate the code
    let code = ActorBuilder::new(Arc::new(schema.clone()), canister.clone());
    let tokens = code.generate();

    tokens.to_string()
}

///
/// ActorBuilder
///

pub(crate) struct ActorBuilder {
    pub(crate) schema: Arc<Schema>,
    pub(crate) canister: Canister,
}

impl ActorBuilder {
    // new
    #[must_use]
    /// Create an actor builder for a specific canister.
    pub const fn new(schema: Arc<Schema>, canister: Canister) -> Self {
        Self { schema, canister }
    }

    // generate
    #[must_use]
    /// Generate the full actor module (db/metrics/query glue).
    pub fn generate(self) -> TokenStream {
        let mut tokens = quote!();

        // shared between all canisters
        tokens.extend(db::generate(&self));
        tokens.extend(metrics::generate(&self));

        quote! {
            #tokens
        }
    }

    // get_stores
    #[must_use]
    /// All stores belonging to the current canister, keyed by path.
    pub fn get_stores(&self) -> Vec<(String, Store)> {
        let canister_path = self.canister.def.path();

        self.schema
            .filter_nodes::<Store>(|node| node.canister == canister_path)
            .map(|(path, store)| (path.to_string(), store.clone()))
            .collect()
    }

    // get_entities
    // helper function to get all the entities for the current canister
    #[must_use]
    /// All entities attached to the current canister, keyed by path.
    pub fn get_entities(&self) -> Vec<(String, Entity)> {
        let canister_path = self.canister.def.path();
        let mut entities = Vec::new();

        for (store_path, _) in self
            .schema
            .filter_nodes::<Store>(|node| node.canister == canister_path)
        {
            for (entity_path, entity) in self
                .schema
                .filter_nodes::<Entity>(|node| node.store == store_path)
            {
                entities.push((entity_path.to_string(), entity.clone()));
            }
        }

        entities
    }
}
