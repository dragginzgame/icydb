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
/// Generate canister actor code for the given schema path.
#[must_use]
pub fn generate(canister_path: &str) -> String {
    // load schema and get the specified canister
    let schema = get_schema().expect("schema must be valid before codegen");

    // Resolve the requested canister path against the registered schema nodes.
    // Build scripts pass absolute Rust paths, while schema registration stores
    // the derive-time module path. Depending on how the fixture crate is wired,
    // those two can differ only by the leading crate segment, so codegen needs
    // one suffix-based fallback instead of panicking on the first exact miss.
    let canister = resolve_canister(&schema, canister_path);

    // create the ActorBuilder and generate the code
    let code = ActorBuilder::new(Arc::new(schema.clone()), canister);
    let tokens = code.generate();

    tokens.to_string()
}

// Resolve one canister path from the registered schema nodes.
// Exact matches stay authoritative. If that misses, accept one unique schema
// canister whose registered path is a suffix of the requested Rust path.
fn resolve_canister(schema: &Schema, canister_path: &str) -> Canister {
    if let Ok(canister) = schema.cast_node::<Canister>(canister_path) {
        return canister.clone();
    }

    let matching_canisters = schema
        .get_nodes::<Canister>()
        .filter(|(path, _)| canister_path.ends_with(path))
        .map(|(path, canister)| (path.to_string(), canister.clone()))
        .collect::<Vec<_>>();

    match matching_canisters.as_slice() {
        [(_, canister)] => canister.clone(),
        [] => panic!(
            "codegen canister path `{canister_path}` was not found; registered canisters: {}",
            schema
                .get_nodes::<Canister>()
                .map(|(path, _)| path.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        ),
        _ => panic!(
            "codegen canister path `{canister_path}` matched multiple registered canisters: {}",
            matching_canisters
                .iter()
                .map(|(path, _)| path.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}

///
/// ActorBuilder
///

pub(crate) struct ActorBuilder {
    pub(crate) schema: Arc<Schema>,
    pub(crate) canister: Canister,
}

impl ActorBuilder {
    /// Create an actor builder for a specific canister.
    #[must_use]
    pub const fn new(schema: Arc<Schema>, canister: Canister) -> Self {
        Self { schema, canister }
    }

    /// Generate the full actor module (db/metrics/query glue).
    #[must_use]
    pub fn generate(self) -> TokenStream {
        let mut tokens = quote!();

        // shared between all canisters
        tokens.extend(db::generate(&self));
        tokens.extend(metrics::generate(&self));

        quote! {
            #tokens
        }
    }

    /// All stores belonging to the current canister, keyed by path.
    #[must_use]
    pub fn get_stores(&self) -> Vec<(String, Store)> {
        let canister_path = self.canister.def().path();

        self.schema
            .filter_nodes::<Store>(|node| node.canister() == canister_path)
            .map(|(path, store)| (path.to_string(), store.clone()))
            .collect()
    }

    /// All entities belonging to the current canister, keyed by path.
    #[must_use]
    pub fn get_entities(&self) -> Vec<(String, Entity)> {
        let canister_path = self.canister.def().path();

        self.schema
            .get_nodes::<Entity>()
            .filter_map(|(path, entity)| {
                let store = self.schema.cast_node::<Store>(entity.store()).ok()?;
                if store.canister() == canister_path {
                    Some((path.to_string(), entity.clone()))
                } else {
                    None
                }
            })
            .collect()
    }
}
