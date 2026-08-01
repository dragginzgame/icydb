//! Module: build::actor
//! Responsibility: host-side generated actor code construction for IcyDB canisters.
//! Does not own: schema validation, runtime session semantics, or build config parsing.
//! Boundary: turns validated schema nodes into generated private Rust capabilities.

mod crate_path;
mod db;
#[allow(
    dead_code,
    reason = "0.217 stages the private declaration compiler before the atomic Patch 5 authority cut"
)]
mod endpoint;

use std::sync::Arc;

use crate::{
    build::get_schema,
    node::{Canister, Entity, Schema, Store},
};
use icydb_schema::encode_schema_fragment;
use proc_macro2::TokenStream;
use quote::quote;
use sha2::{Digest, Sha256};

/// Generate private canister actor code for the given authored schema path.
///
/// # Panics
///
/// Panics if the process-global schema has not validated successfully,
/// `canister_path` does not resolve to a canister node, or the consuming
/// package's `icydb` dependency path cannot be resolved.
#[must_use]
pub fn generate(canister_path: &str) -> String {
    generate_with_crate_path(canister_path, None)
}

fn generate_with_crate_path(canister_path: &str, icydb_crate_path: Option<&str>) -> String {
    // Load the validated schema and resolve the requested canister node.
    let schema = get_schema().expect("schema must be valid before codegen");
    let canister = schema
        .cast_node::<Canister>(canister_path)
        .expect("canister path must resolve to a canister node");
    let fragment = schema
        .schema_fragment_for_canister(canister_path)
        .expect("sealed canister database closure must lower into a schema fragment");

    // Render the canister actor glue from the schema-owned metadata.
    let code = ActorBuilder::new(Arc::new(schema.clone()), canister.clone(), fragment);
    drop(schema);
    let tokens = crate_path::rewrite_icydb_path(code.generate(), icydb_crate_path);

    tokens.to_string()
}

///
/// ActorBuilder
///
/// Internal codegen helper that renders one canister's generated runtime
/// module from the validated schema graph.
///

pub(crate) struct ActorBuilder {
    pub(crate) schema: Arc<Schema>,
    pub(crate) canister: Canister,
    pub(crate) schema_fragment_bytes: Vec<u8>,
    pub(crate) schema_submission_key: String,
}

impl ActorBuilder {
    /// Create an actor builder for a specific canister.
    #[must_use]
    pub fn new(
        schema: Arc<Schema>,
        canister: Canister,
        fragment: icydb_schema::SchemaFragment,
    ) -> Self {
        let schema_fragment_bytes =
            encode_schema_fragment(&fragment).expect("sealed schema fragment must encode");
        let digest = Sha256::digest(schema_fragment_bytes.as_slice());
        let schema_submission_key = format!("generated/{}", hex_bytes(digest.as_slice()));

        Self {
            schema,
            canister,
            schema_fragment_bytes,
            schema_submission_key,
        }
    }

    /// Generate the full actor module (db/metrics/query glue).
    #[must_use]
    pub fn generate(self) -> TokenStream {
        let mut tokens = quote!();

        // Emit private runtime wiring and endpoint capabilities only.
        tokens.extend(db::generate(&self));
        tokens.extend(generate_endpoint_runtime());

        quote! {
            #tokens
        }
    }

    /// All stores belonging to the current canister, keyed by path.
    #[must_use]
    pub fn get_stores(&self) -> Vec<(String, Store)> {
        let canister_path = self.canister_path();

        self.schema
            .filter_nodes::<Store>(|node| node.canister() == canister_path)
            .map(|(path, store)| (path.to_string(), store.clone()))
            .collect()
    }

    /// All entities belonging to the current canister, keyed by path.
    #[must_use]
    pub fn get_entities(&self) -> Vec<(String, Entity)> {
        let canister_path = self.canister_path();

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

    fn canister_path(&self) -> String {
        self.canister.def().path()
    }
}

fn hex_bytes(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

/// Render private operational capabilities and authorization policies used by
/// source-declared endpoint wrappers.
fn generate_endpoint_runtime() -> TokenStream {
    let authorization = endpoint::emit_endpoint_authorization_helpers();

    quote! {
        #authorization

        #[allow(unused_imports)]
        pub(crate) mod endpoint_handlers {
            pub(crate) use super::__icydb_endpoint_handler_schema as schema;

            ::icydb::__icydb_with_sql_items! {
                pub(crate) use super::__icydb_endpoint_handler_fixtures_load as fixtures_load;
                pub(crate) use super::__icydb_endpoint_handler_fixtures_reset as fixtures_reset;
                pub(crate) use super::__icydb_endpoint_handler_sql_ddl as sql_ddl;
                pub(crate) use super::__icydb_endpoint_handler_sql_integrity as sql_integrity;
                pub(crate) use super::__icydb_endpoint_handler_sql_query as sql_query;
                pub(crate) use super::__icydb_endpoint_handler_sql_update_bounded as sql_update_bounded;
                pub(crate) use super::__icydb_endpoint_handler_sql_update_primary_key as sql_update_primary_key;
            }

            #[allow(clippy::unnecessary_wraps)]
            pub(crate) fn metrics(
                window_start_ms: Option<u64>,
            ) -> Result<::icydb::metrics::CompactMetricsReport, ::icydb::Error> {
                Ok(::icydb::metrics::compact_metrics_report(window_start_ms))
            }

            ::icydb::__icydb_with_metrics_extended_items! {
                #[allow(clippy::unnecessary_wraps)]
                pub(crate) fn metrics_extended(
                    window_start_ms: Option<u64>,
                ) -> Result<::icydb::metrics::EventReport, ::icydb::Error> {
                    Ok(::icydb::metrics::metrics_report(window_start_ms))
                }
            }

            #[allow(clippy::unnecessary_wraps)]
            pub(crate) fn metrics_reset() -> Result<(), ::icydb::Error> {
                ::icydb::metrics::metrics_reset_all();
                Ok(())
            }

            pub(crate) fn snapshot() -> Result<::icydb::db::StorageReport, ::icydb::Error> {
                ::icydb::__macro::execute_generated_storage_report(&super::db()?)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::node::{Canister, Def, Schema};
    use proc_macro2::TokenStream;

    use super::ActorBuilder;

    fn compact_tokens(tokens: TokenStream) -> String {
        tokens
            .to_string()
            .chars()
            .filter(|character| !character.is_whitespace())
            .collect()
    }

    fn actor_builder() -> ActorBuilder {
        ActorBuilder::new(
            Arc::new(Schema::new()),
            Canister::new(Def::new("test", "Canister"), "test", 0, 1, 2, 3),
            icydb_schema::SchemaFragment::try_new(Vec::new(), Vec::new())
                .expect("empty test fragment should admit"),
        )
    }

    #[test]
    fn generated_endpoint_runtime_is_private_and_contains_no_export_attributes() {
        let surface = compact_tokens(super::generate_endpoint_runtime());

        for handler in [
            "metrics",
            "metrics_extended",
            "metrics_reset",
            "snapshot",
            "schema",
            "sql_query",
        ] {
            assert!(surface.contains(handler));
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
    fn complete_generated_actor_cannot_create_a_public_endpoint() {
        let surface = compact_tokens(actor_builder().generate());

        assert!(surface.contains("pub(crate)modendpoint_handlers"));
        assert!(surface.contains("pub(crate)modendpoint_authorization"));
        for forbidden in [
            "ic_cdk::query",
            "ic_cdk::update",
            "export_name",
            "no_mangle",
        ] {
            assert!(!surface.contains(forbidden));
        }
    }
}
