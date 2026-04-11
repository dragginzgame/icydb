mod db;

use icydb_schema::{
    build::get_schema,
    node::{Canister, Entity, Schema, Store},
};
use proc_macro2::TokenStream;
use quote::quote;
use std::sync::Arc;

/// Generate canister actor code for the given schema path.
#[must_use]
pub fn generate(canister_path: &str) -> String {
    // Load the validated schema and resolve the requested canister node.
    let schema = get_schema().expect("schema must be valid before codegen");
    let canister = schema.cast_node::<Canister>(canister_path).unwrap();

    // Render the canister actor glue from the schema-owned metadata.
    let code = ActorBuilder::new(Arc::new(schema.clone()), canister.clone());
    let tokens = code.generate();

    tokens.to_string()
}

/// Build-script helper that emits generated actor code for one schema canister path.
///
/// The generated file only contains actor/runtime wiring. Schema registration
/// remains derive-owned and is not performed by this helper.
#[macro_export]
macro_rules! build {
    ($actor:expr) => {
        use std::{env::var, fs::File, io::Write, path::PathBuf};

        // Register the build inputs and generated-code cfg knobs expected by
        // the emitted actor glue.
        println!("cargo:rerun-if-changed=build.rs");
        println!("cargo:rustc-check-cfg=cfg(icydb)");
        println!("cargo:rustc-check-cfg=cfg(feature, values(\"sql\"))");
        println!("cargo:rustc-cfg=icydb");

        // Render the actor module into Cargo's output directory.
        let out_dir = var("OUT_DIR").expect("OUT_DIR not set");
        let output = ::icydb::build::generate($actor);
        let actor_file = PathBuf::from(out_dir.clone()).join("actor.rs");
        let mut file = File::create(actor_file)?;
        file.write_all(output.as_bytes())?;
    };
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

        // Emit the shared runtime wiring and the generated metrics endpoints.
        tokens.extend(db::generate(&self));
        tokens.extend(generate_metrics(&self));

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

/// Render the metrics/snapshot endpoints for a canister actor.
#[must_use]
fn generate_metrics(_builder: &ActorBuilder) -> TokenStream {
    quote! {
        #[::icydb::__reexports::canic_cdk::query]
        pub fn icydb_snapshot() -> Result<::icydb::db::StorageReport, ::icydb::Error> {
            ::icydb::__macro::execute_generated_storage_report(&db())
        }

        #[::icydb::__reexports::canic_cdk::query]
        pub fn icydb_metrics(window_start_ms: Option<u64>) -> Result<::icydb::metrics::EventReport, ::icydb::Error> {
            Ok(::icydb::metrics::metrics_report(window_start_ms))
        }

        #[::icydb::__reexports::canic_cdk::update]
        pub fn icydb_metrics_reset() -> Result<(), ::icydb::Error> {
            ::icydb::metrics::metrics_reset_all();

            Ok(())
        }
    }
}
