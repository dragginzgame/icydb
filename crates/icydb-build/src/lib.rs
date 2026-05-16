mod db;

use icydb_schema::{
    build::get_schema,
    node::{Canister, Entity, Schema, Store},
};
use proc_macro2::TokenStream;
use quote::quote;
use std::sync::Arc;

/// Generate canister actor code for the given schema path and build options.
#[must_use]
pub fn generate_with_options(canister_path: &str, options: BuildOptions) -> String {
    // Load the validated schema and resolve the requested canister node.
    let schema = get_schema().expect("schema must be valid before codegen");
    let canister = schema.cast_node::<Canister>(canister_path).unwrap();

    // Render the canister actor glue from the schema-owned metadata.
    let code = ActorBuilder::new(Arc::new(schema.clone()), canister.clone(), options);
    let tokens = code.generate();

    tokens.to_string()
}

///
/// BuildOptions
///
/// Host-provided actor generation options. Config parsing remains outside this
/// crate; callers pass already-validated booleans into codegen.
///

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BuildOptions {
    sql: BuildSqlOptions,
    metrics: BuildMetricsOptions,
    snapshot_enabled: bool,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct BuildSqlOptions {
    readonly_enabled: bool,
    ddl_enabled: bool,
    fixtures_enabled: bool,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct BuildMetricsOptions {
    enabled: bool,
    reset_enabled: bool,
}

impl BuildOptions {
    /// Build options with generated read-only SQL endpoint emission configured.
    #[must_use]
    pub const fn with_sql_readonly_enabled(mut self, enabled: bool) -> Self {
        self.sql.readonly_enabled = enabled;

        self
    }

    /// Build options with generated SQL DDL/write endpoint emission configured.
    #[must_use]
    pub const fn with_sql_ddl_enabled(mut self, enabled: bool) -> Self {
        self.sql.ddl_enabled = enabled;

        self
    }

    /// Build options with generated SQL fixture lifecycle endpoint emission configured.
    #[must_use]
    pub const fn with_sql_fixtures_enabled(mut self, enabled: bool) -> Self {
        self.sql.fixtures_enabled = enabled;

        self
    }

    /// Build options with generated metrics report endpoint emission configured.
    #[must_use]
    pub const fn with_metrics_enabled(mut self, enabled: bool) -> Self {
        self.metrics.enabled = enabled;

        self
    }

    /// Build options with generated metrics reset endpoint emission configured.
    #[must_use]
    pub const fn with_metrics_reset_enabled(mut self, enabled: bool) -> Self {
        self.metrics.reset_enabled = enabled;

        self
    }

    /// Build options with generated storage snapshot endpoint emission configured.
    #[must_use]
    pub const fn with_snapshot_enabled(mut self, enabled: bool) -> Self {
        self.snapshot_enabled = enabled;

        self
    }

    /// Return whether generated actor glue should export the read-only SQL endpoint.
    #[must_use]
    pub const fn sql_readonly_enabled(self) -> bool {
        self.sql.readonly_enabled
    }

    /// Return whether generated actor glue should export SQL DDL/write endpoints.
    #[must_use]
    pub const fn sql_ddl_enabled(self) -> bool {
        self.sql.ddl_enabled
    }

    /// Return whether generated actor glue should export SQL fixture lifecycle endpoints.
    #[must_use]
    pub const fn sql_fixtures_enabled(self) -> bool {
        self.sql.fixtures_enabled
    }

    /// Return whether generated actor glue should export metrics report endpoints.
    #[must_use]
    pub const fn metrics_enabled(self) -> bool {
        self.metrics.enabled
    }

    /// Return whether generated actor glue should export metrics reset endpoints.
    #[must_use]
    pub const fn metrics_reset_enabled(self) -> bool {
        self.metrics.reset_enabled
    }

    /// Return whether generated actor glue should export storage snapshot endpoints.
    #[must_use]
    pub const fn snapshot_enabled(self) -> bool {
        self.snapshot_enabled
    }

    /// Return whether any generated SQL endpoint surface is enabled.
    #[must_use]
    pub const fn sql_enabled(self) -> bool {
        self.sql_readonly_enabled() || self.sql_ddl_enabled() || self.sql_fixtures_enabled()
    }
}

/// Build-script helper that emits generated actor code with host-provided
/// generation options.
#[macro_export]
macro_rules! build_with_options {
    ($actor:expr, $options:expr) => {
        use std::{env::var, fs::File, io::Write, path::PathBuf};

        // Register the build inputs and generated-code cfg knobs expected by
        // the emitted actor glue.
        println!("cargo:rerun-if-changed=build.rs");
        println!("cargo:rustc-check-cfg=cfg(icydb)");
        println!("cargo:rustc-check-cfg=cfg(feature, values(\"sql\"))");
        println!("cargo:rustc-cfg=icydb");

        // Render the actor module into Cargo's output directory.
        let out_dir = var("OUT_DIR").expect("OUT_DIR not set");
        let output = ::icydb::build::generate_with_options($actor, $options);
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
    pub(crate) options: BuildOptions,
}

impl ActorBuilder {
    /// Create an actor builder for a specific canister.
    #[must_use]
    pub const fn new(schema: Arc<Schema>, canister: Canister, options: BuildOptions) -> Self {
        Self {
            schema,
            canister,
            options,
        }
    }

    /// Generate the full actor module (db/metrics/query glue).
    #[must_use]
    pub fn generate(self) -> TokenStream {
        let mut tokens = quote!();

        // Emit the shared runtime wiring and configured generated endpoints.
        tokens.extend(db::generate(&self));
        tokens.extend(generate_snapshot(&self));
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

/// Render the storage snapshot endpoint for a canister actor.
#[must_use]
fn generate_snapshot(builder: &ActorBuilder) -> TokenStream {
    if builder.options.snapshot_enabled() {
        quote! {
        #[::icydb::__reexports::canic_cdk::query]
        pub fn __icydb_snapshot() -> Result<::icydb::db::StorageReport, ::icydb::Error> {
            ::icydb::__macro::execute_generated_storage_report(&db())
        }
        }
    } else {
        TokenStream::new()
    }
}

/// Render the configured metrics endpoints for a canister actor.
#[must_use]
fn generate_metrics(builder: &ActorBuilder) -> TokenStream {
    let metrics_endpoint = builder.options.metrics_enabled().then(|| {
        quote! {
        #[::icydb::__reexports::canic_cdk::query]
        pub fn __icydb_metrics(window_start_ms: Option<u64>) -> Result<::icydb::metrics::EventReport, ::icydb::Error> {
            Ok(::icydb::metrics::metrics_report(window_start_ms))
        }
        }
    });

    let metrics_reset_endpoint = builder.options.metrics_reset_enabled().then(|| {
        quote! {
        #[::icydb::__reexports::canic_cdk::update]
        pub fn __icydb_metrics_reset() -> Result<(), ::icydb::Error> {
            ::icydb::metrics::metrics_reset_all();

            Ok(())
        }
        }
    });

    quote! {
        #metrics_endpoint
        #metrics_reset_endpoint
    }
}
