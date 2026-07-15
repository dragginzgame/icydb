//! Module: lib
//! Responsibility: host-side generated actor code construction for IcyDB canisters.
//! Does not own: schema validation, runtime session semantics, or build config parsing.
//! Boundary: turns validated schema nodes and build options into generated Rust tokens.

mod db;

use std::sync::Arc;

use icydb_schema::{
    build::get_schema,
    node::{Canister, Entity, Schema, Store},
};
use proc_macro2::TokenStream;
use quote::quote;

/// Generate canister actor code for the given schema path and build options.
///
/// # Panics
///
/// Panics if the process-global schema has not validated successfully or if
/// `canister_path` does not resolve to a canister node.
#[must_use]
pub fn generate_with_options(canister_path: &str, options: BuildOptions) -> String {
    // Load the validated schema and resolve the requested canister node.
    let schema = get_schema().expect("schema must be valid before codegen");
    let canister = schema
        .cast_node::<Canister>(canister_path)
        .expect("canister path must resolve to a canister node");

    // Render the canister actor glue from the schema-owned metadata.
    let code = ActorBuilder::new(Arc::new(schema.clone()), canister.clone(), options);
    drop(schema);
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
    schema_enabled: bool,
}

impl BuildOptions {
    /// Build options with generated read-only SQL endpoint emission configured.
    #[must_use]
    pub const fn with_sql_readonly_enabled(mut self, enabled: bool) -> Self {
        self.sql.surfaces = self.sql.surfaces.with_readonly_enabled(enabled);

        self
    }

    /// Build options with generated SQL DDL endpoint emission configured.
    #[must_use]
    pub const fn with_sql_ddl_enabled(mut self, enabled: bool) -> Self {
        self.sql.surfaces = self.sql.surfaces.with_ddl_enabled(enabled);

        self
    }

    /// Build options with generated SQL fixture lifecycle endpoint emission configured.
    #[must_use]
    pub const fn with_sql_fixtures_enabled(mut self, enabled: bool) -> Self {
        self.sql.surfaces = self.sql.surfaces.with_fixtures_enabled(enabled);

        self
    }

    /// Build options with generated read-only SQL introspection configured.
    #[must_use]
    pub const fn with_sql_introspection_enabled(mut self, enabled: bool) -> Self {
        self.sql.surfaces = self.sql.surfaces.with_introspection_enabled(enabled);

        self
    }

    /// Build options with generated SQL update endpoint policy configured.
    #[must_use]
    pub const fn with_sql_update_policy(mut self, policy: Option<BuildSqlUpdatePolicy>) -> Self {
        self.sql.update_policy = policy;

        self
    }

    /// Build options with generated metrics report endpoint emission configured.
    #[must_use]
    pub const fn with_metrics_enabled(mut self, enabled: bool) -> Self {
        self.metrics.enabled = enabled;

        self
    }

    /// Build options with generated extended metrics report endpoint emission configured.
    #[must_use]
    pub const fn with_metrics_extended_enabled(mut self, enabled: bool) -> Self {
        self.metrics.extended_enabled = enabled;

        self
    }

    /// Build options with generated storage snapshot endpoint emission configured.
    #[must_use]
    pub const fn with_snapshot_enabled(mut self, enabled: bool) -> Self {
        self.snapshot_enabled = enabled;

        self
    }

    /// Build options with generated schema report endpoint emission configured.
    #[must_use]
    pub const fn with_schema_enabled(mut self, enabled: bool) -> Self {
        self.schema_enabled = enabled;

        self
    }

    /// Return whether generated actor glue should export the read-only SQL endpoint.
    #[must_use]
    pub const fn sql_readonly_enabled(self) -> bool {
        self.sql.surfaces.readonly_enabled()
    }

    /// Return whether generated actor glue should export the SQL DDL endpoint.
    #[must_use]
    pub const fn sql_ddl_enabled(self) -> bool {
        self.sql.surfaces.ddl_enabled()
    }

    /// Return whether generated actor glue should export SQL fixture lifecycle endpoints.
    #[must_use]
    pub const fn sql_fixtures_enabled(self) -> bool {
        self.sql.surfaces.fixtures_enabled()
    }

    /// Return whether generated read-only SQL endpoints should admit introspection.
    #[must_use]
    pub const fn sql_introspection_enabled(self) -> bool {
        self.sql.surfaces.introspection_enabled()
    }

    #[must_use]
    pub(crate) const fn sql_surface_flags(self) -> BuildSqlSurfaceFlags {
        self.sql.surfaces
    }

    /// Return the generated SQL update endpoint policy, if explicitly enabled.
    #[must_use]
    pub const fn sql_update_policy(self) -> Option<BuildSqlUpdatePolicy> {
        self.sql.update_policy
    }

    /// Return whether generated actor glue should export the SQL update endpoint.
    #[must_use]
    pub const fn sql_update_enabled(self) -> bool {
        self.sql_update_policy().is_some()
    }

    /// Return whether generated actor glue should export metrics report endpoints.
    #[must_use]
    pub const fn metrics_enabled(self) -> bool {
        self.metrics.enabled
    }

    /// Return whether generated actor glue should export extended metrics report endpoints.
    #[must_use]
    pub const fn metrics_extended_enabled(self) -> bool {
        self.metrics.enabled && self.metrics.extended_enabled
    }

    /// Return whether generated actor glue should export storage snapshot endpoints.
    #[must_use]
    pub const fn snapshot_enabled(self) -> bool {
        self.snapshot_enabled
    }

    /// Return whether generated actor glue should export schema report endpoints.
    #[must_use]
    pub const fn schema_enabled(self) -> bool {
        self.schema_enabled
    }

    /// Return whether any generated SQL endpoint surface is enabled.
    #[must_use]
    pub const fn sql_enabled(self) -> bool {
        self.sql_readonly_enabled()
            || self.sql_ddl_enabled()
            || self.sql_fixtures_enabled()
            || self.sql_update_enabled()
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct BuildSqlOptions {
    surfaces: BuildSqlSurfaceFlags,
    update_policy: Option<BuildSqlUpdatePolicy>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct BuildSqlSurfaceFlags(u8);

impl BuildSqlSurfaceFlags {
    const DDL: u8 = 1 << 1;
    const FIXTURES: u8 = 1 << 2;
    const INTROSPECTION: u8 = 1 << 3;
    const READONLY: u8 = 1;

    #[must_use]
    pub(crate) const fn with_readonly_enabled(self, enabled: bool) -> Self {
        self.with_flag(Self::READONLY, enabled)
    }

    #[must_use]
    pub(crate) const fn with_ddl_enabled(self, enabled: bool) -> Self {
        self.with_flag(Self::DDL, enabled)
    }

    #[must_use]
    pub(crate) const fn with_fixtures_enabled(self, enabled: bool) -> Self {
        self.with_flag(Self::FIXTURES, enabled)
    }

    #[must_use]
    pub(crate) const fn with_introspection_enabled(self, enabled: bool) -> Self {
        self.with_flag(Self::INTROSPECTION, enabled)
    }

    #[must_use]
    pub(crate) const fn readonly_enabled(self) -> bool {
        self.contains(Self::READONLY)
    }

    #[must_use]
    pub(crate) const fn ddl_enabled(self) -> bool {
        self.contains(Self::DDL)
    }

    #[must_use]
    pub(crate) const fn fixtures_enabled(self) -> bool {
        self.contains(Self::FIXTURES)
    }

    #[must_use]
    pub(crate) const fn introspection_enabled(self) -> bool {
        self.contains(Self::INTROSPECTION)
    }

    #[must_use]
    const fn contains(self, flag: u8) -> bool {
        self.0 & flag == flag
    }

    #[must_use]
    const fn with_flag(self, flag: u8, enabled: bool) -> Self {
        if enabled {
            Self(self.0 | flag)
        } else {
            Self(self.0 & !flag)
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct BuildMetricsOptions {
    enabled: bool,
    extended_enabled: bool,
}

impl Default for BuildMetricsOptions {
    fn default() -> Self {
        Self {
            enabled: true,
            extended_enabled: false,
        }
    }
}

/// Generated SQL update endpoint policy selected by actor codegen.

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BuildSqlUpdatePolicy {
    /// Expose only public-safe primary-key `UPDATE` through `icydb_update`.
    PublicPrimaryKeyOnly,
    /// Expose only public-safe bounded deterministic `UPDATE` through `icydb_update`.
    PublicBoundedDeterministic,
}

/// Build-script helper that emits generated actor code with host-provided
/// generation options.
///
/// # Panics
///
/// Panics if Cargo does not provide `OUT_DIR` to the build script process.
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
        let output = $crate::generate_with_options($actor, $options);
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

/// Render the storage snapshot endpoint for a canister actor.
#[must_use]
fn generate_snapshot(builder: &ActorBuilder) -> TokenStream {
    if builder.options.snapshot_enabled() {
        quote! {
        #[::icydb::__reexports::ic_cdk::query(name = "icydb_snapshot")]
        pub fn __icydb_snapshot() -> Result<::icydb::db::StorageReport, ::icydb::Error> {
            ::icydb::__macro::execute_generated_storage_report(&db()?)
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
        #[::icydb::__reexports::ic_cdk::query(name = "icydb_metrics")]
        pub fn __icydb_metrics(window_start_ms: Option<u64>) -> Result<::icydb::metrics::CompactMetricsReport, ::icydb::Error> {
            Ok(::icydb::metrics::compact_metrics_report(window_start_ms))
        }
        }
    });

    let metrics_extended_endpoint = builder.options.metrics_extended_enabled().then(|| {
        quote! {
        #[::icydb::__reexports::ic_cdk::query(name = "icydb_metrics_extended")]
        pub fn __icydb_metrics_extended(window_start_ms: Option<u64>) -> Result<::icydb::metrics::EventReport, ::icydb::Error> {
            Ok(::icydb::metrics::metrics_report(window_start_ms))
        }
        }
    });

    let metrics_reset_endpoint = builder.options.metrics_enabled().then(|| {
        quote! {
        #[::icydb::__reexports::ic_cdk::update(name = "icydb_metrics_reset")]
        pub fn __icydb_metrics_reset() -> Result<(), ::icydb::Error> {
            ::icydb::metrics::metrics_reset_all();

            Ok(())
        }
        }
    });

    quote! {
        #metrics_endpoint
        #metrics_extended_endpoint
        #metrics_reset_endpoint
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use icydb_schema::node::{Canister, Def, Schema};
    use proc_macro2::TokenStream;

    use super::{ActorBuilder, BuildOptions};

    fn compact_tokens(tokens: TokenStream) -> String {
        tokens
            .to_string()
            .chars()
            .filter(|character| !character.is_whitespace())
            .collect()
    }

    fn actor_builder_with_options(options: BuildOptions) -> ActorBuilder {
        ActorBuilder::new(
            Arc::new(Schema::new()),
            Canister::new(Def::new("test", "Canister"), "test", 0, 1, 2),
            options,
        )
    }

    #[test]
    fn default_build_options_enable_minimal_metrics_only() {
        let options = BuildOptions::default();

        assert!(!options.sql_readonly_enabled());
        assert!(!options.sql_ddl_enabled());
        assert!(!options.sql_fixtures_enabled());
        assert!(!options.sql_introspection_enabled());
        assert!(!options.sql_update_enabled());
        assert_eq!(options.sql_update_policy(), None);
        assert!(options.metrics_enabled());
        assert!(!options.metrics_extended_enabled());
        assert!(!options.snapshot_enabled());
        assert!(!options.schema_enabled());
    }

    #[test]
    fn extended_metrics_requires_metrics_surface() {
        let options = BuildOptions::default()
            .with_metrics_enabled(false)
            .with_metrics_extended_enabled(true);

        assert!(!options.metrics_enabled());
        assert!(!options.metrics_extended_enabled());

        let options = options.with_metrics_enabled(true);

        assert!(options.metrics_enabled());
        assert!(options.metrics_extended_enabled());
    }

    #[test]
    fn generated_metrics_surface_uses_public_icydb_endpoint_names() {
        let builder =
            actor_builder_with_options(BuildOptions::default().with_metrics_extended_enabled(true));
        let surface = compact_tokens(super::generate_metrics(&builder));

        assert!(surface.contains("name=\"icydb_metrics\""));
        assert!(surface.contains("name=\"icydb_metrics_extended\""));
        assert!(surface.contains("name=\"icydb_metrics_reset\""));
        assert!(surface.contains("pubfn__icydb_metrics("));
        assert!(surface.contains("pubfn__icydb_metrics_extended("));
        assert!(surface.contains("pubfn__icydb_metrics_reset("));
    }

    #[test]
    fn generated_snapshot_surface_uses_public_icydb_endpoint_name() {
        let builder =
            actor_builder_with_options(BuildOptions::default().with_snapshot_enabled(true));
        let surface = compact_tokens(super::generate_snapshot(&builder));

        assert!(surface.contains("name=\"icydb_snapshot\""));
        assert!(surface.contains("pubfn__icydb_snapshot("));
    }
}
