//! Shared host-side support for `icydb.toml` project configuration.
//!
//! This crate owns existing config discovery, TOML parsing, and validation for
//! build scripts and CLI commands. Runtime crates and generated actor code
//! should consume only the generated actor source emitted by `icydb-build`.

mod emit;
mod error;
mod model;
mod parse;
mod resolve;

pub use emit::emit_config_for_build_script;
pub use error::ConfigError;
pub use model::{
    GeneratedBuildTarget, GeneratedCanisterConfig, GeneratedIcydbConfig,
    GeneratedSqlIntrospectionPolicy, GeneratedSqlUpdatePolicy, ResolvedIcydbConfig,
};
pub use parse::load_resolved_icydb_toml;
pub use resolve::resolve_existing_icydb_toml;

/// Standard IcyDB project configuration filename.
pub const ICYDB_CONFIG_FILE_NAME: &str = "icydb.toml";
/// Build-script environment variable used to resolve target-sensitive defaults.
pub const ICYDB_BUILD_TARGET_ENV: &str = "ICYDB_BUILD_TARGET";
const CONFIG_PATH_ENV: &str = "ICYDB_CONFIG_PATH";

#[cfg(test)]
mod tests;
