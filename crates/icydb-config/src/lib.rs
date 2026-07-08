//! Module: lib
//! Responsibility: shared host-side support for `icydb.toml` project configuration.
//! Does not own: runtime configuration, generated actor code, or schema semantics.
//! Boundary: exposes resolved generated-config models for build scripts and CLI tools.
//!
//! This crate owns existing config discovery, TOML parsing, and validation for
//! build scripts and CLI commands. Runtime crates and generated actor code
//! should consume only the generated actor source emitted behind the public
//! `icydb::build` facade.

mod emit;
mod error;
mod model;
mod parse;
mod resolve;
#[cfg(test)]
mod tests;

pub use emit::{emit_config_for_build_script, emit_configured_canister_for_build_script};
pub use error::ConfigError;
pub use model::{
    GeneratedBuildTarget, GeneratedCanisterConfig, GeneratedIcydbConfig, GeneratedMetricsMode,
    GeneratedMetricsPolicy, GeneratedSqlIntrospectionPolicy, GeneratedSqlUpdatePolicy,
    ResolvedIcydbConfig,
};
pub use parse::load_resolved_icydb_toml;
pub use resolve::resolve_existing_icydb_toml;

/// Standard IcyDB project configuration filename.
pub const ICYDB_CONFIG_FILE_NAME: &str = "icydb.toml";
/// Build-script environment variable used to resolve target-sensitive defaults.
pub const ICYDB_BUILD_TARGET_ENV: &str = "ICYDB_BUILD_TARGET";
const CONFIG_PATH_ENV: &str = "ICYDB_CONFIG_PATH";

/// Emit generated actor glue for one canister using the effective
/// `icydb.toml` switches.
///
/// Downstream canister build scripts should normally call this through
/// `icydb::build::build_configured_canister!()`.
#[macro_export]
macro_rules! build_configured_canister {
    ($canister_ty:ty, $canister_path:literal, $canister_name:literal) => {{
        let _ = ::std::any::TypeId::of::<$canister_ty>();
        $crate::emit_configured_canister_for_build_script($canister_path, $canister_name)?;
    }};
}
