//! Host-only build helper for `icydb.toml` project configuration.
//!
//! This crate owns TOML parsing for build scripts. Runtime crates and generated
//! actor code should consume only the generated actor source emitted by
//! `icydb-build`.

mod emit;
mod error;
mod model;
mod parse;
mod resolve;

pub use emit::emit_config_for_build_script;
pub use error::ConfigBuildError;
pub use model::{GeneratedCanisterConfig, GeneratedIcydbConfig, ResolvedIcydbConfig};
pub use parse::load_resolved_icydb_toml;

const CONFIG_FILE_NAME: &str = "icydb.toml";
const CONFIG_PATH_ENV: &str = "ICYDB_CONFIG_PATH";

#[cfg(test)]
mod tests;
