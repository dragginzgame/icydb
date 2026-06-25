//! Module: error
//! Responsibility: path-aware diagnostics for host-side config loading.
//! Does not own: runtime database errors, schema diagnostics, or CLI rendering policy.
//! Boundary: captures config discovery, read, parse, and canister-name validation failures.

use std::{env, io, path::PathBuf};

use thiserror::Error as ThisError;

/// Config loading error with path-aware diagnostics.

#[derive(Debug, ThisError)]
pub enum ConfigError {
    #[error("failed to read IcyDB config at '{}': {source}", path.display())]
    Read { path: PathBuf, source: io::Error },

    #[error("failed to parse IcyDB config at '{}': {source}", path.display())]
    Parse {
        path: PathBuf,
        source: toml::de::Error,
    },

    #[error("failed to resolve current directory for IcyDB config discovery: {source}")]
    CurrentDir { source: io::Error },

    #[error("failed to resolve generated actor output directory from OUT_DIR: {source}")]
    OutDir { source: env::VarError },

    #[error("failed to write generated actor at '{}': {source}", path.display())]
    WriteGeneratedActor { path: PathBuf, source: io::Error },

    #[error("IcyDB config at '{}' contains an empty canister name", path.display())]
    EmptyCanisterName { path: PathBuf },

    #[error(
        "IcyDB config at '{}' contains canister '{canister}', but canister names must be lower snake_case ASCII",
        path.display()
    )]
    InvalidCanisterName { path: PathBuf, canister: String },

    #[error(
        "IcyDB config at '{}' contains canister '{canister}' but the generated schema has no matching canister",
        path.display()
    )]
    UnknownCanister { path: PathBuf, canister: String },

    #[error("generated schema canister name '{canister}' is not lower snake_case ASCII")]
    InvalidKnownCanisterName { canister: String },

    #[error("generated schema canister name '{first}' is duplicated by '{second}'")]
    AmbiguousKnownCanister { first: String, second: String },
}
