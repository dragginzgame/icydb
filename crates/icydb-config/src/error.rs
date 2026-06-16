//! Module: error
//! Responsibility: path-aware diagnostics for host-side config loading.
//! Does not own: runtime database errors, schema diagnostics, or CLI rendering policy.
//! Boundary: captures config discovery, read, parse, and canister-name validation failures.

use std::{io, path::PathBuf};

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

    #[error("IcyDB config at '{}' contains an empty canister name", path.display())]
    EmptyCanisterName { path: PathBuf },

    #[error(
        "IcyDB config at '{}' has ambiguous canister names '{first}' and '{second}' after normalization",
        path.display()
    )]
    AmbiguousCanisterName {
        path: PathBuf,
        first: String,
        second: String,
    },

    #[error(
        "IcyDB config at '{}' contains canister '{canister}' but the generated schema has no matching canister",
        path.display()
    )]
    UnknownCanister { path: PathBuf, canister: String },

    #[error(
        "generated schema canister names '{first}' and '{second}' are ambiguous after normalization"
    )]
    AmbiguousKnownCanister { first: String, second: String },
}
