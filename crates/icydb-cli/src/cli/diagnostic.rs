//! Module: CLI diagnostic command arguments.
//! Responsibility: define diagnostic lookup and structured-error input arguments.
//! Does not own: diagnostic registry rendering or canister error transport.
//! Boundary: select one diagnostic input plus optional schema resolvers.

use std::path::{Path, PathBuf};

use clap::Args;

use crate::cli::{DEFAULT_ENVIRONMENT, ICP_ENVIRONMENT_ENV};

///
/// DiagnosticArgs
///
/// DiagnosticArgs selects a compact code or a complete public error payload.
/// Manual codes remain strings so users can paste either `E7` or `7`.
///

#[derive(Args, Debug)]
pub(crate) struct DiagnosticArgs {
    /// Compact IcyDB error code, for example E7, 7, E190, or 190.
    #[arg(
        value_name = "CODE",
        required_unless_present = "error_json",
        conflicts_with = "error_json"
    )]
    code: Option<String>,

    /// Read one public IcyDB Error JSON object from PATH, or - for stdin (64 KiB maximum).
    #[arg(long, value_name = "PATH", conflicts_with_all = ["code", "facts"])]
    error_json: Option<PathBuf>,

    /// Add one numeric diagnostic fact as TAG=VALUE or LABEL=VALUE.
    #[arg(long = "fact", value_name = "TAG=VALUE")]
    facts: Vec<String>,

    /// Resolve accepted schema identities from a bounded exported artifact.
    #[arg(long, value_name = "PATH")]
    artifact: Option<PathBuf>,

    /// Resolve accepted schema identities from exact fingerprint-bound source metadata.
    #[arg(long, value_name = "PATH")]
    source_metadata: Option<PathBuf>,

    /// Resolve accepted schema identities from this canister's explicit schema endpoint.
    #[arg(long, value_name = "CANISTER")]
    canister: Option<String>,

    /// Target icp-cli environment for live schema resolution.
    #[arg(short, long, env = ICP_ENVIRONMENT_ENV, value_name = "ENV")]
    environment: Option<String>,
}

impl DiagnosticArgs {
    pub(crate) fn code(&self) -> Option<&str> {
        self.code.as_deref()
    }

    pub(crate) fn error_json(&self) -> Option<&Path> {
        self.error_json.as_deref()
    }

    pub(crate) const fn facts(&self) -> &[String] {
        self.facts.as_slice()
    }

    pub(crate) fn artifact(&self) -> Option<&Path> {
        self.artifact.as_deref()
    }

    pub(crate) fn source_metadata(&self) -> Option<&Path> {
        self.source_metadata.as_deref()
    }

    pub(crate) fn canister_name(&self) -> Option<&str> {
        self.canister.as_deref()
    }

    pub(crate) fn environment(&self) -> &str {
        self.environment.as_deref().unwrap_or(DEFAULT_ENVIRONMENT)
    }
}
