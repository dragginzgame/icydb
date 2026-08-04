//! Module: CLI diagnostic command arguments.
//! Responsibility: define compact diagnostic-code lookup clap surfaces.
//! Does not own: diagnostic registry rendering or canister error transport.
//! Boundary: exposes parsed diagnostic-code input to the command dispatcher.

use std::path::{Path, PathBuf};

use clap::Args;

use crate::cli::{DEFAULT_ENVIRONMENT, ICP_ENVIRONMENT_ENV};

///
/// DiagnosticArgs
///
/// DiagnosticArgs owns host-side lookup of compact IcyDB error codes. The code
/// is intentionally a string so users can paste either `E7` or `7`.
///

#[derive(Args, Debug)]
pub(crate) struct DiagnosticArgs {
    /// Compact IcyDB error code, for example E7, 7, E190, or 190.
    #[arg(value_name = "CODE")]
    code: String,

    /// Add one numeric diagnostic fact as TAG=VALUE or LABEL=VALUE.
    #[arg(long = "fact", value_name = "TAG=VALUE")]
    facts: Vec<String>,

    /// Resolve accepted schema identities from a bounded exported artifact.
    #[arg(long, value_name = "PATH")]
    artifact: Option<PathBuf>,

    /// Resolve accepted schema identities from this canister's explicit schema endpoint.
    #[arg(long, value_name = "CANISTER")]
    canister: Option<String>,

    /// Target icp-cli environment for live schema resolution.
    #[arg(short, long, env = ICP_ENVIRONMENT_ENV, value_name = "ENV")]
    environment: Option<String>,
}

impl DiagnosticArgs {
    pub(crate) fn code(&self) -> &str {
        &self.code
    }

    pub(crate) const fn facts(&self) -> &[String] {
        self.facts.as_slice()
    }

    pub(crate) fn artifact(&self) -> Option<&Path> {
        self.artifact.as_deref()
    }

    pub(crate) fn canister_name(&self) -> Option<&str> {
        self.canister.as_deref()
    }

    pub(crate) fn environment(&self) -> &str {
        self.environment.as_deref().unwrap_or(DEFAULT_ENVIRONMENT)
    }
}
