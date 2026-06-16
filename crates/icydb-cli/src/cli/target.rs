//! Module: CLI shared target arguments.
//! Responsibility: define reusable canister/environment selectors.
//! Does not own: top-level command dispatch or command execution.
//! Boundary: exposes parsed target values through stable accessors.

use clap::Args;

use crate::cli::{DEFAULT_ENVIRONMENT, ICP_ENVIRONMENT_ENV};

///
/// CanisterTarget
///
/// CanisterTarget is the shared target selector for icp-cli-backed commands. It
/// keeps explicit canister selection and the environment override consistent
/// across lifecycle commands.
///

#[derive(Args, Clone, Debug)]
pub(crate) struct CanisterTarget {
    /// Target ICP canister name.
    #[arg(value_name = "CANISTER")]
    canister: String,

    /// Target icp-cli environment.
    #[arg(
        short,
        long,
        env = ICP_ENVIRONMENT_ENV,
        default_value = DEFAULT_ENVIRONMENT,
        value_name = "ENV"
    )]
    environment: String,
}

impl CanisterTarget {
    pub(crate) const fn canister_name(&self) -> &str {
        self.canister.as_str()
    }

    pub(crate) const fn environment(&self) -> &str {
        self.environment.as_str()
    }
}

#[derive(Args, Clone, Debug)]
pub(crate) struct EnvironmentTarget {
    /// Target icp-cli environment.
    #[arg(
        short,
        long,
        env = ICP_ENVIRONMENT_ENV,
        default_value = DEFAULT_ENVIRONMENT,
        value_name = "ENV"
    )]
    environment: String,
}

impl EnvironmentTarget {
    pub(crate) const fn environment(&self) -> &str {
        self.environment.as_str()
    }
}
