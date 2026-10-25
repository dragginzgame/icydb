//! Module: CLI canister lifecycle arguments.
//! Responsibility: define `icydb canister` clap surfaces and accessors.
//! Does not own: canister lifecycle execution or ICP command construction.
//! Boundary: exposes parsed lifecycle command values to the dispatcher.

use std::path::{Path, PathBuf};

use clap::{Args, Subcommand, ValueHint};

use crate::cli::{CanisterTarget, EnvironmentTarget};

///
/// CanisterCommand
///
/// CanisterCommand owns local canister lifecycle operations that were formerly
/// exposed as SQL shell flags. The subcommands mirror icp-cli operations closely
/// so lifecycle effects stay explicit.
///

#[derive(Debug, Subcommand)]
pub(crate) enum CanisterCommand {
    /// List known local IcyDB canisters and whether icp-cli has an id for them.
    List(EnvironmentTarget),
    /// Deploy the canister, preserving stable memory on existing installs.
    Deploy(CanisterTarget),
    /// Erase stable memory by reinstalling the selected canister, then reload fixtures.
    Refresh {
        #[command(flatten)]
        target: CanisterTarget,
        /// Confirm erasing stable memory in the selected environment.
        #[arg(long, required = true)]
        yes: bool,
    },
    /// Build and upgrade the canister without resetting stable memory.
    Upgrade(UpgradeArgs),
    /// Show icp-cli status for the selected canister.
    Status(CanisterTarget),
}

///
/// UpgradeArgs
///
/// UpgradeArgs carries the local canister upgrade inputs. The optional wasm
/// override supports advanced flows; by default ICP selects the build output
/// for the selected project and environment.
///

#[derive(Args, Debug)]
pub(crate) struct UpgradeArgs {
    #[command(flatten)]
    target: CanisterTarget,

    /// Wasm path to install after build; defaults to ICP's build output.
    #[arg(long, value_name = "PATH", value_hint = ValueHint::FilePath)]
    wasm: Option<PathBuf>,
}

impl UpgradeArgs {
    pub(crate) const fn target(&self) -> &CanisterTarget {
        &self.target
    }

    pub(crate) fn wasm(&self) -> Option<&Path> {
        self.wasm.as_deref()
    }
}
