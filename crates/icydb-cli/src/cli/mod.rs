use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};

pub(crate) const DEFAULT_CANISTER: &str = "demo_rpg";
pub(crate) const DEFAULT_ENVIRONMENT: &str = "demo";

///
/// CliArgs
///
/// CliArgs owns the top-level process argument surface for the developer CLI.
/// The initial keyword selects a functional family so SQL execution and
/// canister lifecycle operations do not share one flag namespace.
///

#[derive(Debug, Parser)]
#[command(
    name = "icydb",
    about = "Developer CLI tools for IcyDB",
    long_about = None
)]
pub(crate) struct CliArgs {
    #[command(subcommand)]
    pub(crate) command: CliCommand,
}

///
/// CliCommand
///
/// CliCommand is the top-level functional-family dispatch for the developer
/// CLI. Each variant owns one user-facing keyword so future command growth can
/// stay grouped by intent instead of growing one shared flag bag.
///

#[derive(Debug, Subcommand)]
pub(crate) enum CliCommand {
    /// Run SQL against an IcyDB canister.
    Sql(SqlArgs),

    /// Manage a local ICP canister.
    #[command(subcommand)]
    Canister(CanisterCommand),
}

///
/// SqlArgs
///
/// SqlArgs owns the SQL shell command surface. It preserves the interactive
/// shell, explicit `--sql`, environment defaults, and trailing SQL convenience
/// form while keeping SQL-specific flags under the `sql` keyword.
///

#[derive(Args, Debug)]
#[command(trailing_var_arg = true)]
pub(crate) struct SqlArgs {
    /// Target ICP canister name.
    #[arg(short, long, env = "SQLQ_CANISTER")]
    pub(crate) canister: Option<String>,

    /// Target icp-cli environment.
    #[arg(short, long, env = "ICP_ENVIRONMENT", default_value = DEFAULT_ENVIRONMENT)]
    pub(crate) environment: String,

    /// Interactive shell history file.
    #[arg(long, env = "SQLQ_HISTORY_FILE", default_value = ".cache/sql_history")]
    pub(crate) history_file: PathBuf,

    /// Execute one SQL statement and exit.
    #[arg(long, conflicts_with = "trailing_sql")]
    pub(crate) sql: Option<String>,

    /// SQL statement passed without --sql.
    #[arg(value_name = "SQL", allow_hyphen_values = true)]
    pub(crate) trailing_sql: Vec<String>,
}

///
/// CanisterTarget
///
/// CanisterTarget is the shared target selector for icp-cli-backed commands. It
/// keeps the canister default and environment override consistent across SQL
/// and lifecycle commands.
///

#[derive(Args, Clone, Debug)]
pub(crate) struct CanisterTarget {
    /// Target ICP canister name.
    #[arg(short, long, env = "SQLQ_CANISTER")]
    pub(crate) canister: Option<String>,

    /// Target icp-cli environment.
    #[arg(short, long, env = "ICP_ENVIRONMENT", default_value = DEFAULT_ENVIRONMENT)]
    pub(crate) environment: String,
}

impl CanisterTarget {
    pub(crate) fn canister_name(&self) -> &str {
        self.canister.as_deref().unwrap_or(DEFAULT_CANISTER)
    }

    pub(crate) const fn environment(&self) -> &str {
        self.environment.as_str()
    }
}

#[derive(Args, Clone, Debug)]
pub(crate) struct EnvironmentTarget {
    /// Target icp-cli environment.
    #[arg(short, long, env = "ICP_ENVIRONMENT", default_value = DEFAULT_ENVIRONMENT)]
    pub(crate) environment: String,
}

impl EnvironmentTarget {
    pub(crate) const fn environment(&self) -> &str {
        self.environment.as_str()
    }
}

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
    /// Reinstall the canister when it already exists.
    Reinstall(CanisterTarget),
    /// Refresh the canister by rebuilding and reinstalling it, clearing stable memory.
    Refresh(CanisterTarget),
    /// Build and upgrade the canister without resetting stable memory.
    Upgrade(UpgradeArgs),
    /// Show icp-cli status for the selected canister.
    Status(CanisterTarget),
}

///
/// UpgradeArgs
///
/// UpgradeArgs carries the local canister upgrade inputs. The optional wasm
/// override supports advanced flows while the default path preserves the
/// previous local SQL helper upgrade behavior.
///

#[derive(Args, Debug)]
pub(crate) struct UpgradeArgs {
    #[command(flatten)]
    pub(crate) target: CanisterTarget,

    /// Wasm path to install after build.
    #[arg(long)]
    pub(crate) wasm: Option<PathBuf>,
}
