use std::path::{Path, PathBuf};

use clap::{Args, Parser, Subcommand};

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

    /// Inspect and validate IcyDB TOML config.
    #[command(subcommand)]
    Config(ConfigCommand),

    /// Manage a local ICP canister.
    #[command(subcommand)]
    Canister(CanisterCommand),
}

///
/// SqlArgs
///
/// SqlArgs owns the SQL shell command surface. It preserves the interactive
/// shell, explicit `--sql`, ICP environment defaults, and trailing SQL
/// convenience form while keeping SQL-specific flags under the `sql` keyword.
///

#[derive(Args, Debug)]
#[command(trailing_var_arg = true)]
pub(crate) struct SqlArgs {
    /// Target ICP canister name.
    #[arg(short, long, required = true)]
    pub(crate) canister: String,

    /// Target icp-cli environment.
    #[arg(short, long, env = "ICP_ENVIRONMENT", default_value = DEFAULT_ENVIRONMENT)]
    pub(crate) environment: String,

    /// Interactive shell history file.
    #[arg(long, default_value = ".cache/sql_history")]
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
    #[arg(short, long, required = true)]
    pub(crate) canister: String,

    /// Target icp-cli environment.
    #[arg(short, long, env = "ICP_ENVIRONMENT", default_value = DEFAULT_ENVIRONMENT)]
    pub(crate) environment: String,
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
    #[arg(short, long, env = "ICP_ENVIRONMENT", default_value = DEFAULT_ENVIRONMENT)]
    pub(crate) environment: String,
}

impl EnvironmentTarget {
    pub(crate) const fn environment(&self) -> &str {
        self.environment.as_str()
    }
}

///
/// ConfigCommand
///
/// ConfigCommand owns read-only inspection of `icydb.toml`. It can optionally
/// compare configured canister SQL surfaces against an explicit ICP environment.
///

#[derive(Debug, Subcommand)]
pub(crate) enum ConfigCommand {
    /// Show resolved IcyDB config and compare it with the selected ICP environment.
    Show(ConfigArgs),
    /// Validate resolved IcyDB config against the selected ICP environment.
    Check(ConfigArgs),
}

///
/// ConfigArgs
///
/// ConfigArgs carries the read-only config resolver inputs. `start_dir`
/// defaults to the current working directory; pass a canister crate directory
/// to inspect the same nearest-ancestor config that build scripts consume.
///

#[derive(Args, Clone, Debug)]
pub(crate) struct ConfigArgs {
    /// Directory to start nearest `icydb.toml` discovery from.
    #[arg(long)]
    pub(crate) start_dir: Option<PathBuf>,

    /// Optional icp-cli environment used for sync checks.
    #[arg(short, long, env = "ICP_ENVIRONMENT")]
    pub(crate) environment: Option<String>,
}

impl ConfigArgs {
    pub(crate) fn environment(&self) -> Option<&str> {
        self.environment.as_deref()
    }

    pub(crate) fn start_dir(&self) -> Option<&Path> {
        self.start_dir.as_deref()
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
    /// Refresh the selected ICP canister, clearing its stable memory.
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
