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

    /// Read storage inventory from an IcyDB canister.
    Snapshot(CanisterTarget),

    /// Read or reset metrics on an IcyDB canister.
    Metrics(MetricsArgs),

    /// Read accepted schema metadata from an IcyDB canister.
    Schema(CanisterTarget),

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
/// keeps explicit canister selection and the environment override consistent
/// across lifecycle commands.
///

#[derive(Args, Clone, Debug)]
pub(crate) struct CanisterTarget {
    /// Target ICP canister name.
    #[arg(value_name = "CANISTER")]
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
/// MetricsArgs
///
/// MetricsArgs owns the generated metrics endpoint command surface. The reset
/// switch keeps normal read usage short while still making the destructive
/// operation explicit.
///

#[derive(Args, Debug)]
pub(crate) struct MetricsArgs {
    #[command(flatten)]
    pub(crate) target: CanisterTarget,

    /// Only include metrics windows starting at this millisecond timestamp.
    #[arg(long, conflicts_with = "reset")]
    pub(crate) window_start_ms: Option<u64>,

    /// Reset in-memory metrics instead of reading the metrics report.
    #[arg(long)]
    pub(crate) reset: bool,
}

impl MetricsArgs {
    pub(crate) const fn target(&self) -> &CanisterTarget {
        &self.target
    }

    pub(crate) const fn window_start_ms(&self) -> Option<u64> {
        self.window_start_ms
    }

    pub(crate) const fn reset(&self) -> bool {
        self.reset
    }
}

///
/// ConfigCommand
///
/// ConfigCommand owns creation and inspection of `icydb.toml`. Inspection can
/// optionally compare configured canister surfaces against an explicit ICP
/// environment.
///

#[derive(Debug, Subcommand)]
pub(crate) enum ConfigCommand {
    /// Create a default IcyDB config file.
    Init(ConfigInitArgs),
    /// Show resolved IcyDB config, optionally comparing it with an ICP environment.
    Show(ConfigArgs),
    /// Validate resolved IcyDB config and optionally check an ICP environment.
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
/// ConfigInitArgs
///
/// ConfigInitArgs carries the inputs for creating a new DB-surface config.
/// It writes to the workspace root when one is visible from `start_dir`,
/// otherwise to `start_dir` itself.
///

#[derive(Args, Clone, Debug)]
#[allow(
    clippy::struct_excessive_bools,
    reason = "clap flag bags intentionally mirror independent command-line switches"
)]
pub(crate) struct ConfigInitArgs {
    /// Directory used to choose where `icydb.toml` should be written.
    #[arg(long)]
    pub(crate) start_dir: Option<PathBuf>,

    /// Canister whose generated DB endpoint surfaces should be configured.
    #[arg(short, long, required = true)]
    pub(crate) canister: String,

    /// Also generate the DDL endpoint.
    #[arg(long)]
    pub(crate) ddl: bool,

    /// Also generate fixture lifecycle endpoints.
    #[arg(long)]
    pub(crate) fixtures: bool,

    /// Also generate metrics report endpoint.
    #[arg(long)]
    pub(crate) metrics: bool,

    /// Also generate the metrics reset endpoint.
    #[arg(long = "metrics-reset")]
    pub(crate) metrics_reset: bool,

    /// Also generate storage snapshot endpoint.
    #[arg(long)]
    pub(crate) snapshot: bool,

    /// Also generate accepted schema report endpoint.
    #[arg(long)]
    pub(crate) schema: bool,

    /// Generate all currently supported DB endpoint families.
    #[arg(long)]
    pub(crate) all: bool,

    /// Disable the default readonly SQL endpoint.
    #[arg(long = "no-readonly")]
    pub(crate) no_readonly: bool,

    /// Replace an existing target config file.
    #[arg(long)]
    pub(crate) force: bool,
}

impl ConfigInitArgs {
    pub(crate) fn start_dir(&self) -> Option<&Path> {
        self.start_dir.as_deref()
    }

    pub(crate) const fn canister_name(&self) -> &str {
        self.canister.as_str()
    }

    pub(crate) const fn readonly(&self) -> bool {
        !self.no_readonly
    }

    pub(crate) const fn ddl(&self) -> bool {
        self.ddl || self.all
    }

    pub(crate) const fn fixtures(&self) -> bool {
        self.fixtures || self.all
    }

    pub(crate) const fn metrics(&self) -> bool {
        self.metrics || self.all
    }

    pub(crate) const fn metrics_reset(&self) -> bool {
        self.metrics_reset || self.all
    }

    pub(crate) const fn snapshot(&self) -> bool {
        self.snapshot || self.all
    }

    pub(crate) const fn schema(&self) -> bool {
        self.schema || self.all
    }

    pub(crate) const fn force(&self) -> bool {
        self.force
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
    /// Refresh the selected ICP canister and reload fixtures when available.
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
