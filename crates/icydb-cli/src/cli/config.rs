//! Module: CLI config command arguments.
//! Responsibility: define `icydb config` clap surfaces and stable accessors.
//! Does not own: config file resolution, rendering, or validation.
//! Boundary: exposes parsed config command values to the config owner.

use std::path::{Path, PathBuf};

use clap::{ArgAction, Args, Subcommand, ValueEnum, ValueHint};

use crate::cli::ICP_ENVIRONMENT_ENV;

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
    #[arg(long, value_name = "DIR", value_hint = ValueHint::DirPath)]
    start_dir: Option<PathBuf>,

    /// Optional icp-cli environment used for sync checks.
    #[arg(short, long, env = ICP_ENVIRONMENT_ENV, value_name = "ENV")]
    environment: Option<String>,
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
pub(crate) struct ConfigInitArgs {
    /// Directory used to choose where `icydb.toml` should be written.
    #[arg(long, value_name = "DIR", value_hint = ValueHint::DirPath)]
    start_dir: Option<PathBuf>,

    /// Canister whose generated DB endpoint surfaces should be configured.
    #[arg(short, long, value_name = "CANISTER")]
    canister: String,

    #[command(flatten)]
    surfaces: ConfigInitSurfaceArgs,

    /// Replace an existing target config file.
    #[arg(long)]
    force: bool,
}

#[derive(Args, Clone, Debug)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "clap flag bags intentionally mirror independent command-line switches"
)]
struct ConfigInitSurfaceArgs {
    /// Also generate the DDL endpoint.
    #[arg(long)]
    ddl: bool,

    /// Also generate fixture lifecycle endpoints.
    #[arg(long)]
    fixtures: bool,

    /// Also generate the primary-key-only SQL update endpoint.
    #[arg(long)]
    update: bool,

    /// Generated SQL update endpoint policy to enable.
    #[arg(long = "update-policy", value_enum, value_name = "POLICY")]
    update_policy: Option<ConfigInitUpdatePolicy>,

    /// Generate metrics report endpoint. Enabled by default.
    #[arg(long)]
    metrics: bool,

    /// Disable the default metrics report endpoint.
    #[arg(long = "no-metrics")]
    no_metrics: bool,

    /// Also generate extended metrics report endpoint.
    #[arg(long = "metrics-extended")]
    metrics_extended: bool,

    /// Also generate extended metrics report endpoint for IC builds.
    #[arg(long = "metrics-extended-ic")]
    metrics_extended_ic: bool,

    /// Also generate storage snapshot endpoint.
    #[arg(long)]
    snapshot: bool,

    /// Also generate accepted schema report endpoint.
    #[arg(long)]
    schema: bool,

    /// Generate all currently supported DB endpoint families.
    #[arg(long)]
    all: bool,

    /// Disable the default readonly SQL endpoint.
    #[arg(long = "no-readonly", action = ArgAction::SetFalse, default_value_t = true)]
    readonly: bool,
}

impl ConfigInitArgs {
    pub(crate) fn start_dir(&self) -> Option<&Path> {
        self.start_dir.as_deref()
    }

    pub(crate) const fn canister_name(&self) -> &str {
        self.canister.as_str()
    }

    pub(crate) const fn readonly(&self) -> bool {
        self.surfaces.readonly
    }

    pub(crate) const fn ddl(&self) -> bool {
        self.surfaces.ddl()
    }

    pub(crate) const fn fixtures(&self) -> bool {
        self.surfaces.fixtures()
    }

    pub(crate) fn update_config_value(&self) -> String {
        self.surfaces.update_config_value()
    }

    #[cfg(test)]
    pub(crate) const fn metrics(&self) -> bool {
        self.surfaces.metrics()
    }

    pub(crate) const fn metrics_local_config_value(&self) -> &'static str {
        self.surfaces.metrics_local_config_value()
    }

    pub(crate) const fn metrics_ic_config_value(&self) -> &'static str {
        self.surfaces.metrics_ic_config_value()
    }

    #[cfg(test)]
    pub(crate) const fn metrics_extended_local(&self) -> bool {
        self.surfaces.metrics_extended_local()
    }

    #[cfg(test)]
    pub(crate) const fn metrics_extended_ic(&self) -> bool {
        self.surfaces.metrics_extended_ic()
    }

    pub(crate) const fn snapshot(&self) -> bool {
        self.surfaces.snapshot()
    }

    pub(crate) const fn schema(&self) -> bool {
        self.surfaces.schema()
    }

    pub(crate) const fn force(&self) -> bool {
        self.force
    }
}

impl ConfigInitSurfaceArgs {
    const fn ddl(&self) -> bool {
        self.surface_enabled(self.ddl)
    }

    const fn fixtures(&self) -> bool {
        self.surface_enabled(self.fixtures)
    }

    fn update_config_value(&self) -> String {
        match self.update_policy {
            Some(policy) => format!("\"{}\"", policy.config_value()),
            None if self.surface_enabled(self.update) => String::from("true"),
            None => String::from("false"),
        }
    }

    const fn metrics(&self) -> bool {
        let _ = self.metrics;

        !self.no_metrics
    }

    const fn metrics_local_config_value(&self) -> &'static str {
        self.metrics_mode_config_value(self.metrics_extended_local())
    }

    const fn metrics_ic_config_value(&self) -> &'static str {
        self.metrics_mode_config_value(self.metrics_extended_ic())
    }

    const fn metrics_mode_config_value(&self, extended: bool) -> &'static str {
        if !self.metrics() {
            "off"
        } else if extended {
            "extended"
        } else {
            "simple"
        }
    }

    const fn metrics_extended_local(&self) -> bool {
        !self.no_metrics && self.surface_enabled(self.metrics_extended)
    }

    const fn metrics_extended_ic(&self) -> bool {
        !self.no_metrics && self.metrics_extended_ic
    }

    const fn snapshot(&self) -> bool {
        self.surface_enabled(self.snapshot)
    }

    const fn schema(&self) -> bool {
        self.surface_enabled(self.schema)
    }

    const fn surface_enabled(&self, enabled: bool) -> bool {
        enabled || self.all
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub(crate) enum ConfigInitUpdatePolicy {
    /// Public-safe at-most-one-row primary-key update policy.
    #[value(name = "primary-key")]
    PrimaryKey,
    /// Public-safe explicit primary-key-ordered bounded update policy.
    Bounded,
}

impl ConfigInitUpdatePolicy {
    pub(crate) const fn config_value(self) -> &'static str {
        match self {
            Self::PrimaryKey => "primary_key",
            Self::Bounded => "bounded",
        }
    }
}
