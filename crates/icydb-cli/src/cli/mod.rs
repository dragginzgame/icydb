//! Module: CLI argument surface.
//! Responsibility: define clap parsing structs and stable accessors for command inputs.
//! Does not own: command execution, config resolution, or rendered output.
//! Boundary: exposes parsed command values to the command dispatcher and owner modules.

mod canister;
mod config;
mod diagnostic;
mod metrics;
mod schema;
mod sql;
mod target;

pub(crate) use canister::CanisterCommand;
pub(crate) use config::{ConfigArgs, ConfigCommand, ConfigInitArgs};
pub(crate) use diagnostic::DiagnosticArgs;
pub(crate) use metrics::MetricsArgs;
pub(crate) use schema::SchemaCommand;
pub(crate) use sql::{SqlArgs, SqlShellFields};
pub(crate) use target::{CanisterTarget, EnvironmentTarget};

use clap::{Parser, Subcommand};

pub(crate) const DEFAULT_ENVIRONMENT: &str = "demo";
pub(super) const ICP_ENVIRONMENT_ENV: &str = "ICP_ENVIRONMENT";

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
    long_about = None,
    version
)]
pub(crate) struct CliArgs {
    #[command(subcommand)]
    command: CliCommand,
}

impl CliArgs {
    pub(crate) fn into_command(self) -> CliCommand {
        self.command
    }
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

    /// Explain a compact IcyDB error code such as E7 or E190.
    Diagnostic(DiagnosticArgs),

    /// Inspect accepted and generated schema metadata from an IcyDB canister.
    #[command(subcommand)]
    Schema(SchemaCommand),

    /// Inspect and validate IcyDB TOML config.
    #[command(subcommand)]
    Config(ConfigCommand),

    /// Manage a local ICP canister.
    #[command(subcommand)]
    Canister(CanisterCommand),
}
