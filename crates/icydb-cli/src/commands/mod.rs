//! Module: CLI command dispatch.
//! Responsibility: route parsed CLI commands to their owning command family.
//! Does not own: command implementation, argument parsing, or output rendering.
//! Boundary: exposes the top-level parsed-command runner used by `main`.

use crate::{
    cli::{CanisterCommand, CanisterTarget, CliArgs, CliCommand, ConfigCommand, SchemaCommand},
    config::{check_config, init_config, show_config},
    icp::{deploy_canister, list_canisters, refresh_canister, status_canister, upgrade_canister},
    observability::{
        run_metrics_command, run_schema_check_command, run_schema_show_command,
        run_snapshot_command,
    },
    shell::run_sql_command,
};

type TargetCommand = fn(&str, &str) -> Result<(), String>;

/// Dispatch a parsed CLI invocation to the owning command family.
pub(crate) fn run_cli(args: CliArgs) -> Result<(), String> {
    match args.into_command() {
        CliCommand::Sql(args) => run_sql_command(args),
        CliCommand::Snapshot(target) => run_snapshot_command(target),
        CliCommand::Metrics(args) => run_metrics_command(args),
        CliCommand::Schema(args) => run_schema_command(args),
        CliCommand::Config(args) => run_config_command(args),
        CliCommand::Canister(args) => run_canister_command(args),
    }
}

fn run_schema_command(command: SchemaCommand) -> Result<(), String> {
    match command {
        SchemaCommand::Show(target) => run_schema_show_command(target),
        SchemaCommand::Check(target) => run_schema_check_command(target),
    }
}

fn run_config_command(command: ConfigCommand) -> Result<(), String> {
    match command {
        ConfigCommand::Init(args) => init_config(args),
        ConfigCommand::Show(args) => show_config(args),
        ConfigCommand::Check(args) => check_config(args),
    }
}

fn run_canister_command(command: CanisterCommand) -> Result<(), String> {
    match command {
        CanisterCommand::List(args) => list_canisters(args.environment()),
        CanisterCommand::Deploy(target) => run_target_command(&target, deploy_canister),
        CanisterCommand::Refresh(target) => run_target_command(&target, refresh_canister),
        CanisterCommand::Upgrade(args) => upgrade_canister(
            args.target().environment(),
            args.target().canister_name(),
            args.wasm(),
        ),
        CanisterCommand::Status(target) => run_target_command(&target, status_canister),
    }
}

fn run_target_command(target: &CanisterTarget, command: TargetCommand) -> Result<(), String> {
    command(target.environment(), target.canister_name())
}
