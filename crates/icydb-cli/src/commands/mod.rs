use crate::{
    cli::{CanisterCommand, CliArgs, CliCommand, ConfigCommand},
    config::{check_config, init_config, show_config},
    icp::{deploy_canister, list_canisters, refresh_canister, status_canister, upgrade_canister},
    observability::{run_metrics_command, run_snapshot_command},
    shell::run_sql_command,
};

/// Dispatch a parsed CLI invocation to the owning command family.
pub(crate) fn run_cli(args: CliArgs) -> Result<(), String> {
    match args.command {
        CliCommand::Sql(args) => run_sql_command(args),
        CliCommand::Snapshot(target) => run_snapshot_command(target),
        CliCommand::Metrics(args) => run_metrics_command(args),
        CliCommand::Config(args) => run_config_command(args),
        CliCommand::Canister(args) => run_canister_command(args),
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
        CanisterCommand::Deploy(target) => {
            deploy_canister(target.environment(), target.canister_name())
        }
        CanisterCommand::Refresh(target) => {
            refresh_canister(target.environment(), target.canister_name())
        }
        CanisterCommand::Upgrade(args) => upgrade_canister(
            args.target.environment(),
            args.target.canister_name(),
            args.wasm.as_ref(),
        ),
        CanisterCommand::Status(target) => {
            status_canister(target.environment(), target.canister_name())
        }
    }
}
