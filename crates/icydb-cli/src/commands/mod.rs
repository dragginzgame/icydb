use crate::{
    cli::{CanisterCommand, CliArgs, CliCommand},
    icp::{deploy_canister, list_canisters, reinstall_canister, status_canister, upgrade_canister},
    shell::run_sql_command,
};

/// Dispatch a parsed CLI invocation to the owning command family.
pub(crate) fn run_cli(args: CliArgs) -> Result<(), String> {
    match args.command {
        CliCommand::Sql(args) => run_sql_command(args),
        CliCommand::Canister(args) => run_canister_command(args),
    }
}

fn run_canister_command(command: CanisterCommand) -> Result<(), String> {
    match command {
        CanisterCommand::List(args) => list_canisters(args.environment()),
        CanisterCommand::Deploy(target) => {
            deploy_canister(target.environment(), target.canister_name())
        }
        CanisterCommand::Reinstall(target) | CanisterCommand::Refresh(target) => {
            reinstall_canister(target.environment(), target.canister_name())
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
