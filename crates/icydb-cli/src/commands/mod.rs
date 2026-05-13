use crate::{
    cli::{CanisterCommand, CliArgs, CliCommand, DemoCommand},
    icp::{
        deploy_canister, fresh_demo, list_canisters, reinstall_canister, reload_demo_data,
        reset_demo_data, seed_demo_data, status_canister, upgrade_canister,
    },
    shell::run_sql_command,
};

/// Dispatch a parsed CLI invocation to the owning command family.
pub(crate) fn run_cli(args: CliArgs) -> Result<(), String> {
    match args.command {
        CliCommand::Sql(args) => run_sql_command(args),
        CliCommand::Canister(args) => run_canister_command(args),
        CliCommand::Demo(args) => run_demo_command(args),
    }
}

fn run_canister_command(command: CanisterCommand) -> Result<(), String> {
    match command {
        CanisterCommand::List(args) => list_canisters(args.environment()),
        CanisterCommand::Deploy(target) => {
            deploy_canister(target.environment(), target.canister_name())
        }
        CanisterCommand::Reinstall(target) => {
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

fn run_demo_command(command: DemoCommand) -> Result<(), String> {
    match command {
        DemoCommand::Reset(target) => reset_demo_data(target.environment(), target.canister_name()),
        DemoCommand::Seed(target) => seed_demo_data(target.environment(), target.canister_name()),
        DemoCommand::Reload(target) => {
            reload_demo_data(target.environment(), target.canister_name())
        }
        DemoCommand::Fresh(target) => fresh_demo(target.environment(), target.canister_name()),
    }
}
