use crate::{
    cli::{CanisterCommand, CliArgs, CliCommand, DevCommand, FixturesCommand},
    dfx::{
        deploy_canister, list_canisters, load_default_fixtures, reinstall_canister,
        reload_fixtures, reset_fixtures, upgrade_canister,
    },
    shell::run_sql_command,
};

/// Dispatch a parsed CLI invocation to the owning command family.
pub(crate) fn run_cli(args: CliArgs) -> Result<(), String> {
    match args.command {
        CliCommand::Sql(args) => run_sql_command(args),
        CliCommand::Canister(args) => run_canister_command(args),
        CliCommand::Fixtures(args) => run_fixtures_command(args),
        CliCommand::Dev(args) => run_dev_command(args),
    }
}

fn run_canister_command(command: CanisterCommand) -> Result<(), String> {
    match command {
        CanisterCommand::List => list_canisters(),
        CanisterCommand::Deploy(target) => deploy_canister(target.canister_name()),
        CanisterCommand::Reinstall(target) => reinstall_canister(target.canister_name()),
        CanisterCommand::Upgrade(args) => {
            upgrade_canister(args.target.canister_name(), args.wasm.as_ref())
        }
    }
}

fn run_fixtures_command(command: FixturesCommand) -> Result<(), String> {
    match command {
        FixturesCommand::Reset(target) => reset_fixtures(target.canister_name()),
        FixturesCommand::LoadDefault(target) => load_default_fixtures(target.canister_name()),
        FixturesCommand::Reload(target) => reload_fixtures(target.canister_name()),
    }
}

fn run_dev_command(command: DevCommand) -> Result<(), String> {
    match command {
        DevCommand::Init(target) => {
            reinstall_canister(target.canister_name())?;
            reload_fixtures(target.canister_name())
        }
    }
}
