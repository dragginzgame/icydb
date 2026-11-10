//! Module: build_fixture_canister
//! Responsibility: stage one parsed fixture build.
//! Does not own: policy or the ICP network.
//! Boundary: turns CLI input into typed build options.

use icydb_testing_build::{
    CanisterBuildOptions, CanisterBuildProfile, CanisterSqlMode, CanisterWasmProfile,
    stage_canister_for_icp_with_options,
};

use clap::Parser;

#[derive(Parser)]
#[command(about = "Stage one supported fixture canister")]
struct Args {
    #[arg(default_value = "demo_rpg")]
    canister: String,
    #[arg(long, default_value = "local", value_parser = CanisterBuildProfile::parse)]
    build_profile: CanisterBuildProfile,
    #[arg(long, default_value = "debug", value_parser = CanisterWasmProfile::parse)]
    profile: CanisterWasmProfile,
    #[arg(long, default_value = "on", value_parser = CanisterSqlMode::parse)]
    sql_mode: CanisterSqlMode,
}

fn main() {
    let args = Args::parse();
    let options = CanisterBuildOptions {
        build_profile: args.build_profile,
        profile: args.profile,
        sql_mode: args.sql_mode,
    };
    match stage_canister_for_icp_with_options(&args.canister, options) {
        Ok(wasm_path) => println!("staged wasm: {}", wasm_path.display()),
        Err(err) => {
            eprintln!("{err}");
            std::process::exit(1);
        }
    }
}

///
/// TESTS
///
#[cfg(test)]
mod tests;
