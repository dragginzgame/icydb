//! Module: ICP canister command workflows.
//! Responsibility: run high-level canister lifecycle commands through icp-cli.
//! Does not own: generic canister calls, process execution semantics, or project discovery.
//! Boundary: exposes CLI command handlers and test-covered fixture call construction through icp.

use std::{path::Path, process::Command};

mod fixtures;

use crate::{
    icp::{
        process::{canister_id, canister_is_installed, run_external_command},
        project::known_canisters,
    },
    table::{ColumnAlign, append_indented_table},
};
type CanisterListRow = [String; 3];

const CANISTER_LIST_HEADERS: [&str; 3] = ["canister", "created", "principal"];
const CANISTER_LIST_ALIGNMENTS: [ColumnAlign; 3] =
    [ColumnAlign::Left, ColumnAlign::Left, ColumnAlign::Left];

/// Print canisters known to the selected local ICP environment and their local id status.
pub(super) fn list_canisters(environment: &str) -> Result<(), String> {
    let canisters = known_canisters(environment)?;
    if canisters.is_empty() {
        println!("No canisters were found in icp.yaml for environment '{environment}'.");

        return Ok(());
    }

    let rows = canisters
        .into_iter()
        .map(|canister| canister_list_row(environment, canister))
        .collect::<Result<Vec<_>, _>>()?;
    print_canister_table(environment, rows.as_slice());

    Ok(())
}

// Convert one ICP canister name into the row shape printed by `canister list`.
fn canister_list_row(environment: &str, canister: String) -> Result<CanisterListRow, String> {
    Ok(match canister_id(environment, canister.as_str())? {
        Some(id) => [canister, "created".to_string(), id],
        None => [canister, "not created".to_string(), "-".to_string()],
    })
}

// Print the local canister inventory with principal as the final column.
fn print_canister_table(environment: &str, rows: &[CanisterListRow]) {
    let mut output = format!("Known IcyDB canisters in environment '{environment}':\n");
    append_indented_table(
        &mut output,
        "  ",
        &CANISTER_LIST_HEADERS,
        rows,
        &CANISTER_LIST_ALIGNMENTS,
    );
    print!("{output}");
}

/// Deploy a local ICP canister without forcing reinstall mode.
pub(super) fn deploy_canister(environment: &str, canister: &str) -> Result<(), String> {
    eprintln!("[icydb] deploying canister '{canister}' in environment '{environment}'");

    run_external_command(deploy_command(environment, canister), "icp deploy")
}

/// Deploy a canister and request reinstall mode only when refresh targets an existing install.
fn reinstall_for_refresh(environment: &str, canister: &str) -> Result<(), String> {
    eprintln!(
        "[icydb] reinstalling canister '{canister}' when already installed in environment '{environment}'"
    );
    let mut command = deploy_command(environment, canister);
    // A failed probe cannot authorize deployment under the uninstalled path.
    if canister_is_installed(environment, canister)? {
        command.arg("--mode").arg("reinstall").arg("--yes");
    }

    run_external_command(command, "icp deploy reinstall")
}

/// Deploy or reinstall the canister, then load fixtures or report a failed fixture call.
pub(super) fn refresh_canister(environment: &str, canister: &str) -> Result<(), String> {
    reinstall_for_refresh(environment, canister)?;
    fixtures::load_after_refresh(environment, canister)
}

/// Build and upgrade a local canister without clearing stable memory.
pub(super) fn upgrade_canister(
    environment: &str,
    canister: &str,
    wasm: Option<&Path>,
) -> Result<(), String> {
    eprintln!(
        "[icydb] building canister '{canister}' for stable-memory-preserving upgrade in environment '{environment}'"
    );
    run_external_command(build_command(environment, canister), "icp build")?;

    if let Some(wasm_path) = wasm
        && !wasm_path.is_file()
    {
        return Err(format!(
            "expected wasm not found after build: {}",
            wasm_path.display()
        ));
    }

    eprintln!("[icydb] upgrading canister '{canister}' without demo data reset");
    run_external_command(
        install_upgrade_command(environment, canister, wasm),
        "icp canister install --mode upgrade",
    )
}

pub(super) fn install_upgrade_command(
    environment: &str,
    canister: &str,
    wasm: Option<&Path>,
) -> Command {
    let mut command = Command::new("icp");
    command
        .arg("canister")
        .arg("install")
        .arg(canister)
        .arg("--mode")
        .arg("upgrade");
    // ICP owns project/environment discovery and selects its own build output.
    // Only an explicit caller override supplies an artifact path.
    if let Some(wasm) = wasm {
        command.arg("--wasm").arg(wasm);
    }
    append_environment_args(&mut command, environment);

    command
}

/// Show icp-cli status for one local canister without changing lifecycle state.
pub(super) fn status_canister(environment: &str, canister: &str) -> Result<(), String> {
    eprintln!("[icydb] reading canister status for '{canister}' in environment '{environment}'");

    run_external_command(status_command(environment, canister), "icp canister status")
}

pub(super) fn deploy_command(environment: &str, canister: &str) -> Command {
    let mut command = Command::new("icp");
    command.arg("deploy").arg(canister);
    append_environment_args(&mut command, environment);

    command
}

pub(super) fn build_command(environment: &str, canister: &str) -> Command {
    let mut command = Command::new("icp");
    command.arg("build").arg(canister);
    append_environment_args(&mut command, environment);

    command
}

pub(super) fn status_command(environment: &str, canister: &str) -> Command {
    let mut command = Command::new("icp");
    command.arg("canister").arg("status").arg(canister);
    append_environment_args(&mut command, environment);

    command
}

#[cfg(test)]
pub(super) fn fixtures_load_command(
    environment: &str,
    canister: &str,
    argument_file: &std::path::Path,
) -> Command {
    fixtures::fixtures_load_command(environment, canister, argument_file)
}

#[cfg(test)]
pub(super) fn top_up_command(environment: &str, canister: &str, amount: &str) -> Command {
    fixtures::top_up_command(environment, canister, amount)
}

#[cfg(test)]
pub(super) fn parse_canister_cycles(status: &str) -> Option<u128> {
    fixtures::parse_canister_cycles(status)
}

fn append_environment_args(command: &mut Command, environment: &str) {
    command.arg("--environment").arg(environment);
}
