use std::{path::PathBuf, process::Command};

use crate::{
    cli::DEFAULT_CANISTER,
    icp::{
        process::{
            call_unit_method, canister_id, canister_is_installed, run_external_command,
            unreachable_network_hint,
        },
        project::{known_canisters, require_created_canister},
    },
};

type CanisterListRow = (String, &'static str, &'static str, String);

/// Print canisters known to the selected local ICP environment and their local id status.
pub(crate) fn list_canisters(environment: &str) -> Result<(), String> {
    let canisters = known_canisters(environment)?;
    if canisters.is_empty() {
        println!("No canisters were found in icp.yaml for environment '{environment}'.");

        return Ok(());
    }

    let rows = canisters
        .into_iter()
        .map(|canister| canister_list_row(environment, canister))
        .collect::<Vec<_>>();
    print_canister_table(environment, rows.as_slice());

    Ok(())
}

// Convert one ICP canister name into the row shape printed by `canister list`.
fn canister_list_row(environment: &str, canister: String) -> CanisterListRow {
    let default = if canister == DEFAULT_CANISTER {
        "yes"
    } else {
        "no"
    };

    match canister_id(environment, canister.as_str()) {
        Ok(Some(id)) => (canister, default, "created", id),
        Err(err) if unreachable_network_hint(err.as_str()).is_some() => (
            canister,
            default,
            "unknown",
            "local ICP network is not reachable".to_string(),
        ),
        Ok(None) | Err(_) => (canister, default, "not created", "-".to_string()),
    }
}

// Print the local canister inventory with principal as the final column.
fn print_canister_table(environment: &str, rows: &[CanisterListRow]) {
    let canister_width = table_width(
        "canister",
        rows.iter().map(|(canister, _, _, _)| canister.as_str()),
    );
    let default_width = table_width("default", rows.iter().map(|(_, default, _, _)| *default));
    let created_width = table_width("created", rows.iter().map(|(_, _, created, _)| *created));
    let canister_heading = "canister";
    let default_heading = "default";
    let created_heading = "created";
    let principal_heading = "principal";

    println!("Known IcyDB canisters in environment '{environment}':");
    println!(
        "  {canister_heading:<canister_width$}  {default_heading:<default_width$}  {created_heading:<created_width$}  {principal_heading}"
    );
    for (canister, default, created, principal) in rows {
        println!(
            "  {canister:<canister_width$}  {default:<default_width$}  {created:<created_width$}  {principal}"
        );
    }
}

// Keep simple text tables aligned without introducing a formatting dependency.
fn table_width<'a>(heading: &str, values: impl Iterator<Item = &'a str>) -> usize {
    values.map(str::len).max().unwrap_or(0).max(heading.len())
}

/// Deploy a local ICP canister without forcing reinstall mode.
pub(crate) fn deploy_canister(environment: &str, canister: &str) -> Result<(), String> {
    eprintln!("[icydb] deploying canister '{canister}' in environment '{environment}'");
    let mut command = Command::new("icp");
    command
        .arg("deploy")
        .arg(canister)
        .arg("--environment")
        .arg(environment);

    run_external_command(command, "icp deploy")
}

/// Deploy a canister and request reinstall mode only when an install exists.
pub(crate) fn reinstall_canister(environment: &str, canister: &str) -> Result<(), String> {
    eprintln!(
        "[icydb] reinstalling canister '{canister}' when already installed in environment '{environment}'"
    );
    let mut command = Command::new("icp");
    command
        .arg("deploy")
        .arg(canister)
        .arg("--environment")
        .arg(environment);
    if canister_is_installed(environment, canister).unwrap_or(false) {
        command.arg("--mode").arg("reinstall").arg("--yes");
    }

    run_external_command(command, "icp deploy reinstall")
}

/// Build and upgrade a local canister without clearing stable memory.
pub(crate) fn upgrade_canister(
    environment: &str,
    canister: &str,
    wasm: Option<&PathBuf>,
) -> Result<(), String> {
    let wasm_path = wasm
        .cloned()
        .unwrap_or_else(|| default_canister_wasm_path(canister));

    eprintln!(
        "[icydb] building canister '{canister}' for stable-memory-preserving upgrade in environment '{environment}'"
    );
    let mut build = Command::new("icp");
    build
        .arg("build")
        .arg(canister)
        .arg("--environment")
        .arg(environment);
    run_external_command(build, "icp build")?;

    if !wasm_path.is_file() {
        return Err(format!(
            "expected wasm not found after build: {}",
            wasm_path.display()
        ));
    }

    eprintln!("[icydb] upgrading canister '{canister}' without demo data reset");
    let mut install = Command::new("icp");
    install
        .arg("canister")
        .arg("install")
        .arg(canister)
        .arg("--mode")
        .arg("upgrade")
        .arg("--wasm")
        .arg(wasm_path)
        .arg("--environment")
        .arg(environment);

    run_external_command(install, "icp canister install --mode upgrade")
}

/// Show icp-cli status for one local canister without changing lifecycle state.
pub(crate) fn status_canister(environment: &str, canister: &str) -> Result<(), String> {
    eprintln!("[icydb] reading canister status for '{canister}' in environment '{environment}'");
    let mut command = Command::new("icp");
    command
        .arg("canister")
        .arg("status")
        .arg(canister)
        .arg("--environment")
        .arg(environment);

    run_external_command(command, "icp canister status")
}

/// Erase demo data on a local IcyDB canister.
pub(crate) fn reset_demo_data(environment: &str, canister: &str) -> Result<(), String> {
    eprintln!("[icydb] resetting demo data on '{canister}' in environment '{environment}'");
    call_canister_unit_method(environment, canister, "fixtures_reset")
}

/// Load the default demo data set on a local IcyDB canister.
pub(crate) fn seed_demo_data(environment: &str, canister: &str) -> Result<(), String> {
    eprintln!("[icydb] loading default demo data on '{canister}' in environment '{environment}'");
    call_canister_unit_method(environment, canister, "fixtures_load_default")
}

/// Erase and then reload default demo data on a local IcyDB canister.
pub(crate) fn reload_demo_data(environment: &str, canister: &str) -> Result<(), String> {
    reset_demo_data(environment, canister)?;
    seed_demo_data(environment, canister)
}

/// Reinstall the demo canister and reload the default demo data set.
pub(crate) fn fresh_demo(environment: &str, canister: &str) -> Result<(), String> {
    reinstall_canister(environment, canister)?;
    reload_demo_data(environment, canister)
}

fn call_canister_unit_method(
    environment: &str,
    canister: &str,
    method: &str,
) -> Result<(), String> {
    require_created_canister(environment, canister)?;
    call_unit_method(environment, canister, method)
}

fn default_canister_wasm_path(canister: &str) -> PathBuf {
    PathBuf::from(format!(".icp/local/canisters/{canister}/{canister}.wasm"))
}
