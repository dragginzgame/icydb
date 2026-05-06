use std::{path::PathBuf, process::Command};

use crate::dfx::{
    process::{
        call_unit_method, canister_id, canister_is_installed, run_external_command,
        unreachable_daemon_hint,
    },
    project::{known_canisters, require_created_canister},
};

use crate::cli::DEFAULT_CANISTER;

/// Print canisters known to the local dfx project and their local id status.
pub(crate) fn list_canisters() -> Result<(), String> {
    let canisters = known_canisters()?;
    if canisters.is_empty() {
        println!("No canisters were found in dfx.json.");

        return Ok(());
    }

    println!("Known IcyDB canisters:");
    for canister in canisters {
        let label = if canister == DEFAULT_CANISTER {
            format!("{canister} (default)")
        } else {
            canister.clone()
        };
        match canister_id(canister.as_str()) {
            Ok(Some(id)) => println!("  {label}: created as {id}"),
            Err(err) if unreachable_daemon_hint(err.as_str()).is_some() => {
                println!("  {label}: dfx local daemon is not reachable");
            }
            Ok(None) | Err(_) => println!("  {label}: not created locally"),
        }
    }

    Ok(())
}

/// Deploy a local dfx canister without forcing reinstall mode.
pub(crate) fn deploy_canister(canister: &str) -> Result<(), String> {
    eprintln!("[icydb] deploying canister '{canister}'");
    let mut command = Command::new("dfx");
    command.arg("deploy").arg(canister);

    run_external_command(command, "dfx deploy")
}

/// Deploy a canister and request reinstall mode only when an install exists.
pub(crate) fn reinstall_canister(canister: &str) -> Result<(), String> {
    eprintln!("[icydb] reinstalling canister '{canister}' when already installed");
    let mut command = Command::new("dfx");
    command.arg("deploy").arg(canister);
    if canister_is_installed(canister).unwrap_or(false) {
        command.arg("--mode").arg("reinstall").arg("--yes");
    }

    run_external_command(command, "dfx deploy reinstall")
}

/// Build and upgrade a local canister without clearing stable memory.
pub(crate) fn upgrade_canister(canister: &str, wasm: Option<&PathBuf>) -> Result<(), String> {
    let wasm_path = wasm
        .cloned()
        .unwrap_or_else(|| default_canister_wasm_path(canister));

    eprintln!("[icydb] building canister '{canister}' for stable-memory-preserving upgrade");
    let mut build = Command::new("dfx");
    build.arg("build").arg(canister);
    run_external_command(build, "dfx build")?;

    if !wasm_path.is_file() {
        return Err(format!(
            "expected wasm not found after build: {}",
            wasm_path.display()
        ));
    }

    eprintln!("[icydb] upgrading canister '{canister}' without demo data reset");
    let mut install = Command::new("dfx");
    install
        .arg("canister")
        .arg("install")
        .arg(canister)
        .arg("--mode")
        .arg("upgrade")
        .arg("--wasm")
        .arg(wasm_path);

    run_external_command(install, "dfx canister install --mode upgrade")
}

/// Show dfx status for one local canister without changing lifecycle state.
pub(crate) fn status_canister(canister: &str) -> Result<(), String> {
    eprintln!("[icydb] reading canister status for '{canister}'");
    let mut command = Command::new("dfx");
    command.arg("canister").arg("status").arg(canister);

    run_external_command(command, "dfx canister status")
}

/// Erase demo data on a local IcyDB canister.
pub(crate) fn reset_demo_data(canister: &str) -> Result<(), String> {
    eprintln!("[icydb] resetting demo data on '{canister}'");
    call_canister_unit_method(canister, "fixtures_reset")
}

/// Load the default demo data set on a local IcyDB canister.
pub(crate) fn seed_demo_data(canister: &str) -> Result<(), String> {
    eprintln!("[icydb] loading default demo data on '{canister}'");
    call_canister_unit_method(canister, "fixtures_load_default")
}

/// Erase and then reload default demo data on a local IcyDB canister.
pub(crate) fn reload_demo_data(canister: &str) -> Result<(), String> {
    reset_demo_data(canister)?;
    seed_demo_data(canister)
}

/// Reinstall the demo canister and reload the default demo data set.
pub(crate) fn fresh_demo(canister: &str) -> Result<(), String> {
    reinstall_canister(canister)?;
    reload_demo_data(canister)
}

fn call_canister_unit_method(canister: &str, method: &str) -> Result<(), String> {
    require_created_canister(canister)?;
    call_unit_method(canister, method)
}

fn default_canister_wasm_path(canister: &str) -> PathBuf {
    PathBuf::from(format!(".dfx/local/canisters/{canister}/{canister}.wasm"))
}
