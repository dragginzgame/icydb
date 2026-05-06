use crate::cli::DEFAULT_CANISTER;
use serde_json::Value;
use std::{
    path::PathBuf,
    process::{Command, Stdio},
};

const DFX_JSON_PATH: &str = "dfx.json";

/// Print canisters known to the local dfx project and their local id status.
pub(crate) fn list_canisters() -> Result<(), String> {
    let canisters = known_canisters()?;
    if canisters.is_empty() {
        println!("No canisters were found in {DFX_JSON_PATH}.");

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
            Some(id) => println!("  {label}: created as {id}"),
            None => println!("  {label}: not created locally"),
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
    if canister_is_installed(canister) {
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

    eprintln!("[icydb] upgrading canister '{canister}' without fixture reset");
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

/// Erase all fixtures on a local IcyDB canister.
pub(crate) fn reset_fixtures(canister: &str) -> Result<(), String> {
    eprintln!("[icydb] resetting fixtures on '{canister}'");
    call_canister_unit_method(canister, "fixtures_reset")
}

/// Load default fixtures on a local IcyDB canister.
pub(crate) fn load_default_fixtures(canister: &str) -> Result<(), String> {
    eprintln!("[icydb] loading default fixtures on '{canister}'");
    call_canister_unit_method(canister, "fixtures_load_default")
}

/// Erase and then reload default fixtures on a local IcyDB canister.
pub(crate) fn reload_fixtures(canister: &str) -> Result<(), String> {
    reset_fixtures(canister)?;
    load_default_fixtures(canister)
}

/// Fail with IcyDB-specific setup guidance when dfx has no local canister id.
pub(crate) fn require_created_canister(canister: &str) -> Result<(), String> {
    if canister_id(canister).is_some() {
        return Ok(());
    }

    Err(missing_canister_message(canister))
}

fn call_canister_unit_method(canister: &str, method: &str) -> Result<(), String> {
    require_created_canister(canister)?;

    let mut command = Command::new("dfx");
    command
        .arg("canister")
        .arg("call")
        .arg(canister)
        .arg(method)
        .arg("()");

    run_external_command(command, "dfx canister call")
}

fn default_canister_wasm_path(canister: &str) -> PathBuf {
    PathBuf::from(format!(".dfx/local/canisters/{canister}/{canister}.wasm"))
}

fn canister_is_installed(canister: &str) -> bool {
    Command::new("dfx")
        .arg("canister")
        .arg("status")
        .arg(canister)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|status| status.success())
}

fn canister_id(canister: &str) -> Option<String> {
    let output = Command::new("dfx")
        .arg("canister")
        .arg("id")
        .arg(canister)
        .stdin(Stdio::null())
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }

    let id = String::from_utf8_lossy(output.stdout.as_slice())
        .trim()
        .to_string();

    (!id.is_empty()).then_some(id)
}

fn missing_canister_message(canister: &str) -> String {
    let mut message = format!("canister '{canister}' is not created in the local dfx environment.");
    if canister == DEFAULT_CANISTER {
        message.push_str(" `icydb sql` defaults to '");
        message.push_str(DEFAULT_CANISTER);
        message.push_str("' when --canister is omitted.");
    }
    message.push_str(
        "\nRun `icydb dev init` to create/reinstall the default demo canister and load fixtures.",
    );
    message.push_str("\nRun `icydb canister list` to see known local canisters.");

    if let Ok(canisters) = known_canisters()
        && !canisters.is_empty()
    {
        message.push_str("\nKnown canisters from dfx.json: ");
        message.push_str(canisters.join(", ").as_str());
    }

    message
}

fn known_canisters() -> Result<Vec<String>, String> {
    let contents = std::fs::read_to_string(DFX_JSON_PATH)
        .map_err(|err| format!("read {DFX_JSON_PATH}: {err}"))?;
    let value = serde_json::from_str::<Value>(contents.as_str())
        .map_err(|err| format!("parse {DFX_JSON_PATH}: {err}"))?;
    let Some(canisters) = value.get("canisters").and_then(Value::as_object) else {
        return Ok(Vec::new());
    };
    let mut names = canisters.keys().cloned().collect::<Vec<_>>();
    names.sort();

    Ok(names)
}

fn run_external_command(mut command: Command, label: &str) -> Result<(), String> {
    let status = command
        .stdin(Stdio::null())
        .status()
        .map_err(|err| format!("{label}: {err}"))?;
    if status.success() {
        return Ok(());
    }

    Err(format!("{label} failed with {status}"))
}
