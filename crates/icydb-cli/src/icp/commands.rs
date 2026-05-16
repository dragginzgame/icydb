use std::{
    path::PathBuf,
    process::{Command, Stdio},
};

use crate::{
    config::{FIXTURES_LOAD_ENDPOINT, configured_endpoint_enabled},
    icp::{
        process::{
            canister_id, canister_is_installed, run_external_command, unreachable_network_hint,
        },
        project::known_canisters,
    },
};

type CanisterListRow = (String, &'static str, String);

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
    match canister_id(environment, canister.as_str()) {
        Ok(Some(id)) => (canister, "created", id),
        Err(err) if unreachable_network_hint(err.as_str()).is_some() => (
            canister,
            "unknown",
            "local ICP network is not reachable".to_string(),
        ),
        Ok(None) | Err(_) => (canister, "not created", "-".to_string()),
    }
}

// Print the local canister inventory with principal as the final column.
fn print_canister_table(environment: &str, rows: &[CanisterListRow]) {
    let canister_width = table_width(
        "canister",
        rows.iter().map(|(canister, _, _)| canister.as_str()),
    );
    let created_width = table_width("created", rows.iter().map(|(_, created, _)| *created));
    let canister_heading = "canister";
    let created_heading = "created";
    let principal_heading = "principal";

    println!("Known IcyDB canisters in environment '{environment}':");
    println!(
        "  {canister_heading:<canister_width$}  {created_heading:<created_width$}  {principal_heading}"
    );
    for (canister, created, principal) in rows {
        println!("  {canister:<canister_width$}  {created:<created_width$}  {principal}");
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

/// Deploy a canister and request reinstall mode only when refresh targets an existing install.
fn reinstall_for_refresh(environment: &str, canister: &str) -> Result<(), String> {
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

/// Refresh a local canister and load deterministic fixtures when the endpoint exists.
pub(crate) fn refresh_canister(environment: &str, canister: &str) -> Result<(), String> {
    let load_fixtures = configured_endpoint_enabled(canister, FIXTURES_LOAD_ENDPOINT)?;
    reinstall_for_refresh(environment, canister)?;
    if load_fixtures {
        return load_fixtures_after_refresh(environment, canister);
    }

    eprintln!(
        "[icydb] fixture loading is not enabled for canister '{canister}' in icydb.toml; skipping fixture load"
    );
    Ok(())
}

fn load_fixtures_after_refresh(environment: &str, canister: &str) -> Result<(), String> {
    eprintln!("[icydb] loading fixtures for canister '{canister}' in environment '{environment}'");
    let output = fixtures_load_command(environment, canister)
        .stdin(Stdio::null())
        .output()
        .map_err(|err| err.to_string())?;
    if output.status.success() {
        print!("{}", String::from_utf8_lossy(output.stdout.as_slice()));
        return Ok(());
    }

    let stderr = String::from_utf8_lossy(output.stderr.as_slice())
        .trim()
        .to_string();
    if looks_like_missing_fixtures_endpoint(stderr.as_str()) {
        eprintln!(
            "[icydb] fixture endpoint '{}' is not exported by '{canister}'; skipping fixture load",
            FIXTURES_LOAD_ENDPOINT.method(),
        );
        return Ok(());
    }

    Err(format!(
        "icp canister call {} failed: {stderr}",
        FIXTURES_LOAD_ENDPOINT.method(),
    ))
}

pub(crate) fn fixtures_load_command(environment: &str, canister: &str) -> Command {
    let mut command = Command::new("icp");
    command
        .arg("canister")
        .arg("call")
        .arg(canister)
        .arg(FIXTURES_LOAD_ENDPOINT.method())
        .arg("()")
        .arg("--environment")
        .arg(environment);

    command
}

fn looks_like_missing_fixtures_endpoint(stderr: &str) -> bool {
    stderr.contains("CanisterMethodNotFound")
        || stderr.contains("has no update method")
        || stderr.contains("has no query method")
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

fn default_canister_wasm_path(canister: &str) -> PathBuf {
    PathBuf::from(format!(".icp/local/canisters/{canister}/{canister}.wasm"))
}
