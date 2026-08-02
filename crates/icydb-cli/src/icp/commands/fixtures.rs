//! Module: ICP fixture refresh helpers.
//! Responsibility: load generated fixtures and reserve local cycles before refresh.
//! Does not own: generic canister lifecycle commands or project discovery.
//! Boundary: exposes fixture command construction and cycle parsing to the ICP command owner.

use std::process::{Command, Stdio};

use crate::{
    endpoint::FIXTURES_LOAD_ENDPOINT,
    icp::{
        commands::append_environment_args,
        process::{
            CanisterStatusOutput, canister_status_command, output_stderr, run_external_command,
        },
        project::environment_targets_local,
    },
};

const LOCAL_FIXTURE_CYCLES_TOP_UP_AMOUNT: &str = "100t";
const LOCAL_FIXTURE_CYCLES_TOP_UP_AMOUNT_CYCLES: u128 = 100_000_000_000_000;
const LOCAL_FIXTURE_CYCLES_TOP_UP_THRESHOLD: u128 = LOCAL_FIXTURE_CYCLES_TOP_UP_AMOUNT_CYCLES / 2;

pub(super) fn load_after_refresh(environment: &str, canister: &str) -> Result<(), String> {
    ensure_local_fixture_cycles(environment, canister)?;

    eprintln!("[icydb] loading fixtures for canister '{canister}' in environment '{environment}'");
    let stdout = run_fixtures_load(environment, canister)?;
    print!("{stdout}");

    Ok(())
}

fn ensure_local_fixture_cycles(environment: &str, canister: &str) -> Result<(), String> {
    if !environment_targets_local(environment) {
        eprintln!(
            "[icydb] environment '{environment}' does not target the local ICP network; skipping automatic fixture cycles top-up"
        );

        return Ok(());
    }

    let Some(cycles) = read_canister_cycles(environment, canister)? else {
        eprintln!(
            "[icydb] could not read current cycles for canister '{canister}' in environment '{environment}'; skipping automatic fixture cycles top-up"
        );

        return Ok(());
    };

    if cycles >= LOCAL_FIXTURE_CYCLES_TOP_UP_THRESHOLD {
        eprintln!("[icydb] canister '{canister}' has {cycles} cycles; skipping fixture top-up");

        return Ok(());
    }

    eprintln!(
        "[icydb] local fixture refresh reserves cycles for '{canister}': current={cycles}, threshold={LOCAL_FIXTURE_CYCLES_TOP_UP_THRESHOLD}; topping up {LOCAL_FIXTURE_CYCLES_TOP_UP_AMOUNT}"
    );
    run_external_command(
        top_up_command(environment, canister, LOCAL_FIXTURE_CYCLES_TOP_UP_AMOUNT),
        "icp canister top-up",
    )
}

fn read_canister_cycles(environment: &str, canister: &str) -> Result<Option<u128>, String> {
    let output = canister_status_command(environment, canister, CanisterStatusOutput::Capture)
        .stdin(Stdio::null())
        .output()
        .map_err(|err| err.to_string())?;
    if !output.status.success() {
        return Err(output_stderr(output.stderr.as_slice()));
    }

    Ok(parse_canister_cycles(
        String::from_utf8_lossy(output.stdout.as_slice()).as_ref(),
    ))
}

fn run_fixtures_load(environment: &str, canister: &str) -> Result<String, String> {
    let output = fixtures_load_command(environment, canister)
        .stdin(Stdio::null())
        .output()
        .map_err(|err| err.to_string())?;
    fixture_load_result(
        output.status.success(),
        output.stdout.as_slice(),
        output.stderr.as_slice(),
    )
}

fn fixture_load_result(success: bool, stdout: &[u8], stderr: &[u8]) -> Result<String, String> {
    if success {
        return Ok(String::from_utf8_lossy(stdout).to_string());
    }

    Err(fixture_load_error(output_stderr(stderr)))
}

fn fixture_load_error(stderr: String) -> String {
    format!(
        "icp canister call {} failed: {stderr}",
        FIXTURES_LOAD_ENDPOINT.method(),
    )
}

pub(super) fn fixtures_load_command(environment: &str, canister: &str) -> Command {
    let mut command = Command::new("icp");
    command
        .arg("canister")
        .arg("call")
        .arg(canister)
        .arg(FIXTURES_LOAD_ENDPOINT.method())
        .arg("()");
    append_environment_args(&mut command, environment);

    command
}

pub(super) fn parse_canister_cycles(status: &str) -> Option<u128> {
    status.lines().find_map(|line| {
        let cycles = line.trim().strip_prefix("Cycles:")?.trim();
        cycles.replace('_', "").parse::<u128>().ok()
    })
}

pub(super) fn top_up_command(environment: &str, canister: &str, amount: &str) -> Command {
    let mut command = Command::new("icp");
    command
        .arg("canister")
        .arg("top-up")
        .arg("--amount")
        .arg(amount)
        .arg(canister);
    append_environment_args(&mut command, environment);

    command
}

#[cfg(test)]
mod tests {
    use super::fixture_load_result;

    #[test]
    fn deployed_fixture_method_absence_remains_an_authoritative_failure() {
        let error = fixture_load_result(false, b"", b"CanisterMethodNotFound")
            .expect_err("a missing deployed fixture endpoint must fail refresh");

        assert_eq!(
            error,
            "icp canister call icydb_fixtures_load failed: CanisterMethodNotFound",
        );
    }
}
