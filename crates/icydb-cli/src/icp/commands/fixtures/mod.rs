//! Module: ICP fixture refresh helpers.
//! Responsibility: load generated fixtures and reserve local cycles before refresh.
//! Does not own: generic canister lifecycle commands or project discovery.
//! Boundary: projects shared ICP status facts into local fixture funding policy.

use ic_host_text::escape::terminal_text;
use ic_host_tools::icp::{StatusOutput, decode_canister_cycles};
use icydb::__macro::RpcMethod;

use std::process::Command;

use crate::icp::{
    IcpCallKind,
    commands::append_environment_args,
    process::{
        PROBE_STDOUT_BYTES, canister_status_command, output_stderr, probe_output,
        run_external_command, update_output,
    },
    project::environment_targets_local,
};

const LOCAL_FIXTURE_CYCLES_TOP_UP_AMOUNT: &str = "100t";
const LOCAL_FIXTURE_CYCLES_TOP_UP_AMOUNT_CYCLES: u128 = 100_000_000_000_000;
const LOCAL_FIXTURE_CYCLES_TOP_UP_THRESHOLD: u128 = LOCAL_FIXTURE_CYCLES_TOP_UP_AMOUNT_CYCLES / 2;

pub(super) fn load_after_refresh(environment: &str, canister: &str) -> Result<(), String> {
    ensure_local_fixture_cycles(environment, canister)?;

    eprintln!(
        "[icydb] loading fixtures for canister '{}' in environment '{}'",
        terminal_text(canister),
        terminal_text(environment)
    );
    let stdout = run_fixtures_load(environment, canister)?;
    print!("{stdout}");

    Ok(())
}

fn ensure_local_fixture_cycles(environment: &str, canister: &str) -> Result<(), String> {
    if !environment_targets_local(environment) {
        eprintln!(
            "[icydb] local ICP network is not confirmed for environment '{}'; skipping automatic fixture cycles top-up",
            terminal_text(environment),
        );

        return Ok(());
    }

    let Some(cycles) = read_canister_cycles(environment, canister)? else {
        eprintln!(
            "[icydb] could not read current cycles for canister '{}' in environment '{}'; skipping automatic fixture cycles top-up",
            terminal_text(canister),
            terminal_text(environment),
        );

        return Ok(());
    };

    if cycles >= LOCAL_FIXTURE_CYCLES_TOP_UP_THRESHOLD {
        eprintln!(
            "[icydb] canister '{}' has {cycles} cycles; skipping fixture top-up",
            terminal_text(canister)
        );

        return Ok(());
    }

    eprintln!(
        "[icydb] local fixture refresh reserves cycles for '{}': current={cycles}, threshold={LOCAL_FIXTURE_CYCLES_TOP_UP_THRESHOLD}; topping up {LOCAL_FIXTURE_CYCLES_TOP_UP_AMOUNT}",
        terminal_text(canister),
    );
    run_external_command(
        top_up_command(environment, canister, LOCAL_FIXTURE_CYCLES_TOP_UP_AMOUNT),
        "icp canister top-up",
    )
}

fn read_canister_cycles(environment: &str, canister: &str) -> Result<Option<u128>, String> {
    let output = probe_output(&mut canister_status_command(
        environment,
        canister,
        StatusOutput::Json,
    ))
    .map_err(|err| err.to_string())?;
    if !output.status.success() {
        return Err(output_stderr(output.stderr.as_slice()));
    }

    // Missing public-only balances stay unknown; malformed supplied balances
    // must stop the refresh before funding or loading fixtures.
    decode_canister_cycles(&output.stdout, PROBE_STDOUT_BYTES).map_err(|error| {
        format!(
            "parse icp cycle balance JSON: {}",
            terminal_text(&error.to_string())
        )
    })
}

fn run_fixtures_load(environment: &str, canister: &str) -> Result<String, String> {
    let arguments = icydb::__macro::RpcMethod::FixturesLoad
        .encode_request((), icydb::wire::limits(icydb::wire::MAX_REQUEST_BYTES))
        .map_err(|error| error.to_string())?;
    let response = crate::icp::call::call_hex(
        &arguments,
        |path| fixtures_load_command(environment, canister, path),
        str::to_owned,
        IcpCallKind::Update,
        update_output,
    );
    fixture_load_result(response)
}

fn fixture_load_result(response: Result<Vec<u8>, String>) -> Result<String, String> {
    let bytes = response.map_err(|error| fixture_load_error(&error))?;
    let result: Result<(), icydb::Error> = icydb::__macro::RpcMethod::FixturesLoad
        .decode_response(&bytes, icydb::wire::limits(icydb::wire::MAX_REPLY_BYTES))
        .map_err(|error| {
            fixture_load_error(&IcpCallKind::Update.reply_error(
                crate::diagnostic::render_wire_decode_error(RpcMethod::FixturesLoad, error),
            ))
        })?;
    result
        .map(|()| String::new())
        .map_err(|error| fixture_load_error(&crate::diagnostic::render_error(&error)))
}

fn fixture_load_error(stderr: &str) -> String {
    format!(
        "icp canister call {} failed: {stderr}",
        RpcMethod::FixturesLoad.name(),
    )
}

pub(super) fn fixtures_load_command(
    environment: &str,
    canister: &str,
    argument_file: &std::path::Path,
) -> Command {
    crate::icp::call::icp_update_command(
        environment,
        canister,
        RpcMethod::FixturesLoad.name(),
        argument_file,
    )
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
mod tests;
