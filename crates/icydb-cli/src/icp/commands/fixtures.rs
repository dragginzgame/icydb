//! Module: ICP fixture refresh helpers.
//! Responsibility: load generated fixtures and reserve local cycles before refresh.
//! Does not own: generic canister lifecycle commands or project discovery.
//! Boundary: exposes fixture command construction and cycle parsing to the ICP command owner.

use std::process::{Command, Stdio};

use crate::{
    config::FIXTURES_LOAD_ENDPOINT,
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
    match run_fixtures_load(environment, canister)? {
        FixtureLoadOutcome::Loaded(stdout) => {
            print!("{stdout}");
            Ok(())
        }
        FixtureLoadOutcome::SkippedMissingEndpoint => Ok(()),
        FixtureLoadOutcome::InsufficientCycles { stderr }
        | FixtureLoadOutcome::Failed { stderr } => Err(fixture_load_error(stderr)),
    }
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

enum FixtureLoadOutcome {
    Loaded(String),
    SkippedMissingEndpoint,
    InsufficientCycles { stderr: String },
    Failed { stderr: String },
}

fn run_fixtures_load(environment: &str, canister: &str) -> Result<FixtureLoadOutcome, String> {
    let output = fixtures_load_command(environment, canister)
        .stdin(Stdio::null())
        .output()
        .map_err(|err| err.to_string())?;
    if output.status.success() {
        return Ok(FixtureLoadOutcome::Loaded(
            String::from_utf8_lossy(output.stdout.as_slice()).to_string(),
        ));
    }

    let stderr = output_stderr(output.stderr.as_slice());
    if looks_like_missing_fixtures_endpoint(stderr.as_str()) {
        eprintln!(
            "[icydb] fixture endpoint '{}' is not exported by '{canister}'; skipping fixture load",
            FIXTURES_LOAD_ENDPOINT.method(),
        );
        return Ok(FixtureLoadOutcome::SkippedMissingEndpoint);
    }

    if looks_like_insufficient_cycles(stderr.as_str()) {
        return Ok(FixtureLoadOutcome::InsufficientCycles { stderr });
    }

    Ok(FixtureLoadOutcome::Failed { stderr })
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

fn looks_like_missing_fixtures_endpoint(stderr: &str) -> bool {
    stderr.contains("CanisterMethodNotFound")
        || stderr.contains("has no update method")
        || stderr.contains("has no query method")
}

fn looks_like_insufficient_cycles(stderr: &str) -> bool {
    let lowered = stderr.to_ascii_lowercase();

    lowered.contains("insufficient cycles")
        || lowered.contains("cannot grow memory")
        || stderr.contains("IC0532")
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
