use ic_host_tools::icp::StatusOutput;
use std::process::Command;

use crate::icp::{call, commands, process, project};

pub(crate) fn hex_response_bytes(output: &str) -> Result<Vec<u8>, String> {
    call::hex_response_bytes(output)
}

pub(crate) fn icp_query_command(
    environment: &str,
    canister: &str,
    method: &str,
    argument_file: &std::path::Path,
) -> Command {
    call::icp_query_command(environment, canister, method, argument_file)
}

pub(crate) fn icp_update_command(
    environment: &str,
    canister: &str,
    method: &str,
    argument_file: &std::path::Path,
) -> Command {
    call::icp_update_command(environment, canister, method, argument_file)
}

pub(crate) fn fixtures_load_command(
    environment: &str,
    canister: &str,
    argument_file: &std::path::Path,
) -> Command {
    commands::fixtures_load_command(environment, canister, argument_file)
}

pub(crate) fn deploy_command(environment: &str, canister: &str) -> Command {
    commands::deploy_command(environment, canister)
}

pub(crate) fn build_command(environment: &str, canister: &str) -> Command {
    commands::build_command(environment, canister)
}

pub(crate) fn install_upgrade_command(
    environment: &str,
    canister: &str,
    wasm: Option<&std::path::Path>,
) -> Command {
    commands::install_upgrade_command(environment, canister, wasm)
}

pub(crate) fn status_command(environment: &str, canister: &str) -> Command {
    commands::status_command(environment, canister)
}

pub(crate) fn top_up_command(environment: &str, canister: &str, amount: &str) -> Command {
    commands::top_up_command(environment, canister, amount)
}

pub(crate) fn canister_status_json_command(environment: &str, canister: &str) -> Command {
    process::canister_status_command(environment, canister, StatusOutput::Json)
}

pub(crate) fn canister_status_id_command(environment: &str, canister: &str) -> Command {
    process::canister_status_command(environment, canister, StatusOutput::IdOnly)
}

pub(crate) fn parse_manifest_canisters(
    contents: &str,
    environment: &str,
) -> Result<Vec<String>, String> {
    project::parse_manifest_canisters(contents, environment)
}
