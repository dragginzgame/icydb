//! Module: ICP command integration.
//! Responsibility: expose focused helpers for invoking and inspecting local ICP tooling.
//! Does not own: CLI argument parsing, observability rendering, or SQL shell routing.
//! Boundary: keeps process construction and project discovery behind crate-private helpers.

mod call;
mod commands;
mod process;
mod project;

use std::path::Path;

pub(crate) fn call_query_hex(
    environment: &str,
    canister: &str,
    method: &str,
    candid_arg: &str,
    error_message: impl FnOnce(&str) -> String,
) -> Result<Vec<u8>, String> {
    call::call_query_hex(environment, canister, method, candid_arg, error_message)
}

pub(crate) fn call_update_hex(
    environment: &str,
    canister: &str,
    method: &str,
    candid_arg: &str,
    error_message: impl FnOnce(&str) -> String,
) -> Result<Vec<u8>, String> {
    call::call_update_hex(environment, canister, method, candid_arg, error_message)
}

pub(crate) fn list_canisters(environment: &str) -> Result<(), String> {
    commands::list_canisters(environment)
}

pub(crate) fn deploy_canister(environment: &str, canister: &str) -> Result<(), String> {
    commands::deploy_canister(environment, canister)
}

pub(crate) fn refresh_canister(environment: &str, canister: &str) -> Result<(), String> {
    commands::refresh_canister(environment, canister)
}

pub(crate) fn upgrade_canister(
    environment: &str,
    canister: &str,
    wasm: Option<&Path>,
) -> Result<(), String> {
    commands::upgrade_canister(environment, canister, wasm)
}

pub(crate) fn status_canister(environment: &str, canister: &str) -> Result<(), String> {
    commands::status_canister(environment, canister)
}

pub(crate) fn known_canisters(environment: &str) -> Result<Vec<String>, String> {
    project::known_canisters(environment)
}

pub(crate) fn require_created_canister(environment: &str, canister: &str) -> Result<(), String> {
    project::require_created_canister(environment, canister)
}

#[cfg(test)]
pub(crate) mod test_support {
    use std::process::Command;

    pub(crate) fn hex_response_bytes(output: &str) -> Result<Vec<u8>, String> {
        super::call::hex_response_bytes(output)
    }

    pub(crate) fn icp_query_command(
        environment: &str,
        canister: &str,
        method: &str,
        candid_arg: &str,
    ) -> Command {
        super::call::icp_query_command(environment, canister, method, candid_arg)
    }

    pub(crate) fn icp_update_command(
        environment: &str,
        canister: &str,
        method: &str,
        candid_arg: &str,
    ) -> Command {
        super::call::icp_update_command(environment, canister, method, candid_arg)
    }

    pub(crate) fn fixtures_load_command(environment: &str, canister: &str) -> Command {
        super::commands::fixtures_load_command(environment, canister)
    }

    pub(crate) fn deploy_command(environment: &str, canister: &str) -> Command {
        super::commands::deploy_command(environment, canister)
    }

    pub(crate) fn build_command(environment: &str, canister: &str) -> Command {
        super::commands::build_command(environment, canister)
    }

    pub(crate) fn install_upgrade_command(
        environment: &str,
        canister: &str,
        wasm_path: std::path::PathBuf,
    ) -> Command {
        super::commands::install_upgrade_command(environment, canister, wasm_path)
    }

    pub(crate) fn status_command(environment: &str, canister: &str) -> Command {
        super::commands::status_command(environment, canister)
    }

    pub(crate) fn canister_status_check_command(environment: &str, canister: &str) -> Command {
        super::process::canister_status_check_command(environment, canister)
    }

    pub(crate) fn canister_status_id_command(environment: &str, canister: &str) -> Command {
        super::process::canister_status_id_command(environment, canister)
    }

    pub(crate) fn parse_manifest_canisters(contents: &str, environment: &str) -> Vec<String> {
        super::project::parse_manifest_canisters(contents, environment)
    }

    pub(crate) fn unreachable_network_hint(message: &str) -> Option<&'static str> {
        super::process::unreachable_network_hint(message)
    }
}
