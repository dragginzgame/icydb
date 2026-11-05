//! Module: ICP command integration.
//! Responsibility: expose focused helpers for invoking and inspecting local ICP tooling.
//! Does not own: CLI argument parsing, observability rendering, or SQL shell routing.
//! Boundary: keeps process construction and project discovery behind crate-private helpers.

mod call;
mod commands;
mod process;
mod project;

use std::path::Path;

pub(crate) use call::IcpCallKind;

pub(crate) fn call_query_hex(
    environment: &str,
    canister: &str,
    method: &str,
    wire_arg: &[u8],
    error_message: impl FnOnce(&str) -> String,
) -> Result<Vec<u8>, String> {
    call::call_query_hex(environment, canister, method, wire_arg, error_message)
}

pub(crate) fn call_update_hex(
    environment: &str,
    canister: &str,
    method: &str,
    wire_arg: &[u8],
    error_message: impl FnOnce(&str) -> String,
) -> Result<Vec<u8>, String> {
    call::call_update_hex(environment, canister, method, wire_arg, error_message)
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

pub(crate) fn require_created_canister(environment: &str, canister: &str) -> Result<(), String> {
    project::require_created_canister(environment, canister)
}

#[cfg(test)]
pub(crate) mod support_tests;
