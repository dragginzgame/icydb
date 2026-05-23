//! Module: shell canister calls.
//! Responsibility: build shell SQL Candid arguments, run ICP calls, and decorate known recovery errors.
//! Does not own: SQL routing, response decoding, or final shell rendering.
//! Boundary: exposes shell-call wire helpers to the shell runner and recovery hint tests.

use std::process::Stdio;

use crate::icp::{hex_response_bytes, icp_query_command, icp_update_command};

pub(super) fn candid_escape_string(sql: &str) -> String {
    let mut escaped = String::with_capacity(sql.len());
    for ch in sql.chars() {
        match ch {
            '\\' => escaped.push_str("\\\\"),
            '"' => escaped.push_str("\\\""),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            _ => escaped.push(ch),
        }
    }

    escaped
}

pub(super) fn icp_query(
    environment: &str,
    canister: &str,
    method: &str,
    escaped_sql: &str,
) -> Result<Vec<u8>, String> {
    let candid_arg = format!("(\"{escaped_sql}\")");
    let output = icp_query_command(environment, canister, method, candid_arg.as_str())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .map_err(|err| err.to_string())?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let error = format!(
            "IcyDB SQL query method '{method}' failed on canister '{canister}' in environment '{environment}': {}",
            stderr.trim()
        );
        return Err(sql_error_with_recovery_hint(
            error.as_str(),
            environment,
            canister,
        ));
    }

    let stdout = String::from_utf8(output.stdout).map_err(|err| err.to_string())?;

    hex_response_bytes(stdout.as_str())
}

pub(super) fn icp_update(
    environment: &str,
    canister: &str,
    method: &str,
    escaped_sql: &str,
) -> Result<Vec<u8>, String> {
    let candid_arg = format!("(\"{escaped_sql}\")");
    let output = icp_update_command(environment, canister, method, candid_arg.as_str())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .map_err(|err| err.to_string())?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(sql_error_with_recovery_hint(
            stderr.trim(),
            environment,
            canister,
        ));
    }

    let stdout = String::from_utf8(output.stdout).map_err(|err| err.to_string())?;

    hex_response_bytes(stdout.as_str())
}

pub(super) fn sql_error_with_recovery_hint(
    error: &str,
    environment: &str,
    canister: &str,
) -> String {
    if !looks_like_stale_demo_sql_surface(error) {
        return error.to_string();
    }

    format!("{error}\n\n{}", sql_recovery_hint(environment, canister))
}

fn looks_like_stale_demo_sql_surface(error: &str) -> bool {
    error.contains("has no query method '__icydb_query'")
        || (error.contains("startup index rebuild failed")
            && error.contains("store '")
            && error.contains("' not found"))
}

fn sql_recovery_hint(environment: &str, canister: &str) -> String {
    format!(
        "This looks like stale wasm or stable-memory schema state for '{canister}' in environment '{environment}'. If this is disposable, run `icydb canister refresh {canister} --environment {environment}`; otherwise repair it or use `icydb canister upgrade {canister} --environment {environment}` intentionally."
    )
}
