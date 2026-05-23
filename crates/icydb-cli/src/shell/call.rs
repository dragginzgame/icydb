//! Module: shell canister calls.
//! Responsibility: build shell SQL Candid arguments, run ICP calls, and decorate known recovery errors.
//! Does not own: SQL routing, response decoding, or final shell rendering.
//! Boundary: exposes shell-call wire helpers to the shell runner and recovery hint tests.

use crate::icp::{call_query_hex, call_update_hex};

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
    call_query_hex(
        environment,
        canister,
        method,
        candid_arg.as_str(),
        |stderr| {
            let error = format!(
                "IcyDB SQL query method '{method}' failed on canister '{canister}' in environment '{environment}': {}",
                stderr
            );
            sql_error_with_recovery_hint(error.as_str(), environment, canister)
        },
    )
}

pub(super) fn icp_update(
    environment: &str,
    canister: &str,
    method: &str,
    escaped_sql: &str,
) -> Result<Vec<u8>, String> {
    let candid_arg = format!("(\"{escaped_sql}\")");
    call_update_hex(
        environment,
        canister,
        method,
        candid_arg.as_str(),
        |stderr| sql_error_with_recovery_hint(stderr, environment, canister),
    )
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
