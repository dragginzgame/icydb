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
    let candid_arg = sql_candid_arg(escaped_sql);
    call_query_hex(
        environment,
        canister,
        method,
        candid_arg.as_str(),
        sql_call_error_mapper("query", environment, canister, method),
    )
}

pub(super) fn icp_update(
    environment: &str,
    canister: &str,
    method: &str,
    escaped_sql: &str,
) -> Result<Vec<u8>, String> {
    let candid_arg = sql_candid_arg(escaped_sql);
    call_update_hex(
        environment,
        canister,
        method,
        candid_arg.as_str(),
        sql_call_error_mapper("DDL", environment, canister, method),
    )
}

fn sql_candid_arg(escaped_sql: &str) -> String {
    format!("(\"{escaped_sql}\")")
}

fn sql_call_error(
    call_kind: &str,
    environment: &str,
    canister: &str,
    method: &str,
    stderr: &str,
) -> String {
    format!(
        "IcyDB SQL {call_kind} method '{method}' failed on canister '{canister}' in environment '{environment}': {stderr}",
    )
}

fn sql_call_error_mapper<'a>(
    call_kind: &'a str,
    environment: &'a str,
    canister: &'a str,
    method: &'a str,
) -> impl FnOnce(&str) -> String + 'a {
    move |stderr| {
        let error = sql_call_error(call_kind, environment, canister, method, stderr);

        sql_error_with_recovery_hint(error.as_str(), environment, canister)
    }
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
    stale_sql_method_missing(error) || stale_startup_index_rebuild(error)
}

fn stale_sql_method_missing(error: &str) -> bool {
    error.contains("has no query method '__icydb_query'")
}

fn stale_startup_index_rebuild(error: &str) -> bool {
    error.contains("startup index rebuild failed")
        && error.contains("store '")
        && error.contains("' not found")
}

fn sql_recovery_hint(environment: &str, canister: &str) -> String {
    format!(
        "This looks like stale wasm or stable-memory schema state for '{canister}' in environment '{environment}'. If this is disposable, run `icydb canister refresh {canister} --environment {environment}`; otherwise repair it or use `icydb canister upgrade {canister} --environment {environment}` intentionally."
    )
}
