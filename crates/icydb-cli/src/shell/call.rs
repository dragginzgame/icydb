//! Module: shell canister calls.
//! Responsibility: build shell SQL Candid arguments, run ICP calls, and preserve failure context.
//! Does not own: SQL routing, response decoding, or final shell rendering.
//! Boundary: exposes shell-call wire helpers to the shell runner.

use crate::icp::{call_query_hex, call_update_hex, encode_candid_args};

pub(super) fn icp_query(
    environment: &str,
    canister: &str,
    method: &str,
    sql: &str,
) -> Result<Vec<u8>, String> {
    let candid_arg = encode_candid_args((sql,))?;
    call_query_hex(
        environment,
        canister,
        method,
        &candid_arg,
        sql_call_error_mapper("query", environment, canister, method),
    )
}

pub(super) fn icp_update(
    environment: &str,
    canister: &str,
    method: &str,
    sql: &str,
) -> Result<Vec<u8>, String> {
    let candid_arg = encode_candid_args((sql,))?;
    call_update_hex(
        environment,
        canister,
        method,
        &candid_arg,
        sql_call_error_mapper("DDL", environment, canister, method),
    )
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
    move |stderr| sql_call_error(call_kind, environment, canister, method, stderr)
}
