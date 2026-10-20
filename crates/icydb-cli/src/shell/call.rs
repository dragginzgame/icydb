//! Module: shell canister calls.
//! Responsibility: build shell SQL Wire arguments, run ICP calls, and preserve failure context.
//! Does not own: SQL routing, response decoding, or final shell rendering.
//! Boundary: exposes shell-call wire helpers to the shell runner.

use icydb::wire::Endpoint;

use crate::icp::{call_query_hex, call_update_hex};

pub(super) fn icp_query(
    environment: &str,
    canister: &str,
    endpoint: Endpoint,
    sql: &str,
) -> Result<Vec<u8>, String> {
    let method = endpoint.name();
    let wire_arg = endpoint
        .encode_request(sql)
        .map_err(|error| error.to_string())?;
    call_query_hex(environment, canister, method, &wire_arg, |stderr| {
        sql_call_error("query", environment, canister, method, stderr)
    })
}

pub(super) fn icp_update(
    environment: &str,
    canister: &str,
    endpoint: Endpoint,
    sql: &str,
) -> Result<Vec<u8>, String> {
    let method = endpoint.name();
    let wire_arg = endpoint
        .encode_request(sql)
        .map_err(|error| error.to_string())?;
    call_update_hex(environment, canister, method, &wire_arg, |stderr| {
        sql_call_error("DDL", environment, canister, method, stderr)
    })
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
