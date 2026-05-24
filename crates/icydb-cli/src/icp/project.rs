//! Module: ICP project discovery.
//! Responsibility: resolve known canisters and enforce local canister creation preconditions.
//! Does not own: canister lifecycle commands, generic process execution, or endpoint config.
//! Boundary: exposes project-local canister discovery and setup checks to CLI command surfaces.

use std::process::{Command, Output, Stdio};

use serde_json::Value;

use crate::icp::process::{canister_id, unreachable_network_hint};

const ICP_YAML_PATH: &str = "icp.yaml";

/// Fail with IcyDB-specific setup guidance when icp-cli has no local canister id.
pub(super) fn require_created_canister(environment: &str, canister: &str) -> Result<(), String> {
    match canister_id(environment, canister) {
        Ok(Some(_)) => Ok(()),
        Ok(None) => Err(missing_canister_message(environment, canister)),
        Err(err) => Err(unreachable_network_hint(err.as_str())
            .map(str::to_string)
            .unwrap_or(err)),
    }
}

/// Read canister names from the selected icp-cli environment.
pub(super) fn known_canisters(environment: &str) -> Result<Vec<String>, String> {
    known_canisters_from_icp(environment).or_else(|_| known_canisters_from_manifest(environment))
}

fn known_canisters_from_icp(environment: &str) -> Result<Vec<String>, String> {
    let output = icp_canister_list_output(environment)?;
    if !output.status.success() {
        return Err(output_stderr(output.stderr.as_slice()));
    }

    parse_icp_canister_list(output.stdout.as_slice())
}

fn icp_canister_list_output(environment: &str) -> Result<Output, String> {
    Command::new("icp")
        .arg("canister")
        .arg("list")
        .arg("--json")
        .arg("--environment")
        .arg(environment)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .map_err(|err| err.to_string())
}

fn parse_icp_canister_list(output: &[u8]) -> Result<Vec<String>, String> {
    let value = serde_json::from_slice::<Value>(output)
        .map_err(|err| format!("parse icp canister list --json: {err}"))?;
    let Some(canisters) = value.get("canisters").and_then(Value::as_array) else {
        return Ok(Vec::new());
    };

    Ok(sorted_canister_names(
        canisters
            .iter()
            .filter_map(Value::as_str)
            .map(str::to_string),
    ))
}

fn known_canisters_from_manifest(environment: &str) -> Result<Vec<String>, String> {
    let contents = std::fs::read_to_string(ICP_YAML_PATH)
        .map_err(|err| format!("read {ICP_YAML_PATH}: {err}"))?;

    let Some(environment_body) = environment_manifest_body(contents.as_str(), environment) else {
        return Ok(Vec::new());
    };
    let Some(line) = environment_body
        .lines()
        .map(str::trim)
        .find(|line| line.starts_with("canisters:"))
    else {
        return Ok(Vec::new());
    };
    let Some((_, value)) = line.split_once(':') else {
        return Ok(Vec::new());
    };

    let trimmed = value.trim();
    if !trimmed.starts_with('[') || !trimmed.ends_with(']') {
        return Ok(Vec::new());
    }

    Ok(sorted_canister_names(
        trimmed
            .trim_start_matches('[')
            .trim_end_matches(']')
            .split(',')
            .map(str::trim)
            .filter(|name| !name.is_empty())
            .map(str::to_string),
    ))
}

fn sorted_canister_names(names: impl Iterator<Item = String>) -> Vec<String> {
    let mut names = names.collect::<Vec<_>>();
    names.sort();

    names
}

fn output_stderr(stderr: &[u8]) -> String {
    String::from_utf8_lossy(stderr).trim().to_string()
}

fn environment_manifest_body<'a>(contents: &'a str, environment: &str) -> Option<&'a str> {
    let marker = format!("- name: {environment}");
    let start = contents.find(marker.as_str())?;
    let body = &contents[start + marker.len()..];
    let end = body.find("\n  - name:").unwrap_or(body.len());

    Some(&body[..end])
}

fn missing_canister_message(environment: &str, canister: &str) -> String {
    let mut message = missing_canister_base_message(environment, canister);

    if let Ok(canisters) = known_canisters(environment)
        && !canisters.is_empty()
    {
        append_known_canisters(&mut message, canisters.as_slice());
    }

    message
}

fn missing_canister_base_message(environment: &str, canister: &str) -> String {
    format!(
        "canister '{canister}' is not created in the '{environment}' ICP environment.\nRun `icydb canister refresh {canister} --environment {environment}` to rebuild and refresh that canister.\nRun `icydb canister list --environment {environment}` to see known local canisters.\nThe CLI never starts or stops the ICP network; manage that lifecycle outside icydb."
    )
}

fn append_known_canisters(message: &mut String, canisters: &[String]) {
    message.push_str("\nKnown canisters from icp-cli: ");
    message.push_str(canisters.join(", ").as_str());
}
