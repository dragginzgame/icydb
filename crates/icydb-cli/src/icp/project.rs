//! Module: ICP project discovery.
//! Responsibility: resolve known canisters and enforce local canister creation preconditions.
//! Does not own: canister lifecycle commands, generic process execution, or endpoint config.
//! Boundary: exposes project-local canister discovery and setup checks to CLI command surfaces.

use std::process::{Command, Output, Stdio};

use serde_json::Value;

use crate::icp::process::{canister_id, output_stderr, unreachable_network_hint};

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

    Ok(parse_manifest_canisters(contents.as_str(), environment))
}

pub(super) fn parse_manifest_canisters(contents: &str, environment: &str) -> Vec<String> {
    let mut in_environments = false;
    let mut in_target_environment = false;
    for line in contents.lines().map(str::trim) {
        if line == "environments:" {
            in_environments = true;
            in_target_environment = false;
            continue;
        }

        if !in_environments {
            continue;
        }

        if let Some(name) = environment_name(line) {
            if in_target_environment {
                return Vec::new();
            }
            in_target_environment = name == environment;
            continue;
        }

        if !in_target_environment {
            continue;
        }

        let Some(value) = line.strip_prefix("canisters:") else {
            continue;
        };

        return parse_inline_canister_list(value);
    }

    Vec::new()
}

fn sorted_canister_names(names: impl Iterator<Item = String>) -> Vec<String> {
    let mut names = names.collect::<Vec<_>>();
    names.sort();

    names
}

fn environment_name(line: &str) -> Option<&str> {
    let name = line
        .strip_prefix("- name:")?
        .trim()
        .trim_matches(['"', '\'']);

    (!name.is_empty()).then_some(name)
}

fn parse_inline_canister_list(value: &str) -> Vec<String> {
    let trimmed = value.trim();
    if !trimmed.starts_with('[') || !trimmed.ends_with(']') {
        return Vec::new();
    }

    sorted_canister_names(
        trimmed
            .trim_start_matches('[')
            .trim_end_matches(']')
            .split(',')
            .filter_map(parse_manifest_name),
    )
}

fn parse_manifest_name(value: &str) -> Option<String> {
    let name = value.trim().trim_matches(['"', '\'']);

    (!name.is_empty()).then(|| name.to_string())
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
