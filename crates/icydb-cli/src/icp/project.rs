use serde_json::Value;

use crate::icp::process::{canister_id, unreachable_network_hint};

const ICP_YAML_PATH: &str = "icp.yaml";

/// Fail with IcyDB-specific setup guidance when icp-cli has no local canister id.
pub(crate) fn require_created_canister(environment: &str, canister: &str) -> Result<(), String> {
    match canister_id(environment, canister) {
        Ok(Some(_)) => Ok(()),
        Ok(None) => Err(missing_canister_message(environment, canister)),
        Err(err) => Err(unreachable_network_hint(err.as_str())
            .map(str::to_string)
            .unwrap_or(err)),
    }
}

/// Read canister names from the selected icp-cli environment.
pub(crate) fn known_canisters(environment: &str) -> Result<Vec<String>, String> {
    known_canisters_from_icp(environment).or_else(|_| known_canisters_from_manifest(environment))
}

fn known_canisters_from_icp(environment: &str) -> Result<Vec<String>, String> {
    let output = std::process::Command::new("icp")
        .arg("canister")
        .arg("list")
        .arg("--json")
        .arg("--environment")
        .arg(environment)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .output()
        .map_err(|err| err.to_string())?;
    if !output.status.success() {
        return Err(String::from_utf8_lossy(output.stderr.as_slice())
            .trim()
            .to_string());
    }

    let value = serde_json::from_slice::<Value>(output.stdout.as_slice())
        .map_err(|err| format!("parse icp canister list --json: {err}"))?;
    let Some(canisters) = value.get("canisters").and_then(Value::as_array) else {
        return Ok(Vec::new());
    };

    let mut names = canisters
        .iter()
        .filter_map(Value::as_str)
        .map(str::to_string)
        .collect::<Vec<_>>();
    names.sort();

    Ok(names)
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

    let mut names = trimmed
        .trim_start_matches('[')
        .trim_end_matches(']')
        .split(',')
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .map(str::to_string)
        .collect::<Vec<_>>();
    names.sort();

    Ok(names)
}

fn environment_manifest_body<'a>(contents: &'a str, environment: &str) -> Option<&'a str> {
    let marker = format!("- name: {environment}");
    let start = contents.find(marker.as_str())?;
    let body = &contents[start + marker.len()..];
    let end = body.find("\n  - name:").unwrap_or(body.len());

    Some(&body[..end])
}

fn missing_canister_message(environment: &str, canister: &str) -> String {
    let mut message =
        format!("canister '{canister}' is not created in the '{environment}' ICP environment.");
    message.push_str("\nRun `icydb canister refresh --canister ");
    message.push_str(canister);
    message.push_str("` to rebuild and reinstall that canister.");
    message.push_str("\nRun `icydb canister list` to see known local canisters.");
    message.push_str(
        "\nThe CLI never starts or stops the ICP network; manage that lifecycle outside icydb.",
    );

    if let Ok(canisters) = known_canisters(environment)
        && !canisters.is_empty()
    {
        message.push_str("\nKnown canisters from icp-cli: ");
        message.push_str(canisters.join(", ").as_str());
    }

    message
}
