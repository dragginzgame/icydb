//! Module: ICP project discovery.
//! Responsibility: resolve known canisters and enforce local canister creation preconditions.
//! Does not own: canister lifecycle commands, generic process execution, or endpoint publication.
//! Boundary: exposes project-local canister discovery and setup checks to CLI command surfaces.

mod manifest;

use std::{
    io,
    process::{Command, Output, Stdio},
};

use ic_host_text::escape::terminal_text;
use ic_host_tools::icp_project::environment_network;

use crate::icp::process::{PROBE_STDOUT_BYTES, canister_id, output_stderr, probe_output};

/// Fail with IcyDB-specific setup guidance when icp-cli has no local canister id.
pub(super) fn require_created_canister(environment: &str, canister: &str) -> Result<(), String> {
    match canister_id(environment, canister) {
        Ok(Some(_)) => Ok(()),
        Ok(None) => Err(missing_canister_message(environment, canister)),
        Err(err) => Err(err),
    }
}

/// Read canister names from the selected icp-cli environment.
pub(super) fn known_canisters(environment: &str) -> Result<Vec<String>, String> {
    let fallback_reason = match icp_canister_list_output(environment) {
        // A successful response must satisfy its schema. Malformed data and
        // capture failures cannot become a successful manifest fallback.
        Ok(output) if output.status.success() => return parse_icp_canister_list(&output.stdout),
        Ok(output) => format!(
            "icp canister list for environment '{environment}' failed with {}: {}",
            output.status,
            output_stderr(&output.stderr),
            environment = terminal_text(environment),
        ),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            format!(
                "run icp canister list for environment '{}': {error}",
                terminal_text(environment)
            )
        }
        Err(error) => {
            return Err(format!(
                "run icp canister list for environment '{}': {error}",
                terminal_text(environment),
            ));
        }
    };
    // Fallback remains supported, but neither success nor a second failure may
    // erase the observation that selected it. Escape the complete warning,
    // including caller-supplied environment text, at the presentation boundary.
    let canisters = manifest::known_canisters(environment)
        .map_err(|error| format!("{fallback_reason}\n{error}"))?;
    eprintln!(
        "[icydb] {}; using canister names from icp.yaml",
        output_stderr(fallback_reason.as_bytes()),
    );
    Ok(canisters)
}

/// Return whether the selected project environment targets the local ICP network.
pub(super) fn environment_targets_local(environment: &str) -> bool {
    // ICP resolves parent projects, implicit environments and recipes. Unknown
    // facts must never enable funding from a raw manifest or a name shortcut.
    let Ok(output) = probe_output(
        Command::new("icp")
            .args(["project", "show"])
            .stdin(Stdio::null()),
    ) else {
        return false;
    };
    output.status.success()
        && environment_network(&output.stdout, environment, PROBE_STDOUT_BYTES)
            .is_ok_and(|network| network.as_deref() == Some("local"))
}

fn icp_canister_list_output(environment: &str) -> io::Result<Output> {
    probe_output(
        Command::new("icp")
            .arg("canister")
            .arg("list")
            .arg("--json")
            .arg("--environment")
            .arg(environment),
    )
}

fn parse_icp_canister_list(output: &[u8]) -> Result<Vec<String>, String> {
    #[derive(serde::Deserialize)]
    struct Inventory {
        canisters: Vec<String>,
    }
    let inventory = serde_json::from_slice::<Inventory>(output)
        .map_err(|err| format!("parse icp canister list --json: {err}"))?;
    Ok(sorted_canister_names(inventory.canisters.into_iter()))
}

#[cfg(test)]
pub(super) fn parse_manifest_canisters(
    contents: &str,
    environment: &str,
) -> Result<Vec<String>, String> {
    manifest::parse_canisters(contents, environment)
}

fn sorted_canister_names(names: impl Iterator<Item = String>) -> Vec<String> {
    let mut names = names.collect::<Vec<_>>();
    names.sort();

    names
}

fn missing_canister_message(environment: &str, canister: &str) -> String {
    let mut message = missing_canister_base_message(environment, canister);

    match known_canisters(environment) {
        Ok(canisters) if !canisters.is_empty() => {
            append_known_canisters(&mut message, canisters.as_slice());
        }
        Err(error) => {
            message.push_str("\nCould not read known canisters: ");
            message.push_str(&error);
        }
        Ok(_) => {}
    }

    message
}

fn missing_canister_base_message(environment: &str, canister: &str) -> String {
    format!(
        "canister '{canister}' is not created in the '{environment}' ICP environment.\nVerify that the environment was selected intentionally, then run `icydb canister list --environment {environment}` to see its known canisters.\nDo not refresh unless this explicitly selected environment is disposable and you intend to rebuild its canister state.\nThe CLI never starts or stops the ICP network; manage that lifecycle outside icydb.",
        canister = terminal_text(canister),
        environment = terminal_text(environment),
    )
}

fn append_known_canisters(message: &mut String, canisters: &[String]) {
    message.push_str("\nKnown canisters from icp-cli: ");
    message.push_str(&terminal_text(&canisters.join(", ")));
}

#[cfg(test)]
mod tests {
    use super::parse_icp_canister_list;

    #[test]
    fn inventory_requires_the_complete_canister_array() {
        for source in ["{}", r#"{"canisters":42}"#, r#"{"canisters":["a",42]}"#] {
            assert!(parse_icp_canister_list(source.as_bytes()).is_err());
        }
        assert!(parse_icp_canister_list(b"{\"canisters\":[\"\xff\"]}").is_err());
    }

    #[test]
    fn inventory_retains_empty_valid_lists_sorting_and_unknown_fields() {
        assert_eq!(
            parse_icp_canister_list(br#"{"canisters":[]}"#).unwrap(),
            Vec::<String>::new()
        );
        assert_eq!(
            parse_icp_canister_list(br#"{"canisters":["b","a"],"extra":true}"#).unwrap(),
            ["a", "b"]
        );
    }
}
