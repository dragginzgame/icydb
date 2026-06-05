//! Module: ICP manifest discovery.
//! Responsibility: read and parse project-local `icp.yaml` environment metadata.
//! Does not own: icp-cli JSON discovery, process execution, or setup diagnostics.
//! Boundary: exposes manifest-derived canister names and network targeting to project discovery.

const ICP_YAML_PATH: &str = "icp.yaml";

pub(super) fn known_canisters(environment: &str) -> Result<Vec<String>, String> {
    let contents = std::fs::read_to_string(ICP_YAML_PATH)
        .map_err(|err| format!("read {ICP_YAML_PATH}: {err}"))?;

    Ok(parse_canisters(contents.as_str(), environment))
}

pub(super) fn environment_targets_local(environment: &str) -> Result<bool, String> {
    let contents = std::fs::read_to_string(ICP_YAML_PATH)
        .map_err(|err| format!("read {ICP_YAML_PATH}: {err}"))?;

    Ok(parse_environment_network(contents.as_str(), environment)
        .is_some_and(|network| network == "local"))
}

pub(super) fn parse_canisters(contents: &str, environment: &str) -> Vec<String> {
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

pub(super) fn parse_environment_network<'a>(
    contents: &'a str,
    environment: &str,
) -> Option<&'a str> {
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
                return None;
            }
            in_target_environment = name == environment;
            continue;
        }

        if !in_target_environment {
            continue;
        }

        let Some(network) = line.strip_prefix("network:") else {
            continue;
        };
        let network = network.trim().trim_matches(['"', '\'']);

        return (!network.is_empty()).then_some(network);
    }

    None
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

fn sorted_canister_names(names: impl Iterator<Item = String>) -> Vec<String> {
    let mut names = names.collect::<Vec<_>>();
    names.sort();

    names
}
