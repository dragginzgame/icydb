//! Module: ICP manifest discovery.
//! Responsibility: project fallback canister names from project-local `icp.yaml`.
//! Does not own: effective project resolution, YAML mechanics or process execution.
//! Boundary: read an admitted regular file, then project its declared environment inventory.

use std::{io, path::Path};

use ic_host_fs::read::open_regular_file;
use ic_host_text::{
    escape::terminal_text,
    yaml::{self, DecodePolicy},
};
use serde::Deserialize;

const ICP_YAML_PATH: &str = "icp.yaml";

#[derive(Deserialize)]
struct Manifest {
    environments: Option<Vec<Environment>>,
}

#[derive(Deserialize)]
struct Environment {
    name: String,
    canisters: Option<Vec<String>>,
}

pub(super) fn known_canisters(environment: &str) -> Result<Vec<String>, String> {
    let contents = read_manifest()?;
    parse_canisters(&contents, environment)
}

fn read_manifest() -> Result<String, String> {
    // Admit the selected descriptor before reading so a FIFO cannot block at
    // open. Host FS owns that check; regular-file symlinks remain supported.
    let file = open_regular_file(Path::new(ICP_YAML_PATH))
        .map_err(|err| format!("read {ICP_YAML_PATH}: {err}"))?;
    io::read_to_string(file).map_err(|err| format!("read {ICP_YAML_PATH}: {err}"))
}

pub(super) fn parse_canisters(contents: &str, environment: &str) -> Result<Vec<String>, String> {
    // The file read retains its existing size policy. This extent admits only
    // the supplied buffer; it is not a new file or parser-memory ceiling.
    // Ignore unrelated tagged fields, without resolving YAML merges or recipes.
    let manifest: Option<Manifest> = yaml::from_slice(
        contents.as_bytes(),
        contents.len(),
        DecodePolicy {
            ignore_unknown_tags: true,
            preserve_merge_keys: true,
        },
    )
    .map_err(|error| {
        format!(
            "parse {ICP_YAML_PATH}: {}",
            terminal_text(&error.to_string())
        )
    })?;
    let mut canisters = manifest
        .and_then(|manifest| manifest.environments)
        .and_then(|environments| {
            environments
                .into_iter()
                .find(|item| item.name == environment)
        })
        .and_then(|item| item.canisters)
        .unwrap_or_default();
    canisters.retain(|name| !name.is_empty());
    canisters.sort();
    Ok(canisters)
}
