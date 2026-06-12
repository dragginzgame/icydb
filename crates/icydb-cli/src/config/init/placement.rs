//! Module: CLI config-init placement.
//! Responsibility: choose whether `config init` uses an existing config or creates a new one.
//! Does not own: config parsing, default TOML rendering, or user-facing writes.
//! Boundary: returns an explicit placement outcome to command orchestration.

use std::{
    io,
    path::{Path, PathBuf},
    process::{Command, Output},
};

use serde::Deserialize;

/// Resolved `icydb config init` output location.
#[derive(Debug, Eq, PartialEq)]
pub(super) enum ConfigInitPlacement {
    /// A config is already discoverable from the start directory.
    ExistingConfig(PathBuf),
    /// No config exists; create a new default config at this path.
    CreateAt(PathBuf),
}

impl ConfigInitPlacement {
    pub(super) fn path(&self) -> &Path {
        match self {
            Self::ExistingConfig(path) | Self::CreateAt(path) => path.as_path(),
        }
    }
}

pub(super) fn resolve_config_init_placement(
    start_dir: &Path,
    existing_config_path: impl FnOnce(&Path) -> Option<PathBuf>,
) -> Result<ConfigInitPlacement, String> {
    if let Some(path) = existing_config_path(start_dir) {
        return Ok(ConfigInitPlacement::ExistingConfig(path));
    }

    Ok(ConfigInitPlacement::CreateAt(new_config_path(start_dir)?))
}

fn new_config_path(start_dir: &Path) -> Result<PathBuf, String> {
    Ok(cargo_metadata_workspace_root(start_dir)?
        .unwrap_or_else(|| start_dir.to_path_buf())
        .join(icydb_config::ICYDB_CONFIG_FILE_NAME))
}

fn cargo_metadata_workspace_root(start_dir: &Path) -> Result<Option<PathBuf>, String> {
    if !has_ancestor_cargo_manifest(start_dir) {
        return Ok(None);
    }

    let output = Command::new("cargo")
        .arg("metadata")
        .arg("--no-deps")
        .arg("--format-version")
        .arg("1")
        .current_dir(start_dir)
        .output()
        .map_err(|err| cargo_metadata_command_error(start_dir, err))?;
    if !output.status.success() {
        return Err(cargo_metadata_status_error(start_dir, &output));
    }

    serde_json::from_slice::<CargoMetadata>(output.stdout.as_slice())
        .map(|metadata| Some(metadata.workspace_root))
        .map_err(|err| {
            format!(
                "parse cargo metadata for config placement from '{}': {err}",
                start_dir.display()
            )
        })
}

fn has_ancestor_cargo_manifest(start_dir: &Path) -> bool {
    start_dir
        .ancestors()
        .any(|ancestor| ancestor.join("Cargo.toml").is_file())
}

fn cargo_metadata_command_error(start_dir: &Path, err: io::Error) -> String {
    format!(
        "run cargo metadata for config placement from '{}': {err}",
        start_dir.display()
    )
}

fn cargo_metadata_status_error(start_dir: &Path, output: &Output) -> String {
    let stderr = String::from_utf8_lossy(output.stderr.as_slice());
    format!(
        "cargo metadata failed for config placement from '{}': {}",
        start_dir.display(),
        stderr.trim()
    )
}

#[derive(Deserialize)]
struct CargoMetadata {
    workspace_root: PathBuf,
}
