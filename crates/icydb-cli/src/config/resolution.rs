//! Module: CLI config discovery.
//! Responsibility: resolve the active `icydb.toml` path and start directory.
//! Does not own: config creation, reporting, or endpoint surface gates.
//! Boundary: returns canonical config inputs to command owners.

use std::{
    env, fs,
    path::{Path, PathBuf},
};

use super::ResolvedConfig;

pub(super) const CONFIG_FILE_NAME: &str = "icydb.toml";
const CONFIG_PATH_ENV: &str = "ICYDB_CONFIG_PATH";

pub(super) fn load_resolved_config(
    start_dir: Option<&Path>,
) -> Result<(PathBuf, ResolvedConfig), String> {
    let start_dir = resolve_start_dir(start_dir)?;
    let resolved = icydb_config_build::load_resolved_icydb_toml(start_dir.as_path(), &[])
        .map_err(|err| err.to_string())?;

    Ok((start_dir, resolved))
}

pub(super) fn resolve_start_dir(start_dir: Option<&Path>) -> Result<PathBuf, String> {
    let path = start_dir.map_or_else(|| PathBuf::from("."), Path::to_path_buf);

    path.canonicalize()
        .map_err(|err| format!("resolve config start directory '{}': {err}", path.display()))
}

pub(super) fn resolved_config_path(start_dir: &Path) -> Option<PathBuf> {
    if let Some(explicit) = env::var_os(CONFIG_PATH_ENV) {
        return Some(PathBuf::from(explicit));
    }

    for ancestor in start_dir.ancestors() {
        let candidate = ancestor.join(CONFIG_FILE_NAME);
        if candidate.exists() {
            return Some(candidate);
        }
        if is_workspace_root(ancestor) {
            break;
        }
    }

    None
}

pub(super) fn workspace_root(start_dir: &Path) -> Option<PathBuf> {
    start_dir
        .ancestors()
        .find(|ancestor| is_workspace_root(ancestor))
        .map(Path::to_path_buf)
}

fn is_workspace_root(path: &Path) -> bool {
    fs::read_to_string(path.join("Cargo.toml")).is_ok_and(|source| source.contains("[workspace]"))
}
