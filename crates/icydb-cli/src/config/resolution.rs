//! Module: CLI config discovery.
//! Responsibility: resolve the active `icydb.toml` path and start directory.
//! Does not own: config creation, reporting, or endpoint surface gates.
//! Boundary: returns canonical config inputs to command owners.

use std::path::{Path, PathBuf};

use super::ResolvedConfig;

pub(super) fn load_resolved_config(
    start_dir: Option<&Path>,
    known_canisters: &[String],
) -> Result<(PathBuf, ResolvedConfig), String> {
    let start_dir = resolve_start_dir(start_dir)?;
    let known_canisters = known_canisters
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let resolved =
        icydb_config::load_resolved_icydb_toml(start_dir.as_path(), known_canisters.as_slice())
            .map_err(|err| err.to_string())?;

    Ok((start_dir, resolved))
}

pub(super) fn resolve_start_dir(start_dir: Option<&Path>) -> Result<PathBuf, String> {
    let path = start_dir.map_or_else(|| PathBuf::from("."), Path::to_path_buf);

    path.canonicalize()
        .map_err(|err| format!("resolve config start directory '{}': {err}", path.display()))
}
