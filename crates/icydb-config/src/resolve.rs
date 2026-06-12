//! Module: config path resolution.
//! Responsibility: locate the effective `icydb.toml` path for host build tools.
//! Does not own: TOML parsing, validation, or generated config semantics.
//! Boundary: returns discovery results without reading the resolved config file.

use std::{
    env,
    path::{Path, PathBuf},
};

use crate::{CONFIG_PATH_ENV, ICYDB_CONFIG_FILE_NAME};

pub(crate) struct ResolvedConfigPath {
    config_path: Option<PathBuf>,
    candidate_paths: Vec<PathBuf>,
}

impl ResolvedConfigPath {
    pub(crate) fn config_path(&self) -> Option<&Path> {
        self.config_path.as_deref()
    }

    pub(crate) fn candidate_paths(&self) -> &[PathBuf] {
        &self.candidate_paths
    }

    pub(crate) fn into_config_path(self) -> Option<PathBuf> {
        self.config_path
    }
}

pub(crate) fn resolve_config_path(manifest_dir: &Path) -> ResolvedConfigPath {
    if let Some(explicit) = env::var_os(CONFIG_PATH_ENV) {
        return ResolvedConfigPath {
            config_path: Some(PathBuf::from(explicit)),
            candidate_paths: Vec::new(),
        };
    }

    resolve_ancestor_config_path(manifest_dir)
}

pub(crate) fn resolve_ancestor_config_path(manifest_dir: &Path) -> ResolvedConfigPath {
    let candidate_paths = config_search_candidates(manifest_dir);
    let config_path = candidate_paths
        .iter()
        .find(|candidate| candidate.is_file())
        .cloned();

    ResolvedConfigPath {
        config_path,
        candidate_paths,
    }
}

/// Locate an existing `icydb.toml` from one start directory.
///
/// This follows IcyDB directory discovery only: ancestor `icydb.toml` files up
/// to the filesystem root. It intentionally ignores `ICYDB_CONFIG_PATH`.
#[must_use]
pub fn resolve_existing_icydb_toml(start_dir: impl AsRef<Path>) -> Option<PathBuf> {
    resolve_ancestor_config_path(start_dir.as_ref()).into_config_path()
}

fn config_search_candidates(manifest_dir: &Path) -> Vec<PathBuf> {
    let mut candidates = Vec::new();
    for ancestor in manifest_dir.ancestors() {
        candidates.push(ancestor.join(ICYDB_CONFIG_FILE_NAME));
    }

    candidates
}
