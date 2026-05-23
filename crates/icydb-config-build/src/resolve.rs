//! Module: config path resolution.
//! Responsibility: locate the effective `icydb.toml` path for host build tools.
//! Does not own: TOML parsing, validation, or generated config semantics.
//! Boundary: returns discovery results without reading the resolved config file.

use std::{
    env, fs,
    path::{Path, PathBuf},
};

use crate::{CONFIG_FILE_NAME, CONFIG_PATH_ENV};

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
    let candidate_paths = config_search_candidates(manifest_dir);
    if let Some(explicit) = env::var_os(CONFIG_PATH_ENV) {
        return ResolvedConfigPath {
            config_path: Some(PathBuf::from(explicit)),
            candidate_paths,
        };
    }

    let config_path = candidate_paths
        .iter()
        .find(|candidate| candidate.exists())
        .cloned();

    ResolvedConfigPath {
        config_path,
        candidate_paths,
    }
}

fn config_search_candidates(manifest_dir: &Path) -> Vec<PathBuf> {
    let mut candidates = Vec::new();
    for ancestor in manifest_dir.ancestors() {
        candidates.push(ancestor.join(CONFIG_FILE_NAME));
        if is_workspace_root(ancestor) {
            break;
        }
    }

    candidates
}

fn is_workspace_root(path: &Path) -> bool {
    let manifest = path.join("Cargo.toml");
    let Ok(source) = fs::read_to_string(manifest) else {
        return false;
    };

    source
        .parse::<toml::Value>()
        .is_ok_and(|manifest| manifest.get("workspace").is_some())
}
