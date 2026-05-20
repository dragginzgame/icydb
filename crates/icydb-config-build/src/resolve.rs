use std::{
    env, fs,
    path::{Path, PathBuf},
};

use crate::{CONFIG_FILE_NAME, CONFIG_PATH_ENV};

pub(crate) struct ResolvedConfigPath {
    pub(crate) config_path: Option<PathBuf>,
    pub(crate) candidate_paths: Vec<PathBuf>,
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

    source.contains("[workspace]")
}
