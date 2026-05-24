use std::{env, path::PathBuf};

use crate::{
    CONFIG_PATH_ENV, ConfigBuildError, GeneratedIcydbConfig, parse::load_icydb_toml,
    resolve::resolve_config_path,
};

const CARGO_MANIFEST_DIR_ENV: &str = "CARGO_MANIFEST_DIR";

/// Resolve and validate config for a canister build script.
///
/// Resolution order:
/// 1. `ICYDB_CONFIG_PATH`
/// 2. nearest `icydb.toml` found by walking up from the canister crate
/// 3. absent config, treated as defaults
pub fn emit_config_for_build_script() -> Result<GeneratedIcydbConfig, ConfigBuildError> {
    println!("cargo:rerun-if-env-changed={CONFIG_PATH_ENV}");
    let manifest_dir = manifest_dir()?;
    let resolved = resolve_config_path(manifest_dir.as_path());
    if let Some(path) = resolved.config_path() {
        println!("cargo:rerun-if-changed={}", path.display());
        load_icydb_toml(path, &[])
    } else {
        for candidate in resolved.candidate_paths() {
            println!("cargo:rerun-if-changed={}", candidate.display());
        }
        Ok(GeneratedIcydbConfig::default())
    }
}

fn manifest_dir() -> Result<PathBuf, ConfigBuildError> {
    env::var_os(CARGO_MANIFEST_DIR_ENV)
        .map(PathBuf::from)
        .map_or_else(
            || env::current_dir().map_err(|source| ConfigBuildError::CurrentDir { source }),
            Ok,
        )
}
