use std::{env, path::PathBuf};

use crate::{
    CONFIG_PATH_ENV, ConfigBuildError, GeneratedIcydbConfig, parse::load_icydb_toml,
    resolve::resolve_config_path,
};

/// Resolve and validate config for a canister build script.
///
/// Resolution order:
/// 1. `ICYDB_CONFIG_PATH`
/// 2. nearest `icydb.toml` found by walking up from the canister crate
/// 3. absent config, treated as defaults
pub fn emit_config_for_build_script() -> Result<GeneratedIcydbConfig, ConfigBuildError> {
    println!("cargo:rerun-if-env-changed={CONFIG_PATH_ENV}");
    let manifest_dir = env::var_os("CARGO_MANIFEST_DIR").map_or_else(
        || env::current_dir().expect("current directory should resolve"),
        PathBuf::from,
    );
    let resolved = resolve_config_path(manifest_dir.as_path());
    if let Some(path) = resolved.config_path.as_ref() {
        println!("cargo:rerun-if-changed={}", path.display());
        load_icydb_toml(path.as_path(), &[])
    } else {
        for candidate in &resolved.candidate_paths {
            println!("cargo:rerun-if-changed={}", candidate.display());
        }
        Ok(GeneratedIcydbConfig::default())
    }
}
